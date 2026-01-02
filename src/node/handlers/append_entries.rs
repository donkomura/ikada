use crate::raft::{self, RaftState};
use crate::rpc::*;
use crate::statemachine::StateMachine;
use std::sync::Arc;
use tokio::sync::Mutex;

enum AppendEntriesError {
    Rejected(AppendEntriesResponse),
    Internal(anyhow::Error),
}

impl From<anyhow::Error> for AppendEntriesError {
    fn from(err: anyhow::Error) -> Self {
        AppendEntriesError::Internal(err)
    }
}

async fn validate_term_and_step_down<T, SM>(
    request_term: u32,
    sender_id: u32,
    state: &mut RaftState<T, SM>,
) -> Result<(), AppendEntriesError>
where
    T: Send + Sync + Clone,
    SM: StateMachine<Command = T>,
{
    if request_term < state.persistent.current_term {
        tracing::warn!(
            id=?state.id,
            req_term=request_term,
            current_term=state.persistent.current_term,
            "Request rejected: term is older than current term"
        );
        return Err(AppendEntriesError::Rejected(AppendEntriesResponse {
            term: state.persistent.current_term,
            success: false,
        }));
    }

    if request_term > state.persistent.current_term {
        state.become_follower(request_term, Some(sender_id));
        if let Err(e) = state.persist().await {
            tracing::error!(id=?state.id, error=?e, "Failed to persist state after term update");
            return Err(AppendEntriesError::Rejected(AppendEntriesResponse {
                term: state.persistent.current_term,
                success: false,
            }));
        }
    } else if request_term == state.persistent.current_term {
        state.leader_id = Some(sender_id);
        if state.role.is_candidate() || state.role.is_leader() {
            state.become_follower(request_term, Some(sender_id));
        }
    }

    Ok(())
}

fn verify_log_match<T, SM>(
    prev_log_index: u32,
    prev_log_term: u32,
    state: &RaftState<T, SM>,
) -> Result<(), AppendEntriesError>
where
    T: Send + Sync + Clone,
    SM: StateMachine<Command = T>,
{
    if prev_log_index > 0 {
        if prev_log_index > state.get_last_log_idx() {
            tracing::warn!(
                id=?state.id,
                prev_log_index=prev_log_index,
                last_log_idx=state.get_last_log_idx(),
                "Request rejected: prev_log_index exceeds log length"
            );
            return Err(AppendEntriesError::Rejected(AppendEntriesResponse {
                term: state.persistent.current_term,
                success: false,
            }));
        }

        let prev_log_entry =
            &state.persistent.log[(prev_log_index - 1) as usize];
        if prev_log_entry.term != prev_log_term {
            tracing::warn!(
                id=?state.id,
                prev_log_index=prev_log_index,
                prev_log_term=prev_log_term,
                actual_term=prev_log_entry.term,
                "Request rejected: prev_log_term mismatch"
            );
            return Err(AppendEntriesError::Rejected(AppendEntriesResponse {
                term: state.persistent.current_term,
                success: false,
            }));
        }
    }

    Ok(())
}

fn detect_and_truncate_conflicts<T, SM>(
    entries: &[LogEntry],
    start_index: u32,
    state: &mut RaftState<T, SM>,
) -> Option<u32>
where
    T: Send + Sync + Clone,
    SM: StateMachine<Command = T>,
{
    for (i, entry) in entries.iter().enumerate() {
        let log_index = start_index + i as u32;

        if log_index <= state.get_last_log_idx() {
            let existing_term =
                state.persistent.log[(log_index - 1) as usize].term;

            if existing_term != entry.term {
                state.persistent.log.truncate((log_index - 1) as usize);
                tracing::info!(
                    id=?state.id,
                    conflict_index=log_index,
                    old_term=existing_term,
                    new_term=entry.term,
                    "Truncated log due to conflict"
                );
                return Some(log_index);
            }
        }
    }

    None
}

fn deserialize_and_append<T, SM>(
    entries: &[LogEntry],
    start_index: u32,
    sender_id: u32,
    state: &mut RaftState<T, SM>,
) -> Result<bool, AppendEntriesError>
where
    T: Send + Sync + Clone + serde::de::DeserializeOwned,
    SM: StateMachine<Command = T>,
{
    let mut appended_count = 0;
    let mut log_modified = false;

    for (i, entry) in entries.iter().enumerate() {
        let log_index = start_index + i as u32;

        if log_index > state.get_last_log_idx() {
            let command = match bincode::deserialize(&entry.command) {
                Ok(cmd) => cmd,
                Err(e) => {
                    tracing::error!(id=?state.id, error=?e, "Failed to deserialize command");
                    return Err(AppendEntriesError::Rejected(
                        AppendEntriesResponse {
                            term: state.persistent.current_term,
                            success: false,
                        },
                    ));
                }
            };
            let log_entry = raft::Entry {
                term: entry.term,
                command,
            };
            state.persistent.log.push(log_entry);
            log_modified = true;
            appended_count += 1;
        }
    }

    if appended_count > 0 {
        tracing::info!(
            id=?state.id,
            sender_id=sender_id,
            start_index=start_index,
            entries_received=entries.len(),
            entries_appended=appended_count,
            new_log_len=state.persistent.log.len(),
            "Appended entries to log"
        );
    }

    Ok(log_modified)
}

async fn synchronize_log<T, SM>(
    entries: &[LogEntry],
    start_index: u32,
    sender_id: u32,
    state: &mut RaftState<T, SM>,
    client_manager: Option<
        Arc<Mutex<crate::client_manager::ClientResponseManager<SM::Response>>>,
    >,
) -> Result<(), AppendEntriesError>
where
    T: Send + Sync + Clone + serde::de::DeserializeOwned,
    SM: StateMachine<Command = T>,
    SM::Response: 'static,
{
    let conflict_at =
        detect_and_truncate_conflicts(entries, start_index, state);

    if let Some(log_index) = conflict_at
        && let Some(manager) = &client_manager
    {
        manager.lock().await.clear_from(log_index);
    }

    let append_modified =
        deserialize_and_append(entries, start_index, sender_id, state)?;
    let log_modified = conflict_at.is_some() || append_modified;

    if log_modified && let Err(e) = state.persist().await {
        tracing::error!(id=?state.id, error=?e, "Failed to persist state after log modification");
        return Err(AppendEntriesError::Rejected(AppendEntriesResponse {
            term: state.persistent.current_term,
            success: false,
        }));
    }

    Ok(())
}

async fn advance_commit_index<T, SM>(
    new_commit_index: u32,
    state: &mut RaftState<T, SM>,
) -> Result<(), AppendEntriesError>
where
    T: Send + Sync + Clone,
    SM: StateMachine<Command = T>,
{
    if new_commit_index > state.commit_index {
        let old_commit = state.commit_index;
        state.commit_index = new_commit_index.min(state.get_last_log_idx());
        tracing::debug!(
            id=?state.id,
            old_commit_index=old_commit,
            new_commit_index=state.commit_index,
            requested_commit=new_commit_index,
            "Advanced commit index"
        );
    }

    if state.commit_index > state.last_applied {
        tracing::debug!(
            id=?state.id,
            commit_index=state.commit_index,
            last_applied=state.last_applied,
            "Applying committed entries to state machine"
        );
        state.apply_committed().await?;
    }

    Ok(())
}

pub async fn handle_append_entries<T, SM>(
    req: &AppendEntriesRequest,
    state: Arc<Mutex<RaftState<T, SM>>>,
    client_manager: Option<
        Arc<Mutex<crate::client_manager::ClientResponseManager<SM::Response>>>,
    >,
) -> anyhow::Result<AppendEntriesResponse>
where
    T: Send + Sync + Clone + serde::Serialize + serde::de::DeserializeOwned,
    SM: StateMachine<Command = T>,
    SM::Response: 'static,
{
    let mut state = state.lock().await;

    let result = async {
        validate_term_and_step_down(req.term, req.leader_id, &mut state)
            .await?;
        verify_log_match(req.prev_log_index, req.prev_log_term, &state)?;
        synchronize_log(
            &req.entries,
            req.prev_log_index + 1,
            req.leader_id,
            &mut state,
            client_manager,
        )
        .await?;
        advance_commit_index(req.leader_commit, &mut state).await?;
        Ok(())
    }
    .await;

    match result {
        Ok(()) => Ok(AppendEntriesResponse {
            term: state.persistent.current_term,
            success: true,
        }),
        Err(AppendEntriesError::Rejected(response)) => Ok(response),
        Err(AppendEntriesError::Internal(e)) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::Command;
    use crate::raft;
    use tokio::sync::mpsc;

    fn create_test_storage<T: Send + Sync + Clone + 'static>()
    -> Box<dyn crate::storage::Storage<T>> {
        Box::new(crate::storage::MemStorage::default())
    }

    fn create_test_state_machine() -> crate::statemachine::NoOpStateMachine {
        crate::statemachine::NoOpStateMachine::default()
    }

    #[derive(Clone, Default)]
    struct RecordingStateMachine {
        applied_commands: Arc<Mutex<Vec<bytes::Bytes>>>,
    }

    #[async_trait::async_trait]
    impl crate::statemachine::StateMachine for RecordingStateMachine {
        type Command = bytes::Bytes;
        type Response = ();

        async fn apply(
            &mut self,
            command: &Self::Command,
        ) -> anyhow::Result<Self::Response> {
            self.applied_commands.lock().await.push(command.clone());
            Ok(())
        }
    }

    impl RecordingStateMachine {
        fn new() -> Self {
            Self {
                applied_commands: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    #[tokio::test]
    async fn test_append_entries_with_higher_term_converts_to_follower()
    -> anyhow::Result<()> {
        let mut initial_state = RaftState::new(
            1,
            create_test_storage(),
            create_test_state_machine(),
        );
        initial_state.persistent.current_term = 50;
        initial_state.persistent.voted_for = Some(1);
        // Set to Leader role directly for test
        initial_state.role =
            crate::raft::RoleState::Leader(crate::raft::LeaderState::new(
                &[],
                initial_state.get_last_log_idx(),
            ));
        let state = Arc::new(Mutex::new(initial_state));

        let (cmd_tx, mut cmd_rx) = mpsc::channel::<Command>(32);
        let (_heartbeat_tx, _heartbeat_rx) =
            mpsc::unbounded_channel::<(u32, u32)>();
        let (_client_tx, _client_rx) = mpsc::channel::<bytes::Bytes>(32);
        let state_clone = Arc::clone(&state);

        tokio::spawn(async move {
            while let Some(cmd) = cmd_rx.recv().await {
                if let Command::AppendEntries(req, resp_tx) = cmd {
                    let resp =
                        handle_append_entries(&req, state_clone.clone(), None)
                            .await
                            .unwrap();
                    let _ = resp_tx.send(resp);
                }
            }
        });

        let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
        let req = AppendEntriesRequest {
            term: 100,
            leader_id: 99999,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: Vec::new(),
            leader_commit: 0,
        };

        cmd_tx.send(Command::AppendEntries(req, resp_tx)).await?;

        let _response = resp_rx.await?;

        let final_state = state.lock().await;
        assert_eq!(final_state.persistent.current_term, 100);
        assert!(final_state.role.is_follower());
        assert_eq!(final_state.persistent.voted_for, None);
        assert_eq!(final_state.leader_id, Some(99999));

        Ok(())
    }

    #[tokio::test]
    async fn test_append_entries_conflict_resolution() -> anyhow::Result<()> {
        let mut follower_state = RaftState::new(
            1,
            create_test_storage(),
            create_test_state_machine(),
        );
        follower_state.persistent.current_term = 3;
        follower_state.persistent.log.push(raft::Entry {
            term: 1,
            command: bytes::Bytes::from(&b"cmd1"[..]),
        });
        follower_state.persistent.log.push(raft::Entry {
            term: 1,
            command: bytes::Bytes::from(&b"cmd2"[..]),
        });
        follower_state.persistent.log.push(raft::Entry {
            term: 2,
            command: bytes::Bytes::from(&b"cmd3_old"[..]),
        });
        follower_state.persistent.log.push(raft::Entry {
            term: 2,
            command: bytes::Bytes::from(&b"cmd4_old"[..]),
        });
        let state = Arc::new(Mutex::new(follower_state));

        let req = AppendEntriesRequest {
            term: 3,
            leader_id: 2,
            prev_log_index: 2,
            prev_log_term: 1,
            entries: vec![
                LogEntry {
                    term: 3,
                    command: bincode::serialize(&bytes::Bytes::from(
                        &b"cmd3_new"[..],
                    ))
                    .unwrap(),
                },
                LogEntry {
                    term: 3,
                    command: bincode::serialize(&bytes::Bytes::from(
                        &b"cmd4_new"[..],
                    ))
                    .unwrap(),
                },
                LogEntry {
                    term: 3,
                    command: bincode::serialize(&bytes::Bytes::from(
                        &b"cmd5_new"[..],
                    ))
                    .unwrap(),
                },
            ],
            leader_commit: 0,
        };

        let response = handle_append_entries(&req, state.clone(), None).await?;

        assert!(response.success);
        assert_eq!(response.term, 3);

        let final_state = state.lock().await;
        assert_eq!(
            final_state.persistent.log.len(),
            5,
            "Log should have 5 entries"
        );

        assert_eq!(final_state.persistent.log[0].term, 1);
        assert_eq!(final_state.persistent.log[0].command.as_ref(), b"cmd1");
        assert_eq!(final_state.persistent.log[1].term, 1);
        assert_eq!(final_state.persistent.log[1].command.as_ref(), b"cmd2");

        assert_eq!(final_state.persistent.log[2].term, 3);
        assert_eq!(final_state.persistent.log[2].command.as_ref(), b"cmd3_new");
        assert_eq!(final_state.persistent.log[3].term, 3);
        assert_eq!(final_state.persistent.log[3].command.as_ref(), b"cmd4_new");

        assert_eq!(final_state.persistent.log[4].term, 3);
        assert_eq!(final_state.persistent.log[4].command.as_ref(), b"cmd5_new");

        Ok(())
    }

    #[tokio::test]
    async fn test_append_entries_no_conflict_append_only() -> anyhow::Result<()>
    {
        let mut follower_state = RaftState::new(
            1,
            create_test_storage(),
            create_test_state_machine(),
        );
        follower_state.persistent.current_term = 2;
        follower_state.persistent.log.push(raft::Entry {
            term: 1,
            command: bytes::Bytes::from(&b"cmd1"[..]),
        });
        follower_state.persistent.log.push(raft::Entry {
            term: 1,
            command: bytes::Bytes::from(&b"cmd2"[..]),
        });
        let state = Arc::new(Mutex::new(follower_state));

        let req = AppendEntriesRequest {
            term: 2,
            leader_id: 2,
            prev_log_index: 2,
            prev_log_term: 1,
            entries: vec![
                LogEntry {
                    term: 2,
                    command: bincode::serialize(&bytes::Bytes::from(
                        &b"cmd3"[..],
                    ))
                    .unwrap(),
                },
                LogEntry {
                    term: 2,
                    command: bincode::serialize(&bytes::Bytes::from(
                        &b"cmd4"[..],
                    ))
                    .unwrap(),
                },
            ],
            leader_commit: 0,
        };

        let response = handle_append_entries(&req, state.clone(), None).await?;

        assert!(response.success);

        let final_state = state.lock().await;
        assert_eq!(final_state.persistent.log.len(), 4);
        assert_eq!(final_state.persistent.log[2].term, 2);
        assert_eq!(final_state.persistent.log[2].command.as_ref(), b"cmd3");
        assert_eq!(final_state.persistent.log[3].term, 2);
        assert_eq!(final_state.persistent.log[3].command.as_ref(), b"cmd4");

        Ok(())
    }

    #[tokio::test]
    async fn test_append_entries_updates_commit_index_and_applies_entries()
    -> anyhow::Result<()> {
        let state_machine = RecordingStateMachine::new();
        let applied_commands = state_machine.applied_commands.clone();

        let mut follower_state =
            RaftState::new(1, create_test_storage(), state_machine);
        follower_state.persistent.current_term = 2;
        follower_state.persistent.log.push(raft::Entry {
            term: 1,
            command: bytes::Bytes::from(&b"cmd1"[..]),
        });
        follower_state.persistent.log.push(raft::Entry {
            term: 1,
            command: bytes::Bytes::from(&b"cmd2"[..]),
        });
        follower_state.persistent.log.push(raft::Entry {
            term: 2,
            command: bytes::Bytes::from(&b"cmd3"[..]),
        });
        assert_eq!(follower_state.commit_index, 0);
        assert_eq!(follower_state.last_applied, 0);
        let state = Arc::new(Mutex::new(follower_state));

        let req = AppendEntriesRequest {
            term: 2,
            leader_id: 2,
            prev_log_index: 3,
            prev_log_term: 2,
            entries: vec![],
            leader_commit: 2,
        };

        let response = handle_append_entries(&req, state.clone(), None).await?;
        assert!(response.success);

        let final_state = state.lock().await;
        assert_eq!(final_state.commit_index, 2);
        assert_eq!(final_state.last_applied, 2);

        let commands = applied_commands.lock().await;
        assert_eq!(commands.len(), 2);
        assert_eq!(commands[0].as_ref(), b"cmd1");
        assert_eq!(commands[1].as_ref(), b"cmd2");

        Ok(())
    }

    #[tokio::test]
    async fn test_append_entries_clears_pending_responses_on_conflict()
    -> anyhow::Result<()> {
        use crate::client_manager::ClientResponseManager;

        let mut follower_state = RaftState::new(
            1,
            create_test_storage(),
            create_test_state_machine(),
        );
        follower_state.persistent.current_term = 3;
        follower_state.persistent.log.push(raft::Entry {
            term: 1,
            command: bytes::Bytes::from(&b"cmd1"[..]),
        });
        follower_state.persistent.log.push(raft::Entry {
            term: 2,
            command: bytes::Bytes::from(&b"cmd2_old"[..]),
        });
        follower_state.persistent.log.push(raft::Entry {
            term: 2,
            command: bytes::Bytes::from(&b"cmd3_old"[..]),
        });

        let client_manager = Arc::new(Mutex::new(ClientResponseManager::new()));

        let (tx1, _rx1) = tokio::sync::oneshot::channel();
        let (tx2, mut rx2) = tokio::sync::oneshot::channel();
        let (tx3, mut rx3) = tokio::sync::oneshot::channel();
        client_manager.lock().await.register(1, tx1);
        client_manager.lock().await.register(2, tx2);
        client_manager.lock().await.register(3, tx3);

        let state = Arc::new(Mutex::new(follower_state));

        let req = AppendEntriesRequest {
            term: 3,
            leader_id: 2,
            prev_log_index: 1,
            prev_log_term: 1,
            entries: vec![LogEntry {
                term: 3,
                command: bincode::serialize(&bytes::Bytes::from(
                    &b"cmd2_new"[..],
                ))
                .unwrap(),
            }],
            leader_commit: 0,
        };

        let response = handle_append_entries(
            &req,
            state.clone(),
            Some(client_manager.clone()),
        )
        .await?;
        assert!(response.success);

        let final_state = state.lock().await;
        assert_eq!(final_state.persistent.log.len(), 2);
        assert_eq!(final_state.persistent.log[1].term, 3);
        assert_eq!(final_state.persistent.log[1].command.as_ref(), b"cmd2_new");
        drop(final_state);

        drop(client_manager);

        assert!(
            matches!(
                rx2.try_recv(),
                Err(tokio::sync::oneshot::error::TryRecvError::Closed)
            ),
            "Channel for log index 2 should be closed"
        );
        assert!(
            matches!(
                rx3.try_recv(),
                Err(tokio::sync::oneshot::error::TryRecvError::Closed)
            ),
            "Channel for log index 3 should be closed"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_append_entries_rejects_prev_log_term_mismatch()
    -> anyhow::Result<()> {
        let mut follower_state = RaftState::new(
            1,
            create_test_storage(),
            create_test_state_machine(),
        );
        follower_state.persistent.current_term = 3;
        follower_state.persistent.log.push(raft::Entry {
            term: 1,
            command: bytes::Bytes::from(&b"cmd1"[..]),
        });
        follower_state.persistent.log.push(raft::Entry {
            term: 2,
            command: bytes::Bytes::from(&b"cmd2"[..]),
        });
        let state = Arc::new(Mutex::new(follower_state));

        let req = AppendEntriesRequest {
            term: 3,
            leader_id: 2,
            prev_log_index: 2,
            prev_log_term: 1,
            entries: vec![],
            leader_commit: 0,
        };

        let response = handle_append_entries(&req, state.clone(), None).await?;
        assert!(!response.success);
        assert_eq!(response.term, 3);

        let final_state = state.lock().await;
        assert_eq!(final_state.persistent.log.len(), 2);

        Ok(())
    }

    #[tokio::test]
    async fn test_append_entries_rejects_older_term() -> anyhow::Result<()> {
        let mut follower_state = RaftState::new(
            1,
            create_test_storage(),
            create_test_state_machine(),
        );
        follower_state.persistent.current_term = 5;
        let state = Arc::new(Mutex::new(follower_state));

        let req = AppendEntriesRequest {
            term: 3,
            leader_id: 2,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        };

        let response = handle_append_entries(&req, state.clone(), None).await?;
        assert!(!response.success);
        assert_eq!(response.term, 5);

        let final_state = state.lock().await;
        assert_eq!(final_state.persistent.current_term, 5);

        Ok(())
    }

    #[tokio::test]
    async fn test_append_entries_rejects_prev_log_index_exceeds_log_length()
    -> anyhow::Result<()> {
        let mut follower_state = RaftState::new(
            1,
            create_test_storage(),
            create_test_state_machine(),
        );
        follower_state.persistent.current_term = 3;
        follower_state.persistent.log.push(raft::Entry {
            term: 1,
            command: bytes::Bytes::from(&b"cmd1"[..]),
        });
        follower_state.persistent.log.push(raft::Entry {
            term: 2,
            command: bytes::Bytes::from(&b"cmd2"[..]),
        });
        let state = Arc::new(Mutex::new(follower_state));

        let req = AppendEntriesRequest {
            term: 3,
            leader_id: 2,
            prev_log_index: 5,
            prev_log_term: 2,
            entries: vec![],
            leader_commit: 0,
        };

        let response = handle_append_entries(&req, state.clone(), None).await?;
        assert!(!response.success);
        assert_eq!(response.term, 3);

        let final_state = state.lock().await;
        assert_eq!(final_state.persistent.log.len(), 2);

        Ok(())
    }

    #[tokio::test]
    async fn test_append_entries_commit_index_capped_by_log_length()
    -> anyhow::Result<()> {
        let mut follower_state = RaftState::new(
            1,
            create_test_storage(),
            create_test_state_machine(),
        );
        follower_state.persistent.current_term = 2;
        follower_state.persistent.log.push(raft::Entry {
            term: 1,
            command: bytes::Bytes::from(&b"cmd1"[..]),
        });
        follower_state.persistent.log.push(raft::Entry {
            term: 2,
            command: bytes::Bytes::from(&b"cmd2"[..]),
        });
        let state = Arc::new(Mutex::new(follower_state));

        let req = AppendEntriesRequest {
            term: 2,
            leader_id: 2,
            prev_log_index: 2,
            prev_log_term: 2,
            entries: vec![],
            leader_commit: 999,
        };

        let response = handle_append_entries(&req, state.clone(), None).await?;
        assert!(response.success);

        let final_state = state.lock().await;
        assert_eq!(final_state.commit_index, 2);

        Ok(())
    }

    #[tokio::test]
    async fn test_append_entries_does_not_reapply_already_applied_entries()
    -> anyhow::Result<()> {
        let state_machine = RecordingStateMachine::new();
        let applied_commands = state_machine.applied_commands.clone();

        let mut follower_state =
            RaftState::new(1, create_test_storage(), state_machine);
        follower_state.persistent.current_term = 2;
        follower_state.persistent.log.push(raft::Entry {
            term: 1,
            command: bytes::Bytes::from(&b"cmd1"[..]),
        });
        follower_state.persistent.log.push(raft::Entry {
            term: 1,
            command: bytes::Bytes::from(&b"cmd2"[..]),
        });
        follower_state.persistent.log.push(raft::Entry {
            term: 2,
            command: bytes::Bytes::from(&b"cmd3"[..]),
        });
        follower_state.commit_index = 1;
        follower_state.last_applied = 0;
        follower_state.apply_committed().await?;

        assert_eq!(applied_commands.lock().await.len(), 1);

        let state = Arc::new(Mutex::new(follower_state));

        let req = AppendEntriesRequest {
            term: 2,
            leader_id: 2,
            prev_log_index: 3,
            prev_log_term: 2,
            entries: vec![],
            leader_commit: 3,
        };

        let response = handle_append_entries(&req, state.clone(), None).await?;
        assert!(response.success);

        let final_state = state.lock().await;
        assert_eq!(final_state.commit_index, 3);
        assert_eq!(final_state.last_applied, 3);

        let commands = applied_commands.lock().await;
        assert_eq!(commands.len(), 3);
        assert_eq!(commands[0].as_ref(), b"cmd1");
        assert_eq!(commands[1].as_ref(), b"cmd2");
        assert_eq!(commands[2].as_ref(), b"cmd3");

        Ok(())
    }

    #[tokio::test]
    async fn test_append_entries_idempotent_duplicate_request()
    -> anyhow::Result<()> {
        let state_machine = RecordingStateMachine::new();
        let applied_commands = state_machine.applied_commands.clone();

        let mut follower_state =
            RaftState::new(1, create_test_storage(), state_machine);
        follower_state.persistent.current_term = 2;
        follower_state.persistent.log.push(raft::Entry {
            term: 1,
            command: bytes::Bytes::from(&b"cmd1"[..]),
        });
        let state = Arc::new(Mutex::new(follower_state));

        let req = AppendEntriesRequest {
            term: 2,
            leader_id: 2,
            prev_log_index: 1,
            prev_log_term: 1,
            entries: vec![LogEntry {
                term: 2,
                command: bincode::serialize(&bytes::Bytes::from(&b"cmd2"[..]))
                    .unwrap(),
            }],
            leader_commit: 2,
        };

        let response1 =
            handle_append_entries(&req, state.clone(), None).await?;
        assert!(response1.success);

        {
            let commands = applied_commands.lock().await;
            assert_eq!(commands.len(), 2);
            assert_eq!(commands[0].as_ref(), b"cmd1");
            assert_eq!(commands[1].as_ref(), b"cmd2");
        }

        let response2 =
            handle_append_entries(&req, state.clone(), None).await?;
        assert!(response2.success);

        {
            let final_state = state.lock().await;
            assert_eq!(final_state.persistent.log.len(), 2);
            assert_eq!(final_state.persistent.log[0].term, 1);
            assert_eq!(final_state.persistent.log[0].command.as_ref(), b"cmd1");
            assert_eq!(final_state.persistent.log[1].term, 2);
            assert_eq!(final_state.persistent.log[1].command.as_ref(), b"cmd2");
            assert_eq!(final_state.commit_index, 2);
            assert_eq!(final_state.last_applied, 2);
        }

        let commands = applied_commands.lock().await;
        assert_eq!(commands.len(), 2);

        Ok(())
    }

    #[tokio::test]
    async fn test_append_entries_candidate_receives_same_term_becomes_follower()
    -> anyhow::Result<()> {
        let mut candidate_state = RaftState::new(
            1,
            create_test_storage(),
            create_test_state_machine(),
        );
        candidate_state.persistent.current_term = 5;
        candidate_state.role = crate::raft::RoleState::Candidate;
        candidate_state.persistent.voted_for = Some(1);
        let state = Arc::new(Mutex::new(candidate_state));

        let req = AppendEntriesRequest {
            term: 5,
            leader_id: 2,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        };

        let response = handle_append_entries(&req, state.clone(), None).await?;
        assert!(response.success);

        let final_state = state.lock().await;
        assert!(final_state.role.is_follower());
        assert_eq!(final_state.persistent.current_term, 5);
        assert_eq!(final_state.leader_id, Some(2));

        Ok(())
    }

    #[tokio::test]
    async fn test_append_entries_prev_log_index_zero_success()
    -> anyhow::Result<()> {
        let mut follower_state = RaftState::new(
            1,
            create_test_storage(),
            create_test_state_machine(),
        );
        follower_state.persistent.current_term = 1;
        let state = Arc::new(Mutex::new(follower_state));

        let req = AppendEntriesRequest {
            term: 1,
            leader_id: 2,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![
                LogEntry {
                    term: 1,
                    command: bincode::serialize(&bytes::Bytes::from(
                        &b"cmd1"[..],
                    ))
                    .unwrap(),
                },
                LogEntry {
                    term: 1,
                    command: bincode::serialize(&bytes::Bytes::from(
                        &b"cmd2"[..],
                    ))
                    .unwrap(),
                },
            ],
            leader_commit: 0,
        };

        let response = handle_append_entries(&req, state.clone(), None).await?;
        assert!(response.success);

        let final_state = state.lock().await;
        assert_eq!(final_state.persistent.log.len(), 2);
        assert_eq!(final_state.persistent.log[0].term, 1);
        assert_eq!(final_state.persistent.log[0].command.as_ref(), b"cmd1");
        assert_eq!(final_state.persistent.log[1].term, 1);
        assert_eq!(final_state.persistent.log[1].command.as_ref(), b"cmd2");

        Ok(())
    }

    #[tokio::test]
    async fn test_append_entries_partial_match_overwrites_from_conflict_point()
    -> anyhow::Result<()> {
        let mut follower_state = RaftState::new(
            1,
            create_test_storage(),
            create_test_state_machine(),
        );
        follower_state.persistent.current_term = 3;
        follower_state.persistent.log.push(raft::Entry {
            term: 1,
            command: bytes::Bytes::from(&b"cmd1"[..]),
        });
        follower_state.persistent.log.push(raft::Entry {
            term: 1,
            command: bytes::Bytes::from(&b"cmd2"[..]),
        });
        follower_state.persistent.log.push(raft::Entry {
            term: 2,
            command: bytes::Bytes::from(&b"cmd3"[..]),
        });
        follower_state.persistent.log.push(raft::Entry {
            term: 2,
            command: bytes::Bytes::from(&b"cmd4_old"[..]),
        });
        follower_state.persistent.log.push(raft::Entry {
            term: 2,
            command: bytes::Bytes::from(&b"cmd5_old"[..]),
        });
        let state = Arc::new(Mutex::new(follower_state));

        let req = AppendEntriesRequest {
            term: 3,
            leader_id: 2,
            prev_log_index: 3,
            prev_log_term: 2,
            entries: vec![
                LogEntry {
                    term: 3,
                    command: bincode::serialize(&bytes::Bytes::from(
                        &b"cmd4_new"[..],
                    ))
                    .unwrap(),
                },
                LogEntry {
                    term: 3,
                    command: bincode::serialize(&bytes::Bytes::from(
                        &b"cmd5_new"[..],
                    ))
                    .unwrap(),
                },
            ],
            leader_commit: 0,
        };

        let response = handle_append_entries(&req, state.clone(), None).await?;
        assert!(response.success);

        let final_state = state.lock().await;
        assert_eq!(final_state.persistent.log.len(), 5);
        assert_eq!(final_state.persistent.log[0].term, 1);
        assert_eq!(final_state.persistent.log[0].command.as_ref(), b"cmd1");
        assert_eq!(final_state.persistent.log[1].term, 1);
        assert_eq!(final_state.persistent.log[1].command.as_ref(), b"cmd2");
        assert_eq!(final_state.persistent.log[2].term, 2);
        assert_eq!(final_state.persistent.log[2].command.as_ref(), b"cmd3");
        assert_eq!(final_state.persistent.log[3].term, 3);
        assert_eq!(final_state.persistent.log[3].command.as_ref(), b"cmd4_new");
        assert_eq!(final_state.persistent.log[4].term, 3);
        assert_eq!(final_state.persistent.log[4].command.as_ref(), b"cmd5_new");

        Ok(())
    }

    #[tokio::test]
    async fn test_append_entries_rejection_does_not_change_commit_index()
    -> anyhow::Result<()> {
        let mut follower_state = RaftState::new(
            1,
            create_test_storage(),
            create_test_state_machine(),
        );
        follower_state.persistent.current_term = 3;
        follower_state.persistent.log.push(raft::Entry {
            term: 1,
            command: bytes::Bytes::from(&b"cmd1"[..]),
        });
        follower_state.persistent.log.push(raft::Entry {
            term: 2,
            command: bytes::Bytes::from(&b"cmd2"[..]),
        });
        follower_state.commit_index = 1;
        follower_state.last_applied = 1;
        let state = Arc::new(Mutex::new(follower_state));

        let req = AppendEntriesRequest {
            term: 3,
            leader_id: 2,
            prev_log_index: 2,
            prev_log_term: 1,
            entries: vec![],
            leader_commit: 5,
        };

        let response = handle_append_entries(&req, state.clone(), None).await?;
        assert!(!response.success);
        assert_eq!(response.term, 3);

        let final_state = state.lock().await;
        assert_eq!(final_state.persistent.log.len(), 2);
        assert_eq!(final_state.commit_index, 1);
        assert_eq!(final_state.last_applied, 1);

        Ok(())
    }
}
