use crate::raft::{self, RaftState};
use crate::rpc::*;
use crate::statemachine::StateMachine;
use crate::types::{LogIndex, NodeId, Term};
use anyhow::Context;
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
    request_term: Term,
    sender_id: NodeId,
    state: &mut RaftState<T, SM>,
) -> Result<(), AppendEntriesError>
where
    T: Send + Sync + Clone,
    SM: StateMachine<Command = T>,
{
    use crate::core::term::{TermAction, validate_request_term};

    let is_candidate_or_leader =
        state.role.is_candidate() || state.role.is_leader();

    match validate_request_term(
        request_term,
        state.persistent.current_term,
        sender_id,
        is_candidate_or_leader,
    ) {
        TermAction::Reject => {
            tracing::warn!(
                id=?state.id,
                req_term=%request_term,
                current_term=%state.persistent.current_term,
                "Request rejected: term is older than current term"
            );
            return Err(AppendEntriesError::Rejected(AppendEntriesResponse {
                term: state.persistent.current_term,
                success: false,
            }));
        }
        TermAction::StepDown {
            new_term,
            leader_id,
        } => {
            let term_changed = new_term > state.persistent.current_term;
            state.become_follower(new_term, leader_id);
            if term_changed && let Err(e) = state.persist().await {
                tracing::error!(id=?state.id, error=?e, "Failed to persist state after term update");
                return Err(AppendEntriesError::Rejected(
                    AppendEntriesResponse {
                        term: state.persistent.current_term,
                        success: false,
                    },
                ));
            }
        }
        TermAction::Accept => {
            state.leader_id = Some(sender_id);
        }
    }

    Ok(())
}

fn verify_log_match<T, SM>(
    prev_log_index: LogIndex,
    prev_log_term: Term,
    state: &RaftState<T, SM>,
) -> Result<(), AppendEntriesError>
where
    T: Send + Sync + Clone,
    SM: StateMachine<Command = T>,
{
    use crate::core::log::{self, LogMatchError};

    log::verify_log_match(prev_log_index, prev_log_term, &state.persistent.log)
        .map_err(|e| {
            match &e {
                LogMatchError::IndexExceedsLog {
                    prev_log_index,
                    log_len,
                } => {
                    tracing::warn!(
                        id=?state.id,
                        prev_log_index=%prev_log_index,
                        log_len=%log_len,
                        "Request rejected: prev_log_index exceeds log length"
                    );
                }
                LogMatchError::TermMismatch {
                    prev_log_index,
                    expected_term,
                    actual_term,
                } => {
                    tracing::warn!(
                        id=?state.id,
                        prev_log_index=%prev_log_index,
                        prev_log_term=%expected_term,
                        actual_term=%actual_term,
                        "Request rejected: prev_log_term mismatch"
                    );
                }
                LogMatchError::InvalidIndex { prev_log_index } => {
                    tracing::warn!(
                        id=?state.id,
                        prev_log_index=%prev_log_index,
                        "Request rejected: invalid prev_log_index"
                    );
                }
            }
            match e {
                LogMatchError::InvalidIndex { prev_log_index } => {
                    AppendEntriesError::Internal(anyhow::anyhow!(
                        "prev_log_index {} has no valid array index",
                        prev_log_index
                    ))
                }
                _ => AppendEntriesError::Rejected(AppendEntriesResponse {
                    term: state.persistent.current_term,
                    success: false,
                }),
            }
        })
}

fn detect_and_truncate_conflicts<T, SM>(
    entries: &[LogEntry],
    start_index: LogIndex,
    state: &mut RaftState<T, SM>,
) -> Option<LogIndex>
where
    T: Send + Sync + Clone,
    SM: StateMachine<Command = T>,
{
    let entry_terms: Vec<_> = entries
        .iter()
        .enumerate()
        .map(|(i, e)| (e.term, i))
        .collect();

    let conflict = crate::core::log::detect_conflict(
        &entry_terms,
        start_index,
        &state.persistent.log,
    );

    if let Some((conflict_index, arr_idx)) = conflict {
        let old_term = state.persistent.log[arr_idx].term;
        let new_term = entries
            [conflict_index.as_u32() as usize - start_index.as_u32() as usize]
            .term;
        state.persistent.log.truncate(arr_idx);
        tracing::info!(
            id=?state.id,
            conflict_index=%conflict_index,
            old_term=%old_term,
            new_term=%new_term,
            "Truncated log due to conflict"
        );
        return Some(conflict_index);
    }

    None
}

fn deserialize_and_append<T, SM>(
    entries: &[LogEntry],
    start_index: LogIndex,
    sender_id: NodeId,
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
            sender_id=%sender_id,
            start_index=%start_index,
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
    start_index: LogIndex,
    sender_id: NodeId,
    state: &mut RaftState<T, SM>,
    request_tracker: Option<
        Arc<Mutex<crate::request_tracker::RequestTracker<SM::Response>>>,
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
        && let Some(tracker) = &request_tracker
    {
        tracker.lock().await.clear_from(log_index);
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
    new_commit_index: LogIndex,
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
            old_commit_index=%old_commit,
            new_commit_index=%state.commit_index,
            requested_commit=%new_commit_index,
            "Advanced commit index"
        );
    }

    if state.commit_index > state.last_applied {
        tracing::debug!(
            id=?state.id,
            commit_index=%state.commit_index,
            last_applied=%state.last_applied,
            "Applying committed entries to state machine"
        );
        state
            .apply_committed()
            .await
            .context("failed to apply committed entries")?;
    }

    Ok(())
}

#[tracing::instrument(skip(state, request_tracker), fields(term = %req.term, leader_id = %req.leader_id, prev_log_index = %req.prev_log_index, entries_count = req.entries.len()))]
pub async fn handle_append_entries<T, SM>(
    req: &AppendEntriesRequest,
    state: Arc<Mutex<RaftState<T, SM>>>,
    request_tracker: Option<
        Arc<Mutex<crate::request_tracker::RequestTracker<SM::Response>>>,
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
            request_tracker,
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

        async fn snapshot(&self) -> anyhow::Result<Vec<u8>> {
            Ok(Vec::new())
        }

        async fn restore(&mut self, _data: &[u8]) -> anyhow::Result<()> {
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
            1u32,
            create_test_storage(),
            create_test_state_machine(),
        );
        initial_state.persistent.current_term = Term::new(50);
        initial_state.persistent.voted_for = Some(NodeId::new(1));
        // Set to Leader role directly for test
        initial_state.role =
            crate::raft::Role::Leader(crate::raft::LeaderState::new(
                &[],
                initial_state.get_last_log_idx(),
            ));
        let state = Arc::new(Mutex::new(initial_state));

        let (cmd_tx, mut cmd_rx) = mpsc::channel::<Command>(32);
        let (_heartbeat_tx, _heartbeat_rx) =
            mpsc::unbounded_channel::<(Term, NodeId)>();
        let (_client_tx, _client_rx) = mpsc::channel::<bytes::Bytes>(32);
        let state_clone = Arc::clone(&state);

        tokio::spawn(async move {
            while let Some(cmd) = cmd_rx.recv().await {
                if let Command::AppendEntries(req, resp_tx, _span) = cmd {
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
            term: Term::new(100),
            leader_id: NodeId::new(99999),
            prev_log_index: LogIndex::new(0),
            prev_log_term: Term::new(0),
            entries: Vec::new(),
            leader_commit: LogIndex::new(0),
        };

        cmd_tx
            .send(Command::AppendEntries(
                req,
                resp_tx,
                tracing::Span::current(),
            ))
            .await?;

        let _response = resp_rx.await?;

        let final_state = state.lock().await;
        assert_eq!(final_state.persistent.current_term, Term::new(100));
        assert!(final_state.role.is_follower());
        assert_eq!(final_state.persistent.voted_for, None);
        assert_eq!(final_state.leader_id, Some(NodeId::new(99999)));

        Ok(())
    }

    #[tokio::test]
    async fn test_append_entries_conflict_resolution() -> anyhow::Result<()> {
        let mut follower_state = RaftState::new(
            1u32,
            create_test_storage(),
            create_test_state_machine(),
        );
        follower_state.persistent.current_term = Term::new(3);
        follower_state.persistent.log.push(raft::Entry {
            term: Term::new(1),
            command: bytes::Bytes::from(&b"cmd1"[..]),
        });
        follower_state.persistent.log.push(raft::Entry {
            term: Term::new(1),
            command: bytes::Bytes::from(&b"cmd2"[..]),
        });
        follower_state.persistent.log.push(raft::Entry {
            term: Term::new(2),
            command: bytes::Bytes::from(&b"cmd3_old"[..]),
        });
        follower_state.persistent.log.push(raft::Entry {
            term: Term::new(2),
            command: bytes::Bytes::from(&b"cmd4_old"[..]),
        });
        let state = Arc::new(Mutex::new(follower_state));

        let req = AppendEntriesRequest {
            term: Term::new(3),
            leader_id: NodeId::new(2),
            prev_log_index: LogIndex::new(2),
            prev_log_term: Term::new(1),
            entries: vec![
                LogEntry {
                    term: Term::new(3),
                    command: bincode::serialize(&bytes::Bytes::from(
                        &b"cmd3_new"[..],
                    ))
                    .unwrap()
                    .into(),
                },
                LogEntry {
                    term: Term::new(3),
                    command: bincode::serialize(&bytes::Bytes::from(
                        &b"cmd4_new"[..],
                    ))
                    .unwrap()
                    .into(),
                },
                LogEntry {
                    term: Term::new(3),
                    command: bincode::serialize(&bytes::Bytes::from(
                        &b"cmd5_new"[..],
                    ))
                    .unwrap()
                    .into(),
                },
            ],
            leader_commit: LogIndex::new(0),
        };

        let response = handle_append_entries(&req, state.clone(), None).await?;

        assert!(response.success);
        assert_eq!(response.term, Term::new(3));

        let final_state = state.lock().await;
        assert_eq!(
            final_state.persistent.log.len(),
            5,
            "Log should have 5 entries"
        );

        assert_eq!(final_state.persistent.log[0].term, Term::new(1));
        assert_eq!(final_state.persistent.log[0].command.as_ref(), b"cmd1");
        assert_eq!(final_state.persistent.log[1].term, Term::new(1));
        assert_eq!(final_state.persistent.log[1].command.as_ref(), b"cmd2");

        assert_eq!(final_state.persistent.log[2].term, Term::new(3));
        assert_eq!(final_state.persistent.log[2].command.as_ref(), b"cmd3_new");
        assert_eq!(final_state.persistent.log[3].term, Term::new(3));
        assert_eq!(final_state.persistent.log[3].command.as_ref(), b"cmd4_new");

        assert_eq!(final_state.persistent.log[4].term, Term::new(3));
        assert_eq!(final_state.persistent.log[4].command.as_ref(), b"cmd5_new");

        Ok(())
    }

    #[tokio::test]
    async fn test_append_entries_no_conflict_append_only() -> anyhow::Result<()>
    {
        let mut follower_state = RaftState::new(
            1u32,
            create_test_storage(),
            create_test_state_machine(),
        );
        follower_state.persistent.current_term = Term::new(2);
        follower_state.persistent.log.push(raft::Entry {
            term: Term::new(1),
            command: bytes::Bytes::from(&b"cmd1"[..]),
        });
        follower_state.persistent.log.push(raft::Entry {
            term: Term::new(1),
            command: bytes::Bytes::from(&b"cmd2"[..]),
        });
        let state = Arc::new(Mutex::new(follower_state));

        let req = AppendEntriesRequest {
            term: Term::new(2),
            leader_id: NodeId::new(2),
            prev_log_index: LogIndex::new(2),
            prev_log_term: Term::new(1),
            entries: vec![
                LogEntry {
                    term: Term::new(2),
                    command: bincode::serialize(&bytes::Bytes::from(
                        &b"cmd3"[..],
                    ))
                    .unwrap()
                    .into(),
                },
                LogEntry {
                    term: Term::new(2),
                    command: bincode::serialize(&bytes::Bytes::from(
                        &b"cmd4"[..],
                    ))
                    .unwrap()
                    .into(),
                },
            ],
            leader_commit: LogIndex::new(0),
        };

        let response = handle_append_entries(&req, state.clone(), None).await?;

        assert!(response.success);

        let final_state = state.lock().await;
        assert_eq!(final_state.persistent.log.len(), 4);
        assert_eq!(final_state.persistent.log[2].term, Term::new(2));
        assert_eq!(final_state.persistent.log[2].command.as_ref(), b"cmd3");
        assert_eq!(final_state.persistent.log[3].term, Term::new(2));
        assert_eq!(final_state.persistent.log[3].command.as_ref(), b"cmd4");

        Ok(())
    }

    #[tokio::test]
    async fn test_append_entries_updates_commit_index_and_applies_entries()
    -> anyhow::Result<()> {
        let state_machine = RecordingStateMachine::new();
        let applied_commands = state_machine.applied_commands.clone();

        let mut follower_state =
            RaftState::new(1u32, create_test_storage(), state_machine);
        follower_state.persistent.current_term = Term::new(2);
        follower_state.persistent.log.push(raft::Entry {
            term: Term::new(1),
            command: bytes::Bytes::from(&b"cmd1"[..]),
        });
        follower_state.persistent.log.push(raft::Entry {
            term: Term::new(1),
            command: bytes::Bytes::from(&b"cmd2"[..]),
        });
        follower_state.persistent.log.push(raft::Entry {
            term: Term::new(2),
            command: bytes::Bytes::from(&b"cmd3"[..]),
        });
        assert_eq!(follower_state.commit_index, LogIndex::new(0));
        assert_eq!(follower_state.last_applied, LogIndex::new(0));
        let state = Arc::new(Mutex::new(follower_state));

        let req = AppendEntriesRequest {
            term: Term::new(2),
            leader_id: NodeId::new(2),
            prev_log_index: LogIndex::new(3),
            prev_log_term: Term::new(2),
            entries: vec![],
            leader_commit: LogIndex::new(2),
        };

        let response = handle_append_entries(&req, state.clone(), None).await?;
        assert!(response.success);

        let final_state = state.lock().await;
        assert_eq!(final_state.commit_index, LogIndex::new(2));
        assert_eq!(final_state.last_applied, LogIndex::new(2));

        let commands = applied_commands.lock().await;
        assert_eq!(commands.len(), 2);
        assert_eq!(commands[0].as_ref(), b"cmd1");
        assert_eq!(commands[1].as_ref(), b"cmd2");

        Ok(())
    }

    #[tokio::test]
    async fn test_append_entries_clears_pending_responses_on_conflict()
    -> anyhow::Result<()> {
        use crate::request_tracker::RequestTracker;

        let mut follower_state = RaftState::new(
            1u32,
            create_test_storage(),
            create_test_state_machine(),
        );
        follower_state.persistent.current_term = Term::new(3);
        follower_state.persistent.log.push(raft::Entry {
            term: Term::new(1),
            command: bytes::Bytes::from(&b"cmd1"[..]),
        });
        follower_state.persistent.log.push(raft::Entry {
            term: Term::new(2),
            command: bytes::Bytes::from(&b"cmd2_old"[..]),
        });
        follower_state.persistent.log.push(raft::Entry {
            term: Term::new(2),
            command: bytes::Bytes::from(&b"cmd3_old"[..]),
        });

        let request_tracker = Arc::new(Mutex::new(RequestTracker::new()));

        let (tx1, _rx1) = tokio::sync::oneshot::channel();
        let (tx2, mut rx2) = tokio::sync::oneshot::channel();
        let (tx3, mut rx3) = tokio::sync::oneshot::channel();
        let timeout =
            std::time::Instant::now() + std::time::Duration::from_secs(30);
        request_tracker.lock().await.track_write(
            LogIndex::new(1),
            tx1,
            timeout,
        );
        request_tracker.lock().await.track_write(
            LogIndex::new(2),
            tx2,
            timeout,
        );
        request_tracker.lock().await.track_write(
            LogIndex::new(3),
            tx3,
            timeout,
        );

        let state = Arc::new(Mutex::new(follower_state));

        let req = AppendEntriesRequest {
            term: Term::new(3),
            leader_id: NodeId::new(2),
            prev_log_index: LogIndex::new(1),
            prev_log_term: Term::new(1),
            entries: vec![LogEntry {
                term: Term::new(3),
                command: bincode::serialize(&bytes::Bytes::from(
                    &b"cmd2_new"[..],
                ))
                .unwrap()
                .into(),
            }],
            leader_commit: LogIndex::new(0),
        };

        let response = handle_append_entries(
            &req,
            state.clone(),
            Some(request_tracker.clone()),
        )
        .await?;
        assert!(response.success);

        let final_state = state.lock().await;
        assert_eq!(final_state.persistent.log.len(), 2);
        assert_eq!(final_state.persistent.log[1].term, Term::new(3));
        assert_eq!(final_state.persistent.log[1].command.as_ref(), b"cmd2_new");
        drop(final_state);

        drop(request_tracker);

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
            1u32,
            create_test_storage(),
            create_test_state_machine(),
        );
        follower_state.persistent.current_term = Term::new(3);
        follower_state.persistent.log.push(raft::Entry {
            term: Term::new(1),
            command: bytes::Bytes::from(&b"cmd1"[..]),
        });
        follower_state.persistent.log.push(raft::Entry {
            term: Term::new(2),
            command: bytes::Bytes::from(&b"cmd2"[..]),
        });
        let state = Arc::new(Mutex::new(follower_state));

        let req = AppendEntriesRequest {
            term: Term::new(3),
            leader_id: NodeId::new(2),
            prev_log_index: LogIndex::new(2),
            prev_log_term: Term::new(1),
            entries: vec![],
            leader_commit: LogIndex::new(0),
        };

        let response = handle_append_entries(&req, state.clone(), None).await?;
        assert!(!response.success);
        assert_eq!(response.term, Term::new(3));

        let final_state = state.lock().await;
        assert_eq!(final_state.persistent.log.len(), 2);

        Ok(())
    }

    #[tokio::test]
    async fn test_append_entries_rejects_older_term() -> anyhow::Result<()> {
        let mut follower_state = RaftState::new(
            1u32,
            create_test_storage(),
            create_test_state_machine(),
        );
        follower_state.persistent.current_term = Term::new(5);
        let state = Arc::new(Mutex::new(follower_state));

        let req = AppendEntriesRequest {
            term: Term::new(3),
            leader_id: NodeId::new(2),
            prev_log_index: LogIndex::new(0),
            prev_log_term: Term::new(0),
            entries: vec![],
            leader_commit: LogIndex::new(0),
        };

        let response = handle_append_entries(&req, state.clone(), None).await?;
        assert!(!response.success);
        assert_eq!(response.term, Term::new(5));

        let final_state = state.lock().await;
        assert_eq!(final_state.persistent.current_term, Term::new(5));

        Ok(())
    }

    #[tokio::test]
    async fn test_append_entries_rejects_prev_log_index_exceeds_log_length()
    -> anyhow::Result<()> {
        let mut follower_state = RaftState::new(
            1u32,
            create_test_storage(),
            create_test_state_machine(),
        );
        follower_state.persistent.current_term = Term::new(3);
        follower_state.persistent.log.push(raft::Entry {
            term: Term::new(1),
            command: bytes::Bytes::from(&b"cmd1"[..]),
        });
        follower_state.persistent.log.push(raft::Entry {
            term: Term::new(2),
            command: bytes::Bytes::from(&b"cmd2"[..]),
        });
        let state = Arc::new(Mutex::new(follower_state));

        let req = AppendEntriesRequest {
            term: Term::new(3),
            leader_id: NodeId::new(2),
            prev_log_index: LogIndex::new(5),
            prev_log_term: Term::new(2),
            entries: vec![],
            leader_commit: LogIndex::new(0),
        };

        let response = handle_append_entries(&req, state.clone(), None).await?;
        assert!(!response.success);
        assert_eq!(response.term, Term::new(3));

        let final_state = state.lock().await;
        assert_eq!(final_state.persistent.log.len(), 2);

        Ok(())
    }

    #[tokio::test]
    async fn test_append_entries_commit_index_capped_by_log_length()
    -> anyhow::Result<()> {
        let mut follower_state = RaftState::new(
            1u32,
            create_test_storage(),
            create_test_state_machine(),
        );
        follower_state.persistent.current_term = Term::new(2);
        follower_state.persistent.log.push(raft::Entry {
            term: Term::new(1),
            command: bytes::Bytes::from(&b"cmd1"[..]),
        });
        follower_state.persistent.log.push(raft::Entry {
            term: Term::new(2),
            command: bytes::Bytes::from(&b"cmd2"[..]),
        });
        let state = Arc::new(Mutex::new(follower_state));

        let req = AppendEntriesRequest {
            term: Term::new(2),
            leader_id: NodeId::new(2),
            prev_log_index: LogIndex::new(2),
            prev_log_term: Term::new(2),
            entries: vec![],
            leader_commit: LogIndex::new(999),
        };

        let response = handle_append_entries(&req, state.clone(), None).await?;
        assert!(response.success);

        let final_state = state.lock().await;
        assert_eq!(final_state.commit_index, LogIndex::new(2));

        Ok(())
    }

    #[tokio::test]
    async fn test_append_entries_does_not_reapply_already_applied_entries()
    -> anyhow::Result<()> {
        let state_machine = RecordingStateMachine::new();
        let applied_commands = state_machine.applied_commands.clone();

        let mut follower_state =
            RaftState::new(1u32, create_test_storage(), state_machine);
        follower_state.persistent.current_term = Term::new(2);
        follower_state.persistent.log.push(raft::Entry {
            term: Term::new(1),
            command: bytes::Bytes::from(&b"cmd1"[..]),
        });
        follower_state.persistent.log.push(raft::Entry {
            term: Term::new(1),
            command: bytes::Bytes::from(&b"cmd2"[..]),
        });
        follower_state.persistent.log.push(raft::Entry {
            term: Term::new(2),
            command: bytes::Bytes::from(&b"cmd3"[..]),
        });
        follower_state.commit_index = LogIndex::new(1);
        follower_state.last_applied = LogIndex::new(0);
        follower_state.apply_committed().await?;

        assert_eq!(applied_commands.lock().await.len(), 1);

        let state = Arc::new(Mutex::new(follower_state));

        let req = AppendEntriesRequest {
            term: Term::new(2),
            leader_id: NodeId::new(2),
            prev_log_index: LogIndex::new(3),
            prev_log_term: Term::new(2),
            entries: vec![],
            leader_commit: LogIndex::new(3),
        };

        let response = handle_append_entries(&req, state.clone(), None).await?;
        assert!(response.success);

        let final_state = state.lock().await;
        assert_eq!(final_state.commit_index, LogIndex::new(3));
        assert_eq!(final_state.last_applied, LogIndex::new(3));

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
            RaftState::new(1u32, create_test_storage(), state_machine);
        follower_state.persistent.current_term = Term::new(2);
        follower_state.persistent.log.push(raft::Entry {
            term: Term::new(1),
            command: bytes::Bytes::from(&b"cmd1"[..]),
        });
        let state = Arc::new(Mutex::new(follower_state));

        let req = AppendEntriesRequest {
            term: Term::new(2),
            leader_id: NodeId::new(2),
            prev_log_index: LogIndex::new(1),
            prev_log_term: Term::new(1),
            entries: vec![LogEntry {
                term: Term::new(2),
                command: bincode::serialize(&bytes::Bytes::from(&b"cmd2"[..]))
                    .unwrap()
                    .into(),
            }],
            leader_commit: LogIndex::new(2),
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
            assert_eq!(final_state.persistent.log[0].term, Term::new(1));
            assert_eq!(final_state.persistent.log[0].command.as_ref(), b"cmd1");
            assert_eq!(final_state.persistent.log[1].term, Term::new(2));
            assert_eq!(final_state.persistent.log[1].command.as_ref(), b"cmd2");
            assert_eq!(final_state.commit_index, LogIndex::new(2));
            assert_eq!(final_state.last_applied, LogIndex::new(2));
        }

        let commands = applied_commands.lock().await;
        assert_eq!(commands.len(), 2);

        Ok(())
    }

    #[tokio::test]
    async fn test_append_entries_candidate_receives_same_term_becomes_follower()
    -> anyhow::Result<()> {
        let mut candidate_state = RaftState::new(
            1u32,
            create_test_storage(),
            create_test_state_machine(),
        );
        candidate_state.persistent.current_term = Term::new(5);
        candidate_state.role = crate::raft::Role::Candidate;
        candidate_state.persistent.voted_for = Some(NodeId::new(1));
        let state = Arc::new(Mutex::new(candidate_state));

        let req = AppendEntriesRequest {
            term: Term::new(5),
            leader_id: NodeId::new(2),
            prev_log_index: LogIndex::new(0),
            prev_log_term: Term::new(0),
            entries: vec![],
            leader_commit: LogIndex::new(0),
        };

        let response = handle_append_entries(&req, state.clone(), None).await?;
        assert!(response.success);

        let final_state = state.lock().await;
        assert!(final_state.role.is_follower());
        assert_eq!(final_state.persistent.current_term, Term::new(5));
        assert_eq!(final_state.leader_id, Some(NodeId::new(2)));

        Ok(())
    }

    #[tokio::test]
    async fn test_append_entries_prev_log_index_zero_success()
    -> anyhow::Result<()> {
        let mut follower_state = RaftState::new(
            1u32,
            create_test_storage(),
            create_test_state_machine(),
        );
        follower_state.persistent.current_term = Term::new(1);
        let state = Arc::new(Mutex::new(follower_state));

        let req = AppendEntriesRequest {
            term: Term::new(1),
            leader_id: NodeId::new(2),
            prev_log_index: LogIndex::new(0),
            prev_log_term: Term::new(0),
            entries: vec![
                LogEntry {
                    term: Term::new(1),
                    command: bincode::serialize(&bytes::Bytes::from(
                        &b"cmd1"[..],
                    ))
                    .unwrap()
                    .into(),
                },
                LogEntry {
                    term: Term::new(1),
                    command: bincode::serialize(&bytes::Bytes::from(
                        &b"cmd2"[..],
                    ))
                    .unwrap()
                    .into(),
                },
            ],
            leader_commit: LogIndex::new(0),
        };

        let response = handle_append_entries(&req, state.clone(), None).await?;
        assert!(response.success);

        let final_state = state.lock().await;
        assert_eq!(final_state.persistent.log.len(), 2);
        assert_eq!(final_state.persistent.log[0].term, Term::new(1));
        assert_eq!(final_state.persistent.log[0].command.as_ref(), b"cmd1");
        assert_eq!(final_state.persistent.log[1].term, Term::new(1));
        assert_eq!(final_state.persistent.log[1].command.as_ref(), b"cmd2");

        Ok(())
    }

    #[tokio::test]
    async fn test_append_entries_partial_match_overwrites_from_conflict_point()
    -> anyhow::Result<()> {
        let mut follower_state = RaftState::new(
            1u32,
            create_test_storage(),
            create_test_state_machine(),
        );
        follower_state.persistent.current_term = Term::new(3);
        follower_state.persistent.log.push(raft::Entry {
            term: Term::new(1),
            command: bytes::Bytes::from(&b"cmd1"[..]),
        });
        follower_state.persistent.log.push(raft::Entry {
            term: Term::new(1),
            command: bytes::Bytes::from(&b"cmd2"[..]),
        });
        follower_state.persistent.log.push(raft::Entry {
            term: Term::new(2),
            command: bytes::Bytes::from(&b"cmd3"[..]),
        });
        follower_state.persistent.log.push(raft::Entry {
            term: Term::new(2),
            command: bytes::Bytes::from(&b"cmd4_old"[..]),
        });
        follower_state.persistent.log.push(raft::Entry {
            term: Term::new(2),
            command: bytes::Bytes::from(&b"cmd5_old"[..]),
        });
        let state = Arc::new(Mutex::new(follower_state));

        let req = AppendEntriesRequest {
            term: Term::new(3),
            leader_id: NodeId::new(2),
            prev_log_index: LogIndex::new(3),
            prev_log_term: Term::new(2),
            entries: vec![
                LogEntry {
                    term: Term::new(3),
                    command: bincode::serialize(&bytes::Bytes::from(
                        &b"cmd4_new"[..],
                    ))
                    .unwrap()
                    .into(),
                },
                LogEntry {
                    term: Term::new(3),
                    command: bincode::serialize(&bytes::Bytes::from(
                        &b"cmd5_new"[..],
                    ))
                    .unwrap()
                    .into(),
                },
            ],
            leader_commit: LogIndex::new(0),
        };

        let response = handle_append_entries(&req, state.clone(), None).await?;
        assert!(response.success);

        let final_state = state.lock().await;
        assert_eq!(final_state.persistent.log.len(), 5);
        assert_eq!(final_state.persistent.log[0].term, Term::new(1));
        assert_eq!(final_state.persistent.log[0].command.as_ref(), b"cmd1");
        assert_eq!(final_state.persistent.log[1].term, Term::new(1));
        assert_eq!(final_state.persistent.log[1].command.as_ref(), b"cmd2");
        assert_eq!(final_state.persistent.log[2].term, Term::new(2));
        assert_eq!(final_state.persistent.log[2].command.as_ref(), b"cmd3");
        assert_eq!(final_state.persistent.log[3].term, Term::new(3));
        assert_eq!(final_state.persistent.log[3].command.as_ref(), b"cmd4_new");
        assert_eq!(final_state.persistent.log[4].term, Term::new(3));
        assert_eq!(final_state.persistent.log[4].command.as_ref(), b"cmd5_new");

        Ok(())
    }

    #[tokio::test]
    async fn test_append_entries_rejection_does_not_change_commit_index()
    -> anyhow::Result<()> {
        let mut follower_state = RaftState::new(
            1u32,
            create_test_storage(),
            create_test_state_machine(),
        );
        follower_state.persistent.current_term = Term::new(3);
        follower_state.persistent.log.push(raft::Entry {
            term: Term::new(1),
            command: bytes::Bytes::from(&b"cmd1"[..]),
        });
        follower_state.persistent.log.push(raft::Entry {
            term: Term::new(2),
            command: bytes::Bytes::from(&b"cmd2"[..]),
        });
        follower_state.commit_index = LogIndex::new(1);
        follower_state.last_applied = LogIndex::new(1);
        let state = Arc::new(Mutex::new(follower_state));

        let req = AppendEntriesRequest {
            term: Term::new(3),
            leader_id: NodeId::new(2),
            prev_log_index: LogIndex::new(2),
            prev_log_term: Term::new(1),
            entries: vec![],
            leader_commit: LogIndex::new(5),
        };

        let response = handle_append_entries(&req, state.clone(), None).await?;
        assert!(!response.success);
        assert_eq!(response.term, Term::new(3));

        let final_state = state.lock().await;
        assert_eq!(final_state.persistent.log.len(), 2);
        assert_eq!(final_state.commit_index, LogIndex::new(1));
        assert_eq!(final_state.last_applied, LogIndex::new(1));

        Ok(())
    }
}
