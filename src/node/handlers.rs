//! RPC request handlers as pure functions.
//!
//! These functions process Raft RPCs independently of the Node structure,
//! taking Arc<Mutex<RaftState>> directly. This design allows:
//! - Easy unit testing without mocking the entire Node
//! - Clear separation between consensus logic and node lifecycle
//! - Potential reuse in different execution contexts (e.g., embedded scenarios)

use crate::raft::{self, RaftState, Role};
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

/// Validates the request term and steps down to follower if necessary.
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
        state.persistent.current_term = request_term;
        state.role = Role::Follower;
        state.persistent.voted_for = None;
        state.leader_id = Some(sender_id);
        if let Err(e) = state.persist().await {
            tracing::error!(id=?state.id, error=?e, "Failed to persist state after term update");
            return Err(AppendEntriesError::Rejected(AppendEntriesResponse {
                term: state.persistent.current_term,
                success: false,
            }));
        }
    } else if request_term == state.persistent.current_term {
        state.leader_id = Some(sender_id);
        if matches!(state.role, Role::Candidate | Role::Leader) {
            state.role = Role::Follower;
        }
    }

    Ok(())
}

/// Checks if the log at the given index matches the expected term.
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

/// Detects and resolves log conflicts by truncating conflicting entries.
/// Returns true if log was modified.
fn detect_and_truncate_conflicts<T, SM>(
    entries: &[LogEntry],
    start_index: u32,
    state: &mut RaftState<T, SM>,
    client_manager: Option<
        Arc<Mutex<crate::client_manager::ClientResponseManager<SM::Response>>>,
    >,
) -> bool
where
    T: Send + Sync + Clone,
    SM: StateMachine<Command = T>,
    SM::Response: 'static,
{
    let mut log_modified = false;
    for (i, entry) in entries.iter().enumerate() {
        let log_index = start_index + i as u32;

        if log_index <= state.get_last_log_idx() {
            let existing_term =
                state.persistent.log[(log_index - 1) as usize].term;

            if existing_term != entry.term {
                state.persistent.log.truncate((log_index - 1) as usize);

                if let Some(manager) = client_manager {
                    let manager_clone = Arc::clone(&manager);
                    tokio::spawn(async move {
                        manager_clone.lock().await.clear_from(log_index);
                    });
                }

                log_modified = true;
                tracing::info!(
                    id=?state.id,
                    conflict_index=log_index,
                    old_term=existing_term,
                    new_term=entry.term,
                    "Truncated log due to conflict"
                );
                break;
            }
        }
    }

    log_modified
}

/// Deserializes and appends new log entries that don't exist yet.
/// Returns true if log was modified.
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

/// Resolves conflicts, appends entries, and persists changes if needed.
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
    let conflict_modified = detect_and_truncate_conflicts(
        entries,
        start_index,
        state,
        client_manager,
    );
    let append_modified =
        deserialize_and_append(entries, start_index, sender_id, state)?;
    let log_modified = conflict_modified || append_modified;

    if log_modified && let Err(e) = state.persist().await {
        tracing::error!(id=?state.id, error=?e, "Failed to persist state after log modification");
        return Err(AppendEntriesError::Rejected(AppendEntriesResponse {
            term: state.persistent.current_term,
            success: false,
        }));
    }

    Ok(())
}

/// Advances commit index and applies newly committed entries.
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

/// Handles AppendEntries RPC from leader.
/// Returns success only if log consistency checks pass (Raft §5.3).
///
/// Raft Algorithm - Log Replication:
/// This function handles two scenarios:
/// - Initial replication (Steps 4-5): Follower appends new entries to local log (uncommitted)
/// - Commit propagation (Steps 10-11): Follower applies committed entries to state machine
///
/// Both can happen in the same RPC (entries + commit_index update)
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

/// Handles RequestVote RPC from candidate.
/// Grants vote only if log is at least as up-to-date (Raft §5.4).
///
/// Raft Algorithm - Leader Election Step 3:
/// Step 3: Node evaluates RequestVote and grants vote if:
///         - Candidate's term is at least as current
///         - Haven't voted for another candidate in this term
///         - Candidate's log is at least as up-to-date
pub async fn handle_request_vote<T, SM>(
    req: &RequestVoteRequest,
    state: Arc<Mutex<RaftState<T, SM>>>,
) -> RequestVoteResponse
where
    T: Send + Sync + Clone,
    SM: StateMachine<Command = T>,
{
    let (current_term, vote_granted) = {
        let mut state = state.lock().await;

        if req.term > state.persistent.current_term {
            state.persistent.current_term = req.term;
            state.role = Role::Follower;
            state.persistent.voted_for = None;
            if let Err(e) = state.persist().await {
                tracing::error!(id=?state.id, error=?e, "Failed to persist state after term update in RequestVote");
                return RequestVoteResponse {
                    term: state.persistent.current_term,
                    vote_granted: false,
                };
            }
        }

        tracing::info!(id=?state.id, request.body=?req, "Command::RequestVote");

        let vote_granted = if req.term < state.persistent.current_term {
            tracing::warn!(
                id=?state.id,
                candidate_id=req.candidate_id,
                req_term=req.term,
                current_term=state.persistent.current_term,
                "RequestVote rejected: candidate term is older"
            );
            false
        } else if state.persistent.voted_for.is_some()
            && state.persistent.voted_for.unwrap() != req.candidate_id
        {
            tracing::warn!(
                id=?state.id,
                candidate_id=req.candidate_id,
                voted_for=?state.persistent.voted_for,
                "RequestVote rejected: already voted for another candidate"
            );
            false
        } else {
            let last_log_term = state.get_last_log_term();
            let last_log_idx = state.get_last_log_idx();

            let log_is_up_to_date = if req.last_log_term != last_log_term {
                req.last_log_term > last_log_term
            } else {
                req.last_log_index >= last_log_idx
            };

            if log_is_up_to_date {
                state.persistent.voted_for = Some(req.candidate_id);
                if let Err(e) = state.persist().await {
                    tracing::error!(id=?state.id, error=?e, "Failed to persist state after voting");
                    return RequestVoteResponse {
                        term: state.persistent.current_term,
                        vote_granted: false,
                    };
                }
                true
            } else {
                tracing::warn!(
                    id=?state.id,
                    candidate_id=req.candidate_id,
                    req_last_log_term=req.last_log_term,
                    req_last_log_index=req.last_log_index,
                    last_log_term=last_log_term,
                    last_log_idx=last_log_idx,
                    "RequestVote rejected: candidate's log is not up-to-date"
                );
                false
            }
        };

        (state.persistent.current_term, vote_granted)
    };

    RequestVoteResponse {
        term: current_term,
        vote_granted,
    }
}

pub(crate) fn validate_leadership<T, SM>(
    state: &RaftState<T, SM>,
) -> Result<(), CommandResponse>
where
    T: Send + Sync + Clone,
    SM: StateMachine<Command = T>,
{
    if !matches!(state.role, Role::Leader) {
        return Err(CommandResponse {
            success: false,
            leader_hint: state.leader_id,
            data: None,
            error: Some("Not the leader".to_string()),
        });
    }

    Ok(())
}

/// Waits for a log entry to be replicated to a majority of nodes.
///
/// Raft Algorithm - Log Replication Steps 6-7:
/// This function polls match_index to check if a majority of nodes have replicated the entry.
/// The actual replication happens asynchronously via periodic heartbeats.
async fn wait_for_majority_replication<T, SM>(
    state: Arc<Mutex<RaftState<T, SM>>>,
    log_index: u32,
    peer_count: usize,
    timeout: std::time::Duration,
) -> Result<(), CommandResponse>
where
    T: Send + Sync + Clone,
    SM: StateMachine<Command = T>,
{
    let start = std::time::Instant::now();
    let poll_interval = std::time::Duration::from_millis(10);

    loop {
        if start.elapsed() > timeout {
            return Err(CommandResponse {
                success: false,
                leader_hint: None,
                data: None,
                error: Some(
                    "Timeout waiting for majority replication".to_string(),
                ),
            });
        }

        let is_replicated = {
            let state_guard = state.lock().await;

            if !matches!(state_guard.role, Role::Leader) {
                return Err(CommandResponse {
                    success: false,
                    leader_hint: state_guard.leader_id,
                    data: None,
                    error: Some("No longer the leader".to_string()),
                });
            }

            let total_nodes = peer_count + 1;
            let majority = total_nodes / 2;
            let mut count = 1;

            for match_idx in state_guard.match_index.values() {
                if *match_idx >= log_index {
                    count += 1;
                }
            }

            count > majority
        };

        if is_replicated {
            return Ok(());
        }

        tokio::time::sleep(poll_interval).await;
    }
}

/// Appends a command to the leader's log.
///
/// Raft Algorithm - Log Replication Step 2:
/// Step 2: Leader receives the entry and appends it to its local log
///         (not yet committed to state machine)
async fn append_command_to_log<T, SM>(
    state: &mut RaftState<T, SM>,
    command: T,
    client_manager: Arc<
        Mutex<crate::client_manager::ClientResponseManager<SM::Response>>,
    >,
) -> Result<(u32, tokio::sync::oneshot::Receiver<SM::Response>), CommandResponse>
where
    T: Send + Sync + Clone,
    SM: StateMachine<Command = T>,
{
    let term = state.persistent.current_term;
    let log_index = state.persistent.log.len() as u32 + 1;

    // Append entry to log
    state.persistent.log.push(crate::raft::Entry {
        term,
        command: command.clone(),
    });

    // Persist the log entry
    if let Err(e) = state.persist().await {
        return Err(CommandResponse {
            success: false,
            leader_hint: None,
            data: None,
            error: Some(format!("Failed to persist log: {}", e)),
        });
    }

    // Register a response channel for this log entry
    let (result_tx, result_rx) = tokio::sync::oneshot::channel();
    client_manager.lock().await.register(log_index, result_tx);

    tracing::debug!(
        id = state.id,
        log_index = log_index,
        term = term,
        "Leader appended command to log"
    );

    Ok((log_index, result_rx))
}

/// Waits for the state machine to apply a command and return the result.
///
/// Raft Algorithm - Log Replication Step 9:
/// Step 9: Leader returns success response to client after entry is committed
///         and applied to state machine
async fn wait_for_command_result<SM>(
    result_rx: tokio::sync::oneshot::Receiver<SM::Response>,
    timeout: std::time::Duration,
) -> CommandResponse
where
    SM: StateMachine,
    SM::Response: Clone + serde::Serialize,
{
    match tokio::time::timeout(timeout, result_rx).await {
        Ok(Ok(response)) => match bincode::serialize(&response) {
            Ok(data) => CommandResponse {
                success: true,
                leader_hint: None,
                data: Some(data),
                error: None,
            },
            Err(e) => CommandResponse {
                success: false,
                leader_hint: None,
                data: None,
                error: Some(format!("Failed to serialize response: {}", e)),
            },
        },
        Ok(Err(_)) => CommandResponse {
            success: false,
            leader_hint: None,
            data: None,
            error: Some("Response channel closed".to_string()),
        },
        Err(_) => CommandResponse {
            success: false,
            leader_hint: None,
            data: None,
            error: Some("Request timeout".to_string()),
        },
    }
}

/// Handles client command submission (internal implementation).
/// This function coordinates the client request through the Raft consensus process.
///
/// Raft Algorithm - Log Replication Steps 1-9 (coordination):
/// This function orchestrates Steps 1-9 but delegates execution to other functions:
///
/// Step 1: Client sends a write entry (handled by caller - run_leader receives request)
/// Step 2: Leader appends entry to local log (append_command_to_log)
/// Step 3-7: Wait for majority replication (wait_for_majority_replication polls match_index)
///          - Actual Steps 3-7 execute asynchronously in leader loop via broadcast_heartbeat
/// Step 8: Leader commits after majority replication (handled in run_leader loop)
/// Step 9: Return success response to client (wait_for_command_result)
pub async fn handle_client_request_impl<T, SM>(
    req: &CommandRequest,
    state: Arc<Mutex<RaftState<T, SM>>>,
    timeout: std::time::Duration,
    peer_count: usize,
    client_manager: Arc<
        Mutex<crate::client_manager::ClientResponseManager<SM::Response>>,
    >,
) -> CommandResponse
where
    T: Send + Sync + Clone + serde::Serialize + serde::de::DeserializeOwned,
    SM: StateMachine<Command = T>,
    SM::Response: Clone + serde::Serialize,
{
    let (log_index, result_rx) = {
        let mut state_guard = state.lock().await;

        // Validate that this node is the leader
        if let Err(response) = validate_leadership(&state_guard) {
            return response;
        }

        // Apply any previously committed entries before processing new request
        if state_guard.commit_index > state_guard.last_applied
            && let Err(e) = state_guard.apply_committed().await
        {
            return CommandResponse {
                success: false,
                leader_hint: None,
                data: None,
                error: Some(format!(
                    "Failed to apply committed entries: {}",
                    e
                )),
            };
        }

        let command: T = match bincode::deserialize(&req.command) {
            Ok(cmd) => cmd,
            Err(e) => {
                return CommandResponse {
                    success: false,
                    leader_hint: None,
                    data: None,
                    error: Some(format!(
                        "Failed to deserialize command: {}",
                        e
                    )),
                };
            }
        };

        // Step 2: Leader appends entry to its local log (not yet committed to state machine)
        match append_command_to_log(
            &mut state_guard,
            command.clone(),
            client_manager.clone(),
        )
        .await
        {
            Ok(result) => result,
            Err(response) => return response,
        }
    };

    // Step 3-7: Wait for majority replication
    // The periodic heartbeat loop replicates entries asynchronously
    // We poll match_index to detect when majority replication is achieved
    if let Err(response) = wait_for_majority_replication(
        state.clone(),
        log_index,
        peer_count,
        timeout,
    )
    .await
    {
        return response;
    }

    // Step 8 (partial): Entry has been replicated to majority, waiting for state machine application
    // Step 9: Return success response to client (after state machine applies the entry)
    wait_for_command_result::<SM>(result_rx, timeout).await
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
        initial_state.role = Role::Leader;
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
        assert_eq!(final_state.role, Role::Follower);
        assert_eq!(final_state.persistent.voted_for, None);
        assert_eq!(final_state.leader_id, Some(99999));

        Ok(())
    }

    #[tokio::test]
    async fn test_request_vote_with_higher_term_converts_to_follower()
    -> anyhow::Result<()> {
        let mut initial_state = RaftState::new(
            1,
            create_test_storage(),
            create_test_state_machine(),
        );
        initial_state.persistent.current_term = 50;
        initial_state.role = Role::Leader;
        let state = Arc::new(Mutex::new(initial_state));

        let req = RequestVoteRequest {
            term: 100,
            candidate_id: 99999,
            last_log_index: 0,
            last_log_term: 0,
        };

        let _response = handle_request_vote(&req, state.clone()).await;

        // termが更新され、followerに転向しているべき
        let final_state = state.lock().await;
        assert_eq!(final_state.persistent.current_term, 100);
        assert_eq!(final_state.role, Role::Follower);

        Ok(())
    }

    #[tokio::test]
    async fn test_request_vote_rejected_by_older_log_term() -> anyhow::Result<()>
    {
        let mut initial_state = RaftState::new(
            1,
            create_test_storage(),
            create_test_state_machine(),
        );
        initial_state.persistent.current_term = 10;
        initial_state.persistent.log.push(raft::Entry {
            term: 5,
            command: bytes::Bytes::new(),
        });
        let state = Arc::new(Mutex::new(initial_state));

        // a request which have a old term log
        let req = RequestVoteRequest {
            term: 10,
            candidate_id: 2,
            last_log_index: 1,
            last_log_term: 3,
        };

        let response = handle_request_vote(&req, state.clone()).await;

        assert!(!response.vote_granted);
        assert_eq!(response.term, 10);

        let final_state = state.lock().await;
        assert_eq!(final_state.persistent.voted_for, None);

        Ok(())
    }

    #[tokio::test]
    async fn test_request_vote_log_index_comparison_with_same_term()
    -> anyhow::Result<()> {
        // ケース1: 同じterm、同じindex → 投票される
        let mut state1 = RaftState::new(
            1,
            create_test_storage(),
            create_test_state_machine(),
        );
        state1.persistent.current_term = 10;
        state1.persistent.log.push(raft::Entry {
            term: 5,
            command: bytes::Bytes::new(),
        });
        state1.persistent.log.push(raft::Entry {
            term: 5,
            command: bytes::Bytes::new(),
        });
        state1.persistent.log.push(raft::Entry {
            term: 5,
            command: bytes::Bytes::new(),
        });
        let state1 = Arc::new(Mutex::new(state1));
        let req1 = RequestVoteRequest {
            term: 10,
            candidate_id: 2,
            last_log_index: 3,
            last_log_term: 5,
        };
        let response1 = handle_request_vote(&req1, state1.clone()).await;
        assert!(
            response1.vote_granted,
            "Same term and same index should grant vote"
        );
        assert_eq!(state1.lock().await.persistent.voted_for, Some(2));

        // ケース2: 同じterm、候補者のindexが長い → 投票される
        let mut state2 = RaftState::new(
            1,
            create_test_storage(),
            create_test_state_machine(),
        );
        state2.persistent.current_term = 10;
        state2.persistent.log.push(raft::Entry {
            term: 5,
            command: bytes::Bytes::new(),
        });
        state2.persistent.log.push(raft::Entry {
            term: 5,
            command: bytes::Bytes::new(),
        });
        state2.persistent.log.push(raft::Entry {
            term: 5,
            command: bytes::Bytes::new(),
        });
        let state2 = Arc::new(Mutex::new(state2));
        let req2 = RequestVoteRequest {
            term: 10,
            candidate_id: 3,
            last_log_index: 5,
            last_log_term: 5,
        };
        let response2 = handle_request_vote(&req2, state2.clone()).await;
        assert!(
            response2.vote_granted,
            "Same term and longer index should grant vote"
        );
        assert_eq!(state2.lock().await.persistent.voted_for, Some(3));

        // ケース3: 同じterm、候補者のindexが短い → 拒否される
        let mut state3 = RaftState::new(
            1,
            create_test_storage(),
            create_test_state_machine(),
        );
        state3.persistent.current_term = 10;
        state3.persistent.log.push(raft::Entry {
            term: 5,
            command: bytes::Bytes::new(),
        });
        state3.persistent.log.push(raft::Entry {
            term: 5,
            command: bytes::Bytes::new(),
        });
        state3.persistent.log.push(raft::Entry {
            term: 5,
            command: bytes::Bytes::new(),
        });
        let state3 = Arc::new(Mutex::new(state3));
        let req3 = RequestVoteRequest {
            term: 10,
            candidate_id: 4,
            last_log_index: 2,
            last_log_term: 5,
        };
        let response3 = handle_request_vote(&req3, state3.clone()).await;
        assert!(
            !response3.vote_granted,
            "Same term and shorter index should reject vote"
        );
        assert_eq!(state3.lock().await.persistent.voted_for, None);

        Ok(())
    }

    #[tokio::test]
    async fn test_append_entries_conflict_resolution() -> anyhow::Result<()> {
        // フォロワーの初期ログ: [entry1(term=1), entry2(term=1), entry3(term=2), entry4(term=2)]
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

        // リーダーからのリクエスト: prev_log_term: 1, prev_log_index=2, entries=[entry3(term=3), entry4(term=3), entry5(term=3)]
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

        // レスポンスは成功
        assert!(response.success);
        assert_eq!(response.term, 3);

        // ログの検証
        let final_state = state.lock().await;
        assert_eq!(
            final_state.persistent.log.len(),
            5,
            "Log should have 5 entries"
        );

        // 最初の2つは変更なし
        assert_eq!(final_state.persistent.log[0].term, 1);
        assert_eq!(final_state.persistent.log[0].command.as_ref(), b"cmd1");
        assert_eq!(final_state.persistent.log[1].term, 1);
        assert_eq!(final_state.persistent.log[1].command.as_ref(), b"cmd2");

        // 競合したエントリは新しいもので置き換えられている
        assert_eq!(final_state.persistent.log[2].term, 3);
        assert_eq!(final_state.persistent.log[2].command.as_ref(), b"cmd3_new");
        assert_eq!(final_state.persistent.log[3].term, 3);
        assert_eq!(final_state.persistent.log[3].command.as_ref(), b"cmd4_new");

        // 新しいエントリが追加されている
        assert_eq!(final_state.persistent.log[4].term, 3);
        assert_eq!(final_state.persistent.log[4].command.as_ref(), b"cmd5_new");

        Ok(())
    }

    #[tokio::test]
    async fn test_append_entries_no_conflict_append_only() -> anyhow::Result<()>
    {
        // フォロワーの初期ログ: [entry1(term=1), entry2(term=1)]
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

        // リーダーからのリクエスト: prev_log_term: 1, prev_log_index=2, entries=[entry3(term=2), entry4(term=2)]
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

        // レスポンスは成功
        assert!(response.success);

        // ログの検証: 新しいエントリが追加されている
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
        let (tx2, rx2) = tokio::sync::oneshot::channel();
        let (tx3, rx3) = tokio::sync::oneshot::channel();
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

        // Verify that pending responses 2 and 3 were cleared (channels should be dropped/closed)
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        assert!(
            rx2.await.is_err(),
            "Channel for log index 2 should be closed"
        );
        assert!(
            rx3.await.is_err(),
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
        candidate_state.role = Role::Candidate;
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
        assert_eq!(final_state.role, Role::Follower);
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
