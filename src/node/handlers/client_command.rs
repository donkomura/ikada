use crate::raft::RaftState;
use crate::rpc::*;
use crate::statemachine::StateMachine;
use std::sync::Arc;
use tokio::sync::Mutex;

pub(crate) fn validate_leadership<T, SM>(
    state: &RaftState<T, SM>,
) -> Result<(), CommandResponse>
where
    T: Send + Sync + Clone,
    SM: StateMachine<Command = T>,
{
    if !state.role.is_leader() {
        return Err(CommandResponse {
            success: false,
            leader_hint: state.leader_id,
            data: None,
            error: Some(crate::rpc::CommandError::NotLeader),
        });
    }

    Ok(())
}

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
    let timeout_deadline = tokio::time::sleep(timeout);
    tokio::pin!(timeout_deadline);

    let notifier = {
        let state_guard = state.lock().await;
        state_guard.replication_notifier.clone()
    };

    loop {
        if start.elapsed() > timeout {
            return Err(CommandResponse {
                success: false,
                leader_hint: None,
                data: None,
                error: Some(crate::rpc::CommandError::ReplicationFailed),
            });
        }

        let is_replicated = {
            let state_guard = state.lock().await;

            if !state_guard.role.is_leader() {
                return Err(CommandResponse {
                    success: false,
                    leader_hint: state_guard.leader_id,
                    data: None,
                    error: Some(crate::rpc::CommandError::NotLeader),
                });
            }

            let total_nodes = peer_count + 1;
            let majority = total_nodes / 2;
            let mut count = 1;

            if let Some(leader_state) = state_guard.role.leader_state() {
                for match_idx in leader_state.match_index.values() {
                    if *match_idx >= log_index {
                        count += 1;
                    }
                }
            }

            count > majority
        };

        if is_replicated {
            return Ok(());
        }

        tokio::select! {
            _ = &mut timeout_deadline => {
                return Err(CommandResponse {
                    success: false,
                    leader_hint: None,
                    data: None,
                    error: Some(crate::rpc::CommandError::ReplicationFailed),
                });
            }
            _ = notifier.notified() => {
                continue;
            }
        }
    }
}

async fn append_command_to_log<T, SM>(
    state: &mut RaftState<T, SM>,
    command: T,
    request_tracker: Arc<
        Mutex<crate::request_tracker::RequestTracker<SM::Response>>,
    >,
    tracker_timeout: std::time::Duration,
) -> Result<(u32, tokio::sync::oneshot::Receiver<SM::Response>), CommandResponse>
where
    T: Send + Sync + Clone,
    SM: StateMachine<Command = T>,
{
    let term = state.persistent.current_term;
    let log_index = state.persistent.log.len() as u32 + 1;

    state.persistent.log.push(crate::raft::Entry {
        term,
        command: command.clone(),
    });

    if let Err(e) = state.persist().await {
        return Err(CommandResponse {
            success: false,
            leader_hint: None,
            data: None,
            error: Some(crate::rpc::CommandError::Other(format!(
                "Failed to persist log: {}",
                e
            ))),
        });
    }

    let (result_tx, result_rx) = tokio::sync::oneshot::channel();
    request_tracker.lock().await.track_write(
        log_index,
        result_tx,
        std::time::Instant::now() + tracker_timeout,
    );

    tracing::debug!(
        id = state.id,
        log_index = log_index,
        term = term,
        "Leader appended command to log"
    );

    Ok((log_index, result_rx))
}

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
                error: Some(crate::rpc::CommandError::Other(format!(
                    "Failed to serialize response: {}",
                    e
                ))),
            },
        },
        Ok(Err(_)) => CommandResponse {
            success: false,
            leader_hint: None,
            data: None,
            error: Some(crate::rpc::CommandError::Other(
                "Response channel closed".to_string(),
            )),
        },
        Err(_) => CommandResponse {
            success: false,
            leader_hint: None,
            data: None,
            error: Some(crate::rpc::CommandError::Other(
                "Request timeout".to_string(),
            )),
        },
    }
}

/// Waits for a given log index to reach majority replication, then waits for the
/// state machine application result corresponding to that log index.
///
/// This is intended for leader-side batching where the caller already appended
/// to the log and registered the `RequestTracker` entry.
pub async fn wait_for_write_result<T, SM>(
    state: Arc<Mutex<RaftState<T, SM>>>,
    log_index: u32,
    peer_count: usize,
    timeout: std::time::Duration,
    result_rx: tokio::sync::oneshot::Receiver<SM::Response>,
) -> CommandResponse
where
    T: Send + Sync + Clone,
    SM: StateMachine<Command = T>,
    SM::Response: Clone + serde::Serialize,
{
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

    wait_for_command_result::<SM>(result_rx, timeout).await
}

#[tracing::instrument(skip(state, request_tracker, req))]
pub async fn handle_client_request_impl<T, SM>(
    req: &CommandRequest,
    state: Arc<Mutex<RaftState<T, SM>>>,
    timeout: std::time::Duration,
    peer_count: usize,
    request_tracker: Arc<
        Mutex<crate::request_tracker::RequestTracker<SM::Response>>,
    >,
) -> CommandResponse
where
    T: Send + Sync + Clone + serde::Serialize + serde::de::DeserializeOwned,
    SM: StateMachine<Command = T>,
    SM::Response: Clone + serde::Serialize,
{
    let (log_index, result_rx) = {
        let mut state_guard = state.lock().await;

        if let Err(response) = validate_leadership(&state_guard) {
            return response;
        }

        if state_guard.commit_index > state_guard.last_applied
            && let Err(e) = state_guard.apply_committed().await
        {
            return CommandResponse {
                success: false,
                leader_hint: None,
                data: None,
                error: Some(crate::rpc::CommandError::Other(format!(
                    "Failed to apply committed entries: {}",
                    e
                ))),
            };
        }

        let command: T = match bincode::deserialize(&req.command) {
            Ok(cmd) => cmd,
            Err(e) => {
                return CommandResponse {
                    success: false,
                    leader_hint: None,
                    data: None,
                    error: Some(crate::rpc::CommandError::Other(format!(
                        "Failed to deserialize command: {}",
                        e
                    ))),
                };
            }
        };

        match append_command_to_log(
            &mut state_guard,
            command.clone(),
            request_tracker.clone(),
            timeout,
        )
        .await
        {
            Ok(result) => result,
            Err(response) => return response,
        }
    };

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

    wait_for_command_result::<SM>(result_rx, timeout).await
}

pub async fn handle_read_index_request<T, SM>(
    req: &CommandRequest,
    state: Arc<Mutex<RaftState<T, SM>>>,
    timeout: std::time::Duration,
    peer_count: usize,
) -> CommandResponse
where
    T: Send + Sync + Clone + serde::Serialize + serde::de::DeserializeOwned,
    SM: StateMachine<Command = T>,
    SM::Response: Clone + serde::Serialize,
{
    let (read_index, current_term, _noop_committed) = {
        let state_guard = state.lock().await;

        if let Err(response) = validate_leadership(&state_guard) {
            return response;
        }

        let current_term = state_guard.persistent.current_term;
        let noop_index =
            state_guard.role.leader_state().and_then(|ls| ls.noop_index);
        let commit_index = state_guard.commit_index;

        let noop_committed = if let Some(noop_idx) = noop_index {
            commit_index >= noop_idx
        } else {
            false
        };

        if !noop_committed {
            return CommandResponse {
                success: false,
                leader_hint: None,
                data: None,
                error: Some(crate::rpc::CommandError::NoopNotCommitted),
            };
        }

        let read_index = noop_index
            .map(|n| n.max(commit_index))
            .unwrap_or(commit_index);

        (read_index, current_term, noop_committed)
    };

    if !verify_leadership_with_quorum(
        state.clone(),
        peer_count,
        timeout,
        current_term,
    )
    .await
    {
        return CommandResponse {
            success: false,
            leader_hint: None,
            data: None,
            error: Some(crate::rpc::CommandError::NotLeader),
        };
    }

    let start = std::time::Instant::now();
    let timeout_deadline = tokio::time::sleep(timeout);
    tokio::pin!(timeout_deadline);

    let apply_notifier = {
        let state_guard = state.lock().await;
        state_guard.last_applied_notifier.clone()
    };

    loop {
        if start.elapsed() > timeout {
            return CommandResponse {
                success: false,
                leader_hint: None,
                data: None,
                error: Some(crate::rpc::CommandError::Other(
                    "Read timeout waiting for state machine".to_string(),
                )),
            };
        }

        let should_wait = {
            let state_guard = state.lock().await;
            state_guard.last_applied < read_index
        };

        if !should_wait {
            break;
        }

        tokio::select! {
            _ = &mut timeout_deadline => {
                return CommandResponse {
                    success: false,
                    leader_hint: None,
                    data: None,
                    error: Some(crate::rpc::CommandError::Other("Read timeout waiting for state machine".to_string())),
                };
            }
            _ = apply_notifier.notified() => {
                continue;
            }
        }
    }

    let mut state_guard = state.lock().await;
    let command: T = match bincode::deserialize(&req.command) {
        Ok(cmd) => cmd,
        Err(e) => {
            return CommandResponse {
                success: false,
                leader_hint: None,
                data: None,
                error: Some(crate::rpc::CommandError::Other(format!(
                    "Deserialize error: {}",
                    e
                ))),
            };
        }
    };

    match state_guard.state_machine.apply(&command).await {
        Ok(result) => {
            let response_data = bincode::serialize(&result).ok();
            CommandResponse {
                success: true,
                leader_hint: None,
                data: response_data,
                error: None,
            }
        }
        Err(e) => CommandResponse {
            success: false,
            leader_hint: None,
            data: None,
            error: Some(crate::rpc::CommandError::Other(format!(
                "State machine error: {}",
                e
            ))),
        },
    }
}

async fn verify_leadership_with_quorum<T, SM>(
    state: Arc<Mutex<RaftState<T, SM>>>,
    peer_count: usize,
    timeout: std::time::Duration,
    expected_term: u32,
) -> bool
where
    T: Send + Sync + Clone,
    SM: StateMachine<Command = T>,
{
    let start = std::time::Instant::now();
    let majority = peer_count.div_ceil(2) + 1;
    let timeout_deadline = tokio::time::sleep(timeout);
    tokio::pin!(timeout_deadline);

    let notifier = {
        let state_guard = state.lock().await;
        state_guard.replication_notifier.clone()
    };

    loop {
        if start.elapsed() > timeout {
            return false;
        }

        let (_current_term, confirmed_count) = {
            let state_guard = state.lock().await;

            if state_guard.persistent.current_term != expected_term {
                return false;
            }

            let confirmed =
                if let Some(leader_state) = state_guard.role.leader_state() {
                    leader_state
                        .match_index
                        .values()
                        .filter(|&&idx| idx > 0)
                        .count()
                        + 1
                } else {
                    1
                };

            (state_guard.persistent.current_term, confirmed)
        };

        if confirmed_count >= majority {
            return true;
        }

        tokio::select! {
            _ = &mut timeout_deadline => {
                return false;
            }
            _ = notifier.notified() => {
                continue;
            }
        }
    }
}
