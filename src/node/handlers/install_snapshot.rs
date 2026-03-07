//! InstallSnapshot RPC handler
//!
//! Implements Raft Figure 13 - InstallSnapshot RPC Receiver implementation:
//! 1. Reply immediately if term < currentTerm
//! 2. Save snapshot file and discard any existing or partial snapshot with a smaller index
//! 3. If existing log entry has same index and term as snapshot's last included entry,
//!    retain log entries following it and reply
//! 4. Discard the entire log
//! 5. Reset state machine using snapshot contents (and load snapshot's cluster configuration)

use crate::raft::RaftState;
use crate::rpc::{InstallSnapshotRequest, InstallSnapshotResponse};
use crate::statemachine::StateMachine;
use anyhow::Context;
use std::sync::Arc;
use tokio::sync::Mutex;

#[tracing::instrument(skip(state), fields(term = %req.term, leader_id = %req.leader_id, last_included_index = %req.last_included_index, last_included_term = %req.last_included_term))]
pub async fn handle_install_snapshot<T, SM>(
    req: &InstallSnapshotRequest,
    state: Arc<Mutex<RaftState<T, SM>>>,
) -> anyhow::Result<InstallSnapshotResponse>
where
    T: Send + Sync + Clone + serde::Serialize + serde::de::DeserializeOwned,
    SM: StateMachine<Command = T>,
{
    let mut state = state.lock().await;

    use crate::core::term::{TermAction, validate_request_term};

    let is_candidate_or_leader =
        state.role.is_candidate() || state.role.is_leader();

    match validate_request_term(
        req.term,
        state.persistent.current_term,
        req.leader_id,
        is_candidate_or_leader,
    ) {
        TermAction::Reject => {
            return Ok(InstallSnapshotResponse {
                term: state.persistent.current_term,
            });
        }
        TermAction::StepDown {
            new_term,
            leader_id,
        } => {
            state.become_follower(new_term, leader_id);
            let _ = state.persist().await;
        }
        TermAction::Accept => {}
    }

    // Apply snapshot to state machine and handle log
    state
        .restore_from_snapshot_data(
            req.last_included_index,
            req.last_included_term,
            &req.data,
        )
        .await
        .with_context(|| {
            format!(
                "failed to install snapshot (last_included_index={}, last_included_term={})",
                req.last_included_index, req.last_included_term
            )
        })?;

    Ok(InstallSnapshotResponse {
        term: state.persistent.current_term,
    })
}
