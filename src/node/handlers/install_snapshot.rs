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
use std::sync::Arc;
use tokio::sync::Mutex;

pub async fn handle_install_snapshot<T, SM>(
    req: &InstallSnapshotRequest,
    state: Arc<Mutex<RaftState<T, SM>>>,
) -> anyhow::Result<InstallSnapshotResponse>
where
    T: Send + Sync + Clone + serde::Serialize + serde::de::DeserializeOwned,
    SM: StateMachine<Command = T>,
{
    let mut state = state.lock().await;

    // Step 1: Reply immediately if term < currentTerm
    if req.term < state.persistent.current_term {
        return Ok(InstallSnapshotResponse {
            term: state.persistent.current_term,
        });
    }

    // Update term if necessary (not explicitly in Figure 13, but required by Raft semantics)
    if req.term > state.persistent.current_term {
        state.become_follower(req.term, Some(req.leader_id));
        let _ = state.persist().await;
    }

    // Steps 2-5: Apply snapshot to state machine and handle log
    state
        .restore_from_snapshot_data(
            req.last_included_index,
            req.last_included_term,
            &req.data,
        )
        .await?;

    Ok(InstallSnapshotResponse {
        term: state.persistent.current_term,
    })
}
