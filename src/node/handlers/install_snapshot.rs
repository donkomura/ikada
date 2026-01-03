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

    if req.term < state.persistent.current_term {
        return Ok(InstallSnapshotResponse {
            term: state.persistent.current_term,
        });
    }

    if req.term > state.persistent.current_term {
        state.become_follower(req.term, Some(req.leader_id));
        let _ = state.persist().await;
    }

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
