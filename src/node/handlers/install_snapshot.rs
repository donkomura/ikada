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

    use crate::core::term::{TermAction, validate_leader_rpc_term};

    let is_candidate_or_leader =
        state.role().is_candidate() || state.role().is_leader();

    match validate_leader_rpc_term(
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::RaftState;
    use crate::rpc::InstallSnapshotRequest;
    use crate::types::{LogIndex, NodeId, Term};

    fn create_test_storage<T: Send + Sync + Clone + 'static>()
    -> Box<dyn crate::storage::Storage<T>> {
        Box::new(crate::storage::MemStorage::default())
    }

    fn create_test_state_machine() -> crate::statemachine::NoOpStateMachine {
        crate::statemachine::NoOpStateMachine::default()
    }

    #[tokio::test]
    async fn install_snapshot_higher_term_causes_leader_to_step_down()
    -> anyhow::Result<()> {
        let mut initial_state = RaftState::new(
            1u32,
            create_test_storage(),
            create_test_state_machine(),
        );
        initial_state.persistent.current_term = Term::new(4);
        initial_state.become_candidate();
        initial_state.become_leader(&[]);
        let state = Arc::new(Mutex::new(initial_state));

        let req = InstallSnapshotRequest {
            term: Term::new(10),
            leader_id: NodeId::new(2),
            last_included_index: LogIndex::new(0),
            last_included_term: Term::new(0),
            data: vec![],
        };

        let response = handle_install_snapshot(&req, state.clone()).await?;

        assert_eq!(response.term, Term::new(10));
        let final_state = state.lock().await;
        assert!(final_state.role().is_follower());
        assert_eq!(final_state.persistent.current_term, Term::new(10));
        assert_eq!(final_state.leader_id, Some(NodeId::new(2)));
        assert_eq!(final_state.persistent.voted_for, None);

        Ok(())
    }

    #[tokio::test]
    async fn install_snapshot_same_term_causes_candidate_to_step_down()
    -> anyhow::Result<()> {
        let mut initial_state = RaftState::new(
            1u32,
            create_test_storage(),
            create_test_state_machine(),
        );
        initial_state.persistent.current_term = Term::new(4);
        initial_state.become_candidate();
        let state = Arc::new(Mutex::new(initial_state));

        let req = InstallSnapshotRequest {
            term: Term::new(5),
            leader_id: NodeId::new(3),
            last_included_index: LogIndex::new(0),
            last_included_term: Term::new(0),
            data: vec![],
        };

        let response = handle_install_snapshot(&req, state.clone()).await?;

        assert_eq!(response.term, Term::new(5));
        let final_state = state.lock().await;
        assert!(final_state.role().is_follower());
        assert_eq!(final_state.persistent.current_term, Term::new(5));
        assert_eq!(final_state.leader_id, Some(NodeId::new(3)));

        Ok(())
    }

    #[tokio::test]
    async fn install_snapshot_older_term_is_rejected() -> anyhow::Result<()> {
        let mut initial_state = RaftState::new(
            1u32,
            create_test_storage(),
            create_test_state_machine(),
        );
        initial_state.persistent.current_term = Term::new(10);
        let state = Arc::new(Mutex::new(initial_state));

        let req = InstallSnapshotRequest {
            term: Term::new(5),
            leader_id: NodeId::new(2),
            last_included_index: LogIndex::new(0),
            last_included_term: Term::new(0),
            data: vec![],
        };

        let response = handle_install_snapshot(&req, state.clone()).await?;

        assert_eq!(response.term, Term::new(10));
        let final_state = state.lock().await;
        assert_eq!(final_state.persistent.current_term, Term::new(10));

        Ok(())
    }
}
