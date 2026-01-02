use crate::raft::{RaftState, Role};
use crate::rpc::*;
use crate::statemachine::StateMachine;
use std::sync::Arc;
use tokio::sync::Mutex;

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft;

    fn create_test_storage<T: Send + Sync + Clone + 'static>()
    -> Box<dyn crate::storage::Storage<T>> {
        Box::new(crate::storage::MemStorage::default())
    }

    fn create_test_state_machine() -> crate::statemachine::NoOpStateMachine {
        crate::statemachine::NoOpStateMachine::default()
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
}
