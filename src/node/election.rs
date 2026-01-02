//! Leader election logic.
//!
//! Implements Raft leader election (§5.2) including:
//! - Vote collection from peers
//! - Majority calculation
//! - State transitions (follower ↔ candidate ↔ leader)

use super::Node;
use crate::network::NetworkFactory;
use crate::rpc::*;
use crate::statemachine::StateMachine;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::task::JoinSet;
use tracing::Instrument;

impl<T, SM, NF> Node<T, SM, NF>
where
    T: Send
        + Sync
        + Clone
        + std::fmt::Debug
        + serde::Serialize
        + serde::de::DeserializeOwned
        + 'static,
    SM: StateMachine<Command = T> + std::fmt::Debug + 'static,
    NF: NetworkFactory + Clone + 'static,
{
    /// Initiates an election by requesting votes from all peers.
    ///
    /// Raft Algorithm - Leader Election Step 2:
    /// Step 2: Candidate sends RequestVote RPC to all other nodes in parallel
    pub(super) async fn start_election(&mut self) -> anyhow::Result<()>
    where
        T: Default,
    {
        let mut responses: Vec<RequestVoteResponse> = Vec::new();
        let (
            current_term,
            candidate_id,
            last_log_index,
            last_log_term,
            voted_for,
        ) = {
            let state = self.state.lock().await;
            tracing::info!(id=?state.id, "start_election");
            // Self-vote
            responses.push(RequestVoteResponse {
                term: state.persistent.current_term,
                vote_granted: true,
            });
            (
                state.persistent.current_term,
                state.id,
                state.get_last_log_idx(),
                state.get_last_log_term(),
                state.persistent.voted_for,
            )
        };

        let is_granted =
            { voted_for.is_none() || (voted_for.unwrap() == candidate_id) };

        if !is_granted {
            tracing::info!(
                id = candidate_id,
                "vote not granted, become follower"
            );
            {
                let mut state = self.state.lock().await;
                let current_term = state.persistent.current_term;
                state.become_follower(current_term, None);
            }
            self.heartbeat_failure_count = 0;
            return Ok(());
        }

        let peers: Vec<_> = self
            .peers
            .iter()
            .map(|(addr, client)| (*addr, client.clone()))
            .collect();
        let rpc_timeout = self.config.rpc_timeout;

        tracing::info!(
            candidate_id = candidate_id,
            peer_count = peers.len(),
            "sending request_vote to peers"
        );

        // TODO: Consider using gossip protocol for better scalability
        let mut tasks = JoinSet::new();
        for (addr, client) in peers {
            let req = RequestVoteRequest {
                term: current_term,
                candidate_id,
                last_log_index,
                last_log_term,
            };
            tasks.spawn(Self::send_request_vote(
                addr,
                client,
                req,
                rpc_timeout,
            ));
        }

        while let Some(result) = tasks.join_next().await {
            match result? {
                Ok(res) => {
                    responses.push(res);
                }
                Err(e) => {
                    tracing::warn!(
                        candidate_id = candidate_id,
                        error = ?e,
                        "Failed to send request vote"
                    );
                    continue;
                }
            }
        }

        self.handle_election(responses).await?;

        Ok(())
    }

    /// Processes election results and transitions to leader if majority achieved.
    ///
    /// Raft Algorithm - Leader Election Steps 3-4:
    /// Step 3: Each node evaluates the RequestVote and grants vote based on log up-to-dateness
    /// Step 4: Candidate becomes leader if it receives votes from a majority of nodes
    async fn handle_election(
        &mut self,
        responses: Vec<RequestVoteResponse>,
    ) -> anyhow::Result<()>
    where
        T: Default,
    {
        let (id, current_term) = {
            let state = self.state.lock().await;
            (state.id, state.persistent.current_term)
        };
        tracing::info!(id=id, responses=?responses, "election handled");

        let new_terms: Vec<_> =
            responses.iter().filter(|r| r.term > current_term).collect();
        if !new_terms.is_empty() {
            tracing::info!(
                id = &id,
                term = &current_term,
                "newer term was discovered"
            );
            {
                let mut state = self.state.lock().await;
                let new_term = new_terms.first().unwrap().term;
                state.persistent.current_term = new_term;
                state.become_follower(new_term, None);
            }
            self.heartbeat_failure_count = 0;
            return Ok(());
        }

        // Check for majority: (peers + self)
        let total_nodes = self.peers.len() + 1;
        let vote_granted = responses.iter().filter(|r| r.vote_granted).count()
            > total_nodes / 2;

        if vote_granted {
            tracing::info!(
                id = id,
                voted_count =
                    responses.iter().filter(|r| r.vote_granted).count(),
                "vote granted, become leader"
            );

            // Transition to leader role
            {
                let mut state = self.state.lock().await;
                tracing::info!(id=?state.id, term=state.persistent.current_term, "BECOMING LEADER");

                let peers: Vec<_> = self.peers.keys().copied().collect();
                state.become_leader(&peers);

                // Append no-op entry for ReadIndex optimization
                let current_term = state.persistent.current_term;
                let noop_entry = crate::raft::Entry {
                    term: current_term,
                    command: T::default(),
                };
                state.persistent.log.push(noop_entry);
                let noop_idx = state.get_last_log_idx();
                if let Some(leader_state) = state.role.leader_state_mut() {
                    leader_state.noop_index = Some(noop_idx);
                }
                state.persist().await?;

                tracing::info!(
                    id = ?state.id,
                    noop_index = noop_idx,
                    "Appended no-op entry for ReadIndex"
                );
            }

            self.heartbeat_failure_count = 0;
        }

        Ok(())
    }

    /// Sends RequestVote RPC to a single peer.
    async fn send_request_vote(
        peer_addr: SocketAddr,
        client: Arc<dyn RaftRpcTrait>,
        req: RequestVoteRequest,
        rpc_timeout: Duration,
    ) -> anyhow::Result<RequestVoteResponse> {
        let mut ctx = tarpc::context::current();
        ctx.deadline = Instant::now() + rpc_timeout;

        client
            .request_vote(ctx, req.clone())
            .instrument(tracing::info_span!(
                "request vote from candidate {}",
                req.candidate_id
            ))
            .await
            .map_err(|e| {
                tracing::warn!(
                    candidate_id = req.candidate_id,
                    peer_addr = ?peer_addr,
                    error = ?e,
                    "request_vote RPC failed to peer"
                );
                e
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;

    fn create_test_state_machine() -> crate::statemachine::NoOpStateMachine {
        crate::statemachine::NoOpStateMachine::default()
    }

    fn create_test_node() -> Node<
        bytes::Bytes,
        crate::statemachine::NoOpStateMachine,
        crate::network::mock::MockNetworkFactory,
    > {
        Node::new(
            10101,
            Config::default(),
            create_test_state_machine(),
            crate::network::mock::MockNetworkFactory::new(),
        )
    }

    #[tokio::test]
    async fn check_leader_state_after_become_leader() -> anyhow::Result<()> {
        let node = create_test_node();
        {
            let mut state = node.state.lock().await;
            let peers: Vec<_> = vec![];
            state.become_leader(&peers);
        }
        let state = node.state.lock().await;
        assert!(state.role.is_leader());
        assert_eq!(state.leader_id, Some(state.id));
        Ok(())
    }

    #[tokio::test]
    async fn check_leader_state_after_become_candidate() -> anyhow::Result<()> {
        let node = create_test_node();
        let term = node.state.lock().await.persistent.current_term;
        {
            let mut state = node.state.lock().await;
            state.become_candidate();
        }
        let state = node.state.lock().await;
        assert!(state.role.is_candidate());
        assert_eq!(state.persistent.current_term, term + 1);
        Ok(())
    }

    #[tokio::test]
    async fn check_leader_state_after_become_follower() -> anyhow::Result<()> {
        let node = create_test_node();
        {
            let mut state = node.state.lock().await;
            let current_term = state.persistent.current_term;
            state.become_follower(current_term, None);
        }
        let state = node.state.lock().await;
        assert!(state.role.is_follower());
        Ok(())
    }
}
