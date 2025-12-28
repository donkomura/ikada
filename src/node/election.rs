//! Leader election logic.
//!
//! Implements Raft leader election (§5.2) including:
//! - Vote collection from peers
//! - Majority calculation
//! - State transitions (follower ↔ candidate ↔ leader)

use super::Node;
use crate::network::NetworkFactory;
use crate::raft::Role;
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
    pub(super) async fn start_election(&mut self) -> anyhow::Result<()> {
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
            self.become_follower().await?;
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
    ) -> anyhow::Result<()> {
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
            self.state.lock().await.persistent.current_term =
                new_terms.first().unwrap().term;
            self.become_follower().await?;
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

            self.become_leader().await?;
        }

        Ok(())
    }

    /// Transitions to leader and initializes next_index/match_index.
    ///
    /// Raft Algorithm - Leader Election Step 4 (continued):
    /// Step 4: Node transitions to leader role and initializes replication state for each follower
    pub(super) async fn become_leader(&mut self) -> anyhow::Result<()> {
        let mut state = self.state.lock().await;
        tracing::info!(id=?state.id, term=state.persistent.current_term, "BECOMING LEADER");
        state.role = Role::Leader;
        state.leader_id = Some(state.id);

        let last_log_idx = state.get_last_log_idx();
        for peer_addr in self.peers.keys() {
            state.next_index.insert(*peer_addr, last_log_idx + 1);
            state.match_index.insert(*peer_addr, 0);
        }

        self.heartbeat_failure_count = 0;
        Ok(())
    }

    /// Transitions to follower and clears vote.
    pub(super) async fn become_follower(&mut self) -> anyhow::Result<()> {
        let mut state = self.state.lock().await;
        state.role = Role::Follower;
        state.persistent.voted_for = None;
        state.leader_id = None;

        self.heartbeat_failure_count = 0;
        Ok(())
    }

    /// Transitions to candidate, increments term, and votes for self.
    ///
    /// Raft Algorithm - Leader Election Step 1:
    /// Step 1: Follower becomes candidate, increments term, and votes for itself
    pub(super) async fn become_candidate(&mut self) -> anyhow::Result<()> {
        let mut state = self.state.lock().await;
        state.role = Role::Candidate;
        state.persistent.current_term += 1;
        state.persistent.voted_for = Some(state.id);
        state.persist().await?;
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
        let mut node = create_test_node();
        node.become_leader().await?;
        let state = node.state.lock().await;
        assert_eq!(state.role, Role::Leader);
        assert_eq!(state.leader_id, Some(state.id));
        Ok(())
    }

    #[tokio::test]
    async fn check_leader_state_after_become_candidate() -> anyhow::Result<()> {
        let mut node = create_test_node();
        let term = node.state.lock().await.persistent.current_term;
        node.become_candidate().await?;
        let state = node.state.lock().await;
        assert_eq!(state.role, Role::Candidate);
        assert_eq!(state.persistent.current_term, term + 1);
        Ok(())
    }

    #[tokio::test]
    async fn check_leader_state_after_become_follower() -> anyhow::Result<()> {
        let mut node = create_test_node();
        node.become_follower().await?;
        let state = node.state.lock().await;
        assert_eq!(state.role, Role::Follower);
        Ok(())
    }
}
