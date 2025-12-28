//! Role-based state machine loops.
//!
//! Implements the main Raft node lifecycle:
//! - Role-based dispatch (follower/candidate/leader)
//! - Election timeout and heartbeat handling
//! - Client command processing by leader

use super::{Node, handlers};
use crate::network::NetworkFactory;
use crate::raft::Role;
use crate::statemachine::StateMachine;
use crate::watchdog::WatchDog;
use std::net::SocketAddr;

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
    /// Main event loop that dispatches to role-specific handlers.
    /// Runs indefinitely until an error occurs.
    pub(super) async fn main(
        mut self,
        servers: Vec<SocketAddr>,
    ) -> anyhow::Result<()> {
        self.setup(servers).await?;

        loop {
            let role = {
                let state = self.state.lock().await;
                state.role
            };
            match role {
                Role::Follower => self.run_follower().await,
                Role::Candidate => self.run_candidate().await,
                Role::Leader => self.run_leader().await,
            }?
        }
    }

    /// Runs as follower, waiting for heartbeats from leader.
    /// Transitions to candidate on election timeout.
    ///
    /// Raft Algorithm Step 1: Election Timeout Detection
    /// - Followers wait for heartbeats (AppendEntries) from the leader
    /// - If no heartbeat is received within election_timeout, transition to candidate
    pub async fn run_follower(&mut self) -> anyhow::Result<()> {
        let timeout = self.config.election_timeout;
        let watchdog = WatchDog::default();

        // Wait and reset to avoid immediate timeout on first run
        watchdog.wait().await;
        watchdog.reset(timeout).await;

        loop {
            if !matches!(self.state.lock().await.role, Role::Follower) {
                break;
            }
            tokio::select! {
                Some(_) = self.c.heartbeat_rx.recv() => {
                    watchdog.reset(timeout).await;
                }
                _ = watchdog.wait() => {
                    watchdog.reset(timeout).await;
                    self.become_candidate().await?;
                    break;
                }
            }
        }
        Ok(())
    }

    /// Runs as candidate, starting elections periodically.
    /// Transitions to follower if a valid leader's heartbeat is received.
    pub async fn run_candidate(&mut self) -> anyhow::Result<()> {
        let timeout = self.config.election_timeout;
        let watchdog = WatchDog::default();
        loop {
            if !matches!(self.state.lock().await.role, Role::Candidate) {
                break;
            }
            tokio::select! {
                Some((id, term)) = self.c.heartbeat_rx.recv() => {
                    let current_term = self.state.lock().await.persistent.current_term;
                    if term >= current_term {
                        // Accept heartbeat: requester is the new leader
                        self.state.lock().await.leader_id = Some(id);
                        self.become_follower().await?;
                    }
                },
                _ = watchdog.wait() => {
                    watchdog.reset(timeout).await;
                    self.start_election().await?;
                }
            }
        }
        Ok(())
    }

    /// Runs as leader, processing client commands and sending periodic heartbeats.
    /// Applies committed entries to the state machine after each heartbeat round.
    ///
    /// Raft Algorithm - Log Replication Flow:
    /// This function coordinates steps 1-11 of the log replication process
    pub async fn run_leader(&mut self) -> anyhow::Result<()>
    where
        SM::Response: Clone + serde::Serialize,
    {
        let _id = self.state.lock().await.id;

        let timeout = self.config.heartbeat_interval;
        let watchdog = WatchDog::default();
        watchdog.reset(timeout).await;

        loop {
            if !matches!(self.state.lock().await.role, Role::Leader) {
                break;
            }
            tokio::select! {
                Some((req, resp_tx)) = self.c.client_request_rx.recv() => {
                    // Step 1: Client sends a write entry (data or command) to the leader
                    // Step 2-9 are handled asynchronously in handle_client_request_impl
                    // Process client request asynchronously within leader context
                    let heartbeat_term = *self.last_heartbeat_majority.lock().await;
                    let peer_count = self.peers.len();
                    let rpc_timeout = self.config.rpc_timeout;
                    let state = self.state.clone();

                    tokio::spawn(async move {
                        let resp = handlers::handle_client_request_impl(
                            &req,
                            state,
                            rpc_timeout + std::time::Duration::from_millis(100),
                            heartbeat_term,
                            peer_count,
                        )
                        .await;

                        let _ = resp_tx.send(resp);
                    });
                },
                _ = watchdog.wait() => {
                    // Step 3: Leader sends AppendEntries RPC to all followers in parallel
                    // (includes current commit_index for Step 10)
                    // Step 6-7: Receive Acks from followers
                    // (handled in broadcast_heartbeat -> handle_heartbeat)
                    self.broadcast_heartbeat().await?;

                    // Step 8 (Part 1): Leader calculates new commit_index based on majority Acks
                    self.update_commit_index().await?;

                    // Step 8 (Part 2): Leader applies committed entries to its own state machine
                    self.apply_committed_entries_on_leader().await?;

                    // Step 9: Success responses to clients are sent by handle_client_request_impl
                    // when entries are applied to state machine

                    // Step 10: Next heartbeat will propagate the updated commit_index to followers
                    // Step 11: Followers will apply committed entries when they receive Step 10
                    watchdog.reset(timeout).await;
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use std::time::Duration;

    fn create_test_state_machine() -> crate::statemachine::NoOpStateMachine {
        crate::statemachine::NoOpStateMachine::default()
    }

    fn create_test_node(
        config: Config,
    ) -> Node<
        bytes::Bytes,
        crate::statemachine::NoOpStateMachine,
        crate::network::mock::MockNetworkFactory,
    > {
        Node::new(
            10101,
            config,
            create_test_state_machine(),
            crate::network::mock::MockNetworkFactory::new(),
        )
    }

    #[tokio::test(start_paused = true)]
    async fn election_must_be_done_with_not_candidate() -> anyhow::Result<()> {
        let mut node = create_test_node(Config::default());
        node.become_candidate().await?;
        node.run_candidate().await?;
        assert_ne!(node.state.lock().await.role, Role::Candidate);
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn test_follower_becomes_candidate_after_election_timeout()
    -> anyhow::Result<()> {
        let config = Config {
            election_timeout: Duration::from_millis(2000),
            ..Default::default()
        };
        let mut node = create_test_node(config);
        let result = tokio::spawn(async move { node.run_follower().await });
        // election timeoutを超える時間（2100ms）進める
        tokio::time::advance(Duration::from_millis(2100)).await;
        // スケジューラに制御を渡してwatchdogのタイムアウトを発火させる
        tokio::task::yield_now().await;
        assert!(result.await.is_ok());
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn test_candidate_calls_start_election() -> anyhow::Result<()> {
        let config = Config {
            rpc_timeout: Duration::from_millis(500),
            ..Default::default()
        };
        let mut node = create_test_node(config);
        node.become_candidate().await?;
        let result = tokio::spawn(async move { node.run_candidate().await });
        // 100ms進めてstart_electionが呼ばれる時間を与える
        tokio::time::advance(Duration::from_millis(100)).await;
        // スケジューラに制御を渡してタスクを実行させる
        tokio::task::yield_now().await;
        assert!(result.await.is_ok());
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn test_follower_multiple_heartbeat_resets() -> anyhow::Result<()> {
        let config = Config {
            election_timeout: Duration::from_millis(1000),
            ..Default::default()
        };
        let mut node = create_test_node(config);

        let heartbeat_tx = node.c.heartbeat_tx.clone();
        let result = tokio::spawn(async move { node.run_follower().await });

        // 1回目: 800ms経過後にheartbeat
        tokio::time::advance(Duration::from_millis(800)).await;
        tokio::task::yield_now().await;
        assert!(!result.is_finished());

        heartbeat_tx.send((1, 1)).unwrap();
        tokio::task::yield_now().await;

        // 2回目: さらに800ms経過後にheartbeat（リセットから800ms）
        tokio::time::advance(Duration::from_millis(800)).await;
        tokio::task::yield_now().await;
        assert!(!result.is_finished());

        heartbeat_tx.send((1, 1)).unwrap();
        tokio::task::yield_now().await;

        // 3回目: さらに800ms経過後にheartbeat（リセットから800ms）
        tokio::time::advance(Duration::from_millis(800)).await;
        tokio::task::yield_now().await;
        assert!(!result.is_finished());

        heartbeat_tx.send((1, 1)).unwrap();
        tokio::task::yield_now().await;

        // heartbeat停止: 1100ms経過でタイムアウト
        tokio::time::advance(Duration::from_millis(1100)).await;
        tokio::task::yield_now().await;
        assert!(result.is_finished());

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn test_follower_heartbeat_prevents_timeout() -> anyhow::Result<()> {
        let config = Config {
            election_timeout: Duration::from_millis(1000),
            ..Default::default()
        };
        let mut node = create_test_node(config);

        let heartbeat_tx = node.c.heartbeat_tx.clone();
        let result = tokio::spawn(async move { node.run_follower().await });

        // 1回目: 800ms経過後にheartbeat
        tokio::time::advance(Duration::from_millis(800)).await;
        tokio::task::yield_now().await;
        assert!(!result.is_finished());

        heartbeat_tx.send((1, 1)).unwrap();
        tokio::task::yield_now().await;

        // 2回目: さらに800ms経過後にheartbeat（リセットから800ms）
        tokio::time::advance(Duration::from_millis(800)).await;
        tokio::task::yield_now().await;
        assert!(!result.is_finished());

        heartbeat_tx.send((1, 1)).unwrap();
        tokio::task::yield_now().await;

        // heartbeat停止: 1100ms経過でタイムアウト
        tokio::time::advance(Duration::from_millis(1100)).await;
        tokio::task::yield_now().await;
        assert!(result.is_finished());

        Ok(())
    }
}
