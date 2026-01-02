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
    ) -> anyhow::Result<()>
    where
        T: Default,
    {
        self.setup(servers).await?;

        loop {
            let role = {
                let state = self.state.lock().await;
                state.role.role()
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
            if !self.state.lock().await.role.is_follower() {
                break;
            }
            tokio::select! {
                Some(_) = self.c.heartbeat_rx.recv() => {
                    watchdog.reset(timeout).await;
                }
                _ = watchdog.wait() => {
                    watchdog.reset(timeout).await;
                    {
                        let mut state = self.state.lock().await;
                        state.become_candidate();
                        state.persist().await?;
                    }
                    break;
                }
            }
        }
        Ok(())
    }

    /// Runs as candidate, starting elections periodically.
    /// Transitions to follower if a valid leader's heartbeat is received.
    pub async fn run_candidate(&mut self) -> anyhow::Result<()>
    where
        T: Default,
    {
        let timeout = self.config.election_timeout;
        let watchdog = WatchDog::default();

        // Start first election immediately
        self.start_election().await?;
        watchdog.reset(timeout).await;

        loop {
            if !self.state.lock().await.role.is_candidate() {
                break;
            }
            tokio::select! {
                Some((id, term)) = self.c.heartbeat_rx.recv() => {
                    let current_term = self.state.lock().await.persistent.current_term;
                    if term >= current_term {
                        // Accept heartbeat: requester is the new leader
                        {
                            let mut state = self.state.lock().await;
                            state.leader_id = Some(id);
                            state.become_follower(current_term, Some(id));
                        }
                        self.heartbeat_failure_count = 0;
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

        let heartbeat_timeout = self.config.heartbeat_interval;
        let batch_window = self.config.batch_window;
        let max_batch_size = self.config.max_batch_size;

        let heartbeat_watchdog = WatchDog::default();
        heartbeat_watchdog.reset(heartbeat_timeout).await;

        let mut pending_requests: Vec<(
            crate::rpc::CommandRequest,
            tokio::sync::oneshot::Sender<crate::rpc::CommandResponse>,
        )> = Vec::new();
        let mut batch_timer = tokio::time::interval(batch_window);
        batch_timer
            .set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            if !self.state.lock().await.role.is_leader() {
                break;
            }
            tokio::select! {
                Some((req, resp_tx)) = self.c.client_request_rx.recv() => {
                    pending_requests.push((req, resp_tx));

                    if pending_requests.len() >= max_batch_size {
                        self.flush_request_batch(&mut pending_requests).await?;
                    }
                },
                _ = batch_timer.tick(), if !pending_requests.is_empty() => {
                    self.flush_request_batch(&mut pending_requests).await?;
                },
                _ = heartbeat_watchdog.wait() => {
                    if !pending_requests.is_empty() {
                        self.flush_request_batch(&mut pending_requests).await?;
                    }

                    let has_majority = self.broadcast_heartbeat().await?;

                    if !has_majority {
                        self.heartbeat_failure_count += 1;
                        tracing::warn!(
                            failure_count = self.heartbeat_failure_count,
                            limit = self.config.heartbeat_failure_retry_limit,
                            "Failed to achieve majority in heartbeat"
                        );

                        if self.heartbeat_failure_count >= self.config.heartbeat_failure_retry_limit as usize {
                            tracing::warn!(
                                "Consecutive heartbeat failures exceeded limit, stepping down from leader"
                            );
                            {
                                let mut state = self.state.lock().await;
                                let current_term = state.persistent.current_term;
                                state.become_follower(current_term, None);
                            }
                            self.heartbeat_failure_count = 0;
                            break;
                        }
                    } else {
                        self.heartbeat_failure_count = 0;
                    }

                    self.update_commit_index().await?;
                    self.apply_committed_entries_on_leader().await?;
                    heartbeat_watchdog.reset(heartbeat_timeout).await;
                }
            }
        }
        Ok(())
    }

    /// Flushes accumulated requests by spawning independent handlers.
    ///
    /// Batching allows multiple log entries to be replicated together,
    /// reducing network overhead and improving throughput under load.
    async fn flush_request_batch(
        &mut self,
        requests: &mut Vec<(
            crate::rpc::CommandRequest,
            tokio::sync::oneshot::Sender<crate::rpc::CommandResponse>,
        )>,
    ) -> anyhow::Result<()>
    where
        SM::Response: Clone + serde::Serialize,
    {
        if requests.is_empty() {
            return Ok(());
        }

        let batch_size = requests.len();
        tracing::debug!("Flushing batch of {} requests", batch_size);

        let rpc_timeout = self.config.rpc_timeout;
        let peer_count = self.peers.len();

        for (req, resp_tx) in requests.drain(..) {
            let state = self.state.clone();
            let client_manager = self.client_manager.clone();

            tokio::spawn(async move {
                let resp = handlers::handle_client_request_impl(
                    &req,
                    state,
                    rpc_timeout + std::time::Duration::from_millis(100),
                    peer_count,
                    client_manager,
                )
                .await;

                let _ = resp_tx.send(resp);
            });
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
        {
            let mut state = node.state.lock().await;
            state.become_candidate();
            state.persist().await?;
        }
        node.run_candidate().await?;
        assert!(!node.state.lock().await.role.is_candidate());
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
        {
            let mut state = node.state.lock().await;
            state.become_candidate();
            state.persist().await?;
        }
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
