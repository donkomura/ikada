//! Log replication and commit index management.
//!
//! Implements Raft log replication (§5.3) including:
//! - Heartbeat broadcasting to all followers
//! - Commit index advancement based on majority replication
//! - next_index/match_index tracking for each peer

use super::Node;
use crate::network::NetworkFactory;
use crate::raft::{self};
use crate::rpc::*;
use crate::statemachine::StateMachine;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::task::JoinSet;
use tracing::Instrument;

enum ReplicationResponse {
    AppendEntries(AppendEntriesResponse, u32),
    InstallSnapshot(InstallSnapshotResponse, u32),
}

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
    NF: NetworkFactory<T> + Clone + 'static,
{
    /// Broadcasts AppendEntries RPCs to all followers.
    ///
    /// This function also performs *per-follower pipelining*:
    /// if a follower is behind and has available inflight window capacity,
    /// it may receive multiple AppendEntries RPCs in a single call.
    ///
    /// We intentionally wait for all spawned RPC tasks to complete to avoid
    /// leaving inflight state stuck due to cancelled tasks.
    ///
    /// Raft Algorithm - Log Replication Step 3, 6-7:
    /// Step 3: Leader sends AppendEntries RPC to all followers in parallel
    /// Step 6-7: Receives Acks from followers
    #[tracing::instrument(skip(self), fields(node_id = self.state.try_lock().ok().map(|s| s.id)))]
    pub(super) async fn broadcast_heartbeat(&mut self) -> anyhow::Result<bool> {
        // Lock once and copy all needed data
        let (leader_term, leader_id, leader_commit, log_data, peer_plan) = {
            let state = self.state.lock().await;

            if !state.role.is_leader() {
                return Ok(false);
            }

            // Copy log and peer state data
            let log_entries = state.persistent.log.clone();
            let leader_state = state.role.leader_state().unwrap();
            let plan: Vec<(SocketAddr, u32, usize)> = self.peers
                .keys()
                .filter_map(|addr| {
                    let snapshot_inflight = leader_state
                        .inflight_snapshot
                        .get(addr)
                        .copied()
                        .unwrap_or(false);
                    if snapshot_inflight {
                        tracing::debug!(
                            peer = ?addr,
                            "Skipping peer with inflight InstallSnapshot"
                        );
                        return None;
                    }

                    let inflight_len = leader_state
                        .inflight_append
                        .get(addr)
                        .map(|q| q.len())
                        .unwrap_or(0);
                    let max_inflight = self.config.replication_max_inflight;
                    if inflight_len >= max_inflight {
                        tracing::debug!(
                            peer = ?addr,
                            inflight_len = inflight_len,
                            limit = max_inflight,
                            "Skipping peer: AppendEntries inflight window is full"
                        );
                        return None;
                    }

                    let base_next_idx = if inflight_len > 0 {
                        leader_state
                            .inflight_append
                            .get(addr)
                            .and_then(|q| q.back().copied())
                            .unwrap_or_else(|| {
                                leader_state
                                    .next_index
                                    .get(addr)
                                    .copied()
                                    .unwrap_or(1)
                            })
                            .saturating_add(1)
                    } else {
                        leader_state
                            .next_index
                            .get(addr)
                            .copied()
                            .unwrap_or(1)
                    };

                    let slots = max_inflight.saturating_sub(inflight_len);
                    Some((*addr, base_next_idx, slots))
                })
                .collect();

            (
                state.persistent.current_term,
                state.leader_id.unwrap(),
                state.commit_index,
                log_entries,
                plan,
            )
        };

        let total_nodes = self.peers.len() + 1;
        let majority = total_nodes / 2;

        let rpc_timeout = self.config.rpc_timeout;
        let mut tasks = JoinSet::new();

        // Step 3: Spawn AppendEntries/InstallSnapshot tasks, filling the inflight window per follower.
        for (addr, mut next_idx, slots) in peer_plan {
            let client = self.peers.get(&addr).unwrap().clone();

            for _ in 0..slots {
                if next_idx > 1 && (next_idx - 2) as usize >= log_data.len() {
                    tracing::debug!(
                        peer = ?addr,
                        next_idx = next_idx,
                        log_len = log_data.len(),
                        "next_idx points to compacted log, sending snapshot"
                    );

                    let state = self.state.lock().await;
                    let snapshot_result = state.get_snapshot().await;
                    drop(state);

                    match snapshot_result {
                        Ok(Some((metadata, data))) => {
                            let req = crate::rpc::InstallSnapshotRequest {
                                term: leader_term,
                                leader_id,
                                last_included_index: metadata
                                    .last_included_index,
                                last_included_term: metadata.last_included_term,
                                data,
                            };

                            {
                                let mut state = self.state.lock().await;
                                if let Some(leader_state) =
                                    state.role.leader_state_mut()
                                {
                                    leader_state
                                        .inflight_snapshot
                                        .insert(addr, true);
                                    tracing::debug!(
                                        peer = ?addr,
                                        last_included_index = metadata.last_included_index,
                                        "Marked InstallSnapshot as inflight"
                                    );
                                }
                            }

                            tasks.spawn(Self::send_install_snapshot(
                                addr,
                                client,
                                req,
                                rpc_timeout,
                            ));
                        }
                        Ok(None) => {
                            tracing::warn!(
                                peer = ?addr,
                                "No snapshot available to send"
                            );
                        }
                        Err(e) => {
                            tracing::error!(
                                peer = ?addr,
                                error = ?e,
                                "Failed to load snapshot"
                            );
                        }
                    }

                    // Snapshot transfer is exclusive; do not pipeline AppendEntries concurrently.
                    break;
                }

                let (prev_log_idx, prev_log_term) = if next_idx > 1 {
                    (next_idx - 1, log_data[(next_idx - 2) as usize].term)
                } else {
                    (0, 0)
                };

                let max_entries = self.config.replication_max_entries_per_rpc;
                let entries: Vec<LogEntry> = log_data
                    .iter()
                    .skip((next_idx - 1) as usize)
                    .take(max_entries)
                    .map(|e| {
                        let command =
                            bincode::serialize(&e.command).unwrap_or_default();
                        LogEntry {
                            term: e.term,
                            command: Arc::from(command.into_boxed_slice()),
                        }
                    })
                    .collect();

                let sent_up_to_index = prev_log_idx + entries.len() as u32;

                if !entries.is_empty() {
                    let mut state = self.state.lock().await;
                    if let Some(leader_state) = state.role.leader_state_mut() {
                        leader_state
                            .inflight_append
                            .entry(addr)
                            .or_default()
                            .push_back(sent_up_to_index);
                        let len = leader_state
                            .inflight_append
                            .get(&addr)
                            .map(|q| q.len())
                            .unwrap_or(0);
                        tracing::debug!(
                            peer = ?addr,
                            sent_up_to_index = sent_up_to_index,
                            inflight_len = len,
                            "Enqueued AppendEntries into inflight window"
                        );
                    }
                }

                let req = AppendEntriesRequest {
                    term: leader_term,
                    leader_id,
                    prev_log_index: prev_log_idx,
                    prev_log_term,
                    entries,
                    leader_commit,
                };

                tasks.spawn(
                    Self::send_heartbeat(
                        addr,
                        client.clone(),
                        req,
                        sent_up_to_index,
                        rpc_timeout,
                    )
                    .instrument(tracing::Span::current()),
                );

                // Nothing more to send for this follower in this call.
                if sent_up_to_index < next_idx {
                    break;
                }
                next_idx = sent_up_to_index.saturating_add(1);
                if sent_up_to_index == prev_log_idx {
                    // Heartbeat (no entries); don't spam more in this call.
                    break;
                }
            }
        }

        // Step 6-7: Process responses as they arrive.
        // For liveness, we count unique peers we could contact at least once.
        let mut contacted: HashSet<SocketAddr> = HashSet::new();

        while let Some(result) = tasks.join_next().await {
            match result {
                Ok(Ok((server, response))) => {
                    contacted.insert(server);
                    match response {
                        ReplicationResponse::AppendEntries(
                            res,
                            sent_up_to_index,
                        ) => {
                            self.handle_heartbeat(
                                server,
                                res,
                                sent_up_to_index,
                            )
                            .await?;
                        }
                        ReplicationResponse::InstallSnapshot(
                            res,
                            last_included_index,
                        ) => {
                            self.handle_install_snapshot_response(
                                server,
                                res,
                                last_included_index,
                            )
                            .await?;
                        }
                    }

                    let still_leader = {
                        let state = self.state.lock().await;
                        state.role.is_leader()
                    };

                    if !still_leader {
                        tracing::info!(
                            "Stepped down from leader, aborting broadcast"
                        );
                        return Ok(false);
                    }
                }
                Ok(Err(e)) => {
                    tracing::warn!("Failed to send replication RPC: {:?}", e);
                }
                Err(e) => {
                    tracing::warn!("Task join error: {:?}", e);
                }
            }
        }

        let has_majority = (contacted.len() + 1) > majority;
        Ok(has_majority)
    }

    /// Sends AppendEntries RPC to a single follower.
    async fn send_heartbeat(
        server: SocketAddr,
        client: Arc<dyn RaftRpcTrait>,
        req: AppendEntriesRequest,
        sent_up_to_index: u32,
        rpc_timeout: Duration,
    ) -> anyhow::Result<(SocketAddr, ReplicationResponse)> {
        use crate::trace::TraceContextInjector;

        let mut ctx = tarpc::context::Context::current().with_current_trace();
        ctx.deadline = Instant::now() + rpc_timeout;
        let res = client.append_entries(ctx, req.clone()).await?;
        Ok((
            server,
            ReplicationResponse::AppendEntries(res, sent_up_to_index),
        ))
    }

    /// Sends InstallSnapshot RPC to a single follower.
    async fn send_install_snapshot(
        server: SocketAddr,
        client: Arc<dyn RaftRpcTrait>,
        req: crate::rpc::InstallSnapshotRequest,
        rpc_timeout: Duration,
    ) -> anyhow::Result<(SocketAddr, ReplicationResponse)> {
        let mut ctx = tarpc::context::current();
        ctx.deadline = Instant::now() + rpc_timeout;
        let last_included_index = req.last_included_index;
        let res = client.install_snapshot(ctx, req).await?;
        Ok((
            server,
            ReplicationResponse::InstallSnapshot(res, last_included_index),
        ))
    }

    /// Calculates the new commit index based on match_index from peers.
    /// Returns the highest index replicated on a majority of nodes in the current term (Raft §5.4.2).
    fn new_commit_index(
        current_commit_index: u32,
        current_term: u32,
        log: &[raft::Entry<T>],
        match_index: &HashMap<SocketAddr, u32>,
        peer_count: usize,
    ) -> u32 {
        let log_len = log.len() as u32;
        let total_nodes = peer_count + 1;

        let mut new_commit_index = current_commit_index;
        for n in (current_commit_index + 1)..=log_len {
            if log[(n - 1) as usize].term != current_term {
                continue;
            }
            let mut count = 1; // Leader always has the entry
            for match_idx in match_index.values() {
                if *match_idx >= n {
                    count += 1;
                }
            }
            let majority = total_nodes / 2;
            if count > majority {
                new_commit_index = n;
            }
        }
        new_commit_index
    }

    /// Updates commit_index if a majority of nodes have replicated an entry.
    /// Only the leader can advance commit_index this way (Raft Figure 2).
    /// This is part of Step 8 (commit index calculation only, not state machine application).
    ///
    /// Raft Algorithm - Log Replication Step 8 (Part 1):
    /// Step 8 (Part 1): Calculate new commit_index based on majority replication
    #[tracing::instrument(skip(self), fields(node_id = self.state.try_lock().ok().map(|s| s.id)))]
    pub(super) async fn update_commit_index(&mut self) -> anyhow::Result<()> {
        let mut state = self.state.lock().await;

        // Only leaders can update commit_index this way
        if !state.role.is_leader() {
            return Ok(());
        }

        let current_commit_index = state.commit_index;
        let new_commit_index =
            if let Some(leader_state) = state.role.leader_state() {
                Self::new_commit_index(
                    state.commit_index,
                    state.persistent.current_term,
                    &state.persistent.log,
                    &leader_state.match_index,
                    self.peers.len(),
                )
            } else {
                state.commit_index
            };

        // Update commit_index if we found a higher value
        if new_commit_index > current_commit_index {
            state.commit_index = new_commit_index;
            tracing::info!(
                id = state.id,
                old_commit = current_commit_index,
                new_commit = new_commit_index,
                "Leader advanced commit_index"
            );
        }

        Ok(())
    }

    /// Applies committed entries to the leader's state machine.
    ///
    /// Raft Algorithm - Log Replication Step 8 (Part 2):
    /// Step 8 (Part 2): Leader applies committed entries to its own state machine
    #[tracing::instrument(skip(self), fields(node_id = self.state.try_lock().ok().map(|s| s.id)))]
    pub(super) async fn apply_committed_entries_on_leader(
        &mut self,
    ) -> anyhow::Result<()> {
        let mut state = self.state.lock().await;

        // Only leaders should call this
        if !state.role.is_leader() {
            return Ok(());
        }

        if state.commit_index > state.last_applied {
            state.apply_committed().await?;
        }

        Ok(())
    }

    /// Handles InstallSnapshot response from a follower.
    /// Updates next_index and match_index after successful snapshot installation.
    ///
    /// Following Raft semantics (not explicitly in Figure 13):
    /// - If response term is higher, step down to follower
    /// - Update match_index to last_included_index (follower is now caught up to this point)
    /// - Update next_index to last_included_index + 1 (next AppendEntries will send entries after snapshot)
    pub(super) async fn handle_install_snapshot_response(
        &mut self,
        addr: SocketAddr,
        res: InstallSnapshotResponse,
        last_included_index: u32,
    ) -> anyhow::Result<()> {
        // Check if we need to step down due to higher term
        let check_term = {
            let state = self.state.lock().await;
            res.term > state.persistent.current_term
        };

        if check_term {
            {
                let mut state = self.state.lock().await;
                state.become_follower(res.term, None);
                let _ = state.persist().await;
            }
            self.heartbeat_failure_count = 0;
            return Ok(());
        }

        // Update replication indices to reflect successful snapshot installation
        let notifier = {
            let mut state = self.state.lock().await;
            if let Some(leader_state) = state.role.leader_state_mut() {
                leader_state.match_index.insert(addr, last_included_index);
                leader_state
                    .next_index
                    .insert(addr, last_included_index + 1);
                leader_state.inflight_snapshot.insert(addr, false);
                if let Some(q) = leader_state.inflight_append.get_mut(&addr) {
                    q.clear();
                }
            }
            state.replication_notifier.clone()
        };

        // Notify any tasks waiting on replication progress
        // (e.g., commit index calculation, read index requests)
        notifier.notify_waiters();

        tracing::info!(
            peer = ?addr,
            last_included_index = last_included_index,
            "InstallSnapshot succeeded"
        );

        Ok(())
    }

    /// Handles AppendEntries response from a follower.
    /// Updates next_index and match_index based on success/failure (Raft §5.3).
    ///
    /// Raft Algorithm - Log Replication Steps 6-7:
    /// Step 6-7: Processes Ack (acknowledgment) from followers after they append entries
    pub(super) async fn handle_heartbeat(
        &mut self,
        addr: SocketAddr,
        res: AppendEntriesResponse,
        sent_up_to_index: u32,
    ) -> anyhow::Result<()> {
        let check_term = {
            let state = self.state.lock().await;
            res.term > state.persistent.current_term
        };

        if check_term {
            {
                let mut state = self.state.lock().await;
                state.become_follower(res.term, None);
                let _ = state.persist().await;
            }
            self.heartbeat_failure_count = 0;
            return Ok(());
        }

        let (_id, is_leader, current_term) = {
            let state = self.state.lock().await;
            (
                state.id,
                state.role.is_leader(),
                state.persistent.current_term,
            )
        };
        if is_leader && current_term != res.term {
            return Ok(());
        }

        // Step 6-7: Update replication state based on follower's response
        let notifier = {
            let mut state = self.state.lock().await;
            if let Some(leader_state) = state.role.leader_state_mut() {
                if res.success {
                    let current_match = leader_state
                        .match_index
                        .get(&addr)
                        .copied()
                        .unwrap_or(0);
                    let new_match = current_match.max(sent_up_to_index);
                    leader_state.match_index.insert(addr, new_match);
                    leader_state.next_index.insert(addr, new_match + 1);

                    if let Some(q) = leader_state.inflight_append.get_mut(&addr)
                    {
                        let before = q.len();
                        while let Some(front) = q.front().copied() {
                            if front <= new_match {
                                q.pop_front();
                            } else {
                                break;
                            }
                        }
                        let after = q.len();
                        if before != after {
                            tracing::debug!(
                                peer = ?addr,
                                removed = before - after,
                                inflight_len = after,
                                match_index = new_match,
                                "Consumed inflight AppendEntries on success"
                            );
                        }
                    }
                } else {
                    if let Some(q) = leader_state.inflight_append.get_mut(&addr)
                    {
                        let cleared = q.len();
                        q.clear();
                        if cleared > 0 {
                            tracing::debug!(
                                peer = ?addr,
                                cleared = cleared,
                                "Cleared inflight window due to rejection"
                            );
                        }
                    }
                    let current_next_idx = leader_state
                        .next_index
                        .get(&addr)
                        .copied()
                        .unwrap_or(1);
                    let new_next_idx =
                        current_next_idx.saturating_sub(1).max(1);
                    leader_state.next_index.insert(addr, new_next_idx);
                }
            }
            state.replication_notifier.clone()
        };

        notifier.notify_waiters();

        // Note: Step 8 (commit_index update and state machine application) is now
        // performed once after all Acks are collected in run_leader(), not here

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::raft;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    fn create_test_state_machine() -> crate::statemachine::NoOpStateMachine {
        crate::statemachine::NoOpStateMachine::default()
    }

    fn create_test_node() -> Node<
        bytes::Bytes,
        crate::statemachine::NoOpStateMachine,
        crate::network::mock::MockNetworkFactory,
    > {
        use crate::storage::MemStorage;
        Node::new(
            10101,
            Config::default(),
            create_test_state_machine(),
            crate::network::mock::MockNetworkFactory::new(),
            Box::new(MemStorage::default()),
        )
    }

    #[tokio::test]
    async fn test_commit_only_current_term_entries() -> anyhow::Result<()> {
        let node = create_test_node();

        // 3ノードクラスタを想定（リーダー + 2ピア）
        let peer1 = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 10102);
        let peer2 = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 10103);

        // ログエントリを追加: 古いterm と 現在のterm
        {
            let mut state = node.state.lock().await;
            state.persistent.current_term = 5;
            state.role = raft::Role::Leader(raft::LeaderState::new(
                &[peer1, peer2],
                state.get_last_log_idx(),
            ));
            state.commit_index = 0;

            // 古いterm=3のエントリ
            state.persistent.log.push(raft::Entry {
                term: 3,
                command: bytes::Bytes::from("old_term_cmd1"),
            });
            state.persistent.log.push(raft::Entry {
                term: 3,
                command: bytes::Bytes::from("old_term_cmd2"),
            });

            // 現在のterm=5のエントリ
            state.persistent.log.push(raft::Entry {
                term: 5,
                command: bytes::Bytes::from("current_term_cmd1"),
            });
            state.persistent.log.push(raft::Entry {
                term: 5,
                command: bytes::Bytes::from("current_term_cmd2"),
            });

            // match_indexを設定: すべてのエントリが過半数に複製されている
            if let Some(leader_state) = state.role.leader_state_mut() {
                leader_state.match_index.insert(peer1, 4);
                leader_state.match_index.insert(peer2, 4);
            }
        }

        // new_commit_index()を直接テスト
        let new_commit_index = {
            let state = node.state.lock().await;
            if let Some(leader_state) = state.role.leader_state() {
                Node::<
                    bytes::Bytes,
                    crate::statemachine::NoOpStateMachine,
                    crate::network::mock::MockNetworkFactory,
                >::new_commit_index(
                    state.commit_index,
                    state.persistent.current_term,
                    &state.persistent.log,
                    &leader_state.match_index,
                    2, // peer_count = 2
                )
            } else {
                state.commit_index
            }
        };

        // 期待値: term=3のエントリ（index 1,2）はスキップされ、
        // term=5のエントリ（index 3,4）のみがコミット対象
        // → commit_index = 4
        assert_eq!(
            new_commit_index, 4,
            "Should only commit current term entries (term=5), not old term entries (term=3)"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_network_partition_blocks_communication() -> anyhow::Result<()>
    {
        use crate::network::mock::MockNetworkFactory;
        use crate::rpc::*;

        let addr1: SocketAddr =
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 10201);
        let addr2: SocketAddr =
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 10202);

        struct MockRpcClient;

        #[async_trait::async_trait]
        impl RaftRpcTrait for MockRpcClient {
            async fn append_entries(
                &self,
                _ctx: tarpc::context::Context,
                _req: AppendEntriesRequest,
            ) -> anyhow::Result<AppendEntriesResponse> {
                Ok(AppendEntriesResponse {
                    term: 1,
                    success: true,
                })
            }

            async fn request_vote(
                &self,
                _ctx: tarpc::context::Context,
                _req: RequestVoteRequest,
            ) -> anyhow::Result<RequestVoteResponse> {
                Ok(RequestVoteResponse {
                    term: 1,
                    vote_granted: true,
                })
            }

            async fn client_request(
                &self,
                _ctx: tarpc::context::Context,
                _req: CommandRequest,
            ) -> anyhow::Result<CommandResponse> {
                Ok(CommandResponse {
                    success: true,
                    leader_hint: None,
                    data: None,
                    error: None,
                })
            }

            async fn install_snapshot(
                &self,
                _ctx: tarpc::context::Context,
                _req: InstallSnapshotRequest,
            ) -> anyhow::Result<InstallSnapshotResponse> {
                Ok(InstallSnapshotResponse { term: 1 })
            }
        }

        let factory = MockNetworkFactory::new();
        factory.set_local_addr(addr1).await;
        factory
            .register_mock_client(addr2, Arc::new(MockRpcClient))
            .await;

        let client = <MockNetworkFactory as crate::network::NetworkFactory<
            (),
        >>::connect(&factory, addr2)
        .await?;

        let req = AppendEntriesRequest {
            term: 1,
            leader_id: 1,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        };

        let result = client
            .append_entries(tarpc::context::current(), req.clone())
            .await;
        assert!(
            result.is_ok(),
            "Expected success before partition, got error: {:?}",
            result
        );

        <MockNetworkFactory as crate::network::NetworkFactory<()>>::partition(
            &factory, addr1, addr2,
        )
        .await;

        let result = client
            .append_entries(tarpc::context::current(), req.clone())
            .await;
        assert!(
            result.is_err(),
            "Expected network partition error after partition"
        );

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("partition"),
            "Error should mention network partition, got: {}",
            err_msg
        );

        <MockNetworkFactory as crate::network::NetworkFactory<()>>::heal(
            &factory, addr1, addr2,
        )
        .await;

        let result =
            client.append_entries(tarpc::context::current(), req).await;
        assert!(
            result.is_ok(),
            "Expected success after healing, got error: {:?}",
            result
        );

        Ok(())
    }
}
