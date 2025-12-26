//! Log replication and commit index management.
//!
//! Implements Raft log replication (§5.3) including:
//! - Heartbeat broadcasting to all followers
//! - Commit index advancement based on majority replication
//! - next_index/match_index tracking for each peer

use super::Node;
use crate::network::NetworkFactory;
use crate::raft::{self, Role};
use crate::rpc::*;
use crate::statemachine::StateMachine;
use std::collections::HashMap;
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
    /// Broadcasts AppendEntries RPCs to all followers.
    /// Called periodically by the leader to replicate log entries and maintain authority.
    pub(super) async fn broadcast_heartbeat(&mut self) -> anyhow::Result<bool> {
        let (leader_term, leader_id, leader_commit) = {
            let state = self.state.lock().await;
            (
                state.persistent.current_term,
                state.leader_id.unwrap(),
                state.commit_index,
            )
        };
        let mut requests = Vec::new();
        for (addr, client) in self
            .peers
            .iter()
            .map(|(addr, client)| (addr, client.clone()))
        {
            let state = self.state.lock().await;
            let next_idx = state.next_index.get(addr).copied().unwrap_or(1);
            let (prev_log_idx, prev_log_term) = if next_idx > 1 {
                (
                    next_idx - 1,
                    state.persistent.log[(next_idx - 2) as usize].term,
                )
            } else {
                (0, 0)
            };
            let entries: Vec<LogEntry> = state
                .persistent
                .log
                .iter()
                .skip((next_idx - 1) as usize) // send logs after next_idx
                .map(|e| {
                    let command =
                        bincode::serialize(&e.command).unwrap_or_default();
                    LogEntry {
                        term: e.term,
                        command,
                    }
                })
                .collect();
            let sent_up_to_index = prev_log_idx + entries.len() as u32;

            tracing::debug!(
                id=?leader_id,
                peer=?addr,
                prev_log_index=prev_log_idx,
                prev_log_term=prev_log_term,
                entries_count=entries.len(),
                sent_up_to_index=sent_up_to_index,
                leader_commit=leader_commit,
                "Sending AppendEntries to peer"
            );

            requests.push((
                addr,
                client,
                prev_log_idx,
                prev_log_term,
                entries,
                sent_up_to_index,
            ));
        }
        let rpc_timeout = self.config.rpc_timeout;

        // TODO: optimize algorithm: should be gossip?
        let mut tasks = JoinSet::new();
        for (
            addr,
            client,
            prev_log_index,
            prev_log_term,
            entries,
            sent_up_to_index,
        ) in requests
        {
            let req = AppendEntriesRequest {
                term: leader_term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            };
            tasks.spawn(Self::send_heartbeat(
                *addr,
                client,
                req,
                sent_up_to_index,
                rpc_timeout,
            ));
        }

        while let Some(result) = tasks.join_next().await {
            match result? {
                Ok((server, res, sent_up_to_index)) => {
                    self.handle_heartbeat(server, res, sent_up_to_index)
                        .await?;
                }
                Err(e) => {
                    tracing::warn!("Failed to send heartbeat: {:?}", e);
                }
            }
        }

        Ok(true)
    }

    /// Sends AppendEntries RPC to a single follower.
    async fn send_heartbeat(
        server: SocketAddr,
        client: Arc<dyn RaftRpcTrait>,
        req: AppendEntriesRequest,
        sent_up_to_index: u32,
        rpc_timeout: Duration,
    ) -> anyhow::Result<(SocketAddr, AppendEntriesResponse, u32)> {
        let mut ctx = tarpc::context::current();
        ctx.deadline = Instant::now() + rpc_timeout;
        let res = client
            .append_entries(ctx, req.clone())
            .instrument(tracing::info_span!("append entries to {server}"))
            .await?;
        Ok((server, res, sent_up_to_index))
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
                tracing::trace!(
                    index = n,
                    entry_term = log[(n - 1) as usize].term,
                    current_term = current_term,
                    "Skipping index: not from current term"
                );
                continue;
            }
            let mut count = 1; // Leader always has the entry
            for match_idx in match_index.values() {
                if *match_idx >= n {
                    count += 1;
                }
            }
            let majority = total_nodes / 2;
            tracing::trace!(
                index = n,
                replicated_count = count,
                total_nodes = total_nodes,
                majority = majority,
                has_majority = count > majority,
                "Checking index for commit"
            );
            if count > majority {
                new_commit_index = n;
            }
        }
        new_commit_index
    }

    /// Updates commit_index if a majority of nodes have replicated an entry.
    /// Only the leader can advance commit_index this way (Raft Figure 2).
    pub(super) async fn update_commit_index(&mut self) -> anyhow::Result<()> {
        let mut state = self.state.lock().await;

        // Only leaders can update commit_index this way
        if !matches!(state.role, Role::Leader) {
            return Ok(());
        }

        let current_commit_index = state.commit_index;
        let new_commit_index = Self::new_commit_index(
            state.commit_index,
            state.persistent.current_term,
            &state.persistent.log,
            &state.match_index,
            self.peers.len(),
        );

        // Update commit_index if we found a higher value
        if new_commit_index > current_commit_index {
            state.commit_index = new_commit_index;
            tracing::info!(
                id=?state.id,
                old_commit_index=current_commit_index,
                new_commit_index=new_commit_index,
                match_indices=?state.match_index,
                log_len=state.persistent.log.len(),
                "Updated commit_index: entry replicated on majority"
            );
        }

        Ok(())
    }

    /// Handles AppendEntries response from a follower.
    /// Updates next_index and match_index based on success/failure (Raft §5.3).
    #[tracing::instrument(skip(self))]
    pub(super) async fn handle_heartbeat(
        &mut self,
        addr: SocketAddr,
        res: AppendEntriesResponse,
        sent_up_to_index: u32,
    ) -> anyhow::Result<()> {
        let check_term = {
            let state = self.state.lock().await;
            if res.term > state.persistent.current_term {
                tracing::info!(id=?state.id, "become follower because of term mismatch");
                true
            } else {
                false
            }
        };

        if check_term {
            self.state.lock().await.persistent.current_term = res.term;
            self.become_follower().await?;
            return Ok(());
        }

        let (id, role, current_term) = {
            let state = self.state.lock().await;
            (state.id, state.role, state.persistent.current_term)
        };
        if matches!(role, Role::Leader) && current_term != res.term {
            return Ok(());
        }

        {
            let mut state = self.state.lock().await;
            if res.success {
                let old_match_index =
                    state.match_index.get(&addr).copied().unwrap_or(0);
                state.match_index.insert(addr, sent_up_to_index);
                state.next_index.insert(addr, sent_up_to_index + 1);

                tracing::debug!(
                    id=?id,
                    peer=?addr,
                    old_match_index=old_match_index,
                    new_match_index=sent_up_to_index,
                    next_index=sent_up_to_index + 1,
                    "AppendEntries succeeded"
                );
            } else {
                let current_next_idx =
                    state.next_index.get(&addr).copied().unwrap_or(1);
                let new_next_idx = current_next_idx.saturating_sub(1).max(1);
                state.next_index.insert(addr, new_next_idx);

                tracing::warn!(
                    id=?id,
                    peer=?addr,
                    old_next_index=current_next_idx,
                    new_next_index=new_next_idx,
                    "AppendEntries rejected, decrementing next_index"
                );
            }
        }

        self.update_commit_index().await?;
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
        Node::new(
            10101,
            Config::default(),
            create_test_state_machine(),
            crate::network::mock::MockNetworkFactory::new(),
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
            state.role = Role::Leader;
            state.persistent.current_term = 5;
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
            state.match_index.insert(peer1, 4);
            state.match_index.insert(peer2, 4);
        }

        // new_commit_index()を直接テスト
        let new_commit_index = {
            let state = node.state.lock().await;
            Node::<
                bytes::Bytes,
                crate::statemachine::NoOpStateMachine,
                crate::network::mock::MockNetworkFactory,
            >::new_commit_index(
                state.commit_index,
                state.persistent.current_term,
                &state.persistent.log,
                &state.match_index,
                2, // peer_count = 2
            )
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
}
