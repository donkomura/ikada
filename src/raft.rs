use crate::statemachine::StateMachine;
use crate::storage::Storage;
use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::Notify;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Entry<T: Send + Sync> {
    pub term: u32,
    pub command: T,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PersistentState<T: Send + Sync> {
    pub current_term: u32,
    pub voted_for: Option<u32>,
    pub log: Vec<Entry<T>>,
}

#[derive(Debug, Clone)]
pub struct LeaderState {
    pub next_index: HashMap<SocketAddr, u32>,
    pub match_index: HashMap<SocketAddr, u32>,
    /// AppendEntries in-flight window per peer.
    /// Each element is the `sent_up_to_index` for a single AppendEntries RPC.
    /// We allow multiple concurrent RPCs (pipelining) and compact the queue on acks.
    pub inflight_append: HashMap<SocketAddr, VecDeque<u32>>,
    /// True while an InstallSnapshot RPC is in-flight for the peer.
    /// Snapshot transfer is exclusive with AppendEntries pipelining.
    pub inflight_snapshot: HashMap<SocketAddr, bool>,
    pub noop_index: Option<u32>,
}

impl LeaderState {
    pub fn new(peers: &[SocketAddr], last_log_index: u32) -> Self {
        let mut next_index = HashMap::new();
        let mut match_index = HashMap::new();
        let mut inflight_append = HashMap::new();
        let mut inflight_snapshot = HashMap::new();

        for peer in peers {
            next_index.insert(*peer, last_log_index + 1);
            match_index.insert(*peer, 0);
            inflight_append.insert(*peer, VecDeque::new());
            inflight_snapshot.insert(*peer, false);
        }

        Self {
            next_index,
            match_index,
            inflight_append,
            inflight_snapshot,
            noop_index: None,
        }
    }
}

#[derive(Debug, Clone)]
pub enum Role {
    Follower,
    Candidate,
    Leader(LeaderState),
}

impl Role {
    pub fn is_leader(&self) -> bool {
        matches!(self, Role::Leader(_))
    }

    pub fn is_follower(&self) -> bool {
        matches!(self, Role::Follower)
    }

    pub fn is_candidate(&self) -> bool {
        matches!(self, Role::Candidate)
    }

    pub fn leader_state(&self) -> Option<&LeaderState> {
        match self {
            Role::Leader(state) => Some(state),
            _ => None,
        }
    }

    pub fn leader_state_mut(&mut self) -> Option<&mut LeaderState> {
        match self {
            Role::Leader(state) => Some(state),
            _ => None,
        }
    }
}

pub struct RaftState<T: Send + Sync, SM: StateMachine<Command = T>> {
    // Persistent state on all services
    pub persistent: PersistentState<T>,

    // Volatile state on all servers
    pub commit_index: u32,
    pub last_applied: u32,

    pub role: Role,

    pub leader_id: Option<u32>,
    pub id: u32,

    storage: Box<dyn Storage<T>>,
    pub state_machine: SM,

    pub apply_tx:
        Option<tokio::sync::mpsc::UnboundedSender<(u32, SM::Response)>>,

    pub replication_notifier: Arc<Notify>,
    pub last_applied_notifier: Arc<Notify>,
}

impl<T: Send + Sync + Clone, SM: StateMachine<Command = T>> RaftState<T, SM> {
    pub fn new(id: u32, storage: Box<dyn Storage<T>>, sm: SM) -> Self {
        Self {
            persistent: PersistentState {
                current_term: 1,
                voted_for: None,
                log: Vec::new(),
            },
            role: Role::Follower,
            commit_index: 0,
            last_applied: 0,
            leader_id: None,
            id,
            storage,
            state_machine: sm,
            apply_tx: None,
            replication_notifier: Arc::new(Notify::new()),
            last_applied_notifier: Arc::new(Notify::new()),
        }
    }

    pub async fn persist(&mut self) -> anyhow::Result<()> {
        self.storage.save(&self.persistent).await?;
        Ok(())
    }

    pub async fn load_persisted(
        &mut self,
    ) -> anyhow::Result<Option<PersistentState<T>>> {
        self.storage.load().await
    }

    pub fn restore_from(&mut self, persisted: PersistentState<T>) {
        self.persistent = persisted;
    }
    pub async fn apply_committed(
        &mut self,
    ) -> anyhow::Result<Vec<SM::Response>> {
        let mut responses = Vec::new();
        while self.last_applied < self.commit_index {
            self.last_applied += 1;
            let entry = &self.persistent.log[(self.last_applied - 1) as usize];
            let response = self.state_machine.apply(&entry.command).await?;

            if let Some(apply_tx) = &self.apply_tx {
                let _ = apply_tx.send((self.last_applied, response.clone()));
            }

            responses.push(response);
        }

        if !responses.is_empty() {
            self.last_applied_notifier.notify_waiters();
        }

        Ok(responses)
    }
    pub fn get_last_log_idx(&self) -> u32 {
        self.persistent.log.len() as u32
    }
    pub fn get_last_log_entry(&self) -> Option<&Entry<T>> {
        self.persistent.log.last()
    }
    pub fn get_last_log_term(&self) -> u32 {
        self.persistent.log.last().map(|e| e.term).unwrap_or(0)
    }
    pub fn get_last_voted_term(&self) -> u32 {
        0u32
    }

    pub fn become_follower(&mut self, term: u32, leader_id: Option<u32>) {
        self.persistent.current_term = term;
        self.persistent.voted_for = None;
        self.role = Role::Follower;
        self.leader_id = leader_id;
    }

    pub fn become_candidate(&mut self) {
        self.persistent.current_term += 1;
        self.persistent.voted_for = Some(self.id);
        self.role = Role::Candidate;
        self.leader_id = None;
    }

    pub fn become_leader(&mut self, peers: &[SocketAddr]) {
        let last_log_index = self.get_last_log_idx();
        let leader_state = LeaderState::new(peers, last_log_index);
        self.role = Role::Leader(leader_state);
        self.leader_id = Some(self.id);
    }

    pub async fn create_snapshot(&mut self) -> anyhow::Result<()> {
        use crate::snapshot::SnapshotMetadata;

        if self.last_applied == 0 {
            return Ok(());
        }

        let last_included_index = self.last_applied;
        let last_included_term = self
            .persistent
            .log
            .get((last_included_index - 1) as usize)
            .map(|e| e.term)
            .unwrap_or(0);

        let data = self.state_machine.snapshot().await?;

        let metadata = SnapshotMetadata {
            last_included_index,
            last_included_term,
        };

        self.storage.save_snapshot(&metadata, &data).await?;

        self.persistent.log.drain(0..(last_included_index as usize));

        Ok(())
    }

    pub async fn restore_from_snapshot(&mut self) -> anyhow::Result<bool> {
        let snapshot = self.storage.load_snapshot().await?;

        if let Some((metadata, data)) = snapshot {
            self.state_machine.restore(&data).await?;

            self.last_applied = metadata.last_included_index;
            self.commit_index = metadata.last_included_index;

            if self.persistent.log.len() > metadata.last_included_index as usize
            {
                self.persistent
                    .log
                    .drain(0..(metadata.last_included_index as usize));
            } else {
                self.persistent.log.clear();
            }

            tracing::info!(
                last_included_index = metadata.last_included_index,
                last_included_term = metadata.last_included_term,
                "Snapshot restored successfully"
            );

            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Applies snapshot data received via InstallSnapshot RPC
    ///
    /// Implements Raft Figure 13:
    /// - If existing log entry has same index and term as snapshot's last included entry,
    ///   retain log entries following it
    /// - Otherwise discard the entire log
    /// - Reset state machine using snapshot contents
    pub async fn restore_from_snapshot_data(
        &mut self,
        last_included_index: u32,
        last_included_term: u32,
        data: &[u8],
    ) -> anyhow::Result<()> {
        // Check if existing log entry has same index and term as snapshot's last included entry
        let should_retain_log = if last_included_index > 0
            && (last_included_index as usize) <= self.persistent.log.len()
        {
            let log_index = (last_included_index - 1) as usize;
            self.persistent.log[log_index].term == last_included_term
        } else {
            false
        };

        if should_retain_log {
            // Retain log entries following the snapshot's last included entry
            self.persistent.log.drain(0..(last_included_index as usize));
        } else {
            // Discard the entire log
            self.persistent.log.clear();
        }

        // Reset state machine using snapshot contents
        self.state_machine.restore(data).await?;

        // Update commit_index and last_applied to snapshot's last included index
        self.commit_index = last_included_index;
        self.last_applied = last_included_index;

        tracing::info!(
            last_included_index = last_included_index,
            last_included_term = last_included_term,
            log_retained = should_retain_log,
            "Snapshot installed from leader"
        );

        Ok(())
    }

    pub async fn get_snapshot(
        &self,
    ) -> anyhow::Result<Option<(crate::snapshot::SnapshotMetadata, Vec<u8>)>>
    {
        self.storage.load_snapshot().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::statemachine::{
        KVCommand, KVResponse, KVStateMachine, NoOpStateMachine,
    };
    use crate::storage::MemStorage;

    /// Test that followers reject read requests (linearizability protection)
    ///
    /// Test scenario based on Raft Section 8 - Read-only operations:
    /// 1. Create a leader node and a follower node
    /// 2. Client sends read request to follower: should be rejected with "Not the leader"
    /// 3. This prevents stale reads from followers
    ///
    /// This test verifies that the RPC handler correctly enforces:
    /// - Followers reject client requests immediately
    /// - Error message is "Not the leader"
    /// - Leader hint is provided
    #[tokio::test]
    async fn test_stale_read_from_follower() -> anyhow::Result<()> {
        use crate::config::Config;
        use crate::network::mock::MockNetworkFactory;
        use crate::node::{Command, Node};
        use crate::rpc::CommandRequest;
        use std::sync::Arc;
        use tokio::sync::{Mutex, mpsc};

        let follower_state = Arc::new(Mutex::new(RaftState::new(
            10002,
            Box::new(MemStorage::default()),
            NoOpStateMachine::default(),
        )));
        {
            let mut state = follower_state.lock().await;
            state.persistent.current_term = 1;
            // Follower state set via become_follower
            state.leader_id = Some(10001);
        }

        let follower_network_factory = MockNetworkFactory::new();
        let follower_node = Node::new_with_state(
            Config::default(),
            follower_state.clone(),
            follower_network_factory,
        );

        let (follower_cmd_tx, follower_cmd_rx) = mpsc::channel::<Command>(32);

        let follower_handle = tokio::spawn(async move {
            follower_node
                .run_with_handler(vec![], follower_cmd_rx)
                .await
        });

        let get_command = CommandRequest {
            command: vec![1, 2, 3],
        };

        let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
        follower_cmd_tx
            .send(Command::ClientRequest(get_command.clone(), resp_tx))
            .await?;

        let follower_response = resp_rx.await?;
        assert!(!follower_response.success, "Follower should reject reads");
        assert_eq!(
            follower_response.error,
            Some(crate::rpc::CommandError::NotLeader),
            "Follower should return 'Not the leader' error"
        );
        assert_eq!(
            follower_response.leader_hint,
            Some(10001),
            "Follower should hint the leader"
        );

        follower_handle.abort();

        Ok(())
    }

    /// Test that old leaders step down when they learn about higher terms
    ///
    /// Test scenario based on Raft Section 8 and network partition recovery:
    /// 1. Node 1 is the old leader (term 1)
    /// 2. Node 2 becomes the new leader (term 2) after partition
    /// 3. When partition heals, Node 1 receives AppendEntries with higher term
    /// 4. Node 1 steps down to follower role
    /// 5. Subsequent client requests to Node 1 are rejected
    ///
    /// This test verifies that:
    /// - Old leaders detect higher terms via AppendEntries RPC
    /// - Old leaders step down to follower immediately
    /// - Old leaders reject client requests after stepping down
    /// - This prevents stale reads from old leaders
    #[tokio::test]
    async fn test_stale_read_after_partition_recovery() -> anyhow::Result<()> {
        use crate::config::Config;
        use crate::network::mock::MockNetworkFactory;
        use crate::node::{Command, Node};
        use crate::rpc::{AppendEntriesRequest, CommandRequest};
        use std::net::{IpAddr, Ipv4Addr, SocketAddr};
        use std::sync::Arc;
        use tokio::sync::{Mutex, mpsc};

        let new_leader_addr: SocketAddr =
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 10012);

        let old_leader_state = Arc::new(Mutex::new(RaftState::new(
            10011,
            Box::new(MemStorage::default()),
            KVStateMachine::default(),
        )));
        {
            let mut state = old_leader_state.lock().await;
            state.persistent.current_term = 1;
            state.role =
                Role::Leader(LeaderState::new(&[], state.get_last_log_idx()));
            state.persistent.log.push(Entry {
                term: 1,
                command: KVCommand::Set {
                    key: "test_key".to_string(),
                    value: "value2".to_string(),
                },
            });
            state.commit_index = 1;
            state.apply_committed().await?;
        }

        let network_factory = MockNetworkFactory::new();
        let old_leader_node = Node::new_with_state(
            Config::default(),
            old_leader_state.clone(),
            network_factory,
        );

        let (cmd_tx, cmd_rx) = mpsc::channel::<Command>(32);
        let node_handle = tokio::spawn(async move {
            old_leader_node
                .run_with_handler(vec![new_leader_addr], cmd_rx)
                .await
        });

        let append_entries_from_new_leader = AppendEntriesRequest {
            term: 2,
            leader_id: 10012,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        };

        let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
        cmd_tx
            .send(Command::AppendEntries(
                append_entries_from_new_leader,
                resp_tx,
            ))
            .await?;

        let ae_response = resp_rx.await?;
        assert!(
            ae_response.success,
            "Old leader should accept AppendEntries from new leader"
        );
        assert_eq!(
            ae_response.term, 2,
            "Old leader should update its term to 2"
        );

        {
            let state = old_leader_state.lock().await;
            assert!(
                state.role.is_follower(),
                "Old leader should have stepped down to follower"
            );
            assert_eq!(
                state.persistent.current_term, 2,
                "Old leader should have updated term"
            );
        }

        let get_command = CommandRequest {
            command: bincode::serialize(&KVCommand::Get {
                key: "test_key".to_string(),
            })?,
        };

        let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
        cmd_tx
            .send(Command::ClientRequest(get_command, resp_tx))
            .await?;

        let response = resp_rx.await?;
        assert!(
            !response.success,
            "Old leader (now follower) should reject client requests"
        );
        assert_eq!(
            response.error,
            Some(crate::rpc::CommandError::NotLeader),
            "Should return 'Not the leader' error"
        );

        node_handle.abort();

        Ok(())
    }

    /// Regression test: Linearizable reads must go through leader with read index
    ///
    /// Correct implementation based on Raft Section 8:
    /// 1. Leader receives read request
    /// 2. Leader notes current commit_index as read_index
    /// 3. Leader applies all entries up to read_index
    /// 4. Leader confirms it's still leader (via heartbeat)
    /// 5. Then leader serves the read from its state machine
    #[tokio::test]
    async fn test_linearizable_read_through_leader() -> anyhow::Result<()> {
        let mut leader_state = RaftState::new(
            1,
            Box::new(MemStorage::default()),
            KVStateMachine::default(),
        );
        leader_state.persistent.current_term = 1;
        leader_state.role = Role::Leader(LeaderState::new(
            &[],
            leader_state.get_last_log_idx(),
        ));

        // Leader commits and applies value 1
        leader_state.persistent.log.push(Entry {
            term: 1,
            command: KVCommand::Set {
                key: "test_key".to_string(),
                value: "value1".to_string(),
            },
        });
        leader_state.commit_index = 1;

        // CORRECT: Apply before serving read
        leader_state.apply_committed().await?;
        assert_eq!(leader_state.last_applied, leader_state.commit_index);

        // Now read from leader's state machine
        let read_result = leader_state
            .state_machine
            .apply(&KVCommand::Get {
                key: "test_key".to_string(),
            })
            .await?;

        // Should return the latest committed value
        assert!(
            matches!(read_result, KVResponse::Value(Some(ref v)) if v == "value1"),
            "Leader should return latest committed value 'value1', got {:?}",
            read_result
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_create_snapshot() -> anyhow::Result<()> {
        let mut state = RaftState::new(
            1,
            Box::new(MemStorage::default()),
            KVStateMachine::default(),
        );

        state.persistent.log.push(Entry {
            term: 1,
            command: KVCommand::Set {
                key: "key1".to_string(),
                value: "value1".to_string(),
            },
        });
        state.persistent.log.push(Entry {
            term: 1,
            command: KVCommand::Set {
                key: "key2".to_string(),
                value: "value2".to_string(),
            },
        });
        state.persistent.log.push(Entry {
            term: 2,
            command: KVCommand::Set {
                key: "key3".to_string(),
                value: "value3".to_string(),
            },
        });

        state.commit_index = 3;
        state.apply_committed().await?;

        assert_eq!(state.last_applied, 3);
        assert_eq!(state.persistent.log.len(), 3);

        state.create_snapshot().await?;

        assert_eq!(state.persistent.log.len(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_create_snapshot_with_no_applied_entries() -> anyhow::Result<()>
    {
        let mut state = RaftState::new(
            1,
            Box::new(MemStorage::default()),
            KVStateMachine::default(),
        );

        state.persistent.log.push(Entry {
            term: 1,
            command: KVCommand::Set {
                key: "key1".to_string(),
                value: "value1".to_string(),
            },
        });

        state.create_snapshot().await?;

        assert_eq!(state.persistent.log.len(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_create_snapshot_preserves_unapplied_entries()
    -> anyhow::Result<()> {
        let mut state = RaftState::new(
            1,
            Box::new(MemStorage::default()),
            KVStateMachine::default(),
        );

        for i in 1..=5 {
            state.persistent.log.push(Entry {
                term: 1,
                command: KVCommand::Set {
                    key: format!("key{}", i),
                    value: format!("value{}", i),
                },
            });
        }

        state.commit_index = 3;
        state.apply_committed().await?;

        assert_eq!(state.last_applied, 3);
        assert_eq!(state.persistent.log.len(), 5);

        state.create_snapshot().await?;

        assert_eq!(state.persistent.log.len(), 2);

        assert_eq!(state.persistent.log[0].term, 1);
        assert!(matches!(
            state.persistent.log[0].command,
            KVCommand::Set { ref key, ref value } if key == "key4" && value == "value4"
        ));

        assert_eq!(state.persistent.log[1].term, 1);
        assert!(matches!(
            state.persistent.log[1].command,
            KVCommand::Set { ref key, ref value } if key == "key5" && value == "value5"
        ));

        Ok(())
    }

    #[tokio::test]
    async fn test_restore_from_snapshot() -> anyhow::Result<()> {
        let mut state = RaftState::new(
            1,
            Box::new(MemStorage::default()),
            KVStateMachine::default(),
        );

        for i in 1..=5 {
            state.persistent.log.push(Entry {
                term: 1,
                command: KVCommand::Set {
                    key: format!("key{}", i),
                    value: format!("value{}", i),
                },
            });
        }

        state.commit_index = 5;
        state.apply_committed().await?;

        state.create_snapshot().await?;

        assert_eq!(state.last_applied, 5);
        assert_eq!(state.persistent.log.len(), 0);

        let mut new_state =
            RaftState::new(1, state.storage, KVStateMachine::default());

        let restored = new_state.restore_from_snapshot().await?;
        assert!(restored, "Snapshot should be restored");

        assert_eq!(new_state.last_applied, 5);
        assert_eq!(new_state.commit_index, 5);
        assert_eq!(new_state.persistent.log.len(), 0);

        let result = new_state
            .state_machine
            .apply(&KVCommand::Get {
                key: "key3".to_string(),
            })
            .await?;
        assert!(
            matches!(result, KVResponse::Value(Some(ref v)) if v == "value3"),
            "Restored state machine should contain key3=value3"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_restore_from_snapshot_with_remaining_log()
    -> anyhow::Result<()> {
        let mut state = RaftState::new(
            1,
            Box::new(MemStorage::default()),
            KVStateMachine::default(),
        );

        for i in 1..=5 {
            state.persistent.log.push(Entry {
                term: 1,
                command: KVCommand::Set {
                    key: format!("key{}", i),
                    value: format!("value{}", i),
                },
            });
        }

        state.commit_index = 3;
        state.apply_committed().await?;

        state.create_snapshot().await?;

        assert_eq!(state.last_applied, 3);
        assert_eq!(state.persistent.log.len(), 2);

        let mut new_state =
            RaftState::new(1, state.storage, KVStateMachine::default());

        for i in 1..=5 {
            new_state.persistent.log.push(Entry {
                term: 1,
                command: KVCommand::Set {
                    key: format!("key{}", i),
                    value: format!("value{}", i),
                },
            });
        }

        let restored = new_state.restore_from_snapshot().await?;
        assert!(restored, "Snapshot should be restored");

        assert_eq!(new_state.last_applied, 3);
        assert_eq!(new_state.commit_index, 3);
        assert_eq!(new_state.persistent.log.len(), 2);

        assert_eq!(new_state.persistent.log[0].term, 1);
        assert!(matches!(
            new_state.persistent.log[0].command,
            KVCommand::Set { ref key, ref value } if key == "key4" && value == "value4"
        ));

        Ok(())
    }

    #[tokio::test]
    async fn test_restore_from_snapshot_no_snapshot() -> anyhow::Result<()> {
        let mut state = RaftState::new(
            1,
            Box::new(MemStorage::default()),
            KVStateMachine::default(),
        );

        let restored = state.restore_from_snapshot().await?;
        assert!(!restored, "No snapshot should be found");

        assert_eq!(state.last_applied, 0);
        assert_eq!(state.commit_index, 0);

        Ok(())
    }
}
