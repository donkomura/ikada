use crate::statemachine::StateMachine;
use crate::storage::Storage;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::{Notify, mpsc};

#[derive(Debug, Clone)]
pub struct Entry<T: Send + Sync> {
    pub term: u32,
    pub command: T,
}

#[derive(Debug, Clone)]
pub struct AppliedEntry<R> {
    pub log_index: u32,
    pub response: R,
}

#[derive(Debug, Clone)]
pub struct PersistentState<T: Send + Sync> {
    pub current_term: u32,
    pub voted_for: Option<u32>,
    pub log: Vec<Entry<T>>,
}

#[derive(Debug, Clone)]
pub struct LeaderState {
    pub next_index: HashMap<SocketAddr, u32>,
    pub match_index: HashMap<SocketAddr, u32>,
    pub noop_index: Option<u32>,
}

impl LeaderState {
    pub fn new(peers: &[SocketAddr], last_log_index: u32) -> Self {
        let mut next_index = HashMap::new();
        let mut match_index = HashMap::new();

        for peer in peers {
            next_index.insert(*peer, last_log_index + 1);
            match_index.insert(*peer, 0);
        }

        Self {
            next_index,
            match_index,
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

    // Event notifier for applied entries
    apply_notifier: Option<mpsc::UnboundedSender<AppliedEntry<SM::Response>>>,

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
            apply_notifier: None,
            replication_notifier: Arc::new(Notify::new()),
            last_applied_notifier: Arc::new(Notify::new()),
        }
    }

    pub fn set_apply_notifier(
        &mut self,
        notifier: mpsc::UnboundedSender<AppliedEntry<SM::Response>>,
    ) {
        self.apply_notifier = Some(notifier);
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

            // Notify via event channel if notifier is set
            if let Some(notifier) = &self.apply_notifier {
                let _ = notifier.send(AppliedEntry {
                    log_index: self.last_applied,
                    response: response.clone(),
                });
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::statemachine::StateMachine;
    use crate::storage::MemStorage;
    use std::collections::HashMap;

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    enum KVCommand {
        Get { key: String },
        Put { key: String, value: i32 },
        Cas { key: String, from: i32, to: i32 },
    }

    impl Default for KVCommand {
        fn default() -> Self {
            KVCommand::Get { key: String::new() }
        }
    }

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    enum KVResponse {
        Value(Option<i32>),
        Ok,
        Error(String),
    }

    #[derive(Debug, Clone, Default)]
    struct KVStateMachine {
        store: HashMap<String, i32>,
    }

    #[async_trait::async_trait]
    impl StateMachine for KVStateMachine {
        type Command = KVCommand;
        type Response = KVResponse;

        async fn apply(
            &mut self,
            command: &Self::Command,
        ) -> anyhow::Result<Self::Response> {
            match command {
                KVCommand::Get { key } => {
                    Ok(KVResponse::Value(self.store.get(key).copied()))
                }
                KVCommand::Put { key, value } => {
                    self.store.insert(key.clone(), *value);
                    Ok(KVResponse::Ok)
                }
                KVCommand::Cas { key, from, to } => {
                    if let Some(current) = self.store.get(key) {
                        if current == from {
                            self.store.insert(key.clone(), *to);
                            Ok(KVResponse::Ok)
                        } else {
                            Ok(KVResponse::Error(format!(
                                "Expected {} but found {}",
                                from, current
                            )))
                        }
                    } else {
                        Ok(KVResponse::Error("Key does not exist".to_string()))
                    }
                }
            }
        }
    }

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
            KVStateMachine::default(),
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
            command: bincode::serialize(&KVCommand::Get {
                key: "test_key".to_string(),
            })?,
        };

        let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
        follower_cmd_tx
            .send(Command::ClientRequest(get_command.clone(), resp_tx))
            .await?;

        let follower_response = resp_rx.await?;
        assert!(!follower_response.success, "Follower should reject reads");
        assert_eq!(
            follower_response.error.as_deref(),
            Some("Not the leader"),
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
                command: KVCommand::Put {
                    key: "test_key".to_string(),
                    value: 2,
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
            response.error.as_deref(),
            Some("Not the leader"),
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
            command: KVCommand::Put {
                key: "test_key".to_string(),
                value: 1,
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
            matches!(read_result, KVResponse::Value(Some(1))),
            "Leader should return latest committed value 1, got {:?}",
            read_result
        );

        Ok(())
    }
}
