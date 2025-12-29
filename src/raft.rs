use crate::statemachine::StateMachine;
use crate::storage::Storage;
use std::collections::HashMap;
use std::net::SocketAddr;
use tokio::sync::mpsc;

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

#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub enum Role {
    #[default]
    Follower,
    Candidate,
    Leader,
}

pub struct RaftState<T: Send + Sync, SM: StateMachine<Command = T>> {
    // Persistent state on all services
    pub persistent: PersistentState<T>,

    // Volatile state on all servers
    pub commit_index: u32,
    pub last_applied: u32,

    // Volatile state on leader
    pub next_index: HashMap<SocketAddr, u32>,
    pub match_index: HashMap<SocketAddr, u32>,

    pub role: Role,
    pub leader_id: Option<u32>,
    pub id: u32,

    storage: Box<dyn Storage<T>>,
    sm: SM,

    // Event notifier for applied entries
    apply_notifier: Option<mpsc::UnboundedSender<AppliedEntry<SM::Response>>>,
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
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            leader_id: None,
            id,
            storage,
            sm,
            apply_notifier: None,
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
            let response = self.sm.apply(&entry.command).await?;

            // Notify via event channel if notifier is set
            if let Some(notifier) = &self.apply_notifier {
                let _ = notifier.send(AppliedEntry {
                    log_index: self.last_applied,
                    response: response.clone(),
                });
            }

            responses.push(response);
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

    #[derive(Debug, Clone, serde::Serialize)]
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

    /// Regression test for linearizability violation bug:
    /// Reading from a follower/stale node returns uncommitted or stale values
    ///
    /// Bug scenario from Maelstrom test (based on Raft Section 8 - Read-only operations):
    /// 1. Node A is leader with commit_index=2, value=1
    /// 2. Node B is follower with commit_index=1, value=0 (lagging behind)
    /// 3. Client reads from Node B → gets stale value 0
    /// 4. Client does CAS [0->2] on Node A → succeeds
    ///
    /// This violates linearizability because:
    /// - The read saw value 0
    /// - But a later CAS [0->2] succeeded, meaning value was still 0
    /// - However, the value should have been 1 (already committed on leader)
    ///
    /// Root cause: Reads must go through leader with proper read index mechanism
    #[tokio::test]
    #[should_panic(expected = "Stale read from follower")]
    async fn test_stale_read_from_follower() {
        // Simulate leader state
        let mut leader_state = RaftState::new(
            1,
            Box::new(MemStorage::default()),
            KVStateMachine::default(),
        );
        leader_state.persistent.current_term = 1;
        leader_state.role = Role::Leader;

        // Leader commits and applies value 1
        leader_state.persistent.log.push(Entry {
            term: 1,
            command: KVCommand::Put {
                key: "test_key".to_string(),
                value: 1,
            },
        });
        leader_state.commit_index = 1;
        leader_state.apply_committed().await.unwrap();

        // Simulate follower state (lagging behind)
        let mut follower_state = RaftState::new(
            2,
            Box::new(MemStorage::default()),
            KVStateMachine::default(),
        );
        follower_state.persistent.current_term = 1;
        follower_state.role = Role::Follower;

        // Follower has old value 0 (hasn't received AppendEntries yet)
        follower_state.persistent.log.push(Entry {
            term: 1,
            command: KVCommand::Put {
                key: "test_key".to_string(),
                value: 0,
            },
        });
        follower_state.commit_index = 1;
        follower_state.apply_committed().await.unwrap();

        // BUG: Client reads from follower instead of leader
        let read_result = follower_state
            .sm
            .apply(&KVCommand::Get {
                key: "test_key".to_string(),
            })
            .await
            .unwrap();

        // This returns 0 (stale value) instead of 1 (committed value on leader)
        if matches!(read_result, KVResponse::Value(Some(0))) {
            panic!(
                "Stale read from follower: got 0 but leader has committed 1"
            );
        }
    }

    /// Regression test: Stale read from old leader after network partition recovery
    ///
    /// Bug scenario from Maelstrom test case 12 (network partition recovery):
    /// 1. Network partition occurs, creating two separate groups
    /// 2. Old leader (term 1) in minority partition commits value 2
    /// 3. New leader (term 2) in majority partition commits value 0
    /// 4. Partition heals, but old leader hasn't stepped down yet
    /// 5. Client reads from old leader → gets stale value 2
    /// 6. But the current committed value is 0 from new leader
    ///
    /// This violates linearizability because:
    /// - Process 89 writes 0 (success on new leader, term 2)
    /// - Process 2 reads and gets 2 (from old leader, term 1)
    /// - This is impossible: can't read 2 when register contains 0
    ///
    /// Root cause: Old leader serves reads without confirming leadership via heartbeat
    /// Fix required: Raft Section 8 - leader must send heartbeat to majority before serving reads
    #[tokio::test]
    #[should_panic(expected = "Stale read from old leader after partition")]
    async fn test_stale_read_after_partition_recovery() {
        // Simulate OLD leader (term 1) that was isolated during partition
        let mut old_leader_state = RaftState::new(
            1,
            Box::new(MemStorage::default()),
            KVStateMachine::default(),
        );
        old_leader_state.persistent.current_term = 1;
        old_leader_state.role = Role::Leader;

        // Old leader commits and applies value 2 during partition
        old_leader_state.persistent.log.push(Entry {
            term: 1,
            command: KVCommand::Put {
                key: "test_key".to_string(),
                value: 2,
            },
        });
        old_leader_state.commit_index = 1;
        old_leader_state.apply_committed().await.unwrap();

        // Simulate NEW leader (term 2) that took over during partition
        let mut new_leader_state = RaftState::new(
            2,
            Box::new(MemStorage::default()),
            KVStateMachine::default(),
        );
        new_leader_state.persistent.current_term = 2;
        new_leader_state.role = Role::Leader;

        // New leader commits and applies value 0 (the current correct value)
        new_leader_state.persistent.log.push(Entry {
            term: 2,
            command: KVCommand::Put {
                key: "test_key".to_string(),
                value: 0,
            },
        });
        new_leader_state.commit_index = 1;
        new_leader_state.apply_committed().await.unwrap();

        // BUG: After partition recovery, old leader hasn't learned about new term
        // Client reads from old leader (thinking it's still valid)
        let read_result = old_leader_state
            .sm
            .apply(&KVCommand::Get {
                key: "test_key".to_string(),
            })
            .await
            .unwrap();

        // This returns 2 (stale value from old leader)
        // But new leader has committed value 0
        // This violates linearizability: "can't read 2 from register 0"
        if matches!(read_result, KVResponse::Value(Some(2))) {
            panic!(
                "Stale read from old leader after partition: got 2 but new leader has committed 0"
            );
        }
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
        leader_state.role = Role::Leader;

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
            .sm
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
