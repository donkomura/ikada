use crate::statemachine::StateMachine;
use crate::storage::Storage;
use std::collections::HashMap;
use std::net::SocketAddr;

#[derive(Debug, Clone)]
pub struct Entry<T: Send + Sync> {
    pub term: u32,
    pub command: T,
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

#[derive(Debug)]
pub struct RaftState<T: Send + Sync, SM: StateMachine<Command = T>> {
    // Persistent state on all services
    pub current_term: u32,
    pub voted_for: Option<u32>,
    pub log: Vec<Entry<T>>,

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
}

impl<T: Send + Sync + Clone, SM: StateMachine<Command = T>> RaftState<T, SM> {
    pub fn new(id: u32, storage: Box<dyn Storage<T>>, sm: SM) -> Self {
        Self {
            current_term: 1,
            role: Role::Follower,
            voted_for: None,
            log: Vec::new(),
            commit_index: 0,
            last_applied: 0,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            leader_id: None,
            id,
            storage,
            sm,
        }
    }

    pub async fn persist(&mut self) -> anyhow::Result<()> {
        let data = PersistentState {
            current_term: self.current_term,
            voted_for: self.voted_for,
            log: self.log.clone(),
        };
        self.storage.save(&data).await?;
        Ok(())
    }

    pub async fn load_persisted(
        &mut self,
    ) -> anyhow::Result<Option<PersistentState<T>>> {
        self.storage.load().await
    }

    pub fn restore_from(&mut self, persisted: PersistentState<T>) {
        self.current_term = persisted.current_term;
        self.voted_for = persisted.voted_for;
        self.log = persisted.log;
    }
    pub async fn apply_committed(
        &mut self,
    ) -> anyhow::Result<Vec<SM::Response>> {
        let mut responses = Vec::new();
        while self.last_applied < self.commit_index {
            self.last_applied += 1;
            let entry = &self.log[(self.last_applied - 1) as usize];
            let response = self.sm.apply(&entry.command).await?;
            responses.push(response);
        }
        Ok(responses)
    }
    pub fn get_last_log_idx(&self) -> u32 {
        self.log.len() as u32
    }
    pub fn get_last_log_entry(&self) -> Option<&Entry<T>> {
        self.log.last()
    }
    pub fn get_last_log_term(&self) -> u32 {
        self.log.last().map(|e| e.term).unwrap_or(0)
    }
    pub fn get_last_voted_term(&self) -> u32 {
        0u32
    }
}
