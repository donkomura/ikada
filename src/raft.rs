use std::collections::HashMap;
use std::net::SocketAddr;

#[derive(Debug, Clone)]
pub struct Entry<T> {
    pub term: u32,
    pub command: T,
}

#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub enum Role {
    #[default]
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug)]
pub struct RaftState<T> {
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
}

impl<T> RaftState<T> {
    pub fn new(id: u32) -> Self {
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
        }
    }
    pub fn apply(&mut self, _command: &T) -> anyhow::Result<()> {
        Ok(())
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
