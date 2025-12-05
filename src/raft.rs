use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct Entry {
    pub term: u32,
    pub command: String,
}

#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub enum Role {
    #[default]
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug)]
pub struct RaftState {
    // Persistent state on all services
    pub current_term: u32,
    pub voted_for: Option<u32>,
    pub log: Vec<Entry>,

    // Volatile state on all servers
    pub commit_index: u32,
    pub last_applied: u32,

    // Volatile state on leader
    pub next_index: HashMap<u32, u32>,
    pub match_index: HashMap<u32, u32>,

    pub role: Role,
    pub leader_id: Option<u32>,
    pub id: u32,
}

impl Default for RaftState {
    fn default() -> Self {
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
            id: 0,
        }
    }
}
