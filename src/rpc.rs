use tarpc::serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(crate = "tarpc::serde")]
pub struct LogEntry {
    pub term: u32,
    pub command: Vec<u8>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(crate = "tarpc::serde")]
pub struct AppendEntriesRequest {
    pub term: u32,
    pub leader_id: u32,
    pub prev_log_index: u32,
    pub prev_log_term: u32,
    pub entries: Vec<LogEntry>,
    pub leader_commit: u32,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(crate = "tarpc::serde")]
pub struct AppendEntriesResponse {
    pub term: u32,
    pub success: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(crate = "tarpc::serde")]
pub struct RequestVoteRequest {
    pub term: u32,
    pub candidate_id: u32,
    pub last_log_index: u32,
    pub last_log_term: u32,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(crate = "tarpc::serde")]
pub struct RequestVoteResponse {
    pub term: u32,
    pub vote_granted: bool,
}

#[tarpc::service]
pub trait RaftRpc {
    async fn echo(name: String) -> String;
    async fn append_entries(req: AppendEntriesRequest)
    -> AppendEntriesResponse;
    async fn request_vote(req: RequestVoteRequest) -> RequestVoteResponse;
}
