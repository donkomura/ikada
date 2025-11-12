use tarpc::serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(crate = "tarpc::serde")]
pub struct Entry {
    pub term: u32,
    pub command: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(crate = "tarpc::serde")]
pub struct AppendEntriesRequest {
    pub term: u32,
    pub leader_id: u32,
    pub prev_log_index: u32,
    pub prev_log_term: u32,
    pub entries: Vec<Entry>,
    pub leader_commit: u32,
}

#[tarpc::service]
pub trait RaftRpc {
    async fn echo(name: String) -> String;
    async fn append_entries(req: AppendEntriesRequest);
}
