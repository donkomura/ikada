use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub src: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dest: Option<String>,
    pub body: Body,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Body {
    #[serde(rename = "init")]
    Init(InitBody),
    #[serde(rename = "init_ok")]
    InitOk(InitOkBody),
    #[serde(rename = "read")]
    Read(ReadBody),
    #[serde(rename = "read_ok")]
    ReadOk(ReadOkBody),
    #[serde(rename = "write")]
    Write(WriteBody),
    #[serde(rename = "write_ok")]
    WriteOk(WriteOkBody),
    #[serde(rename = "cas")]
    Cas(CasBody),
    #[serde(rename = "cas_ok")]
    CasOk(CasOkBody),
    #[serde(rename = "error")]
    Error(ErrorBody),
    #[serde(rename = "append_entries")]
    AppendEntries(AppendEntriesBody),
    #[serde(rename = "append_entries_ok")]
    AppendEntriesOk(AppendEntriesOkBody),
    #[serde(rename = "request_vote")]
    RequestVote(RequestVoteBody),
    #[serde(rename = "request_vote_ok")]
    RequestVoteOk(RequestVoteOkBody),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InitBody {
    pub msg_id: u64,
    pub node_id: String,
    pub node_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InitOkBody {
    pub in_reply_to: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadBody {
    pub msg_id: u64,
    pub key: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadOkBody {
    pub in_reply_to: u64,
    pub value: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteBody {
    pub msg_id: u64,
    pub key: Value,
    pub value: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteOkBody {
    pub in_reply_to: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CasBody {
    pub msg_id: u64,
    pub key: Value,
    pub from: Value,
    pub to: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CasOkBody {
    pub in_reply_to: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorBody {
    pub in_reply_to: u64,
    pub code: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
}

pub const ERROR_KEY_NOT_EXIST: u32 = 20;
pub const ERROR_CAS_MISMATCH: u32 = 22;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesBody {
    pub msg_id: u64,
    pub term: u32,
    pub leader_id: u32,
    pub prev_log_index: u32,
    pub prev_log_term: u32,
    pub entries: Vec<Value>,
    pub leader_commit: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesOkBody {
    pub in_reply_to: u64,
    pub term: u32,
    pub success: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVoteBody {
    pub msg_id: u64,
    pub term: u32,
    pub candidate_id: u32,
    pub last_log_index: u32,
    pub last_log_term: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVoteOkBody {
    pub in_reply_to: u64,
    pub term: u32,
    pub vote_granted: bool,
}
