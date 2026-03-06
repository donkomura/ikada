use crate::types::{LogIndex, NodeId, Term};
use std::sync::Arc;
use tarpc::serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(crate = "tarpc::serde")]
pub struct LogEntry {
    pub term: Term,
    #[serde(with = "arc_bytes")]
    pub command: Arc<[u8]>,
}

mod arc_bytes {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::sync::Arc;

    pub fn serialize<S>(
        arc: &Arc<[u8]>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        arc.as_ref().serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Arc<[u8]>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let vec = Vec::<u8>::deserialize(deserializer)?;
        Ok(Arc::from(vec.into_boxed_slice()))
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(crate = "tarpc::serde")]
pub struct AppendEntriesRequest {
    pub term: Term,
    pub leader_id: NodeId,
    pub prev_log_index: LogIndex,
    pub prev_log_term: Term,
    pub entries: Vec<LogEntry>,
    pub leader_commit: LogIndex,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(crate = "tarpc::serde")]
pub struct AppendEntriesResponse {
    pub term: Term,
    pub success: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(crate = "tarpc::serde")]
pub struct RequestVoteRequest {
    pub term: Term,
    pub candidate_id: NodeId,
    pub last_log_index: LogIndex,
    pub last_log_term: Term,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(crate = "tarpc::serde")]
pub struct RequestVoteResponse {
    pub term: Term,
    pub vote_granted: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(crate = "tarpc::serde")]
pub struct CommandRequest {
    pub command: Vec<u8>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(crate = "tarpc::serde")]
pub enum CommandError {
    NotLeader,
    NoopNotCommitted,
    ReplicationFailed,
    Other(String),
}

impl std::fmt::Display for CommandError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CommandError::NotLeader => write!(f, "Not the leader"),
            CommandError::NoopNotCommitted => {
                write!(f, "No-op entry not yet committed")
            }
            CommandError::ReplicationFailed => write!(f, "Replication failed"),
            CommandError::Other(msg) => write!(f, "{}", msg),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(crate = "tarpc::serde")]
pub struct CommandResponse {
    pub success: bool,
    pub leader_hint: Option<NodeId>,
    pub data: Option<Vec<u8>>,
    pub error: Option<CommandError>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(crate = "tarpc::serde")]
pub struct InstallSnapshotRequest {
    pub term: Term,
    pub leader_id: NodeId,
    pub last_included_index: LogIndex,
    pub last_included_term: Term,
    pub data: Vec<u8>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(crate = "tarpc::serde")]
pub struct InstallSnapshotResponse {
    pub term: Term,
}

#[tarpc::service]
pub trait RaftRpc {
    async fn append_entries(req: AppendEntriesRequest)
    -> AppendEntriesResponse;
    async fn request_vote(req: RequestVoteRequest) -> RequestVoteResponse;
    async fn client_request(req: CommandRequest) -> CommandResponse;
    async fn install_snapshot(
        req: InstallSnapshotRequest,
    ) -> InstallSnapshotResponse;
}

/// Trait for Raft RPC client abstraction.
/// This is dyn-compatible, unlike the tarpc-generated RaftRpc trait.
#[async_trait::async_trait]
pub trait RaftRpcTrait: Send + Sync {
    async fn append_entries(
        &self,
        ctx: tarpc::context::Context,
        req: AppendEntriesRequest,
    ) -> anyhow::Result<AppendEntriesResponse>;

    async fn request_vote(
        &self,
        ctx: tarpc::context::Context,
        req: RequestVoteRequest,
    ) -> anyhow::Result<RequestVoteResponse>;

    async fn client_request(
        &self,
        ctx: tarpc::context::Context,
        req: CommandRequest,
    ) -> anyhow::Result<CommandResponse>;

    async fn install_snapshot(
        &self,
        ctx: tarpc::context::Context,
        req: InstallSnapshotRequest,
    ) -> anyhow::Result<InstallSnapshotResponse>;
}

/// Implement RaftRpcTrait for RaftRpcClient (tarpc-generated client)
#[async_trait::async_trait]
impl RaftRpcTrait for RaftRpcClient {
    async fn append_entries(
        &self,
        ctx: tarpc::context::Context,
        req: AppendEntriesRequest,
    ) -> anyhow::Result<AppendEntriesResponse> {
        self.clone()
            .append_entries(ctx, req)
            .await
            .map_err(Into::into)
    }

    async fn request_vote(
        &self,
        ctx: tarpc::context::Context,
        req: RequestVoteRequest,
    ) -> anyhow::Result<RequestVoteResponse> {
        self.clone()
            .request_vote(ctx, req)
            .await
            .map_err(Into::into)
    }

    async fn client_request(
        &self,
        ctx: tarpc::context::Context,
        req: CommandRequest,
    ) -> anyhow::Result<CommandResponse> {
        self.clone()
            .client_request(ctx, req)
            .await
            .map_err(Into::into)
    }

    async fn install_snapshot(
        &self,
        ctx: tarpc::context::Context,
        req: InstallSnapshotRequest,
    ) -> anyhow::Result<InstallSnapshotResponse> {
        self.clone()
            .install_snapshot(ctx, req)
            .await
            .map_err(Into::into)
    }
}
