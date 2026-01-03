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

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(crate = "tarpc::serde")]
pub struct CommandRequest {
    pub command: Vec<u8>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(crate = "tarpc::serde")]
pub struct CommandResponse {
    pub success: bool,
    pub leader_hint: Option<u32>,
    pub data: Option<Vec<u8>>,
    pub error: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(crate = "tarpc::serde")]
pub struct InstallSnapshotRequest {
    pub term: u32,
    pub leader_id: u32,
    pub last_included_index: u32,
    pub last_included_term: u32,
    pub data: Vec<u8>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(crate = "tarpc::serde")]
pub struct InstallSnapshotResponse {
    pub term: u32,
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
