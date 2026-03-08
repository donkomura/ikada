use crate::types::{LogIndex, NodeId, Term};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct RpcContext {
    pub deadline: Option<Instant>,
}

impl RpcContext {
    pub fn background() -> Self {
        Self { deadline: None }
    }

    pub fn with_deadline(mut self, deadline: Instant) -> Self {
        self.deadline = Some(deadline);
        self
    }

    pub fn with_timeout(self, timeout: Duration) -> Self {
        self.with_deadline(Instant::now() + timeout)
    }

    pub fn timeout(&self) -> Option<Duration> {
        self.deadline
            .map(|d| d.saturating_duration_since(Instant::now()))
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
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
pub struct AppendEntriesRequest {
    pub term: Term,
    pub leader_id: NodeId,
    pub prev_log_index: LogIndex,
    pub prev_log_term: Term,
    pub entries: Vec<LogEntry>,
    pub leader_commit: LogIndex,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AppendEntriesResponse {
    pub term: Term,
    pub success: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RequestVoteRequest {
    pub term: Term,
    pub candidate_id: NodeId,
    pub last_log_index: LogIndex,
    pub last_log_term: Term,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RequestVoteResponse {
    pub term: Term,
    pub vote_granted: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CommandRequest {
    pub command: Vec<u8>,
}

#[derive(
    Serialize, Deserialize, Debug, Clone, PartialEq, Eq, thiserror::Error,
)]
pub enum CommandError {
    #[error("Not the leader")]
    NotLeader,
    #[error("No-op entry not yet committed")]
    NoopNotCommitted,
    #[error("Replication failed")]
    ReplicationFailed,
    #[error("{0}")]
    Other(String),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CommandResponse {
    pub success: bool,
    pub leader_hint: Option<NodeId>,
    pub data: Option<Vec<u8>>,
    pub error: Option<CommandError>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct InstallSnapshotRequest {
    pub term: Term,
    pub leader_id: NodeId,
    pub last_included_index: LogIndex,
    pub last_included_term: Term,
    pub data: Vec<u8>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct InstallSnapshotResponse {
    pub term: Term,
}

/// Trait for Raft RPC client abstraction.
/// This is dyn-compatible, unlike framework-specific generated traits.
#[async_trait::async_trait]
pub trait RaftRpcTrait: Send + Sync {
    async fn append_entries(
        &self,
        ctx: RpcContext,
        req: AppendEntriesRequest,
    ) -> anyhow::Result<AppendEntriesResponse>;

    async fn request_vote(
        &self,
        ctx: RpcContext,
        req: RequestVoteRequest,
    ) -> anyhow::Result<RequestVoteResponse>;

    async fn client_request(
        &self,
        ctx: RpcContext,
        req: CommandRequest,
    ) -> anyhow::Result<CommandResponse>;

    async fn install_snapshot(
        &self,
        ctx: RpcContext,
        req: InstallSnapshotRequest,
    ) -> anyhow::Result<InstallSnapshotResponse>;
}

impl RpcContext {
    pub fn into_tonic_request<T>(self, message: T) -> tonic::Request<T> {
        let mut req = tonic::Request::new(message);
        if let Some(timeout) = self.timeout() {
            req.set_timeout(timeout);
        }
        req
    }
}

pub mod proto {
    tonic::include_proto!("raft");
}

impl From<&LogEntry> for proto::LogEntry {
    fn from(e: &LogEntry) -> Self {
        Self {
            term: e.term.as_u32(),
            command: e.command.to_vec(),
        }
    }
}

impl From<proto::LogEntry> for LogEntry {
    fn from(e: proto::LogEntry) -> Self {
        Self {
            term: Term::new(e.term),
            command: Arc::from(e.command.into_boxed_slice()),
        }
    }
}

impl From<&AppendEntriesRequest> for proto::AppendEntriesRequest {
    fn from(r: &AppendEntriesRequest) -> Self {
        Self {
            term: r.term.as_u32(),
            leader_id: r.leader_id.as_u32(),
            prev_log_index: r.prev_log_index.as_u32(),
            prev_log_term: r.prev_log_term.as_u32(),
            entries: r.entries.iter().map(Into::into).collect(),
            leader_commit: r.leader_commit.as_u32(),
        }
    }
}

impl From<proto::AppendEntriesRequest> for AppendEntriesRequest {
    fn from(r: proto::AppendEntriesRequest) -> Self {
        Self {
            term: Term::new(r.term),
            leader_id: NodeId::new(r.leader_id),
            prev_log_index: LogIndex::new(r.prev_log_index),
            prev_log_term: Term::new(r.prev_log_term),
            entries: r.entries.into_iter().map(Into::into).collect(),
            leader_commit: LogIndex::new(r.leader_commit),
        }
    }
}

impl From<&AppendEntriesResponse> for proto::AppendEntriesResponse {
    fn from(r: &AppendEntriesResponse) -> Self {
        Self {
            term: r.term.as_u32(),
            success: r.success,
        }
    }
}

impl From<proto::AppendEntriesResponse> for AppendEntriesResponse {
    fn from(r: proto::AppendEntriesResponse) -> Self {
        Self {
            term: Term::new(r.term),
            success: r.success,
        }
    }
}

impl From<&RequestVoteRequest> for proto::RequestVoteRequest {
    fn from(r: &RequestVoteRequest) -> Self {
        Self {
            term: r.term.as_u32(),
            candidate_id: r.candidate_id.as_u32(),
            last_log_index: r.last_log_index.as_u32(),
            last_log_term: r.last_log_term.as_u32(),
        }
    }
}

impl From<proto::RequestVoteRequest> for RequestVoteRequest {
    fn from(r: proto::RequestVoteRequest) -> Self {
        Self {
            term: Term::new(r.term),
            candidate_id: NodeId::new(r.candidate_id),
            last_log_index: LogIndex::new(r.last_log_index),
            last_log_term: Term::new(r.last_log_term),
        }
    }
}

impl From<&RequestVoteResponse> for proto::RequestVoteResponse {
    fn from(r: &RequestVoteResponse) -> Self {
        Self {
            term: r.term.as_u32(),
            vote_granted: r.vote_granted,
        }
    }
}

impl From<proto::RequestVoteResponse> for RequestVoteResponse {
    fn from(r: proto::RequestVoteResponse) -> Self {
        Self {
            term: Term::new(r.term),
            vote_granted: r.vote_granted,
        }
    }
}

impl From<&CommandRequest> for proto::CommandRequest {
    fn from(r: &CommandRequest) -> Self {
        Self {
            command: r.command.clone(),
        }
    }
}

impl From<proto::CommandRequest> for CommandRequest {
    fn from(r: proto::CommandRequest) -> Self {
        Self { command: r.command }
    }
}

impl From<&CommandResponse> for proto::CommandResponse {
    fn from(r: &CommandResponse) -> Self {
        let (has_error, error_kind, error_message) = match &r.error {
            Some(e) => {
                let (kind, msg) = match e {
                    CommandError::NotLeader => (
                        proto::CommandErrorKind::NotLeader as i32,
                        String::new(),
                    ),
                    CommandError::NoopNotCommitted => (
                        proto::CommandErrorKind::NoopNotCommitted as i32,
                        String::new(),
                    ),
                    CommandError::ReplicationFailed => (
                        proto::CommandErrorKind::ReplicationFailed as i32,
                        String::new(),
                    ),
                    CommandError::Other(s) => {
                        (proto::CommandErrorKind::Other as i32, s.clone())
                    }
                };
                (true, kind, msg)
            }
            None => (false, 0, String::new()),
        };

        Self {
            success: r.success,
            has_leader_hint: r.leader_hint.is_some(),
            leader_hint: r.leader_hint.map(|id| id.as_u32()).unwrap_or(0),
            has_data: r.data.is_some(),
            data: r.data.clone().unwrap_or_default(),
            has_error,
            error_kind,
            error_message,
        }
    }
}

impl From<proto::CommandResponse> for CommandResponse {
    fn from(r: proto::CommandResponse) -> Self {
        let leader_hint = if r.has_leader_hint {
            Some(NodeId::new(r.leader_hint))
        } else {
            None
        };

        let data = if r.has_data { Some(r.data) } else { None };

        let error = if r.has_error {
            let kind = proto::CommandErrorKind::try_from(r.error_kind)
                .unwrap_or(proto::CommandErrorKind::Unspecified);
            Some(match kind {
                proto::CommandErrorKind::NotLeader => CommandError::NotLeader,
                proto::CommandErrorKind::NoopNotCommitted => {
                    CommandError::NoopNotCommitted
                }
                proto::CommandErrorKind::ReplicationFailed => {
                    CommandError::ReplicationFailed
                }
                proto::CommandErrorKind::Other => {
                    CommandError::Other(r.error_message)
                }
                proto::CommandErrorKind::Unspecified => {
                    CommandError::Other(r.error_message)
                }
            })
        } else {
            None
        };

        Self {
            success: r.success,
            leader_hint,
            data,
            error,
        }
    }
}

impl From<&InstallSnapshotRequest> for proto::InstallSnapshotRequest {
    fn from(r: &InstallSnapshotRequest) -> Self {
        Self {
            term: r.term.as_u32(),
            leader_id: r.leader_id.as_u32(),
            last_included_index: r.last_included_index.as_u32(),
            last_included_term: r.last_included_term.as_u32(),
            data: r.data.clone(),
        }
    }
}

impl From<proto::InstallSnapshotRequest> for InstallSnapshotRequest {
    fn from(r: proto::InstallSnapshotRequest) -> Self {
        Self {
            term: Term::new(r.term),
            leader_id: NodeId::new(r.leader_id),
            last_included_index: LogIndex::new(r.last_included_index),
            last_included_term: Term::new(r.last_included_term),
            data: r.data,
        }
    }
}

impl From<&InstallSnapshotResponse> for proto::InstallSnapshotResponse {
    fn from(r: &InstallSnapshotResponse) -> Self {
        Self {
            term: r.term.as_u32(),
        }
    }
}

impl From<proto::InstallSnapshotResponse> for InstallSnapshotResponse {
    fn from(r: proto::InstallSnapshotResponse) -> Self {
        Self {
            term: Term::new(r.term),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rpc_context_with_timeout() {
        let ctx = RpcContext::background().with_timeout(Duration::from_secs(5));
        assert!(ctx.deadline.is_some());
        let timeout = ctx.timeout().unwrap();
        assert!(timeout <= Duration::from_secs(5));
        assert!(timeout > Duration::from_secs(4));
    }

    #[test]
    fn rpc_context_no_deadline() {
        let ctx = RpcContext::background();
        assert!(ctx.deadline.is_none());
        assert!(ctx.timeout().is_none());
    }

    #[test]
    fn append_entries_request_roundtrip() {
        let req = AppendEntriesRequest {
            term: Term::new(5),
            leader_id: NodeId::new(1),
            prev_log_index: LogIndex::new(10),
            prev_log_term: Term::new(4),
            entries: vec![LogEntry {
                term: Term::new(5),
                command: Arc::from(vec![1, 2, 3].into_boxed_slice()),
            }],
            leader_commit: LogIndex::new(9),
        };

        let proto_req: proto::AppendEntriesRequest = (&req).into();
        let back: AppendEntriesRequest = proto_req.into();

        assert_eq!(back.term, req.term);
        assert_eq!(back.leader_id, req.leader_id);
        assert_eq!(back.prev_log_index, req.prev_log_index);
        assert_eq!(back.prev_log_term, req.prev_log_term);
        assert_eq!(back.entries.len(), 1);
        assert_eq!(back.entries[0].term, Term::new(5));
        assert_eq!(&*back.entries[0].command, &[1, 2, 3]);
        assert_eq!(back.leader_commit, req.leader_commit);
    }

    #[test]
    fn append_entries_response_roundtrip() {
        let resp = AppendEntriesResponse {
            term: Term::new(5),
            success: true,
        };
        let proto_resp: proto::AppendEntriesResponse = (&resp).into();
        let back: AppendEntriesResponse = proto_resp.into();
        assert_eq!(back.term, resp.term);
        assert_eq!(back.success, resp.success);
    }

    #[test]
    fn request_vote_roundtrip() {
        let req = RequestVoteRequest {
            term: Term::new(3),
            candidate_id: NodeId::new(2),
            last_log_index: LogIndex::new(5),
            last_log_term: Term::new(2),
        };
        let proto_req: proto::RequestVoteRequest = (&req).into();
        let back: RequestVoteRequest = proto_req.into();
        assert_eq!(back.term, req.term);
        assert_eq!(back.candidate_id, req.candidate_id);
        assert_eq!(back.last_log_index, req.last_log_index);
        assert_eq!(back.last_log_term, req.last_log_term);
    }

    #[test]
    fn request_vote_response_roundtrip() {
        let resp = RequestVoteResponse {
            term: Term::new(3),
            vote_granted: true,
        };
        let proto_resp: proto::RequestVoteResponse = (&resp).into();
        let back: RequestVoteResponse = proto_resp.into();
        assert_eq!(back.term, resp.term);
        assert_eq!(back.vote_granted, resp.vote_granted);
    }

    #[test]
    fn command_response_roundtrip_with_all_fields() {
        let resp = CommandResponse {
            success: true,
            leader_hint: Some(NodeId::new(42)),
            data: Some(vec![10, 20]),
            error: Some(CommandError::Other("test error".to_string())),
        };
        let proto_resp: proto::CommandResponse = (&resp).into();
        let back: CommandResponse = proto_resp.into();
        assert!(back.success);
        assert_eq!(back.leader_hint, Some(NodeId::new(42)));
        assert_eq!(back.data, Some(vec![10, 20]));
        assert_eq!(
            back.error,
            Some(CommandError::Other("test error".to_string()))
        );
    }

    #[test]
    fn command_response_roundtrip_no_optional_fields() {
        let resp = CommandResponse {
            success: false,
            leader_hint: None,
            data: None,
            error: None,
        };
        let proto_resp: proto::CommandResponse = (&resp).into();
        let back: CommandResponse = proto_resp.into();
        assert!(!back.success);
        assert!(back.leader_hint.is_none());
        assert!(back.data.is_none());
        assert!(back.error.is_none());
    }

    #[test]
    fn command_error_variants_roundtrip() {
        for error in [
            CommandError::NotLeader,
            CommandError::NoopNotCommitted,
            CommandError::ReplicationFailed,
            CommandError::Other("custom".to_string()),
        ] {
            let resp = CommandResponse {
                success: false,
                leader_hint: None,
                data: None,
                error: Some(error.clone()),
            };
            let proto_resp: proto::CommandResponse = (&resp).into();
            let back: CommandResponse = proto_resp.into();
            assert_eq!(back.error, Some(error));
        }
    }

    #[test]
    fn install_snapshot_roundtrip() {
        let req = InstallSnapshotRequest {
            term: Term::new(7),
            leader_id: NodeId::new(3),
            last_included_index: LogIndex::new(100),
            last_included_term: Term::new(6),
            data: vec![42; 256],
        };
        let proto_req: proto::InstallSnapshotRequest = (&req).into();
        let back: InstallSnapshotRequest = proto_req.into();
        assert_eq!(back.term, req.term);
        assert_eq!(back.leader_id, req.leader_id);
        assert_eq!(back.last_included_index, req.last_included_index);
        assert_eq!(back.last_included_term, req.last_included_term);
        assert_eq!(back.data, req.data);
    }

    #[test]
    fn install_snapshot_response_roundtrip() {
        let resp = InstallSnapshotResponse { term: Term::new(7) };
        let proto_resp: proto::InstallSnapshotResponse = (&resp).into();
        let back: InstallSnapshotResponse = proto_resp.into();
        assert_eq!(back.term, resp.term);
    }
}
