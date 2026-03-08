use crate::config::Config;
use crate::node::handlers;
use crate::node::{not_leader_response, notify_heartbeat};
use crate::raft::RaftState;
use crate::request_tracker::RequestTracker;
use crate::rpc::*;
use crate::statemachine::StateMachine;
use crate::types::{NodeId, Term};
use anyhow::Context;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc, oneshot};
use tonic::{Request, Response, Status, transport::Server};

pub async fn rpc_server<T, SM>(
    state: Arc<Mutex<RaftState<T, SM>>>,
    request_tracker: Arc<Mutex<RequestTracker<SM::Response>>>,
    heartbeat_tx: mpsc::UnboundedSender<(Term, NodeId)>,
    client_request_tx: mpsc::UnboundedSender<(
        CommandRequest,
        oneshot::Sender<CommandResponse>,
    )>,
    config: Config,
    port: u16,
) -> anyhow::Result<()>
where
    T: Send
        + Sync
        + Clone
        + std::fmt::Debug
        + serde::Serialize
        + serde::de::DeserializeOwned
        + 'static,
    SM: StateMachine<Command = T> + std::fmt::Debug + 'static,
    SM::Response: Clone + serde::Serialize,
{
    let addr: SocketAddr = format!("0.0.0.0:{}", port)
        .parse()
        .context("failed to parse server address")?;

    let server = RaftServer {
        state,
        request_tracker,
        heartbeat_tx,
        client_request_tx,
        config,
    };

    Server::builder()
        .add_service(proto::raft_rpc_server::RaftRpcServer::new(server))
        .serve(addr)
        .await
        .context("failed to serve gRPC server")?;

    Ok(())
}

struct RaftServer<T: Send + Sync, SM: StateMachine<Command = T>> {
    state: Arc<Mutex<RaftState<T, SM>>>,
    request_tracker: Arc<Mutex<RequestTracker<SM::Response>>>,
    heartbeat_tx: mpsc::UnboundedSender<(Term, NodeId)>,
    client_request_tx: mpsc::UnboundedSender<(
        CommandRequest,
        oneshot::Sender<CommandResponse>,
    )>,
    config: Config,
}

impl<T: Send + Sync, SM: StateMachine<Command = T>> Clone
    for RaftServer<T, SM>
{
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
            request_tracker: Arc::clone(&self.request_tracker),
            heartbeat_tx: self.heartbeat_tx.clone(),
            client_request_tx: self.client_request_tx.clone(),
            config: self.config.clone(),
        }
    }
}

#[tonic::async_trait]
impl<T, SM> proto::raft_rpc_server::RaftRpc for RaftServer<T, SM>
where
    T: Send
        + Sync
        + Clone
        + std::fmt::Debug
        + serde::Serialize
        + serde::de::DeserializeOwned
        + 'static,
    SM: StateMachine<Command = T> + std::fmt::Debug + 'static,
    SM::Response: Clone + serde::Serialize,
{
    #[tracing::instrument(skip(self, request), fields(term = %Term::new(request.get_ref().term), leader_id = %NodeId::new(request.get_ref().leader_id), entries_count = request.get_ref().entries.len()))]
    async fn append_entries(
        &self,
        request: Request<proto::AppendEntriesRequest>,
    ) -> Result<Response<proto::AppendEntriesResponse>, Status> {
        let proto_req = request.into_inner();
        let req: AppendEntriesRequest = proto_req.into();

        let current_term = self.state.lock().await.persistent.current_term;
        let resp = handlers::handle_append_entries(
            &req,
            Arc::clone(&self.state),
            Some(Arc::clone(&self.request_tracker)),
        )
        .await
        .unwrap_or_else(|e| {
            tracing::error!(error=?e, "Failed to handle AppendEntries");
            AppendEntriesResponse {
                term: Term::new(0),
                success: false,
            }
        });
        notify_heartbeat(
            req.term,
            req.leader_id,
            current_term,
            &self.heartbeat_tx,
        );

        let proto_resp: proto::AppendEntriesResponse = (&resp).into();
        Ok(Response::new(proto_resp))
    }

    #[tracing::instrument(skip(self, request), fields(term = %Term::new(request.get_ref().term), candidate_id = %NodeId::new(request.get_ref().candidate_id)))]
    async fn request_vote(
        &self,
        request: Request<proto::RequestVoteRequest>,
    ) -> Result<Response<proto::RequestVoteResponse>, Status> {
        let proto_req = request.into_inner();
        let req: RequestVoteRequest = proto_req.into();

        let resp =
            handlers::handle_request_vote(&req, Arc::clone(&self.state)).await;

        let proto_resp: proto::RequestVoteResponse = (&resp).into();
        Ok(Response::new(proto_resp))
    }

    #[tracing::instrument(skip(self, request))]
    async fn client_request(
        &self,
        request: Request<proto::CommandRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        let proto_req = request.into_inner();
        let req: CommandRequest = proto_req.into();

        let (is_leader, leader_id) = {
            let state = self.state.lock().await;
            (state.role().is_leader(), state.leader_id)
        };

        if !is_leader {
            let resp = not_leader_response(leader_id);
            let proto_resp: proto::CommandResponse = (&resp).into();
            return Ok(Response::new(proto_resp));
        }

        let (resp_tx, resp_rx) = oneshot::channel();
        let _ = self.client_request_tx.send((req, resp_tx));
        let resp = resp_rx.await.unwrap_or(CommandResponse {
            success: false,
            leader_hint: None,
            data: None,
            error: Some(crate::rpc::CommandError::Other(
                "Failed to process client request".to_string(),
            )),
        });

        let proto_resp: proto::CommandResponse = (&resp).into();
        Ok(Response::new(proto_resp))
    }

    #[tracing::instrument(skip(self, request), fields(term = %Term::new(request.get_ref().term), leader_id = %NodeId::new(request.get_ref().leader_id), last_included_index = %crate::types::LogIndex::new(request.get_ref().last_included_index)))]
    async fn install_snapshot(
        &self,
        request: Request<proto::InstallSnapshotRequest>,
    ) -> Result<Response<proto::InstallSnapshotResponse>, Status> {
        let proto_req = request.into_inner();
        let req: InstallSnapshotRequest = proto_req.into();

        let current_term = self.state.lock().await.persistent.current_term;
        let resp = handlers::handle_install_snapshot(
            &req,
            Arc::clone(&self.state),
        )
        .await
        .unwrap_or_else(|e| {
            tracing::error!(error=?e, "Failed to handle InstallSnapshot");
            InstallSnapshotResponse { term: Term::new(0) }
        });
        notify_heartbeat(
            req.term,
            req.leader_id,
            current_term,
            &self.heartbeat_tx,
        );

        let proto_resp: proto::InstallSnapshotResponse = (&resp).into();
        Ok(Response::new(proto_resp))
    }
}
