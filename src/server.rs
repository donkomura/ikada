use crate::config::Config;
use crate::node::handlers;
use crate::node::{not_leader_response, notify_heartbeat};
use crate::raft::RaftState;
use crate::request_tracker::RequestTracker;
use crate::rpc::*;
use crate::statemachine::StateMachine;
use crate::types::{NodeId, Term};
use anyhow::Context;
use futures::{future, prelude::*};
use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;
use tarpc::{
    server::{self, Channel, incoming::Incoming},
    tokio_serde::formats::Bincode,
};
use tokio::sync::{Mutex, mpsc, oneshot};

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
    let addr = (IpAddr::V4(Ipv4Addr::LOCALHOST), port);
    let mut listener =
        tarpc::serde_transport::tcp::listen(&addr, Bincode::default)
            .await
            .context("failed to start RPC server")?;
    listener.config_mut().max_frame_length(usize::MAX);
    listener
        .filter_map(|r| future::ready(r.ok()))
        .map(server::BaseChannel::with_defaults)
        .max_channels_per_key(10, |t| {
            t.transport()
                .peer_addr()
                .map(|a| a.ip())
                .unwrap_or(IpAddr::V4(Ipv4Addr::UNSPECIFIED))
        })
        .for_each_concurrent(10, |channel| {
            let server = RaftServer {
                state: Arc::clone(&state),
                request_tracker: Arc::clone(&request_tracker),
                heartbeat_tx: heartbeat_tx.clone(),
                client_request_tx: client_request_tx.clone(),
                config: config.clone(),
            };
            async {
                channel
                    .execute(server.serve())
                    .for_each(|response| async move {
                        tokio::spawn(response);
                    })
                    .await
            }
        })
        .await;
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

impl<T, SM> RaftRpc for RaftServer<T, SM>
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
    #[tracing::instrument(skip(self, _ctx), fields(term = %req.term, leader_id = %req.leader_id, entries_count = req.entries.len()))]
    async fn append_entries(
        self,
        _ctx: tarpc::context::Context,
        req: AppendEntriesRequest,
    ) -> AppendEntriesResponse {
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
        resp
    }

    #[tracing::instrument(skip(self, _ctx), fields(term = %req.term, candidate_id = %req.candidate_id))]
    async fn request_vote(
        self,
        _ctx: tarpc::context::Context,
        req: RequestVoteRequest,
    ) -> RequestVoteResponse {
        handlers::handle_request_vote(&req, Arc::clone(&self.state)).await
    }

    #[tracing::instrument(skip(self, _ctx, req))]
    async fn client_request(
        self,
        _ctx: tarpc::context::Context,
        req: CommandRequest,
    ) -> CommandResponse {
        let (is_leader, leader_id) = {
            let state = self.state.lock().await;
            (state.role().is_leader(), state.leader_id)
        };

        if !is_leader {
            return not_leader_response(leader_id);
        }

        let (resp_tx, resp_rx) = oneshot::channel();
        let _ = self.client_request_tx.send((req, resp_tx));
        resp_rx.await.unwrap_or(CommandResponse {
            success: false,
            leader_hint: None,
            data: None,
            error: Some(crate::rpc::CommandError::Other(
                "Failed to process client request".to_string(),
            )),
        })
    }

    #[tracing::instrument(skip(self, _ctx), fields(term = %req.term, leader_id = %req.leader_id, last_included_index = %req.last_included_index))]
    async fn install_snapshot(
        self,
        _ctx: tarpc::context::Context,
        req: InstallSnapshotRequest,
    ) -> InstallSnapshotResponse {
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
        resp
    }
}
