//! RPC server setup and RaftRpc trait implementation.
//!
//! This module provides:
//! - RPC server based on tarpc with TCP transport
//! - RaftServer that converts RPC requests into Command enum and sends via channel

use crate::node::Command;
use crate::rpc::*;
use futures::{future, prelude::*};
use std::net::{IpAddr, Ipv4Addr};
use tarpc::{
    server::{self, Channel, incoming::Incoming},
    tokio_serde::formats::Json,
};
use tokio::sync::{mpsc, oneshot};

/// Starts the RPC server on the specified port.
/// Listens for incoming tarpc connections and dispatches to RaftServer handlers.
pub async fn rpc_server(
    tx: mpsc::Sender<Command>,
    port: u16,
) -> anyhow::Result<()> {
    let addr = (IpAddr::V4(Ipv4Addr::LOCALHOST), port);
    let mut listener =
        tarpc::serde_transport::tcp::listen(&addr, Json::default)
            .await
            .expect("failed to start RPC server");
    listener.config_mut().max_frame_length(usize::MAX);
    listener
        .filter_map(|r| future::ready(r.ok()))
        .map(server::BaseChannel::with_defaults)
        .max_channels_per_key(10, |t| t.transport().peer_addr().unwrap().ip())
        .for_each_concurrent(10, |channel| async {
            let server = RaftServer { tx: tx.clone() };
            channel
                .execute(server.serve())
                .for_each(|response| async move {
                    tokio::spawn(response);
                })
                .await
        })
        .await;
    Ok(())
}

/// RPC server implementation that forwards requests to the node via channels.
#[derive(Clone)]
struct RaftServer {
    tx: mpsc::Sender<Command>,
}

impl RaftRpc for RaftServer {
    async fn append_entries(
        self,
        _: tarpc::context::Context,
        req: AppendEntriesRequest,
    ) -> AppendEntriesResponse {
        let (resp_tx, resp_rx) = oneshot::channel();
        let _ = self.tx.send(Command::AppendEntries(req, resp_tx)).await;
        resp_rx.await.unwrap_or(AppendEntriesResponse {
            term: 0,
            success: false,
        })
    }

    async fn request_vote(
        self,
        _: tarpc::context::Context,
        req: RequestVoteRequest,
    ) -> RequestVoteResponse {
        let (resp_tx, resp_rx) = oneshot::channel();
        let _ = self.tx.send(Command::RequestVote(req, resp_tx)).await;
        resp_rx.await.unwrap_or(RequestVoteResponse {
            term: 0,
            vote_granted: false,
        })
    }

    async fn client_request(
        self,
        _: tarpc::context::Context,
        req: CommandRequest,
    ) -> CommandResponse {
        let (resp_tx, resp_rx) = oneshot::channel();
        let _ = self.tx.send(Command::ClientRequest(req, resp_tx)).await;
        resp_rx.await.unwrap_or(CommandResponse {
            success: false,
            leader_hint: None,
            data: None,
            error: Some("Failed to process client request".to_string()),
        })
    }
}
