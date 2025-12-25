//! Node module: Core Raft node structure and lifecycle management.
//!
//! This module serves as the entry point for Raft functionality, providing:
//! - Public APIs that external code interacts with
//! - Channel infrastructure for async message passing between components
//! - Command/Response abstractions to decouple RPC layer from consensus logic

mod election;
mod handlers;
mod lifecycle;
mod replication;

use crate::config::Config;
use crate::network::NetworkFactory;
use crate::raft::RaftState;
use crate::rpc::*;
use crate::statemachine::StateMachine;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio::task::JoinSet;

/// Command represents RPC requests with response channels.
/// This enum exists to bridge the RPC server (which receives requests)
/// and the consensus logic (which processes them asynchronously).
pub enum Command {
    AppendEntries(AppendEntriesRequest, oneshot::Sender<AppendEntriesResponse>),
    RequestVote(RequestVoteRequest, oneshot::Sender<RequestVoteResponse>),
    ClientRequest(CommandRequest, oneshot::Sender<CommandResponse>),
}

/// Response wraps optional RPC responses for internal use.
pub enum Response {
    AppendEntries(Option<AppendEntriesResponse>),
    RequestVote(Option<RequestVoteResponse>),
}

/// Chan holds all channels needed for node-internal communication.
/// Separated from Node to keep initialization logic cleaner.
pub(crate) struct Chan<T> {
    pub heartbeat_tx: mpsc::UnboundedSender<(u32, u32)>,
    pub heartbeat_rx: mpsc::UnboundedReceiver<(u32, u32)>,
    pub client_tx: mpsc::Sender<T>,
    pub client_rx: mpsc::Receiver<T>,
}

impl<T> Chan<T> {
    fn new() -> Self {
        let (heartbeat_tx, heartbeat_rx) = mpsc::unbounded_channel();
        let (client_tx, client_rx) = mpsc::channel::<T>(32);
        Self {
            heartbeat_tx,
            heartbeat_rx,
            client_tx,
            client_rx,
        }
    }
}

/// Node is the main Raft participant structure.
/// Fields are pub(crate) to allow submodules (election, replication, etc.)
/// access without exposing internals to external crates.
pub struct Node<
    T: Send + Sync,
    SM: StateMachine<Command = T>,
    NF: NetworkFactory,
> {
    pub(crate) config: Config,
    pub(crate) peers: HashMap<SocketAddr, RaftRpcClient>,
    pub(crate) state: Arc<Mutex<RaftState<T, SM>>>,
    pub(crate) c: Chan<T>,
    pub(crate) network_factory: NF,
}

impl<T, SM, NF> Node<T, SM, NF>
where
    T: Send
        + Sync
        + Clone
        + std::fmt::Debug
        + serde::Serialize
        + serde::de::DeserializeOwned
        + 'static,
    SM: StateMachine<Command = T> + std::fmt::Debug + 'static,
    NF: NetworkFactory + Clone + 'static,
{
    /// Creates a new Raft node.
    /// Port is used as node ID for simplicity in testing/demo scenarios.
    pub fn new(port: u16, config: Config, sm: SM, network_factory: NF) -> Self {
        use crate::storage::MemStorage;
        let storage = Box::new(MemStorage::default());
        let id = port as u32;

        Node {
            config,
            peers: HashMap::default(),
            state: Arc::new(Mutex::new(RaftState::new(id, storage, sm))),
            c: Chan::new(),
            network_factory,
        }
    }

    /// Restores persisted state from storage if available.
    /// This is called before run() to recover from crashes.
    pub async fn restore(&mut self) -> anyhow::Result<()> {
        let mut state = self.state.lock().await;
        let id = state.id;

        match state.load_persisted().await {
            Ok(Some(persisted)) => {
                tracing::info!(
                    id = id,
                    term = persisted.current_term,
                    log_len = persisted.log.len(),
                    "Restoring state from storage"
                );
                state.restore_from(persisted);
                Ok(())
            }
            Ok(None) => {
                tracing::info!(
                    id = id,
                    "No persisted state found, starting fresh"
                );
                Ok(())
            }
            Err(e) => {
                tracing::warn!(id = id, error = ?e, "Failed to load persisted state, starting fresh");
                Ok(())
            }
        }
    }

    /// Starts the Raft node and runs until error or shutdown.
    /// Spawns three tasks: main loop, RPC handler, and RPC server.
    pub async fn run(
        self,
        port: u16,
        servers: Vec<SocketAddr>,
    ) -> anyhow::Result<()> {
        let (tx, rx) = mpsc::channel::<Command>(32);
        let state = Arc::clone(&self.state);
        let heartbeat_tx = self.c.heartbeat_tx.clone();
        let client_tx = self.c.client_tx.clone();
        let mut workers = JoinSet::new();
        workers.spawn(self.main(servers));
        workers.spawn(Self::rpc_handler(state, rx, heartbeat_tx, client_tx));
        workers.spawn(crate::server::rpc_server(tx, port));

        if let Some(res) = workers.join_next().await {
            res??;
        }

        workers.abort_all();
        while workers.join_next().await.is_some() {}

        Ok(())
    }

    /// Dispatches RPC commands to appropriate handlers.
    /// Runs in a separate task to avoid blocking the main consensus loop.
    pub(crate) async fn rpc_handler(
        state: Arc<Mutex<RaftState<T, SM>>>,
        mut rx: mpsc::Receiver<Command>,
        heartbeat_tx: mpsc::UnboundedSender<(u32, u32)>,
        client_tx: mpsc::Sender<T>,
    ) -> anyhow::Result<()> {
        while let Some(cmd) = rx.recv().await {
            match cmd {
                Command::AppendEntries(req, resp_tx) => {
                    let state_clone = Arc::clone(&state);
                    let heartbeat_tx = heartbeat_tx.clone();
                    tokio::spawn(async move {
                        let resp = handlers::handle_append_entries(&req, state_clone.clone())
                            .await
                            .unwrap_or_else(|e| {
                                tracing::error!(error=?e, "Failed to handle AppendEntries");
                                AppendEntriesResponse {
                                    term: 0,
                                    success: false,
                                }
                            });

                        let _ = resp_tx.send(resp.clone());

                        // Notify lifecycle loop of successful AppendEntries to reset election timeout
                        if resp.success {
                            let _state = state_clone.lock().await;
                            let _ =
                                heartbeat_tx.send((req.term, req.leader_id));
                        }
                    });
                }
                Command::RequestVote(req, resp_tx) => {
                    let state_clone = Arc::clone(&state);
                    tokio::spawn(async move {
                        let resp =
                            handlers::handle_request_vote(&req, state_clone)
                                .await;
                        let _ = resp_tx.send(resp);
                    });
                }
                Command::ClientRequest(req, resp_tx) => {
                    let state_clone = Arc::clone(&state);
                    let client_tx = client_tx.clone();
                    tokio::spawn(async move {
                        let resp = handlers::handle_client_request(
                            &req,
                            state_clone,
                            client_tx,
                        )
                        .await;
                        let _ = resp_tx.send(resp);
                    });
                }
            }
        }
        Ok(())
    }

    /// Establishes RPC connection to a peer node.
    pub(crate) async fn connect_to_peer(
        &mut self,
        addr: SocketAddr,
    ) -> anyhow::Result<()> {
        let client = self.network_factory.connect(addr).await?;
        self.peers.insert(addr, client);
        Ok(())
    }

    /// Connects to all peer nodes in the cluster.
    /// Called once at startup before entering the main loop.
    pub(crate) async fn setup(
        &mut self,
        servers: Vec<SocketAddr>,
    ) -> anyhow::Result<()> {
        let id = self.state.lock().await.id;
        for &addr in &servers {
            // Skip self to avoid connecting to own address
            if addr.port() as u32 == id {
                continue;
            }
            match self.connect_to_peer(addr).await {
                Ok(_) => {
                    tracing::info!(id = id, peer = ?addr, "Connected to peer");
                }
                Err(e) => {
                    tracing::warn!(id = id, peer = ?addr, error = ?e, "Failed to connect to peer");
                }
            }
        }
        Ok(())
    }
}
