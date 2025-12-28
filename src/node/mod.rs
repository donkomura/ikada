//! Node module: Core Raft node structure and lifecycle management.
//!
//! This module serves as the entry point for Raft functionality, providing:
//! - Public APIs that external code interacts with
//! - Channel infrastructure for async message passing between components
//! - Command/Response abstractions to decouple RPC layer from consensus logic

mod election;
pub mod handlers;
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
pub struct Chan<T> {
    pub heartbeat_tx: mpsc::UnboundedSender<(u32, u32)>,
    pub heartbeat_rx: mpsc::UnboundedReceiver<(u32, u32)>,
    pub client_tx: mpsc::Sender<T>,
    pub client_rx: mpsc::Receiver<T>,
    pub client_request_tx: mpsc::UnboundedSender<(
        CommandRequest,
        oneshot::Sender<CommandResponse>,
    )>,
    pub client_request_rx: mpsc::UnboundedReceiver<(
        CommandRequest,
        oneshot::Sender<CommandResponse>,
    )>,
}

impl<T> Default for Chan<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Chan<T> {
    pub fn new() -> Self {
        let (heartbeat_tx, heartbeat_rx) = mpsc::unbounded_channel();
        let (client_tx, client_rx) = mpsc::channel::<T>(32);
        let (client_request_tx, client_request_rx) = mpsc::unbounded_channel();
        Self {
            heartbeat_tx,
            heartbeat_rx,
            client_tx,
            client_rx,
            client_request_tx,
            client_request_rx,
        }
    }
}

/// Node is the main Raft participant structure.
/// Fields are pub to allow external integration (e.g., Maelstrom testing).
pub struct Node<
    T: Send + Sync,
    SM: StateMachine<Command = T>,
    NF: NetworkFactory,
> {
    pub config: Config,
    pub peers: HashMap<SocketAddr, Arc<dyn RaftRpcTrait>>,
    pub state: Arc<Mutex<RaftState<T, SM>>>,
    pub c: Chan<T>,
    pub network_factory: NF,
    /// Tracks whether the last heartbeat received majority responses and the term at that time.
    /// Used for Raft Section 8 leadership confirmation.
    pub last_heartbeat_majority: Arc<Mutex<Option<u32>>>,
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
            last_heartbeat_majority: Arc::new(Mutex::new(None)),
        }
    }

    /// Creates a new Raft node with an existing RaftState.
    /// Useful for custom initialization scenarios.
    pub fn new_with_state(
        config: Config,
        state: Arc<Mutex<RaftState<T, SM>>>,
        network_factory: NF,
    ) -> Self {
        Node {
            config,
            peers: HashMap::default(),
            state,
            c: Chan::new(),
            network_factory,
            last_heartbeat_majority: Arc::new(Mutex::new(None)),
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
        let config = self.config.clone();
        let last_heartbeat_majority = self.last_heartbeat_majority.clone();
        let peer_count = self.peers.len();
        let mut workers = JoinSet::new();
        workers.spawn(self.main(servers));
        workers.spawn(Self::rpc_handler(
            state,
            rx,
            heartbeat_tx,
            client_tx,
            config,
            last_heartbeat_majority,
            peer_count,
        ));
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
        config: Config,
        last_heartbeat_majority: Arc<Mutex<Option<u32>>>,
        peer_count: usize,
    ) -> anyhow::Result<()> {
        while let Some(cmd) = rx.recv().await {
            match cmd {
                Command::AppendEntries(req, resp_tx) => {
                    let state_clone = Arc::clone(&state);
                    let heartbeat_tx = heartbeat_tx.clone();
                    tokio::spawn(async move {
                        let current_term =
                            state_clone.lock().await.persistent.current_term;
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

                        // Reset election timeout if we received AppendEntries from current or higher term leader
                        // This prevents unnecessary elections even when log consistency checks fail
                        if req.term >= current_term {
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
                    let timeout = config.rpc_timeout;
                    let last_heartbeat_majority =
                        Arc::clone(&last_heartbeat_majority);
                    tokio::spawn(async move {
                        let heartbeat_term =
                            *last_heartbeat_majority.lock().await;
                        let resp = handlers::handle_client_request_impl(
                            &req,
                            state_clone,
                            client_tx,
                            timeout,
                            heartbeat_term,
                            peer_count,
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
    pub async fn setup(
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

    /// Handles client command synchronously within the leader loop.
    /// This ensures all client requests are processed in-order with proper leadership validation.
    pub async fn handle_client_command(
        &mut self,
        command: T,
    ) -> anyhow::Result<Option<SM::Response>>
    where
        SM::Response: Clone + serde::Serialize,
    {
        let heartbeat_term = *self.last_heartbeat_majority.lock().await;
        let peer_count = self.peers.len();

        let (log_index, result_rx) = {
            let mut state_guard = self.state.lock().await;

            if let Err(_) =
                handlers::validate_leadership(&state_guard, heartbeat_term)
            {
                return Ok(None);
            }

            if state_guard.commit_index > state_guard.last_applied {
                state_guard.apply_committed().await?;
            }

            let term = state_guard.persistent.current_term;
            let log_index = state_guard.persistent.log.len() as u32 + 1;
            state_guard.persistent.log.push(crate::raft::Entry {
                term,
                command: command.clone(),
            });

            state_guard.persist().await?;

            let (result_tx, result_rx) = oneshot::channel();
            state_guard.pending_responses.insert(log_index, result_tx);

            (log_index, result_rx)
        };

        self.broadcast_heartbeat().await?;
        self.update_commit_index().await?;

        let start = std::time::Instant::now();
        let timeout =
            self.config.rpc_timeout + std::time::Duration::from_millis(100);
        let poll_interval = std::time::Duration::from_millis(10);

        loop {
            if start.elapsed() > timeout {
                return Ok(None);
            }

            let is_replicated = {
                let state_guard = self.state.lock().await;

                if !matches!(state_guard.role, crate::raft::Role::Leader) {
                    return Ok(None);
                }

                let total_nodes = peer_count + 1;
                let majority = total_nodes / 2;
                let mut count = 1;

                for match_idx in state_guard.match_index.values() {
                    if *match_idx >= log_index {
                        count += 1;
                    }
                }

                count > majority
            };

            if is_replicated {
                break;
            }

            tokio::time::sleep(poll_interval).await;
        }

        let mut state = self.state.lock().await;
        if state.commit_index > state.last_applied {
            state.apply_committed().await?;
        }

        match tokio::time::timeout(timeout, result_rx).await {
            Ok(Ok(response)) => Ok(Some(response)),
            _ => Ok(None),
        }
    }
}
