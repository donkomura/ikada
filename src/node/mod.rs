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

use crate::client_manager::ClientResponseManager;
use crate::config::Config;
use crate::network::NetworkFactory;
use crate::raft::{AppliedEntry, RaftState};
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
    ReadRequest(CommandRequest, oneshot::Sender<CommandResponse>),
}

/// Chan holds all channels needed for node-internal communication.
/// Separated from Node to keep initialization logic cleaner.
///
/// Channel usage by role:
/// - heartbeat: Used by all roles (Follower/Candidate receive, Leader sends)
/// - client_request: Used only by Leader to process client write requests
pub struct Chan {
    /// Heartbeat notification channel (leader_id, term)
    /// Used by all roles to detect leader presence
    pub heartbeat_tx: mpsc::UnboundedSender<(u32, u32)>,
    pub heartbeat_rx: mpsc::UnboundedReceiver<(u32, u32)>,

    /// Client write request channel
    /// Used only by Leader role to batch and process client commands
    pub client_request_tx: mpsc::UnboundedSender<(
        CommandRequest,
        oneshot::Sender<CommandResponse>,
    )>,
    pub client_request_rx: mpsc::UnboundedReceiver<(
        CommandRequest,
        oneshot::Sender<CommandResponse>,
    )>,
}

impl Default for Chan {
    fn default() -> Self {
        Self::new()
    }
}

impl Chan {
    pub fn new() -> Self {
        let (heartbeat_tx, heartbeat_rx) = mpsc::unbounded_channel();
        let (client_request_tx, client_request_rx) = mpsc::unbounded_channel();
        Self {
            heartbeat_tx,
            heartbeat_rx,
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
    pub c: Chan,
    pub network_factory: NF,
    heartbeat_failure_count: usize,
    pub client_manager: Arc<Mutex<ClientResponseManager<SM::Response>>>,
    #[allow(dead_code)]
    apply_event_tx: mpsc::UnboundedSender<AppliedEntry<SM::Response>>,
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

        let (apply_event_tx, apply_event_rx) = mpsc::unbounded_channel();
        let client_manager = Arc::new(Mutex::new(ClientResponseManager::new()));

        let manager_clone = Arc::clone(&client_manager);
        tokio::spawn(async move {
            let mut rx: mpsc::UnboundedReceiver<AppliedEntry<SM::Response>> =
                apply_event_rx;
            while let Some(event) = rx.recv().await {
                manager_clone
                    .lock()
                    .await
                    .resolve(event.log_index, event.response);
            }
        });

        let mut raft_state = RaftState::new(id, storage, sm);
        raft_state.set_apply_notifier(apply_event_tx.clone());

        Node {
            config,
            peers: HashMap::default(),
            state: Arc::new(Mutex::new(raft_state)),
            c: Chan::new(),
            network_factory,
            heartbeat_failure_count: 0,
            client_manager,
            apply_event_tx,
        }
    }

    /// Creates a new Raft node with an existing RaftState.
    /// Useful for custom initialization scenarios.
    pub fn new_with_state(
        config: Config,
        state: Arc<Mutex<RaftState<T, SM>>>,
        network_factory: NF,
    ) -> Self {
        let (apply_event_tx, apply_event_rx) = mpsc::unbounded_channel();
        let client_manager = Arc::new(Mutex::new(ClientResponseManager::new()));

        let manager_clone = Arc::clone(&client_manager);
        tokio::spawn(async move {
            let mut rx: mpsc::UnboundedReceiver<AppliedEntry<SM::Response>> =
                apply_event_rx;
            while let Some(event) = rx.recv().await {
                manager_clone
                    .lock()
                    .await
                    .resolve(event.log_index, event.response);
            }
        });

        tokio::spawn({
            let state_clone = Arc::clone(&state);
            let apply_event_tx_clone = apply_event_tx.clone();
            async move {
                let mut state = state_clone.lock().await;
                state.set_apply_notifier(apply_event_tx_clone);
            }
        });

        Node {
            config,
            peers: HashMap::default(),
            state,
            c: Chan::new(),
            network_factory,
            heartbeat_failure_count: 0,
            client_manager,
            apply_event_tx,
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
    ) -> anyhow::Result<()>
    where
        T: Default,
    {
        let (tx, rx) = mpsc::channel::<Command>(32);
        let mut workers = JoinSet::new();
        workers.spawn(self.run_with_handler(servers, rx));
        workers.spawn(crate::server::rpc_server(tx, port));

        if let Some(res) = workers.join_next().await {
            res??;
        }

        workers.abort_all();
        while workers.join_next().await.is_some() {}

        Ok(())
    }

    /// Runs the node with an external command handler.
    /// Useful for custom RPC implementations (e.g., Maelstrom integration).
    pub async fn run_with_handler(
        mut self,
        servers: Vec<SocketAddr>,
        cmd_rx: mpsc::Receiver<Command>,
    ) -> anyhow::Result<()>
    where
        T: Default,
    {
        self.setup(servers).await?;

        let state = Arc::clone(&self.state);
        let heartbeat_tx = self.c.heartbeat_tx.clone();
        let client_request_tx = self.c.client_request_tx.clone();
        let client_manager = Arc::clone(&self.client_manager);

        tokio::spawn(Self::rpc_handler(
            state,
            cmd_rx,
            heartbeat_tx,
            client_request_tx,
            client_manager,
        ));

        self.main(vec![]).await
    }

    /// Dispatches RPC commands to appropriate handlers.
    /// Runs in a separate task to avoid blocking the main consensus loop.
    pub(crate) async fn rpc_handler(
        state: Arc<Mutex<RaftState<T, SM>>>,
        mut rx: mpsc::Receiver<Command>,
        heartbeat_tx: mpsc::UnboundedSender<(u32, u32)>,
        client_request_tx: mpsc::UnboundedSender<(
            CommandRequest,
            oneshot::Sender<CommandResponse>,
        )>,
        client_manager: Arc<Mutex<ClientResponseManager<SM::Response>>>,
    ) -> anyhow::Result<()> {
        while let Some(cmd) = rx.recv().await {
            match cmd {
                Command::AppendEntries(req, resp_tx) => {
                    let state_clone = Arc::clone(&state);
                    let heartbeat_tx = heartbeat_tx.clone();
                    let client_manager_clone = Arc::clone(&client_manager);
                    tokio::spawn(async move {
                        let current_term =
                            state_clone.lock().await.persistent.current_term;
                        let resp = handlers::handle_append_entries(&req, state_clone.clone(), Some(client_manager_clone))
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
                    let client_request_tx_clone = client_request_tx.clone();
                    tokio::spawn(async move {
                        // Check if this node is the leader
                        let role = {
                            let state = state_clone.lock().await;
                            state.role
                        };

                        if matches!(role, crate::raft::Role::Leader) {
                            // Send to leader loop for processing
                            let _ =
                                client_request_tx_clone.send((req, resp_tx));
                        } else {
                            // Not a leader, return error with leader hint
                            let leader_hint = {
                                let state = state_clone.lock().await;
                                state.leader_id
                            };
                            let _ = resp_tx.send(CommandResponse {
                                success: false,
                                leader_hint,
                                data: None,
                                error: Some("Not the leader".to_string()),
                            });
                        }
                    });
                }
                Command::ReadRequest(req, resp_tx) => {
                    let state_clone = Arc::clone(&state);
                    let _client_manager_clone = Arc::clone(&client_manager);
                    tokio::spawn(async move {
                        // Check if this node is the leader
                        let role = {
                            let state = state_clone.lock().await;
                            state.role
                        };

                        if matches!(role, crate::raft::Role::Leader) {
                            // Use ReadIndex optimization for reads
                            let peer_count = {
                                let state = state_clone.lock().await;
                                state.match_index.len()
                            };
                            let resp = handlers::handle_read_index_request(
                                &req,
                                state_clone,
                                std::time::Duration::from_millis(2000),
                                peer_count,
                            )
                            .await;
                            let _ = resp_tx.send(resp);
                        } else {
                            // Not a leader, return error with leader hint
                            let leader_hint = {
                                let state = state_clone.lock().await;
                                state.leader_id
                            };
                            let _ = resp_tx.send(CommandResponse {
                                success: false,
                                leader_hint,
                                data: None,
                                error: Some("Not the leader".to_string()),
                            });
                        }
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
}
