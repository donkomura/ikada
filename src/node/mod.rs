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
use crate::request_tracker::RequestTracker;
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
    InstallSnapshot(
        InstallSnapshotRequest,
        oneshot::Sender<InstallSnapshotResponse>,
    ),
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
    pub request_tracker: Arc<Mutex<RequestTracker<SM::Response>>>,
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
    pub fn new(
        port: u16,
        config: Config,
        sm: SM,
        network_factory: NF,
        storage: Box<dyn crate::storage::Storage<T>>,
    ) -> Self {
        let id = port as u32;

        let (apply_tx, mut apply_rx) = tokio::sync::mpsc::unbounded_channel();
        let request_tracker = Arc::new(Mutex::new(RequestTracker::new()));

        let tracker_clone = Arc::clone(&request_tracker);
        tokio::spawn(async move {
            while let Some((log_index, response)) = apply_rx.recv().await {
                tracker_clone
                    .lock()
                    .await
                    .complete_write(log_index, response);
            }
        });

        let mut raft_state = RaftState::new(id, storage, sm);
        raft_state.apply_tx = Some(apply_tx);

        Node {
            config,
            peers: HashMap::default(),
            state: Arc::new(Mutex::new(raft_state)),
            c: Chan::new(),
            network_factory,
            heartbeat_failure_count: 0,
            request_tracker,
        }
    }

    /// Creates a new Raft node with an existing RaftState.
    /// Useful for custom initialization scenarios.
    pub fn new_with_state(
        config: Config,
        state: Arc<Mutex<RaftState<T, SM>>>,
        network_factory: NF,
    ) -> Self {
        let (apply_tx, mut apply_rx) = tokio::sync::mpsc::unbounded_channel();
        let request_tracker = Arc::new(Mutex::new(RequestTracker::new()));

        let tracker_clone = Arc::clone(&request_tracker);
        tokio::spawn(async move {
            while let Some((log_index, response)) = apply_rx.recv().await {
                tracker_clone
                    .lock()
                    .await
                    .complete_write(log_index, response);
            }
        });

        tokio::spawn({
            let state_clone = Arc::clone(&state);
            async move {
                let mut state = state_clone.lock().await;
                state.apply_tx = Some(apply_tx);
            }
        });

        Node {
            config,
            peers: HashMap::default(),
            state,
            c: Chan::new(),
            network_factory,
            heartbeat_failure_count: 0,
            request_tracker,
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
    #[tracing::instrument(skip(self, servers), fields(port = port))]
    pub async fn run(
        self,
        port: u16,
        servers: Vec<SocketAddr>,
    ) -> anyhow::Result<()>
    where
        T: Default,
    {
        use tracing::Instrument;

        let (tx, rx) = mpsc::channel::<Command>(32);
        let mut workers = JoinSet::new();
        workers.spawn(
            self.run_with_handler(servers, rx)
                .instrument(tracing::Span::current()),
        );
        workers.spawn(
            crate::server::rpc_server(tx, port)
                .instrument(tracing::Span::current()),
        );

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
        let request_tracker = Arc::clone(&self.request_tracker);
        let config = self.config.clone();

        tokio::spawn(Self::rpc_handler(
            state,
            cmd_rx,
            heartbeat_tx,
            client_request_tx,
            request_tracker,
            config,
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
        request_tracker: Arc<Mutex<RequestTracker<SM::Response>>>,
        config: Config,
    ) -> anyhow::Result<()> {
        while let Some(cmd) = rx.recv().await {
            match cmd {
                Command::AppendEntries(req, resp_tx) => {
                    let state_clone = Arc::clone(&state);
                    let heartbeat_tx = heartbeat_tx.clone();
                    let tracker_clone = Arc::clone(&request_tracker);
                    tokio::spawn(async move {
                        let current_term =
                            state_clone.lock().await.persistent.current_term;
                        let resp = handlers::handle_append_entries(&req, state_clone.clone(), Some(tracker_clone))
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
                        let is_leader = {
                            let state = state_clone.lock().await;
                            state.role.is_leader()
                        };

                        if is_leader {
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
                                error: Some(
                                    crate::rpc::CommandError::NotLeader,
                                ),
                            });
                        }
                    });
                }
                Command::ReadRequest(req, resp_tx) => {
                    let state_clone = Arc::clone(&state);
                    tokio::spawn(async move {
                        // Check if this node is the leader
                        let is_leader = {
                            let state = state_clone.lock().await;
                            state.role.is_leader()
                        };

                        if is_leader {
                            // Use ReadIndex optimization for reads
                            let peer_count = {
                                let state = state_clone.lock().await;
                                state
                                    .role
                                    .leader_state()
                                    .map(|ls| ls.match_index.len())
                                    .unwrap_or(0)
                            };
                            let resp = handlers::handle_read_index_request(
                                &req,
                                state_clone,
                                config.read_index_timeout,
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
                                error: Some(
                                    crate::rpc::CommandError::NotLeader,
                                ),
                            });
                        }
                    });
                }
                Command::InstallSnapshot(req, resp_tx) => {
                    let state_clone = Arc::clone(&state);
                    let heartbeat_tx = heartbeat_tx.clone();
                    tokio::spawn(async move {
                        let current_term =
                            state_clone.lock().await.persistent.current_term;
                        let resp = handlers::handle_install_snapshot(&req, state_clone.clone())
                            .await
                            .unwrap_or_else(|e| {
                                tracing::error!(error=?e, "Failed to handle InstallSnapshot");
                                InstallSnapshotResponse {
                                    term: 0,
                                }
                            });

                        let _ = resp_tx.send(resp.clone());

                        // Reset election timeout if we received InstallSnapshot from current or higher term leader
                        if req.term >= current_term {
                            let _ =
                                heartbeat_tx.send((req.term, req.leader_id));
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
