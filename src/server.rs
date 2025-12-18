use crate::raft::{self, RaftState, Role};
use crate::rpc::*;
use crate::statemachine::StateMachine;
use crate::watchdog::WatchDog;
use futures::{future, prelude::*};
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use crate::config::Config;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tarpc::{
    client,
    server::{self, Channel, incoming::Incoming},
    tokio_serde::formats::Json,
};
use tokio::{
    sync::{Mutex, mpsc, oneshot},
    task::JoinSet,
};
use tracing::Instrument;

pub enum Command {
    AppendEntries(AppendEntriesRequest, oneshot::Sender<AppendEntriesResponse>),
    RequestVote(RequestVoteRequest, oneshot::Sender<RequestVoteResponse>),
}

pub enum Response {
    AppendEntries(Option<AppendEntriesResponse>),
    RequestVote(Option<RequestVoteResponse>),
}

pub struct Chan<T> {
    heartbeat_tx: mpsc::UnboundedSender<(u32, u32)>,
    heartbeat_rx: mpsc::UnboundedReceiver<(u32, u32)>,
    #[allow(dead_code)]
    client_tx: mpsc::Sender<T>,
    client_rx: mpsc::Receiver<T>,
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

pub struct Node<T: Send + Sync, SM: StateMachine<Command = T>> {
    config: Config,
    peers: HashMap<SocketAddr, RaftRpcClient>,
    server_addr: SocketAddr,
    state: Arc<Mutex<RaftState<T, SM>>>,
    c: Chan<T>,
}

async fn spawn(fut: impl Future<Output = ()> + Send + 'static) {
    tokio::spawn(fut);
}

impl<T, SM> Node<T, SM>
where
    T: Send
        + Sync
        + Clone
        + std::fmt::Debug
        + serde::Serialize
        + serde::de::DeserializeOwned
        + 'static,
    SM: StateMachine<Command = T> + std::fmt::Debug + 'static,
{
    pub fn new(port: u16, config: Config, sm: SM) -> Self {
        use crate::storage::MemStorage;
        let storage = Box::new(MemStorage::default());
        let id = port as u32;

        Node {
            config,
            peers: HashMap::default(),
            server_addr: SocketAddr::from((Ipv4Addr::LOCALHOST, port)),
            state: Arc::new(Mutex::new(RaftState::new(id, storage, sm))),
            c: Chan::new(),
        }
    }

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
    pub async fn run(self, port: u16, servers: Vec<SocketAddr>) -> anyhow::Result<()> {
        // main thread for test
        let (tx, rx) = mpsc::channel::<Command>(32);
        let state = Arc::clone(&self.state);
        let heartbeat_tx = self.c.heartbeat_tx.clone();
        let mut workers = JoinSet::new();
        workers.spawn(self.main(servers));
        workers.spawn(Self::rpc_handler(state, rx, heartbeat_tx));
        workers.spawn(Self::rpc_server(tx, port));

        if let Some(res) = workers.join_next().await {
            res??;
        }

        workers.abort_all();
        while workers.join_next().await.is_some() {}

        Ok(())
    }

    async fn rpc_server(
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
            .max_channels_per_key(1, |t| {
                t.transport().peer_addr().unwrap().ip()
            })
            .map(|channel| {
                let server = RaftServer { tx: tx.clone() };
                channel.execute(server.serve()).for_each(spawn)
            })
            .buffer_unordered(10)
            .for_each(|_| async {})
            .await;
        Ok(())
    }
    async fn append_entries(
        req: &AppendEntriesRequest,
        state: Arc<Mutex<RaftState<T, SM>>>,
    ) -> anyhow::Result<AppendEntriesResponse> {
        let mut state = state.lock().await;
        if req.term < state.current_term {
            tracing::warn!(
                id=?state.id,
                req_term=req.term,
                current_term=state.current_term,
                "AppendEntries rejected: request term is older than current term"
            );
            return Ok(AppendEntriesResponse {
                term: state.current_term,
                success: false,
            });
        }

        if req.term > state.current_term {
            state.current_term = req.term;
            state.role = Role::Follower;
            state.voted_for = None;
            if let Err(e) = state.persist().await {
                tracing::error!(id=?state.id, error=?e, "Failed to persist state after term update");
                return Ok(AppendEntriesResponse {
                    term: state.current_term,
                    success: false,
                });
            }
        }

        if req.prev_log_index > 0 {
            if req.prev_log_index > state.get_last_log_idx() {
                tracing::warn!(
                    id=?state.id,
                    prev_log_index=req.prev_log_index,
                    last_log_idx=state.get_last_log_idx(),
                    "AppendEntries rejected: prev_log_index exceeds log length"
                );
                return Ok(AppendEntriesResponse {
                    term: state.current_term,
                    success: false,
                });
            }

            let prev_log_entry = &state.log[(req.prev_log_index - 1) as usize];
            if prev_log_entry.term != req.prev_log_term {
                tracing::warn!(
                    id=?state.id,
                    prev_log_index=req.prev_log_index,
                    prev_log_term=req.prev_log_term,
                    actual_term=prev_log_entry.term,
                    "AppendEntries rejected: prev_log_term mismatch"
                );
                return Ok(AppendEntriesResponse {
                    term: state.current_term,
                    success: false,
                });
            }
        }

        // If an existing entry conflicts with a new one (same index but different terms),
        // delete the existing entry and all that follow it (§5.3)
        let mut log_modified = false;
        for (i, rpc_entry) in req.entries.iter().enumerate() {
            let log_index = req.prev_log_index + 1 + i as u32;

            // Check if this index exists in the log
            if log_index <= state.get_last_log_idx() {
                let existing_term = state.log[(log_index - 1) as usize].term;

                // If terms don't match, there's a conflict
                if existing_term != rpc_entry.term {
                    // Delete the conflicting entry and all that follow
                    state.log.truncate((log_index - 1) as usize);
                    log_modified = true;
                    tracing::info!(
                        id=?state.id,
                        conflict_index=log_index,
                        old_term=existing_term,
                        new_term=rpc_entry.term,
                        "Truncated log due to conflict"
                    );
                    break;
                }
            }
        }

        // Append any new entries not already in the log (§5.3)
        let start_index = req.prev_log_index + 1;
        for (i, rpc_entry) in req.entries.iter().enumerate() {
            let log_index = start_index + i as u32;

            // Only append if this entry doesn't exist yet
            if log_index > state.get_last_log_idx() {
                let command = match bincode::deserialize(&rpc_entry.command) {
                    Ok(cmd) => cmd,
                    Err(e) => {
                        tracing::error!(id=?state.id, error=?e, "Failed to deserialize command");
                        return Ok(AppendEntriesResponse {
                            term: state.current_term,
                            success: false,
                        });
                    }
                };
                let entry = raft::Entry {
                    term: rpc_entry.term,
                    command,
                };
                state.log.push(entry);
                log_modified = true;
            }
        }

        // Persist if log was modified
        if log_modified && let Err(e) = state.persist().await {
            tracing::error!(id=?state.id, error=?e, "Failed to persist state after log modification");
            return Ok(AppendEntriesResponse {
                term: state.current_term,
                success: false,
            });
        }

        // If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
        if req.leader_commit > state.commit_index {
            state.commit_index =
                req.leader_commit.min(state.get_last_log_idx());
            tracing::debug!(
                id=?state.id,
                new_commit_index=state.commit_index,
                leader_commit=req.leader_commit,
                "Updated commit_index"
            );

            if state.commit_index > state.last_applied {
                state.apply_committed().await?;
            }
        }

        Ok(AppendEntriesResponse {
            term: state.current_term,
            success: true,
        })
    }
    async fn request_vote(
        req: &RequestVoteRequest,
        state: Arc<Mutex<RaftState<T, SM>>>,
    ) -> RequestVoteResponse {
        let (current_term, vote_granted) = {
            let mut state = state.lock().await;

            if req.term > state.current_term {
                state.current_term = req.term;
                state.role = Role::Follower;
                state.voted_for = None;
                if let Err(e) = state.persist().await {
                    tracing::error!(id=?state.id, error=?e, "Failed to persist state after term update in RequestVote");
                    return RequestVoteResponse {
                        term: state.current_term,
                        vote_granted: false,
                    };
                }
            }

            tracing::info!(id=?state.id, request.body=?req, "Command::RequestVote");

            let vote_granted = if req.term < state.current_term {
                tracing::warn!(
                    id=?state.id,
                    candidate_id=req.candidate_id,
                    req_term=req.term,
                    current_term=state.current_term,
                    "RequestVote rejected: candidate term is older"
                );
                false
            } else if state.voted_for.is_some()
                && state.voted_for.unwrap() != req.candidate_id
            {
                tracing::warn!(
                    id=?state.id,
                    candidate_id=req.candidate_id,
                    voted_for=?state.voted_for,
                    "RequestVote rejected: already voted for another candidate"
                );
                false
            } else {
                // check the log is latest
                let last_log_term = state.get_last_log_term();
                let last_log_idx = state.get_last_log_idx();

                let log_is_up_to_date = if req.last_log_term != last_log_term {
                    req.last_log_term > last_log_term
                } else {
                    req.last_log_index >= last_log_idx
                };

                if log_is_up_to_date {
                    state.voted_for = Some(req.candidate_id);
                    if let Err(e) = state.persist().await {
                        tracing::error!(id=?state.id, error=?e, "Failed to persist state after voting");
                        return RequestVoteResponse {
                            term: state.current_term,
                            vote_granted: false,
                        };
                    }
                    true
                } else {
                    tracing::warn!(
                        id=?state.id,
                        candidate_id=req.candidate_id,
                        req_last_log_term=req.last_log_term,
                        req_last_log_index=req.last_log_index,
                        last_log_term=last_log_term,
                        last_log_idx=last_log_idx,
                        "RequestVote rejected: candidate's log is not up-to-date"
                    );
                    false
                }
            };

            (state.current_term, vote_granted)
        };

        RequestVoteResponse {
            term: current_term,
            vote_granted,
        }
    }
    async fn rpc_handler(
        state: Arc<Mutex<RaftState<T, SM>>>,
        mut rx: mpsc::Receiver<Command>,
        heartbeat_tx: mpsc::UnboundedSender<(u32, u32)>,
    ) -> anyhow::Result<()> {
        while let Some(command) = rx.recv().await {
            match command {
                Command::AppendEntries(req, tx) => {
                    let resp =
                        Self::append_entries(&req, Arc::clone(&state)).await?;
                    let (id, current_term) = {
                        let state = state.lock().await;
                        (state.id, state.current_term)
                    };
                    tx.send(resp)
                        .expect("failed to send append entries response");

                    // if we get the heartbeat from leader,
                    // we need to:
                    //   change the state from candidate to follower
                    //   reset the election timer
                    heartbeat_tx
                        .send((id, current_term))
                        .expect("failed to send append notification");
                }
                Command::RequestVote(req, tx) => {
                    let resp =
                        Self::request_vote(&req, Arc::clone(&state)).await;
                    tx.send(resp)
                        .expect("failed to send append entries response");
                }
            }
        }
        Ok(())
    }
    async fn connect_to_peer(
        &mut self,
        addr: SocketAddr,
    ) -> anyhow::Result<()> {
        let transport =
            tarpc::serde_transport::tcp::connect(addr, Json::default).await?;
        let client =
            RaftRpcClient::new(client::Config::default(), transport).spawn();
        self.peers.insert(addr, client);
        Ok(())
    }

    async fn setup(&mut self, servers: Vec<SocketAddr>) -> anyhow::Result<()> {
        let node_id = self.state.lock().await.id;

        let peers_to_connect: Vec<_> = servers
            .iter()
            .filter(|&&server| server != self.server_addr)
            .copied()
            .collect();

        let mut retry_count = 0;
        let max_retries = 30; // Maximum 30 retries (about 3 seconds)

        while self.peers.len() < peers_to_connect.len()
            && retry_count < max_retries
        {
            for &server in &peers_to_connect {
                if self.peers.contains_key(&server) {
                    continue;
                }

                tracing::info!(
                    "Node {} attempting to connect to peer {:?} (attempt {}/{})",
                    node_id,
                    server,
                    retry_count + 1,
                    max_retries
                );

                match self.connect_to_peer(server).await {
                    Ok(_) => {
                        tracing::info!(
                            "Node {} successfully connected to peer {:?}",
                            node_id,
                            server
                        );
                    }
                    Err(e) => {
                        tracing::debug!(
                            "Node {} connection to {:?} failed: {:?}",
                            node_id,
                            server,
                            e
                        );
                    }
                }
            }

            if self.peers.len() < peers_to_connect.len() {
                tokio::time::sleep(Duration::from_millis(100)).await;
                retry_count += 1;
            }
        }

        if self.peers.len() < peers_to_connect.len() {
            tracing::error!(
                "Node {} failed to connect to all peers. Connected: {}/{}",
                node_id,
                self.peers.len(),
                peers_to_connect.len()
            );
        } else {
            tracing::info!(
                "Node {} successfully connected to all {} peers",
                node_id,
                self.peers.len()
            );
        }

        // Verify connections
        tracing::info!(
            "Node {} verifying connections to {} peers",
            node_id,
            self.peers.len()
        );
        let mut failed_peers = Vec::new();

        for (addr, client) in self.peers.clone() {
            let mut ctx = tarpc::context::current();
            ctx.deadline = Instant::now() + Duration::from_secs(2);
            match client.echo(ctx, "connection_check".to_string()).await {
                Ok(_resp) => {
                    tracing::info!(
                        "Node {} connection to {:?} verified",
                        node_id,
                        addr
                    );
                }
                Err(e) => {
                    tracing::error!(
                        "Node {} connection to {:?} verification failed: {:?}",
                        node_id,
                        addr,
                        e
                    );
                    failed_peers.push(addr);
                }
            }
        }

        // Reconnect to failed peers
        for addr in failed_peers {
            tracing::info!(
                "Node {} attempting to reconnect to {:?}",
                node_id,
                addr
            );
            match self.connect_to_peer(addr).await {
                Ok(_) => {
                    tracing::info!(
                        "Node {} successfully reconnected to {:?}",
                        node_id,
                        addr
                    );
                }
                Err(e) => {
                    tracing::error!(
                        "Node {} failed to reconnect to {:?}: {:?}",
                        node_id,
                        addr,
                        e
                    );
                }
            }
        }

        tracing::info!(
            "Node {} setup completed, {} peer connections established",
            node_id,
            self.peers.len()
        );
        Ok(())
    }
    async fn main(mut self, servers: Vec<SocketAddr>) -> anyhow::Result<()> {
        self.setup(servers).await?;

        loop {
            let role = {
                let state = self.state.lock().await;
                state.role
            };
            match role {
                Role::Follower => self.run_follower().await,
                Role::Candidate => self.run_candidate().await,
                Role::Leader => self.run_leader().await,
            }?
        }
    }
    async fn run_follower(&mut self) -> anyhow::Result<()> {
        let timeout = self.config.election_timeout;
        let watchdog = WatchDog::default();

        // 最初に実行されないように wait して timeout を設定する
        watchdog.wait().await;
        watchdog.reset(timeout).await;

        loop {
            if !matches!(self.state.lock().await.role, Role::Follower) {
                break;
            }
            tokio::select! {
                Some(_) = self.c.heartbeat_rx.recv() => {
                    watchdog.reset(timeout).await;
                }
                _ = watchdog.wait() => {
                    watchdog.reset(timeout).await;
                    self.become_candidate().await?;
                    break;
                }
            }
        }
        Ok(())
    }
    async fn run_candidate(&mut self) -> anyhow::Result<()> {
        let timeout = self.config.election_timeout;
        let watchdog = WatchDog::default();
        loop {
            if !matches!(self.state.lock().await.role, Role::Candidate) {
                break;
            }
            tokio::select! {
                Some((id, term)) = self.c.heartbeat_rx.recv() => {
                    let current_term = self.state.lock().await.current_term;
                    if term >= current_term {
                        // the headbeat is accepted in candidate state
                        // requester is a new leader
                        // and this node become a follower
                        self.state.lock().await.leader_id = Some(id);
                        self.become_follower().await?;
                    }
                },
                _ = watchdog.wait() => {
                    watchdog.reset(timeout).await;
                    self.start_election().await?;
                }
            }
        }
        Ok(())
    }
    async fn run_leader(&mut self) -> anyhow::Result<()> {
        let timeout = self.config.heartbeat_interval;
        let watchdog = WatchDog::default();
        loop {
            if !matches!(self.state.lock().await.role, Role::Leader) {
                break;
            }
            tokio::select! {
                Some(order) = self.c.client_rx.recv() => {
                    let mut state = self.state.lock().await;
                    let term = state.current_term;
                    state.log.push(raft::Entry {
                        term,
                        command: order,
                    });
                    if let Err(e) = state.persist().await {
                        tracing::error!(id=?state.id, error=?e, "Failed to persist state after appending log entry");
                    }
                },
                _ = watchdog.wait() => {
                    self.broadcast_heartbeat().await?;
                    let mut state = self.state.lock().await;
                    if state.commit_index > state.last_applied {
                        state.apply_committed().await?;
                    }
                    watchdog.reset(timeout).await;
                }
            }
        }
        Ok(())
    }
    async fn start_election(&mut self) -> anyhow::Result<()> {
        let mut responses: Vec<RequestVoteResponse> = Vec::new();
        let (
            current_term,
            candidate_id,
            last_log_index,
            last_log_term,
            voted_for,
        ) = {
            let state = self.state.lock().await;
            tracing::info!(id=?state.id, state=?state, "start_election");
            // vote for myself
            responses.push(RequestVoteResponse {
                term: state.current_term,
                vote_granted: true,
            });
            (
                state.current_term,
                state.id,
                state.get_last_log_idx(),
                state.get_last_log_term(),
                state.voted_for,
            )
        };

        let is_granted =
            { voted_for.is_none() || (voted_for.unwrap() == candidate_id) };

        if !is_granted {
            tracing::info!(
                id = candidate_id,
                "vote not granted, become follower"
            );
            self.become_follower().await?;
            return Ok(());
        }

        let peers: Vec<_> = self
            .peers
            .iter()
            .map(|(addr, client)| (*addr, client.clone()))
            .collect();
        let rpc_timeout = self.config.rpc_timeout;

        tracing::info!(
            candidate_id = candidate_id,
            peer_count = peers.len(),
            "sending request_vote to peers"
        );

        // TODO: should we send in gossip protocol?
        let mut tasks = JoinSet::new();
        for (addr, client) in peers {
            let req = RequestVoteRequest {
                term: current_term,
                candidate_id,
                last_log_index,
                last_log_term,
            };
            tasks.spawn(Self::send_request_vote(
                addr,
                client,
                req,
                rpc_timeout,
            ));
        }

        while let Some(result) = tasks.join_next().await {
            match result? {
                Ok(res) => {
                    responses.push(res);
                }
                Err(e) => {
                    tracing::warn!(
                        candidate_id = candidate_id,
                        error = ?e,
                        "Failed to send request vote"
                    );
                    continue;
                }
            }
        }

        self.handle_election(responses).await?;

        Ok(())
    }

    async fn handle_election(
        &mut self,
        responses: Vec<RequestVoteResponse>,
    ) -> anyhow::Result<()> {
        let (id, current_term) = {
            let state = self.state.lock().await;
            (state.id, state.current_term)
        };
        tracing::info!(id=id, responses=?responses, "election handled");

        let new_terms: Vec<_> =
            responses.iter().filter(|r| r.term > current_term).collect();
        if !new_terms.is_empty() {
            tracing::info!(
                id = &id,
                term = &current_term,
                "newer term was discovered"
            );
            self.state.lock().await.current_term =
                new_terms.first().unwrap().term;
            self.become_follower().await?;
            return Ok(());
        }

        // 投票が過半数か (peers + 自分自身)
        let total_nodes = self.peers.len() + 1;
        let vote_granted = responses.iter().filter(|r| r.vote_granted).count()
            > total_nodes / 2;

        if vote_granted {
            tracing::info!(
                id = id,
                voted_count =
                    responses.iter().filter(|r| r.vote_granted).count(),
                "vote granted, become leader"
            );

            self.become_leader().await?;
        }

        Ok(())
    }
    async fn become_leader(&mut self) -> anyhow::Result<()> {
        let mut state = self.state.lock().await;
        state.role = Role::Leader;
        state.leader_id = Some(state.id);

        let last_log_idx = state.get_last_log_idx();
        for peer_addr in self.peers.keys() {
            state.next_index.insert(*peer_addr, last_log_idx + 1);
            state.match_index.insert(*peer_addr, 0);
        }
        Ok(())
    }
    async fn become_follower(&mut self) -> anyhow::Result<()> {
        let mut state = self.state.lock().await;
        state.role = Role::Follower;
        state.voted_for = None;
        Ok(())
    }
    async fn become_candidate(&mut self) -> anyhow::Result<()> {
        let mut state = self.state.lock().await;
        state.role = Role::Candidate;
        state.current_term += 1;
        state.voted_for = Some(state.id); // Vote for self
        state.persist().await?;
        Ok(())
    }
    async fn send_request_vote(
        peer_addr: SocketAddr,
        client: RaftRpcClient,
        req: RequestVoteRequest,
        rpc_timeout: Duration,
    ) -> anyhow::Result<RequestVoteResponse> {
        let mut ctx = tarpc::context::current();
        ctx.deadline = Instant::now() + rpc_timeout;

        client
            .request_vote(ctx, req.clone())
            .instrument(tracing::info_span!(
                "request vote from candidate {}",
                req.candidate_id
            ))
            .await
            .map_err(|e| {
                tracing::warn!(
                    candidate_id = req.candidate_id,
                    peer_addr = ?peer_addr,
                    error = ?e,
                    "request_vote RPC failed to peer"
                );
                e.into()
            })
    }
    /// replicate logs to each peer
    async fn broadcast_heartbeat(&mut self) -> anyhow::Result<bool> {
        let (leader_term, leader_id, leader_commit) = {
            let state = self.state.lock().await;
            (
                state.current_term,
                state.leader_id.unwrap(),
                state.commit_index,
            )
        };
        let mut requests = Vec::new();
        for (addr, client) in self
            .peers
            .iter()
            .map(|(addr, client)| (addr, client.clone()))
        {
            let state = self.state.lock().await;
            let next_idx = state.next_index.get(addr).copied().unwrap_or(1);
            let (prev_log_idx, prev_log_term) = if next_idx > 1 {
                (next_idx - 1, state.log[(next_idx - 2) as usize].term)
            } else {
                (0, 0)
            };
            let entries: Vec<LogEntry> = state
                .log
                .iter()
                .skip((next_idx - 1) as usize) // send logs after next_idx
                .map(|e| {
                    let command =
                        bincode::serialize(&e.command).unwrap_or_default();
                    LogEntry {
                        term: e.term,
                        command,
                    }
                })
                .collect();
            let sent_up_to_index = prev_log_idx + entries.len() as u32;
            requests.push((
                addr,
                client,
                prev_log_idx,
                prev_log_term,
                entries,
                sent_up_to_index,
            ));
        }
        let rpc_timeout = self.config.rpc_timeout;

        // TODO: optimize algorithm: should be gossip?
        let mut tasks = JoinSet::new();
        for (
            addr,
            client,
            prev_log_index,
            prev_log_term,
            entries,
            sent_up_to_index,
        ) in requests
        {
            let req = AppendEntriesRequest {
                term: leader_term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            };
            tasks.spawn(Self::send_heartbeat(
                *addr,
                client,
                req,
                sent_up_to_index,
                rpc_timeout,
            ));
        }

        while let Some(result) = tasks.join_next().await {
            match result? {
                Ok((server, res, sent_up_to_index)) => {
                    self.handle_heartbeat(server, res, sent_up_to_index)
                        .await?;
                }
                Err(e) => {
                    tracing::warn!("Failed to send heartbeat: {:?}", e);
                }
            }
        }

        Ok(true)
    }
    async fn send_heartbeat(
        server: SocketAddr,
        client: RaftRpcClient,
        req: AppendEntriesRequest,
        sent_up_to_index: u32,
        rpc_timeout: Duration,
    ) -> anyhow::Result<(SocketAddr, AppendEntriesResponse, u32)> {
        let mut ctx = tarpc::context::current();
        ctx.deadline = Instant::now() + rpc_timeout;
        let res = client
            .append_entries(ctx, req.clone())
            .instrument(tracing::info_span!("append entries to {server}"))
            .await?;
        Ok((server, res, sent_up_to_index))
    }
    fn new_commit_index(
        current_commit_index: u32,
        current_term: u32,
        log: &[raft::Entry<T>],
        match_index: &HashMap<SocketAddr, u32>,
        peer_count: usize,
    ) -> u32 {
        let log_len = log.len() as u32;
        let total_nodes = peer_count + 1;

        let mut new_commit_index = current_commit_index;
        for n in (current_commit_index + 1)..=log_len {
            if log[(n - 1) as usize].term != current_term {
                continue;
            }
            let mut count = 1; // Leader always has the entry
            for match_idx in match_index.values() {
                if *match_idx >= n {
                    count += 1;
                }
            }
            if count > total_nodes / 2 {
                new_commit_index = n;
            }
        }
        new_commit_index
    }

    /// Update commit_index based on match_index according to Raft Figure 2:
    /// "If there exists an N such that N > commitIndex, a majority of
    /// matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N"
    async fn update_commit_index(&mut self) -> anyhow::Result<()> {
        let mut state = self.state.lock().await;

        // Only leaders can update commit_index this way
        if !matches!(state.role, Role::Leader) {
            return Ok(());
        }

        let current_commit_index = state.commit_index;
        let new_commit_index = Self::new_commit_index(
            state.commit_index,
            state.current_term,
            &state.log,
            &state.match_index,
            self.peers.len(),
        );

        // Update commit_index if we found a higher value
        if new_commit_index > current_commit_index {
            state.commit_index = new_commit_index;
            tracing::info!(
                id=?state.id,
                old_commit_index=current_commit_index,
                new_commit_index=new_commit_index,
                "Updated commit_index: entry replicated on majority"
            );
        }

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn handle_heartbeat(
        &mut self,
        addr: SocketAddr,
        res: AppendEntriesResponse,
        sent_up_to_index: u32,
    ) -> anyhow::Result<()> {
        let check_term = {
            let state = self.state.lock().await;
            if res.term > state.current_term {
                tracing::info!(id=?state.id, "become follower because of term mismatch");
                true
            } else {
                false
            }
        };

        if check_term {
            self.state.lock().await.current_term = res.term;
            self.become_follower().await?;
            return Ok(());
        }

        let (id, role, current_term) = {
            let state = self.state.lock().await;
            (state.id, state.role, state.current_term)
        };
        if matches!(role, Role::Leader) && current_term != res.term {
            return Ok(());
        }

        {
            let mut state = self.state.lock().await;
            if res.success {
                // リクエストが成功したら index を更新する
                // prev_log_index + log_len で計算される
                state.match_index.insert(addr, sent_up_to_index);
                state.next_index.insert(addr, sent_up_to_index + 1);

                tracing::debug!(
                    id=?id,
                    peer=?addr,
                    match_index=sent_up_to_index,
                    next_index=sent_up_to_index + 1,
                    "AppendEntries succeeded"
                );
            } else {
                let current_next_idx =
                    state.next_index.get(&addr).copied().unwrap_or(1);
                let new_next_idx = current_next_idx.saturating_sub(1).max(1);
                state.next_index.insert(addr, new_next_idx);

                tracing::warn!(
                    id=?id,
                    peer=?addr,
                    old_next_index=current_next_idx,
                    new_next_index=new_next_idx,
                    "AppendEntries rejected, decrementing next_index"
                );
            }
        }

        self.update_commit_index().await?;
        Ok(())
    }
}

#[derive(Clone)]
struct RaftServer {
    tx: mpsc::Sender<Command>,
}

impl RaftRpc for RaftServer {
    async fn echo(self, _: tarpc::context::Context, name: String) -> String {
        format!("echo: {name}")
    }
    async fn append_entries(
        self,
        _: tarpc::context::Context,
        req: AppendEntriesRequest,
    ) -> AppendEntriesResponse {
        let (tx, rx) = oneshot::channel::<AppendEntriesResponse>();
        self.tx.send(Command::AppendEntries(req, tx)).await.unwrap();
        rx.await.unwrap()
    }

    async fn request_vote(
        self,
        _: tarpc::context::Context,
        req: RequestVoteRequest,
    ) -> RequestVoteResponse {
        let (tx, rx) = oneshot::channel::<RequestVoteResponse>();
        self.tx.send(Command::RequestVote(req, tx)).await.unwrap();
        rx.await.unwrap()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn check_leader_state_after_become_leader() -> anyhow::Result<()> {
        let mut node =
            Node::new(10101, Config::default(), create_test_state_machine());
        node.become_leader().await?;
        let state = node.state.lock().await;
        assert_eq!(state.role, Role::Leader);
        assert_eq!(state.leader_id, Some(state.id));
        Ok(())
    }

    #[tokio::test]
    async fn check_leader_state_after_become_candidate() -> anyhow::Result<()> {
        let mut node =
            Node::new(10101, Config::default(), create_test_state_machine());
        let term = node.state.lock().await.current_term;
        node.become_candidate().await?;
        let state = node.state.lock().await;
        assert_eq!(state.role, Role::Candidate);
        assert_eq!(state.current_term, term + 1);
        Ok(())
    }
    #[tokio::test]
    async fn check_leader_state_after_become_follower() -> anyhow::Result<()> {
        let mut node =
            Node::new(10101, Config::default(), create_test_state_machine());
        node.become_follower().await?;
        let state = node.state.lock().await;
        assert_eq!(state.role, Role::Follower);
        Ok(())
    }
    #[tokio::test(start_paused = true)]
    async fn election_must_be_done_with_not_candidate() -> anyhow::Result<()> {
        let mut node =
            Node::new(10101, Config::default(), create_test_state_machine());
        node.become_candidate().await?;
        node.run_candidate().await?;
        assert_ne!(node.state.lock().await.role, Role::Candidate);
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn test_follower_becomes_candidate_after_election_timeout()
    -> anyhow::Result<()> {
        let config = Config {
            election_timeout: Duration::from_millis(2000),
            ..Default::default()
        };
        let mut node = Node::new(10101, config, create_test_state_machine());
        let result = tokio::spawn(async move { node.run_follower().await });
        // election timeoutを超える時間（2100ms）進める
        tokio::time::advance(Duration::from_millis(2100)).await;
        // スケジューラに制御を渡してwatchdogのタイムアウトを発火させる
        tokio::task::yield_now().await;
        assert!(result.await.is_ok());
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn test_candidate_calls_start_election() -> anyhow::Result<()> {
        let config = Config {
            rpc_timeout: Duration::from_millis(500),
            ..Default::default()
        };
        let mut node = Node::new(10101, config, create_test_state_machine());
        node.become_candidate().await?;
        let result = tokio::spawn(async move { node.run_candidate().await });
        // 100ms進めてstart_electionが呼ばれる時間を与える
        tokio::time::advance(Duration::from_millis(100)).await;
        // スケジューラに制御を渡してタスクを実行させる
        tokio::task::yield_now().await;
        assert!(result.await.is_ok());
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn test_follower_multiple_heartbeat_resets() -> anyhow::Result<()> {
        let config = Config {
            election_timeout: Duration::from_millis(1000),
            ..Default::default()
        };
        let mut node = Node::new(10101, config, create_test_state_machine());

        let heartbeat_tx = node.c.heartbeat_tx.clone();
        let result = tokio::spawn(async move { node.run_follower().await });

        // 1回目: 800ms経過後にheartbeat
        tokio::time::advance(Duration::from_millis(800)).await;
        tokio::task::yield_now().await;
        assert!(!result.is_finished());

        heartbeat_tx.send((1, 1)).unwrap();
        tokio::task::yield_now().await;

        // 2回目: さらに800ms経過後にheartbeat（リセットから800ms）
        tokio::time::advance(Duration::from_millis(800)).await;
        tokio::task::yield_now().await;
        assert!(!result.is_finished());

        heartbeat_tx.send((1, 1)).unwrap();
        tokio::task::yield_now().await;

        // 3回目: さらに800ms経過後にheartbeat（リセットから800ms）
        tokio::time::advance(Duration::from_millis(800)).await;
        tokio::task::yield_now().await;
        assert!(!result.is_finished());

        heartbeat_tx.send((1, 1)).unwrap();
        tokio::task::yield_now().await;

        // heartbeat停止: 1100ms経過でタイムアウト
        tokio::time::advance(Duration::from_millis(1100)).await;
        tokio::task::yield_now().await;
        assert!(result.is_finished());

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn test_follower_heartbeat_prevents_timeout() -> anyhow::Result<()> {
        let config = Config {
            election_timeout: Duration::from_millis(1000),
            ..Default::default()
        };
        let mut node = Node::new(10101, config, create_test_state_machine());

        let heartbeat_tx = node.c.heartbeat_tx.clone();
        let result = tokio::spawn(async move { node.run_follower().await });

        // 1回目: 800ms経過後にheartbeat
        tokio::time::advance(Duration::from_millis(800)).await;
        tokio::task::yield_now().await;
        assert!(!result.is_finished());

        heartbeat_tx.send((1, 1)).unwrap();
        tokio::task::yield_now().await;

        // 2回目: さらに800ms経過後にheartbeat（リセットから800ms）
        tokio::time::advance(Duration::from_millis(800)).await;
        tokio::task::yield_now().await;
        assert!(!result.is_finished());

        heartbeat_tx.send((1, 1)).unwrap();
        tokio::task::yield_now().await;

        // heartbeat停止: 1100ms経過でタイムアウト
        tokio::time::advance(Duration::from_millis(1100)).await;
        tokio::task::yield_now().await;
        assert!(result.is_finished());

        Ok(())
    }

    fn create_test_storage<T: Send + Sync + Clone + 'static>()
    -> Box<dyn crate::storage::Storage<T>> {
        Box::new(crate::storage::MemStorage::default())
    }

    fn create_test_state_machine() -> crate::statemachine::NoOpStateMachine {
        crate::statemachine::NoOpStateMachine::default()
    }

    #[tokio::test]
    async fn test_append_entries_with_higher_term_converts_to_follower()
    -> anyhow::Result<()> {
        let mut initial_state = RaftState::new(
            1,
            create_test_storage(),
            create_test_state_machine(),
        );
        initial_state.current_term = 50;
        initial_state.role = Role::Leader;
        let state = Arc::new(Mutex::new(initial_state));

        let (cmd_tx, cmd_rx) = mpsc::channel::<Command>(32);
        let (heartbeat_tx, _heartbeat_rx) = mpsc::unbounded_channel();
        let state_clone = Arc::clone(&state);

        tokio::spawn(async move {
            Node::rpc_handler(state_clone, cmd_rx, heartbeat_tx).await
        });

        let (resp_tx, resp_rx) = oneshot::channel();
        let req = AppendEntriesRequest {
            term: 100,
            leader_id: 99999,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: Vec::new(),
            leader_commit: 0,
        };

        cmd_tx.send(Command::AppendEntries(req, resp_tx)).await?;

        let _response = resp_rx.await?;

        // termが更新され、followerに転向しているべき
        let final_state = state.lock().await;
        assert_eq!(final_state.current_term, 100);
        assert_eq!(final_state.role, Role::Follower);

        Ok(())
    }

    #[tokio::test]
    async fn test_request_vote_with_higher_term_converts_to_follower()
    -> anyhow::Result<()> {
        let mut initial_state = RaftState::new(
            1,
            create_test_storage(),
            create_test_state_machine(),
        );
        initial_state.current_term = 50;
        initial_state.role = Role::Leader;
        let state = Arc::new(Mutex::new(initial_state));

        let (cmd_tx, cmd_rx) = mpsc::channel::<Command>(32);
        let (heartbeat_tx, _heartbeat_rx) = mpsc::unbounded_channel();
        let state_clone = Arc::clone(&state);

        tokio::spawn(async move {
            Node::rpc_handler(state_clone, cmd_rx, heartbeat_tx).await
        });

        let (resp_tx, resp_rx) = oneshot::channel();
        let req = RequestVoteRequest {
            term: 100,
            candidate_id: 99999,
            last_log_index: 0,
            last_log_term: 0,
        };

        cmd_tx.send(Command::RequestVote(req, resp_tx)).await?;

        let _response = resp_rx.await?;

        // termが更新され、followerに転向しているべき
        let final_state = state.lock().await;
        assert_eq!(final_state.current_term, 100);
        assert_eq!(final_state.role, Role::Follower);

        Ok(())
    }

    #[tokio::test]
    async fn test_request_vote_rejected_by_older_log_term() -> anyhow::Result<()>
    {
        let mut initial_state = RaftState::new(
            1,
            create_test_storage(),
            create_test_state_machine(),
        );
        initial_state.current_term = 10;
        initial_state.log.push(raft::Entry {
            term: 5,
            command: bytes::Bytes::new(),
        });
        let state = Arc::new(Mutex::new(initial_state));

        // a request which have a old term log
        let req = RequestVoteRequest {
            term: 10,
            candidate_id: 2,
            last_log_index: 1,
            last_log_term: 3,
        };

        let response = Node::request_vote(&req, state.clone()).await;

        assert!(!response.vote_granted);
        assert_eq!(response.term, 10);

        let final_state = state.lock().await;
        assert_eq!(final_state.voted_for, None);

        Ok(())
    }

    #[tokio::test]
    async fn test_request_vote_log_index_comparison_with_same_term()
    -> anyhow::Result<()> {
        // ケース1: 同じterm、同じindex → 投票される
        let mut state1 = RaftState::new(
            1,
            create_test_storage(),
            create_test_state_machine(),
        );
        state1.current_term = 10;
        state1.log.push(raft::Entry {
            term: 5,
            command: bytes::Bytes::new(),
        });
        state1.log.push(raft::Entry {
            term: 5,
            command: bytes::Bytes::new(),
        });
        state1.log.push(raft::Entry {
            term: 5,
            command: bytes::Bytes::new(),
        });
        let state1 = Arc::new(Mutex::new(state1));
        let req1 = RequestVoteRequest {
            term: 10,
            candidate_id: 2,
            last_log_index: 3,
            last_log_term: 5,
        };
        let response1 = Node::request_vote(&req1, state1.clone()).await;
        assert!(
            response1.vote_granted,
            "Same term and same index should grant vote"
        );
        assert_eq!(state1.lock().await.voted_for, Some(2));

        // ケース2: 同じterm、候補者のindexが長い → 投票される
        let mut state2 = RaftState::new(
            1,
            create_test_storage(),
            create_test_state_machine(),
        );
        state2.current_term = 10;
        state2.log.push(raft::Entry {
            term: 5,
            command: bytes::Bytes::new(),
        });
        state2.log.push(raft::Entry {
            term: 5,
            command: bytes::Bytes::new(),
        });
        state2.log.push(raft::Entry {
            term: 5,
            command: bytes::Bytes::new(),
        });
        let state2 = Arc::new(Mutex::new(state2));
        let req2 = RequestVoteRequest {
            term: 10,
            candidate_id: 3,
            last_log_index: 5,
            last_log_term: 5,
        };
        let response2 = Node::request_vote(&req2, state2.clone()).await;
        assert!(
            response2.vote_granted,
            "Same term and longer index should grant vote"
        );
        assert_eq!(state2.lock().await.voted_for, Some(3));

        // ケース3: 同じterm、候補者のindexが短い → 拒否される
        let mut state3 = RaftState::new(
            1,
            create_test_storage(),
            create_test_state_machine(),
        );
        state3.current_term = 10;
        state3.log.push(raft::Entry {
            term: 5,
            command: bytes::Bytes::new(),
        });
        state3.log.push(raft::Entry {
            term: 5,
            command: bytes::Bytes::new(),
        });
        state3.log.push(raft::Entry {
            term: 5,
            command: bytes::Bytes::new(),
        });
        let state3 = Arc::new(Mutex::new(state3));
        let req3 = RequestVoteRequest {
            term: 10,
            candidate_id: 4,
            last_log_index: 2,
            last_log_term: 5,
        };
        let response3 = Node::request_vote(&req3, state3.clone()).await;
        assert!(
            !response3.vote_granted,
            "Same term and shorter index should reject vote"
        );
        assert_eq!(state3.lock().await.voted_for, None);

        Ok(())
    }

    #[tokio::test]
    async fn test_append_entries_conflict_resolution() -> anyhow::Result<()> {
        // フォロワーの初期ログ: [entry1(term=1), entry2(term=1), entry3(term=2), entry4(term=2)]
        let mut follower_state = RaftState::new(
            1,
            create_test_storage(),
            create_test_state_machine(),
        );
        follower_state.current_term = 3;
        follower_state.log.push(raft::Entry {
            term: 1,
            command: bytes::Bytes::from(&b"cmd1"[..]),
        });
        follower_state.log.push(raft::Entry {
            term: 1,
            command: bytes::Bytes::from(&b"cmd2"[..]),
        });
        follower_state.log.push(raft::Entry {
            term: 2,
            command: bytes::Bytes::from(&b"cmd3_old"[..]),
        });
        follower_state.log.push(raft::Entry {
            term: 2,
            command: bytes::Bytes::from(&b"cmd4_old"[..]),
        });
        let state = Arc::new(Mutex::new(follower_state));

        // リーダーからのリクエスト: prev_log_term: 1, prev_logprev_log_index=2, entries=[entry3(term=3), entry4(term=3), entry5(term=3)]
        let req = AppendEntriesRequest {
            term: 3,
            leader_id: 2,
            prev_log_index: 2,
            prev_log_term: 1,
            entries: vec![
                LogEntry {
                    term: 3,
                    command: bincode::serialize(&bytes::Bytes::from(
                        &b"cmd3_new"[..],
                    ))
                    .unwrap(),
                },
                LogEntry {
                    term: 3,
                    command: bincode::serialize(&bytes::Bytes::from(
                        &b"cmd4_new"[..],
                    ))
                    .unwrap(),
                },
                LogEntry {
                    term: 3,
                    command: bincode::serialize(&bytes::Bytes::from(
                        &b"cmd5_new"[..],
                    ))
                    .unwrap(),
                },
            ],
            leader_commit: 0,
        };

        let response = Node::append_entries(&req, state.clone()).await?;

        // レスポンスは成功
        assert!(response.success);
        assert_eq!(response.term, 3);

        // ログの検証
        let final_state = state.lock().await;
        assert_eq!(final_state.log.len(), 5, "Log should have 5 entries");

        // 最初の2つは変更なし
        assert_eq!(final_state.log[0].term, 1);
        assert_eq!(final_state.log[0].command.as_ref(), b"cmd1");
        assert_eq!(final_state.log[1].term, 1);
        assert_eq!(final_state.log[1].command.as_ref(), b"cmd2");

        // 競合したエントリは新しいもので置き換えられている
        assert_eq!(final_state.log[2].term, 3);
        assert_eq!(final_state.log[2].command.as_ref(), b"cmd3_new");
        assert_eq!(final_state.log[3].term, 3);
        assert_eq!(final_state.log[3].command.as_ref(), b"cmd4_new");

        // 新しいエントリが追加されている
        assert_eq!(final_state.log[4].term, 3);
        assert_eq!(final_state.log[4].command.as_ref(), b"cmd5_new");

        Ok(())
    }

    #[tokio::test]
    async fn test_append_entries_no_conflict_append_only() -> anyhow::Result<()>
    {
        // フォロワーの初期ログ: [entry1(term=1), entry2(term=1)]
        let mut follower_state = RaftState::new(
            1,
            create_test_storage(),
            create_test_state_machine(),
        );
        follower_state.current_term = 2;
        follower_state.log.push(raft::Entry {
            term: 1,
            command: bytes::Bytes::from(&b"cmd1"[..]),
        });
        follower_state.log.push(raft::Entry {
            term: 1,
            command: bytes::Bytes::from(&b"cmd2"[..]),
        });
        let state = Arc::new(Mutex::new(follower_state));

        // リーダーからのリクエスト: prev_log_term: 1, prev_log_index=2, entries=[entry3(term=2), entry4(term=2)]
        let req = AppendEntriesRequest {
            term: 2,
            leader_id: 2,
            prev_log_index: 2,
            prev_log_term: 1,
            entries: vec![
                LogEntry {
                    term: 2,
                    command: bincode::serialize(&bytes::Bytes::from(
                        &b"cmd3"[..],
                    ))
                    .unwrap(),
                },
                LogEntry {
                    term: 2,
                    command: bincode::serialize(&bytes::Bytes::from(
                        &b"cmd4"[..],
                    ))
                    .unwrap(),
                },
            ],
            leader_commit: 0,
        };

        let response = Node::append_entries(&req, state.clone()).await?;

        // レスポンスは成功
        assert!(response.success);

        // ログの検証: 新しいエントリが追加されている
        let final_state = state.lock().await;
        assert_eq!(final_state.log.len(), 4);
        assert_eq!(final_state.log[2].term, 2);
        assert_eq!(final_state.log[2].command.as_ref(), b"cmd3");
        assert_eq!(final_state.log[3].term, 2);
        assert_eq!(final_state.log[3].command.as_ref(), b"cmd4");

        Ok(())
    }

    #[tokio::test]
    async fn test_commit_only_current_term_entries() -> anyhow::Result<()> {
        use std::net::{IpAddr, Ipv4Addr, SocketAddr};

        let node =
            Node::new(10101, Config::default(), create_test_state_machine());

        // 3ノードクラスタを想定（リーダー + 2ピア）
        let peer1 = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 10102);
        let peer2 = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 10103);

        // ログエントリを追加: 古いterm と 現在のterm
        {
            let mut state = node.state.lock().await;
            state.role = Role::Leader;
            state.current_term = 5;
            state.commit_index = 0;

            // 古いterm=3のエントリ
            state.log.push(raft::Entry {
                term: 3,
                command: bytes::Bytes::from("old_term_cmd1"),
            });
            state.log.push(raft::Entry {
                term: 3,
                command: bytes::Bytes::from("old_term_cmd2"),
            });

            // 現在のterm=5のエントリ
            state.log.push(raft::Entry {
                term: 5,
                command: bytes::Bytes::from("current_term_cmd1"),
            });
            state.log.push(raft::Entry {
                term: 5,
                command: bytes::Bytes::from("current_term_cmd2"),
            });

            // match_indexを設定: すべてのエントリが過半数に複製されている
            state.match_index.insert(peer1, 4);
            state.match_index.insert(peer2, 4);
        }

        // new_commit_index()を直接テスト
        let new_commit_index = {
            let state = node.state.lock().await;
            Node::<bytes::Bytes, crate::statemachine::NoOpStateMachine>::new_commit_index(
                state.commit_index,
                state.current_term,
                &state.log,
                &state.match_index,
                2, // peer_count = 2
            )
        };

        // 期待値: term=3のエントリ（index 1,2）はスキップされ、
        // term=5のエントリ（index 3,4）のみがコミット対象
        // → commit_index = 4
        assert_eq!(
            new_commit_index, 4,
            "Should only commit current term entries (term=5), not old term entries (term=3)"
        );

        Ok(())
    }
}
