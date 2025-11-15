use crate::raft::{RaftState, Role};
use crate::rpc::*;
use futures::{future, prelude::*};
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use tarpc::{
    client,
    server::{self, Channel, incoming::Incoming},
    tokio_serde::formats::Json,
};
use tokio::{
    sync::{Mutex, Notify, mpsc, oneshot},
    time::Instant,
};

pub enum Command {
    AppendEntries(AppendEntriesRequest, oneshot::Sender<AppendEntriesResponse>),
    RequestVote(RequestVoteRequest, oneshot::Sender<RequestVoteResponse>),
}

pub enum Response {
    AppendEntries(Option<AppendEntriesResponse>),
    RequestVote(Option<RequestVoteResponse>),
}

pub struct Config {
    pub servers: Vec<SocketAddr>,
    pub heartbeat_interval: tokio::time::Duration,
    pub election_timeout: tokio::time::Duration,
}

pub struct Node {
    config: Config,
    clients: HashMap<SocketAddr, RaftRpcClient>,
    server_addr: SocketAddr,
    state: Arc<Mutex<RaftState>>,
    watch_dog: WatchDog,
}

pub struct WatchDog {
    last_seen: Mutex<Instant>,
    notify: Notify,
}

impl Default for WatchDog {
    fn default() -> Self {
        Self {
            last_seen: Mutex::new(Instant::now()),
            notify: Notify::new(),
        }
    }
}

impl WatchDog {
    async fn reset(&self) {
        *self.last_seen.lock().await = Instant::now();
        self.notify.notify_one();
    }
    async fn elasped(&self) -> Duration {
        Instant::now() - *self.last_seen.lock().await
    }
    async fn wait_timeout(&self, timeout: Duration) {
        loop {
            let elapsed = self.elasped().await;
            if elapsed >= timeout {
                return;
            }
            let remaining = timeout - elapsed;
            tokio::select! {
                _ = tokio::time::sleep(remaining) => {
                    return;
                }
                _ = self.notify.notified() => {
                    continue;
                }
            }
        }
    }
}
async fn spawn(fut: impl Future<Output = ()> + Send + 'static) {
    tokio::spawn(fut);
}

impl Node {
    pub fn new(port: u16, config: Config) -> Self {
        Node {
            config,
            clients: HashMap::default(),
            server_addr: SocketAddr::from((Ipv4Addr::LOCALHOST, port)),
            state: Arc::new(Mutex::new(RaftState {
                id: port as u32,
                ..Default::default()
            })),
            watch_dog: WatchDog::default(),
        }
    }
    pub async fn run(self, port: u16) -> anyhow::Result<()> {
        // main thread for test
        let (tx, mut rx) = mpsc::channel::<Command>(32);
        let state_clone = Arc::clone(&self.state);

        let main_handle = tokio::spawn(async move { self.main().await });

        let cmd_handle = tokio::spawn(async move {
            while let Some(command) = rx.recv().await {
                match command {
                    Command::AppendEntries(req, tx) => {
                        // TODO
                        tx.send(AppendEntriesResponse {
                            term: req.term,
                            success: true,
                        })
                        .expect("failed to send append entries response");
                    }
                    Command::RequestVote(req, tx) => {
                        let vote_granted = {
                            let mut state = state_clone.lock().await;
                            state.voted_for = Some(req.candidate_id);
                            println!(
                                "[node {}] Command::RequestVote: {:?}",
                                state.id, state
                            );

                            // !(term < currentTerm)
                            if req.term >= state.current_term {
                                state.voted_for = Some(req.candidate_id);
                                true
                            } else {
                                state.voted_for.is_none()
                                    || state.voted_for.unwrap()
                                        == req.candidate_id
                            }
                        };

                        tx.send(RequestVoteResponse {
                            term: req.term,
                            vote_granted,
                        })
                        .expect("failed to send request vote response");
                    }
                }
            }
        });

        // RPC thread
        let rpc_handle = tokio::spawn(async move {
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
        });
        tokio::try_join!(main_handle, rpc_handle, cmd_handle)?;

        Ok(())
    }
    async fn setup(&mut self) -> anyhow::Result<()> {
        for server in self.config.servers.clone() {
            if server == self.server_addr {
                continue;
            }
            if self.clients.contains_key(&server) {
                continue;
            }
            let transport =
                tarpc::serde_transport::tcp::connect(server, Json::default)
                    .await?;
            let client =
                RaftRpcClient::new(client::Config::default(), transport)
                    .spawn();
            self.clients.insert(server, client);
        }
        Ok(())
    }
    async fn main(mut self) -> anyhow::Result<()> {
        self.setup().await?;

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
        let mut interval = tokio::time::interval(timeout / 2);
        loop {
            if !matches!(self.state.lock().await.role, Role::Follower) {
                return Ok(());
            }
            tokio::select! {
                _ = interval.tick() => {}
                _ = self.watch_dog.wait_timeout(timeout) => {
                    self.become_candidate().await?;
                }
            }
        }
    }
    async fn run_candidate(&mut self) -> anyhow::Result<()> {
        let timeout = self.config.election_timeout;
        let mut interval = tokio::time::interval(timeout / 100);
        loop {
            if !matches!(self.state.lock().await.role, Role::Candidate) {
                return Ok(());
            }
            tokio::select! {
                _ = interval.tick() => {}
                _ = self.watch_dog.wait_timeout(timeout) => {
                    self.start_election().await?;
                }
            }
        }
    }
    async fn run_leader(&mut self) -> anyhow::Result<()> {
        let timeout = self.config.heartbeat_interval;
        let mut interval = tokio::time::interval(timeout / 2);
        loop {
            if !matches!(self.state.lock().await.role, Role::Leader) {
                return Ok(());
            }
            interval.tick().await;
            self.broadcast_heartbeat().await?;
        }
    }
    async fn start_election(&mut self) -> anyhow::Result<()> {
        println!("[node {}] start_election", self.state.lock().await.id);
        self.watch_dog.reset().await;

        let mut responses: Vec<RequestVoteResponse> = Vec::new();

        let (
            current_term,
            candidate_id,
            last_log_index,
            last_log_term,
            voted_for,
        ) = {
            let state = self.state.lock().await;
            // vote for myself
            responses.push(RequestVoteResponse {
                term: state.current_term,
                vote_granted: true,
            });
            (
                state.current_term,
                state.id,
                state.log.len() as u32,
                state.log.last().map(|e| e.term).unwrap_or(0),
                state.voted_for,
            )
        };

        for server in self.config.servers.iter() {
            if *server == self.server_addr {
                continue;
            }
            if let Some(client) = self.clients.get(server) {
                let req = RequestVoteRequest {
                    term: current_term,
                    candidate_id,
                    last_log_index,
                    last_log_term,
                };
                let is_granted = {
                    // TODO : check the candidate's log is up to date
                    voted_for.is_none() || (voted_for.unwrap() == candidate_id)
                };

                if !is_granted {
                    self.become_follower().await?;
                    return Ok(());
                }
                responses.push(
                    client
                        .request_vote(
                            tarpc::context::current(),
                            RequestVoteRequest {
                                term: current_term,
                                candidate_id,
                                last_log_index,
                                last_log_term,
                            },
                        )
                        .await?,
                );
            }
        }
        self.handle_election(responses).await?;

        Ok(())
    }

    async fn handle_election(
        &mut self,
        responses: Vec<RequestVoteResponse>,
    ) -> anyhow::Result<()> {
        println!(
            "[node {}] responses: {responses:?}",
            self.state.lock().await.id
        );
        // 投票が過半数か
        let vote_granted = responses.iter().filter(|r| r.vote_granted).count()
            > self.config.servers.len() / 2;

        if vote_granted {
            println!(
                "[node {}] vote_granted, become leader: {vote_granted}",
                self.state.lock().await.id
            );

            // kick heartbeat immediately
            self.watch_dog.reset().await;
            self.become_leader().await?;
        }

        Ok(())
    }
    async fn become_leader(&mut self) -> anyhow::Result<()> {
        let mut state = self.state.lock().await;
        state.role = Role::Leader;
        state.leader_id = Some(state.id);
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
        Ok(())
    }
    async fn broadcast_heartbeat(&mut self) -> anyhow::Result<bool> {
        let (term, leader_id) = {
            let state = self.state.lock().await;
            (state.current_term, state.id)
        };
        let servers = self.config.servers.clone();
        for server in servers.iter() {
            if *server == self.server_addr {
                continue;
            }
            if let Some(client) = self.clients.get(server) {
                let req = AppendEntriesRequest {
                    term,
                    leader_id,
                    prev_log_index: 0,
                    prev_log_term: 0,
                    entries: Vec::new(),
                    leader_commit: 0,
                };
                let res = client
                    .append_entries(tarpc::context::current(), req.clone())
                    .await?;
                self.handle_heartbeat(*server, req.clone(), res.clone())
                    .await?;
            }
        }

        Ok(true)
    }
    async fn handle_heartbeat(
        &mut self,
        addr: SocketAddr,
        req: AppendEntriesRequest,
        res: AppendEntriesResponse,
    ) -> anyhow::Result<()> {
        let check_term = {
            let mut state = self.state.lock().await;
            if res.term > state.current_term {
                state.current_term = res.term;
                true
            } else {
                false
            }
        };

        if check_term {
            self.become_follower().await?;
            return Ok(());
        }

        let state = self.state.lock().await;
        if matches!(state.role, Role::Leader) && state.current_term != res.term
        {
            return Ok(());
        }

        if !res.success {
            // TODO: update
            todo!()
        }
        println!("[node {}] got heartbeat from {addr}: {res:?}", state.id);
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
