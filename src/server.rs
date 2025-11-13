use crate::raft::{RaftState, Role};
use crate::rpc::*;
use futures::{future, prelude::*};
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use tarpc::Request;
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
    AppendEntries(AppendEntriesResponse),
    RequestVote(RequestVoteResponse),
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
    async fn touch(&self) {
        *self.last_seen.lock().await = Instant::now();
        self.notify.notify_one();
    }
    async fn elasped(&self) -> Duration {
        Instant::now() - *self.last_seen.lock().await
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
    pub async fn run(mut self, port: u16) -> anyhow::Result<()> {
        // main thread for test
        let (tx, mut rx) = mpsc::channel::<Command>(32);
        let state_clone = Arc::clone(&self.state);

        let main_handle = tokio::spawn(async move { self.main().await });

        let cmd_handle = tokio::spawn(async move {
            while let Some(command) = rx.recv().await {
                match command {
                    Command::AppendEntries(req, tx) => {
                        let mut success = false;
                        {
                            let state = state_clone.lock().await;
                            println!("[node {}] Command::AppendEntries: {:?}", state.id, state);

                            success = state.current_term >= req.term
                                && state.leader_id == Some(req.leader_id);
                        }

                        tx.send(AppendEntriesResponse {
                            term: req.term,
                            success,
                        })
                        .expect("failed to send append entries response");
                    }
                    Command::RequestVote(req, tx) => {
                        let mut vote_granted = false;
                        {
                            let mut state = state_clone.lock().await;
                            println!("[node {}] Command::RequestVote: {:?}", state.id, state);

                            vote_granted = if state.current_term < req.term {
                                state.voted_for = Some(req.candidate_id);
                                false
                            } else {
                                state.voted_for.is_none()
                                    || state.voted_for.unwrap() == req.candidate_id
                            };
                        }

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
            let mut listener = tarpc::serde_transport::tcp::listen(&addr, Json::default)
                .await
                .expect("failed to start RPC server");
            listener.config_mut().max_frame_length(usize::MAX);
            listener
                .filter_map(|r| future::ready(r.ok()))
                .map(server::BaseChannel::with_defaults)
                .max_channels_per_key(1, |t| t.transport().peer_addr().unwrap().ip())
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
            let transport = tarpc::serde_transport::tcp::connect(server, Json::default).await?;
            let client = RaftRpcClient::new(client::Config::default(), transport).spawn();
            self.clients.insert(server, client);
        }
        Ok(())
    }
    async fn main(mut self) -> anyhow::Result<()> {
        self.setup().await?;

        let timeout = self.config.election_timeout;
        let interval = timeout.clone() / 2;
        loop {
            tokio::select! {
                _ = self.watch_dog.notify.notified() => {
                    self.watch_dog.touch().await; // reset
                }
                _ = tokio::time::sleep(interval) => {}
            }
            let elapsed = self.watch_dog.elasped().await;
            if elapsed > timeout {
                self.start_election().await?;
            }
        }
    }
    async fn start_election(&mut self) -> anyhow::Result<()> {
        {
            let mut state = self.state.lock().await;
            state.role = Role::Candidate;
            state.current_term += 1;
        }
        self.watch_dog.touch().await;

        let mut responses: Vec<RequestVoteResponse> = Vec::new();

        // 自分自身への投票を追加
        let (current_term, candidate_id, last_log_index, last_log_term, node_id) = {
            let state = self.state.lock().await;
            responses.push(RequestVoteResponse {
                term: state.current_term,
                vote_granted: true,
            });
            (
                state.current_term,
                state.id,
                state.log.len() as u32,
                state.log.last().map(|e| e.term).unwrap_or(0),
                state.id,
            )
        };

        for server in self.config.servers.iter() {
            if *server == self.server_addr {
                continue;
            }
            if let Some(client) = self.clients.get(server) {
                println!("[node {}] request vote to: {:?}", node_id, server);
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
        println!("[node {}] request vote responses: {responses:?}", node_id);

        // 投票が過半数か
        let vote_granted =
            responses.iter().filter(|r| r.vote_granted).count() > self.config.servers.len() / 2;
        {
            let mut state = self.state.lock().await;
            if matches!(state.role, Role::Candidate) && vote_granted {
                state.role = Role::Leader;
                state.leader_id = Some(state.id);
                // kick heartbeat immediately
            }

            // TODO
            if state.voted_for.is_none() || (state.voted_for.unwrap() == state.id) {}
        }

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
        println!("append entries: {req:?}");
        let (tx, rx) = oneshot::channel::<AppendEntriesResponse>();
        self.tx.send(Command::AppendEntries(req, tx)).await.unwrap();
        rx.await.unwrap()
    }

    async fn request_vote(
        self,
        _: tarpc::context::Context,
        req: RequestVoteRequest,
    ) -> RequestVoteResponse {
        println!("request vote rpc: {req:?}");
        let (tx, rx) = oneshot::channel::<RequestVoteResponse>();
        self.tx.send(Command::RequestVote(req, tx)).await.unwrap();
        rx.await.unwrap()
    }
}
