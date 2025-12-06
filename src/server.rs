use crate::raft::{RaftState, Role};
use crate::rpc::*;
use futures::{future, prelude::*};
use std::collections::HashMap;
use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tarpc::{
    client,
    server::{self, Channel, incoming::Incoming},
    tokio_serde::formats::Json,
};
use tokio::{
    sync::{Mutex, Notify, mpsc, oneshot},
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

#[derive(Default)]
pub struct Config {
    pub servers: Vec<SocketAddr>,
    pub heartbeat_interval: tokio::time::Duration,
    pub election_timeout: tokio::time::Duration,
    pub rpc_timeout: Duration,
}

pub struct Node {
    config: Config,
    clients: HashMap<SocketAddr, RaftRpcClient>,
    server_addr: SocketAddr,
    state: Arc<Mutex<RaftState>>,
    watch_dog: WatchDog,
}

pub struct WatchDog {
    last_seen: Mutex<tokio::time::Instant>,
    notify: Notify,
}

impl Default for WatchDog {
    fn default() -> Self {
        Self {
            last_seen: Mutex::new(tokio::time::Instant::now()),
            notify: Notify::new(),
        }
    }
}

impl WatchDog {
    async fn reset(&self) {
        *self.last_seen.lock().await = tokio::time::Instant::now();
        self.notify.notify_one();
    }
    async fn elasped(&self) -> Duration {
        tokio::time::Instant::now() - *self.last_seen.lock().await
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
        let (tx, rx) = mpsc::channel::<Command>(32);
        let state = Arc::clone(&self.state);
        let mut workers = JoinSet::new();
        workers.spawn(self.main());
        workers.spawn(Self::cmd_handler(state, rx));
        Self::rpc_server(tx, port).await?;

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
    async fn cmd_handler(
        state: Arc<Mutex<RaftState>>,
        mut rx: mpsc::Receiver<Command>,
    ) -> anyhow::Result<()> {
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
                        let mut state = state.lock().await;
                        state.voted_for = Some(req.candidate_id);
                        tracing::info!(id=?state.id, request.body=?req, "Command::RequestVote");

                        // !(term < currentTerm)
                        if req.term >= state.current_term {
                            state.voted_for = Some(req.candidate_id);
                            true
                        } else {
                            state.voted_for.is_none()
                                || state.voted_for.unwrap() == req.candidate_id
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
                break;
            }
            tokio::select! {
                _ = interval.tick() => {}
                _ = self.watch_dog.wait_timeout(timeout) => {
                    self.become_candidate().await?;
                }
            }
        }
        tracing::info!("begin the election");
        Ok(())
    }
    async fn run_candidate(&mut self) -> anyhow::Result<()> {
        loop {
            if !matches!(self.state.lock().await.role, Role::Candidate) {
                break;
            }
            self.watch_dog.reset().await;
            match self.start_election().await {
                Ok(()) => { break; }
                Err(err) if err.downcast_ref() == Some(&io::ErrorKind::TimedOut) => {
                        tracing::info!("retry the election");
                },
                _ => {}
            }
        }
        Ok(())
    }
    async fn run_leader(&mut self) -> anyhow::Result<()> {
        let timeout = self.config.heartbeat_interval;
        let mut interval = tokio::time::interval(timeout / 2);
        loop {
            if !matches!(self.state.lock().await.role, Role::Leader) {
                break;
            }
            interval.tick().await;
            self.broadcast_heartbeat().await?;
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
                    tracing::info!(
                        id = candidate_id,
                        "vote not granted, become follower"
                    );
                    self.become_follower().await?;
                    return Ok(());
                }
                let mut ctx = tarpc::context::current();
                ctx.deadline = Instant::now() + self.config.rpc_timeout;
                responses.push(client.request_vote(ctx, req.clone()).await?);
            }
        }
        self.handle_election(responses).await?;

        Ok(())
    }

    async fn handle_election(
        &mut self,
        responses: Vec<RequestVoteResponse>,
    ) -> anyhow::Result<()> {
        let id = self.state.lock().await.id;
        tracing::info!(id=id, responses=?responses, "election handled");
        // 投票が過半数か
        let vote_granted = responses.iter().filter(|r| r.vote_granted).count()
            > self.config.servers.len() / 2;

        if vote_granted {
            tracing::info!(
                id = id,
                voted_count =
                    responses.iter().filter(|r| r.vote_granted).count(),
                "vote granted, become leader"
            );

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
                let mut ctx = tarpc::context::current();
                ctx.deadline = Instant::now() + self.config.rpc_timeout;
                let res = client
                    .append_entries(ctx, req.clone())
                    .instrument(tracing::info_span!(
                        "append entries to {server}"
                    ))
                    .await?;
                self.handle_heartbeat(*server, req.clone(), res.clone())
                    .await?;
            }
        }

        Ok(true)
    }
    #[tracing::instrument(skip(self))]
    async fn handle_heartbeat(
        &mut self,
        addr: SocketAddr,
        req: AppendEntriesRequest,
        res: AppendEntriesResponse,
    ) -> anyhow::Result<()> {
        let check_term = {
            let mut state = self.state.lock().await;
            if res.term > state.current_term {
                tracing::info!(id=?state.id, "become follower because of term mismatch");
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
        tracing::info!(id=?state.id, response.body=?res, "got heartbeat");
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
        let mut node = Node::new(10101, Config::default());
        node.become_leader().await?;
        let state = node.state.lock().await;
        assert_eq!(state.role, Role::Leader);
        assert_eq!(state.leader_id, Some(state.id));
        Ok(())
    }
    #[tokio::test]
    async fn check_leader_state_after_become_candidate() -> anyhow::Result<()> {
        let mut node = Node::new(10101, Config::default());
        let term = node.state.lock().await.current_term;
        node.become_candidate().await?;
        let state = node.state.lock().await;
        assert_eq!(state.role, Role::Candidate);
        assert_eq!(state.current_term, term + 1);
        Ok(())
    }
    #[tokio::test]
    async fn check_leader_state_after_become_follower() -> anyhow::Result<()> {
        let mut node = Node::new(10101, Config::default());
        node.become_follower().await?;
        let state = node.state.lock().await;
        assert_eq!(state.role, Role::Follower);
        Ok(())
    }
    #[tokio::test(start_paused = true)]
    async fn election_must_be_done_with_not_candidate() -> anyhow::Result<()> {
        let mut node = Node::new(10101, Config::default());
        // TODO: setup node manualy
        node.become_candidate().await?;
        node.run_candidate().await?;
        assert_ne!(node.state.lock().await.role, Role::Candidate);
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn test_follower_becomes_candidate_after_election_timeout() -> anyhow::Result<()> {
        let config = Config {
            election_timeout: Duration::from_millis(2000),
            ..Default::default()
        };
        let mut node = Node::new(10101, config);

        let result = tokio::spawn(async move {
            node.run_follower().await
        });

        tokio::time::advance(Duration::from_millis(2100)).await;
        tokio::time::sleep(Duration::from_millis(10)).await;

        assert!(result.await.is_ok());

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn test_candidate_calls_start_election() -> anyhow::Result<()> {
        let config = Config {
            rpc_timeout: Duration::from_millis(500),
            ..Default::default()
        };
        let mut node = Node::new(10101, config);

        node.become_candidate().await?;

        let result = tokio::spawn(async move {
            node.run_candidate().await
        });

        tokio::time::advance(Duration::from_millis(100)).await;
        tokio::time::sleep(Duration::from_millis(10)).await;

        assert!(result.await.is_ok());

        Ok(())
    }
}
