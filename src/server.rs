use crate::raft::{self, RaftState, Role};
use crate::rpc::*;
use crate::watchdog::WatchDog;
use futures::{future, prelude::*};
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
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

#[derive(Default)]
pub struct Config {
    pub servers: Vec<SocketAddr>,
    pub heartbeat_interval: tokio::time::Duration,
    pub election_timeout: tokio::time::Duration,
    pub rpc_timeout: Duration,
}

#[derive(Debug)]
pub struct Order {
}

pub struct Chan {
    heartbeat_tx: mpsc::UnboundedSender<(u32,u32)>,
    heartbeat_rx: mpsc::UnboundedReceiver<(u32,u32)>,
    client_tx: mpsc::Sender<Order>,
    client_rx: mpsc::Receiver<Order>,
}

impl Default for Chan {
    fn default() -> Self {
        let (heartbeat_tx, heartbeat_rx) = mpsc::unbounded_channel();
        let (client_tx, client_rx) = mpsc::channel::<Order>(32);
        Self {
            heartbeat_tx,
            heartbeat_rx,
            client_tx,
            client_rx,
        }
    }
}

pub struct Node {
    config: Config,
    peers: HashMap<SocketAddr, RaftRpcClient>,
    server_addr: SocketAddr,
    state: Arc<Mutex<RaftState<Order>>>,
    c: Chan,
}

async fn spawn(fut: impl Future<Output = ()> + Send + 'static) {
    tokio::spawn(fut);
}

impl Node {
    pub fn new(port: u16, config: Config) -> Self {
        Node {
            config,
            peers: HashMap::default(),
            server_addr: SocketAddr::from((Ipv4Addr::LOCALHOST, port)),
            state: Arc::new(Mutex::new(RaftState::new(port as u32))),
            c: Chan::default(),
        }
    }
    pub async fn run(self, port: u16) -> anyhow::Result<()> {
        // main thread for test
        let (tx, rx) = mpsc::channel::<Command>(32);
        let state = Arc::clone(&self.state);
        let heartbeat_tx = self.c.heartbeat_tx.clone();
        let mut workers = JoinSet::new();
        workers.spawn(self.main());
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
    async fn rpc_handler(
        state: Arc<Mutex<RaftState<Order>>>,
        mut rx: mpsc::Receiver<Command>,
        heartbeat_tx: mpsc::UnboundedSender<(u32,u32)>,
    ) -> anyhow::Result<()> {
        while let Some(command) = rx.recv().await {
            match command {
                Command::AppendEntries(req, tx) => {
                    let (id, current_term, success) = {
                        let mut state = state.lock().await;

                        if req.term > state.current_term {
                            state.current_term = req.term;
                            state.role = Role::Follower;
                            state.voted_for = None;
                        }

                        (state.id, state.current_term, true)
                    };

                    tx.send(AppendEntriesResponse {
                        term: current_term,
                        success,
                    })
                    .expect("failed to send append entries response");
                    // TODO: handle this erorr
                    //   called `Result::unwrap()` on an `Err` value: SendError { .. }
                    //   WARN ikada::server: Failed to send request vote: the connection to the server was already shutdown
                    heartbeat_tx.send((id, current_term)).expect("failed to send append notification");
                }
                Command::RequestVote(req, tx) => {
                    let (current_term, vote_granted) = {
                        let mut state = state.lock().await;

                        // RPC受信時のterm比較: req.term > currentTermの場合
                        if req.term > state.current_term {
                            state.current_term = req.term;
                            state.role = Role::Follower;
                            state.voted_for = None;
                        }

                        tracing::info!(id=?state.id, request.body=?req, "Command::RequestVote");

                        // !(term < currentTerm)
                        let vote_granted = if req.term >= state.current_term {
                            state.voted_for = Some(req.candidate_id);
                            true
                        } else {
                            state.voted_for.is_none()
                                || state.voted_for.unwrap() == req.candidate_id
                        };

                        (state.current_term, vote_granted)
                    };

                    tx.send(RequestVoteResponse {
                        term: current_term,
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
            if self.peers.contains_key(&server) {
                continue;
            }
            let transport =
                tarpc::serde_transport::tcp::connect(server, Json::default)
                    .await?;
            let client =
                RaftRpcClient::new(client::Config::default(), transport)
                    .spawn();
            self.peers.insert(server, client);
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
        let watchdog = WatchDog::new();

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
        let watchdog = WatchDog::new();
        loop {
            if !matches!(self.state.lock().await.role, Role::Candidate) {
                break;
            }
            tokio::select! {
                Some((id, term)) = self.c.heartbeat_rx.recv() => {
                    let current_term = self.state.lock().await.current_term;
                    if term >= current_term {
                        // the headbeat is accepted
                        // requester is a new leader
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
        let watchdog = WatchDog::new();
        loop {
            if !matches!(self.state.lock().await.role, Role::Leader) {
                break;
            }
            tokio::select! {
                Some(order) = self.c.client_rx.recv() => {
                    let mut state = self.state.lock().await;
                    state.apply(&order)?;
                    let term = state.current_term;
                    state.log.push(raft::Entry {
                        term,
                        command: order,
                    });
                },
                _ = watchdog.wait() => {
                    self.broadcast_heartbeat().await?;
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

        let peers: Vec<_> = self.peers.iter().map(|(addr, client)| (*addr, client.clone())).collect();
        let rpc_timeout = self.config.rpc_timeout;

        // TODO: should we send in gossip protocol?
        let mut tasks = JoinSet::new();
        for (_, client) in peers {
            let req = RequestVoteRequest {
                term: current_term,
                candidate_id,
                last_log_index,
                last_log_term,
            };
            tasks.spawn(Self::send_request_vote(client, req, rpc_timeout));
        }

        while let Some(result) = tasks.join_next().await {
            match result? {
                Ok(res) => {
                    responses.push(res);
                }
                Err(e) => {
                    tracing::warn!("Failed to send request vote: {:?}", e);
                    // should retry
                    return Err(e);
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
        let id = self.state.lock().await.id;
        tracing::info!(id=id, responses=?responses, "election handled");
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
    async fn send_request_vote(
        client: RaftRpcClient,
        req: RequestVoteRequest,
        rpc_timeout: Duration,
    ) -> anyhow::Result<RequestVoteResponse> {
        let mut ctx = tarpc::context::current();
        ctx.deadline = Instant::now() + rpc_timeout;
        let res = client
            .request_vote(ctx, req.clone())
            .instrument(tracing::info_span!(
                "request vote from candidate {}",
                req.candidate_id
            ))
            .await?;
        Ok(res)
    }
    /// replicate logs to each peer
    async fn broadcast_heartbeat(&mut self) -> anyhow::Result<bool> {
        let (term, leader_id, match_index, last_index) = {
            let state = self.state.lock().await;
            let last_index = state.get_last_log_idx();
            (state.current_term, state.id, last_index, last_index + 1)
        };
        let peers: Vec<_> = self.peers.iter().map(|(addr, client)| (*addr, client.clone())).collect();
        let rpc_timeout = self.config.rpc_timeout;

        // TODO: optimize algorithm: should be gossip?
        let mut tasks = JoinSet::new();
        for (server, client) in peers {
            tasks.spawn(Self::send_heartbeat(server, client, term, leader_id, rpc_timeout));
        }

        while let Some(result) = tasks.join_next().await {
            match result? {
                Ok((server, res)) => {
                    self.handle_heartbeat(server, res, match_index, last_index).await?;
                }
                Err(e) => {
                    tracing::warn!("Failed to send heartbeat: {:?}", e);
                    match e.downcast_ref() {
                        Some(&tarpc::client::RpcError::DeadlineExceeded) => {
                            tracing::warn!("Append Entries RPC was timedout");
                        },
                        Some(&tarpc::client::RpcError::Shutdown) => {
                            tracing::warn!("SHUTDOWN");
                        }
                        _ => {
                            return Err(e);
                        }
                    }
                }
            }
        }

        Ok(true)
    }
    async fn send_heartbeat(
        server: SocketAddr,
        client: RaftRpcClient,
        term: u32,
        leader_id: u32,
        rpc_timeout: Duration,
    ) -> anyhow::Result<(SocketAddr, AppendEntriesResponse)> {
        let req = AppendEntriesRequest {
            term,
            leader_id,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: Vec::new(),
            leader_commit: 0,
        };
        let mut ctx = tarpc::context::current();
        ctx.deadline = Instant::now() + rpc_timeout;
        let res = client
            .append_entries(ctx, req.clone())
            .instrument(tracing::info_span!(
                "append entries to {server}"
            ))
            .await?;
        Ok((server, res))
    }
    #[tracing::instrument(skip(self))]
    async fn handle_heartbeat(
        &mut self,
        addr: SocketAddr,
        res: AppendEntriesResponse,
        match_index: u32,
        last_index: u32,
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
        if matches!(role, Role::Leader) && current_term != res.term
        {
            return Ok(());
        }

        {
            let mut state = self.state.lock().await;
            if res.success {
                // TODO: commit all logs before last_index
                state.match_index.insert(addr, last_index);
                state.next_index.insert(addr, last_index+1);
            } else {
                tracing::warn!(id=?id, response.body=?res, "AppendEntries was rejected: old entries");
                let new_match_index = state.match_index.get(&addr).copied().unwrap_or(last_index).saturating_sub(1);
                state.match_index.insert(addr, new_match_index);
                state.next_index.insert(addr, new_match_index.saturating_sub(1));
            }
        }
        // TODO: res.nextIdx <= lastIdx then should retry
        tracing::info!(id=?id, response.body=?res, "got heartbeat");
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
        let mut node = Node::new(10101, config);
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
        let mut node = Node::new(10101, config);
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
        let mut node = Node::new(10101, config);

        let heartbeat_tx = node.c.heartbeat_tx.clone();
        let result = tokio::spawn(async move { node.run_follower().await });

        // 1回目: 800ms経過後にheartbeat
        tokio::time::advance(Duration::from_millis(800)).await;
        tokio::task::yield_now().await;
        assert!(!result.is_finished());

        heartbeat_tx.send(()).unwrap();
        tokio::task::yield_now().await;

        // 2回目: さらに800ms経過後にheartbeat（リセットから800ms）
        tokio::time::advance(Duration::from_millis(800)).await;
        tokio::task::yield_now().await;
        assert!(!result.is_finished());

        heartbeat_tx.send(()).unwrap();
        tokio::task::yield_now().await;

        // 3回目: さらに800ms経過後にheartbeat（リセットから800ms）
        tokio::time::advance(Duration::from_millis(800)).await;
        tokio::task::yield_now().await;
        assert!(!result.is_finished());

        heartbeat_tx.send(()).unwrap();
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
        let mut node = Node::new(10101, config);

        let heartbeat_tx = node.c.heartbeat_tx.clone();
        let result = tokio::spawn(async move { node.run_follower().await });

        // 1回目: 800ms経過後にheartbeat
        tokio::time::advance(Duration::from_millis(800)).await;
        tokio::task::yield_now().await;
        assert!(!result.is_finished());

        heartbeat_tx.send(()).unwrap();
        tokio::task::yield_now().await;

        // 2回目: さらに800ms経過後にheartbeat（リセットから800ms）
        tokio::time::advance(Duration::from_millis(800)).await;
        tokio::task::yield_now().await;
        assert!(!result.is_finished());

        heartbeat_tx.send(()).unwrap();
        tokio::task::yield_now().await;

        // heartbeat停止: 1100ms経過でタイムアウト
        tokio::time::advance(Duration::from_millis(1100)).await;
        tokio::task::yield_now().await;
        assert!(result.is_finished());

        Ok(())
    }

    #[tokio::test]
    async fn test_append_entries_with_higher_term_converts_to_follower()
    -> anyhow::Result<()> {
        let mut initial_state = RaftState::new(1);
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
        let mut initial_state = RaftState::new(1);
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
}
