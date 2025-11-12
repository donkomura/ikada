use crate::raft::{RaftState, Role};
use crate::rpc::*;
use futures::{future, prelude::*};
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use tarpc::client;
use tarpc::server::Channel;
use tarpc::server::incoming::Incoming;
use tarpc::{server, tokio_serde::formats::Json};
use tokio::sync::mpsc;

pub enum Command {
    AppendEntries(AppendEntriesRequest),
}

pub struct Config {
    pub servers: Vec<SocketAddr>,
    pub heartbeat_interval: tokio::time::Duration,
    pub election_timeout: tokio::time::Duration,
}

pub struct Node {
    config: Config,
    tx: mpsc::Sender<Command>,
    rx: mpsc::Receiver<Command>,
    clients: HashMap<SocketAddr, RaftRpcClient>,
    server_addr: SocketAddr,
    state: RaftState,
}

async fn spawn(fut: impl Future<Output = ()> + Send + 'static) {
    tokio::spawn(fut);
}

impl Node {
    pub fn new(port: u16, config: Config) -> Self {
        let (tx, rx) = mpsc::channel::<Command>(32);
        Node {
            config,
            tx,
            rx,
            clients: HashMap::default(),
            server_addr: SocketAddr::from((Ipv4Addr::LOCALHOST, port)),
            state: RaftState::default(),
        }
    }
    pub async fn run(self, port: u16) -> anyhow::Result<()> {
        // main thread for test
        // TODO: expand this after we impl timeout
        let tx = self.tx.clone();
        let main_handle = tokio::spawn(async move { self.main().await });

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
                    let server = RaftServer {
                        addr: channel.transport().peer_addr().unwrap(),
                        tx: tx.clone(),
                    };
                    channel.execute(server.serve()).for_each(spawn)
                })
                .buffer_unordered(10)
                .for_each(|_| async {})
                .await;
        });
        let (main_result, _) = tokio::try_join!(main_handle, rpc_handle)?;
        main_result?;

        Ok(())
    }
    async fn setup(&mut self) -> anyhow::Result<()> {
        for server in vec![self.server_addr]
            .into_iter()
            .chain(self.config.servers.clone())
        {
            if self.clients.contains_key(&server) {
                return Ok(());
            }
            let transport = tarpc::serde_transport::tcp::connect(server, Json::default).await?;
            let client = RaftRpcClient::new(client::Config::default(), transport).spawn();
            self.clients.insert(server, client);
        }
        Ok(())
    }
    async fn main(mut self) -> anyhow::Result<()> {
        self.setup().await?;

        let mut interval = tokio::time::interval(self.config.heartbeat_interval);
        let tx = self.tx.clone();

        tokio::spawn(async move {
            loop {
                interval.tick().await;

                // TODO: fix
                if matches!(self.state.role, Role::Follower) {
                    tx.send(Command::AppendEntries(AppendEntriesRequest {
                        term: 0,
                        leader_id: 0,
                        prev_log_index: 0,
                        prev_log_term: 0,
                        entries: vec![],
                        leader_commit: 0,
                    }))
                    .await;
                }
            }
        });

        while let Some(c) = self.rx.recv().await {
            self.handle(c).await?;
        }
        Ok(())
    }
    async fn handle(&mut self, command: Command) -> anyhow::Result<()> {
        assert!(!self.clients.is_empty());
        let server_addr = self.server_addr;
        if let Some(client) = self.clients.get(&server_addr) {
            match command {
                Command::AppendEntries(req) => {
                    println!("get append entries: {req:?}");
                }
            }
        } else {
            return Err(anyhow::anyhow!("server address not found"));
        }
        Ok(())
    }
}

#[derive(Clone)]
struct RaftServer {
    addr: SocketAddr,
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
        AppendEntriesResponse {
            term: 0,
            success: true,
        }
    }

    async fn request_vote(
        self,
        _: tarpc::context::Context,
        req: RequestVoteRequest,
    ) -> RequestVoteResponse {
        println!("request vote: {req:?}");
        RequestVoteResponse {
            term: 0,
            vote_granted: true,
        }
    }
}
