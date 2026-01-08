use base64::Engine;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{self, AsyncBufReadExt, BufReader};
use tokio::sync::{Mutex, mpsc, oneshot};

use crate::maelstrom::*;
use crate::maelstrom_network::MaelstromNetworkFactory;
use ikada::config::Config;
use ikada::node::{Command, Node};
use ikada::raft::RaftState;
use ikada::rpc::{
    AppendEntriesRequest, CommandRequest, CommandResponse, LogEntry,
    RequestVoteRequest,
};
use ikada::statemachine::{KVCommand, KVResponse, KVStateMachine};

const BASE_PORT: u16 = 1111;
const FORWARD_MSG_ID_START: u64 = 1000000;
const COMMAND_CHANNEL_SIZE: usize = 128;
const ERROR_GENERAL: u32 = 13;

type RaftStateHandle = Arc<Mutex<RaftState<KVCommand, KVStateMachine>>>;

/// Maelstrom-specific node identification
struct NodeInfo {
    node_id: Option<String>,
    node_ids: Vec<String>,
}

/// ikada Raft state management
struct RaftContext {
    state: Option<RaftStateHandle>,
    cmd_tx: Option<mpsc::Sender<Command<KVCommand>>>,
    task_handle: Option<tokio::task::JoinHandle<()>>,
}

/// Request forwarding management
///
/// Maelstrom doesn't provide built-in request forwarding mechanism.
/// This implementation handles forwarding client requests from followers to leader,
/// tracking response channels for forwarded requests.
struct ForwardManager {
    response_handlers: HashMap<u64, oneshot::Sender<Message>>,
    next_msg_id: u64,
}

impl ForwardManager {
    fn new() -> Self {
        Self {
            response_handlers: HashMap::new(),
            next_msg_id: FORWARD_MSG_ID_START,
        }
    }

    fn next_id(&mut self) -> u64 {
        let id = self.next_msg_id;
        self.next_msg_id += 1;
        id
    }
}

/// Main node for Maelstrom integration
pub struct MaelstromRaftNode {
    node_info: Arc<Mutex<NodeInfo>>,
    raft_context: Arc<Mutex<RaftContext>>,
    network_factory: Arc<Mutex<Option<MaelstromNetworkFactory>>>,
    forward_manager: Arc<Mutex<ForwardManager>>,
    outgoing_tx: mpsc::UnboundedSender<Message>,
}

impl Clone for MaelstromRaftNode {
    fn clone(&self) -> Self {
        Self {
            node_info: Arc::clone(&self.node_info),
            raft_context: Arc::clone(&self.raft_context),
            network_factory: Arc::clone(&self.network_factory),
            forward_manager: Arc::clone(&self.forward_manager),
            outgoing_tx: self.outgoing_tx.clone(),
        }
    }
}

impl MaelstromRaftNode {
    pub fn new(outgoing_tx: mpsc::UnboundedSender<Message>) -> Self {
        Self {
            node_info: Arc::new(Mutex::new(NodeInfo {
                node_id: None,
                node_ids: Vec::new(),
            })),
            raft_context: Arc::new(Mutex::new(RaftContext {
                state: None,
                cmd_tx: None,
                task_handle: None,
            })),
            network_factory: Arc::new(Mutex::new(None)),
            forward_manager: Arc::new(Mutex::new(ForwardManager::new())),
            outgoing_tx,
        }
    }

    async fn get_next_forward_msg_id(&self) -> u64 {
        let mut forward_mgr = self.forward_manager.lock().await;
        forward_mgr.next_id()
    }

    async fn get_node_id(&self) -> Option<String> {
        let info = self.node_info.lock().await;
        info.node_id.clone()
    }

    pub async fn run(&self) -> anyhow::Result<()> {
        let stdin = io::stdin();
        let reader = BufReader::new(stdin);
        let mut lines = reader.lines();

        while let Some(line) = lines.next_line().await? {
            let msg: Message = serde_json::from_str(&line)?;

            if let Some(response_msg) = self.handle_message(msg).await? {
                self.send_message(response_msg)?;
            }
        }

        Ok(())
    }

    fn send_message(&self, msg: Message) -> anyhow::Result<()> {
        self.outgoing_tx
            .send(msg)
            .map_err(|e| anyhow::anyhow!("Failed to send message: {}", e))
    }

    async fn handle_message(
        &self,
        msg: Message,
    ) -> anyhow::Result<Option<Message>> {
        // Handle responses to forwarded requests
        // This is part of our custom forwarding implementation
        if let Some(in_reply_to) = self.get_in_reply_to(&msg.body) {
            let mut forward_mgr = self.forward_manager.lock().await;
            if let Some(tx) = forward_mgr.response_handlers.remove(&in_reply_to)
            {
                let _ = tx.send(msg);
                return Ok(None);
            }
        }

        match msg.body {
            Body::Init(init_body) => self.handle_init(msg.src, init_body).await,
            Body::Read(read_body) => {
                let node = self.clone();
                let src = msg.src;
                tokio::spawn(async move {
                    if let Ok(Some(response)) =
                        node.handle_read(src, read_body).await
                    {
                        let _ = node.send_message(response);
                    }
                });
                Ok(None)
            }
            Body::Write(write_body) => {
                let node = self.clone();
                let src = msg.src;
                tokio::spawn(async move {
                    if let Ok(Some(response)) =
                        node.handle_write(src, write_body).await
                    {
                        let _ = node.send_message(response);
                    }
                });
                Ok(None)
            }
            Body::Cas(cas_body) => {
                let node = self.clone();
                let src = msg.src;
                tokio::spawn(async move {
                    if let Ok(Some(response)) =
                        node.handle_cas(src, cas_body).await
                    {
                        let _ = node.send_message(response);
                    }
                });
                Ok(None)
            }
            Body::AppendEntries(ae_body) => {
                self.handle_append_entries(msg.src, ae_body).await
            }
            Body::RequestVote(rv_body) => {
                self.handle_request_vote(msg.src, rv_body).await
            }
            Body::AppendEntriesOk(_) | Body::RequestVoteOk(_) => {
                let network_factory = self.network_factory.lock().await;
                if let Some(nf) = network_factory.as_ref() {
                    nf.handle_incoming_message(msg).await;
                }
                Ok(None)
            }
            _ => Ok(None),
        }
    }

    fn get_in_reply_to(&self, body: &Body) -> Option<u64> {
        match body {
            Body::ReadOk(b) => Some(b.in_reply_to),
            Body::WriteOk(b) => Some(b.in_reply_to),
            Body::CasOk(b) => Some(b.in_reply_to),
            Body::Error(b) => Some(b.in_reply_to),
            _ => None,
        }
    }

    /// Initialize the Maelstrom Raft node
    async fn handle_init(
        &self,
        src: String,
        body: InitBody,
    ) -> anyhow::Result<Option<Message>> {
        {
            let mut info = self.node_info.lock().await;
            info.node_id = Some(body.node_id.clone());
            info.node_ids = body.node_ids.clone();
        }

        let network_factory = MaelstromNetworkFactory::new(
            body.node_id.clone(),
            self.outgoing_tx.clone(),
        );

        // Create node mapping
        let node_mapping: HashMap<u32, String> = body
            .node_ids
            .iter()
            .enumerate()
            .map(|(i, id)| (BASE_PORT as u32 + i as u32, id.clone()))
            .collect();
        network_factory.set_node_mapping(node_mapping).await;

        *self.network_factory.lock().await = Some(network_factory.clone());

        let own_port = self.get_own_port(&body.node_id, &body.node_ids);
        let peers = self.get_peer_addresses(&body.node_id, &body.node_ids);

        use ikada::storage::MemStorage;
        let storage = Box::new(MemStorage::default());
        let state = Arc::new(Mutex::new(RaftState::new(
            own_port as u32,
            storage,
            KVStateMachine::default(),
        )));

        let (cmd_tx, cmd_rx) = mpsc::channel(COMMAND_CHANNEL_SIZE);

        {
            let mut context = self.raft_context.lock().await;
            context.state = Some(Arc::clone(&state));
            context.cmd_tx = Some(cmd_tx);
        }

        // Maelstrom-optimized timeouts
        let timeout_ms = {
            use rand::Rng;
            let base_ms = 150;
            let max_ms = 300;
            rand::rng().random_range(base_ms..=max_ms)
        };
        let config = Config {
            heartbeat_interval: tokio::time::Duration::from_millis(10),
            election_timeout: tokio::time::Duration::from_millis(timeout_ms),
            rpc_timeout: std::time::Duration::from_millis(100),
            heartbeat_failure_retry_limit: 5,
            batch_window: tokio::time::Duration::from_millis(10),
            max_batch_size: 100,
            replication_max_inflight: 4,
            replication_max_entries_per_rpc: 128,
            snapshot_threshold: 10000,
            read_index_timeout: std::time::Duration::from_millis(100),
            storage_dir: std::path::PathBuf::from("."),
        };

        let node = Node::new_with_state(config, state, network_factory.clone());

        let task_handle = tokio::spawn(async move {
            let _ = node.run_with_handler(peers, cmd_rx).await;
        });

        {
            let mut context = self.raft_context.lock().await;
            context.task_handle = Some(task_handle);
        }

        Ok(Some(Message {
            src: body.node_id,
            dest: Some(src),
            body: Body::InitOk(InitOkBody {
                in_reply_to: body.msg_id,
            }),
        }))
    }

    fn get_own_port(&self, node_id: &str, node_ids: &[String]) -> u16 {
        let index = node_ids.iter().position(|id| id == node_id).unwrap();
        BASE_PORT + index as u16
    }

    fn get_peer_addresses(
        &self,
        node_id: &str,
        node_ids: &[String],
    ) -> Vec<SocketAddr> {
        node_ids
            .iter()
            .enumerate()
            .filter(|(_, id)| *id != node_id)
            .map(|(i, _)| {
                SocketAddr::from(([127, 0, 0, 1], BASE_PORT + i as u16))
            })
            .collect()
    }

    async fn get_node_id_by_hint(&self, hint: u32) -> Option<String> {
        let info = self.node_info.lock().await;
        let index = (hint - BASE_PORT as u32) as usize;
        info.node_ids.get(index).cloned()
    }

    async fn execute_command(
        &self,
        cmd: KVCommand,
    ) -> anyhow::Result<KVResponse> {
        let response = self.send_to_raft(cmd.clone(), false).await?;

        if response.success {
            return self.parse_success_response(response);
        }

        self.handle_command_failure(cmd, response).await
    }

    async fn execute_read_command(
        &self,
        cmd: KVCommand,
    ) -> anyhow::Result<KVResponse> {
        let response = self.send_to_raft(cmd.clone(), true).await?;

        if response.success {
            return self.parse_success_response(response);
        }

        // For reads, if no-op not committed yet, fall back to normal path
        if response.error == Some(ikada::rpc::CommandError::NoopNotCommitted) {
            return self.execute_command(cmd).await;
        }

        self.handle_command_failure(cmd, response).await
    }

    async fn send_to_raft(
        &self,
        cmd: KVCommand,
        use_read_index: bool,
    ) -> anyhow::Result<CommandResponse> {
        let cmd_tx = self.get_command_channel().await?;
        let cmd_bytes = bincode::serialize(&cmd)?;
        let (resp_tx, resp_rx) = oneshot::channel();
        let span = tracing::Span::current();

        let command = if use_read_index {
            Command::ReadRequest(
                CommandRequest { command: cmd_bytes },
                resp_tx,
                span,
            )
        } else {
            Command::ClientRequest(
                CommandRequest { command: cmd_bytes },
                resp_tx,
                span,
            )
        };

        cmd_tx.send(command).await?;

        Ok(resp_rx.await?)
    }

    async fn get_command_channel(
        &self,
    ) -> anyhow::Result<mpsc::Sender<Command<KVCommand>>> {
        let context = self.raft_context.lock().await;
        context
            .cmd_tx
            .clone()
            .ok_or_else(|| anyhow::anyhow!("Node not initialized"))
    }

    fn parse_success_response(
        &self,
        response: CommandResponse,
    ) -> anyhow::Result<KVResponse> {
        let data = response
            .data
            .ok_or_else(|| anyhow::anyhow!("No response data"))?;
        Ok(bincode::deserialize(&data)?)
    }

    async fn handle_command_failure(
        &self,
        cmd: KVCommand,
        response: CommandResponse,
    ) -> anyhow::Result<KVResponse> {
        if let Some(leader_node_id) = self.should_forward_to_leader().await? {
            return self.forward_to_leader(leader_node_id, cmd).await;
        }

        Err(anyhow::anyhow!(
            "Command failed: {}",
            response
                .error
                .map(|e| e.to_string())
                .unwrap_or_else(|| "Unknown error".to_string())
        ))
    }

    /// Check if we should forward to leader and return leader's node ID.
    /// Returns None if:
    /// - We are the leader ourselves
    /// - No leader is known
    /// - Leader ID points to ourselves (stale state after partition)
    async fn should_forward_to_leader(&self) -> anyhow::Result<Option<String>> {
        let context = self.raft_context.lock().await;
        let state = context
            .state
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Node not initialized"))?;
        let state_inner = state.lock().await;

        if state_inner.role.is_leader() {
            return Ok(None);
        }

        let Some(leader_hint) = state_inner.leader_id else {
            return Ok(None);
        };

        let my_node_id = self
            .get_node_id()
            .await
            .ok_or_else(|| anyhow::anyhow!("Node ID not set"))?;
        let leader_node_id = self.get_node_id_by_hint(leader_hint).await;

        if leader_node_id.as_ref() == Some(&my_node_id) {
            drop(state_inner);
            let mut state_mut = state.lock().await;
            state_mut.leader_id = None;
            return Ok(None);
        }

        Ok(leader_node_id)
    }

    /// Forward client request to the leader
    ///
    /// Maelstrom doesn't provide built-in forwarding, so we implement it here.
    /// Converts KVCommand to Maelstrom message, sends to leader, and waits for response.
    async fn forward_to_leader(
        &self,
        leader_node_id: String,
        cmd: KVCommand,
    ) -> anyhow::Result<KVResponse> {
        let my_node_id = self
            .get_node_id()
            .await
            .ok_or_else(|| anyhow::anyhow!("Node ID not set"))?;

        if leader_node_id == my_node_id {
            return Err(anyhow::anyhow!("Cannot forward to self"));
        }

        let msg_id = self.get_next_forward_msg_id().await;
        let (tx, rx) = oneshot::channel();

        {
            let mut forward_mgr = self.forward_manager.lock().await;
            forward_mgr.response_handlers.insert(msg_id, tx);
        }

        let msg = match &cmd {
            KVCommand::Get { key } => {
                let key_value: serde_json::Value = serde_json::from_str(key)
                    .unwrap_or_else(|_| serde_json::Value::String(key.clone()));

                Message {
                    src: my_node_id.clone(),
                    dest: Some(leader_node_id.clone()),
                    body: Body::Read(ReadBody {
                        msg_id,
                        key: key_value,
                    }),
                }
            }
            KVCommand::Set { key, value } => {
                let key_value: serde_json::Value = serde_json::from_str(key)
                    .unwrap_or_else(|_| serde_json::Value::String(key.clone()));
                let value_json: serde_json::Value = serde_json::from_str(value)
                    .unwrap_or_else(|_| {
                        serde_json::Value::String(value.clone())
                    });

                Message {
                    src: my_node_id.clone(),
                    dest: Some(leader_node_id.clone()),
                    body: Body::Write(WriteBody {
                        msg_id,
                        key: key_value,
                        value: value_json,
                    }),
                }
            }
            KVCommand::Delete { .. } => {
                return Err(anyhow::anyhow!(
                    "Delete command not supported for forwarding"
                ));
            }
            KVCommand::CompareAndSet { .. } => {
                return Err(anyhow::anyhow!(
                    "CompareAndSet command not supported for forwarding"
                ));
            }
        };

        self.outgoing_tx
            .send(msg)
            .map_err(|e| anyhow::anyhow!("Failed to forward: {}", e))?;

        let response =
            tokio::time::timeout(std::time::Duration::from_secs(5), rx)
                .await
                .map_err(|_| anyhow::anyhow!("Forward request timed out"))??;

        match response.body {
            Body::ReadOk(body) => {
                let value_str = body.value.to_string();
                Ok(KVResponse::Value(Some(value_str)))
            }
            Body::WriteOk(_) => Ok(KVResponse::Success),
            Body::Error(body) => {
                if body.code == ERROR_KEY_NOT_EXIST {
                    Ok(KVResponse::Value(None))
                } else {
                    Err(anyhow::anyhow!(
                        "Leader returned error: {:?}",
                        body.text
                    ))
                }
            }
            _ => Err(anyhow::anyhow!("Unexpected response type from leader")),
        }
    }

    /// Generic message forwarding to leader
    ///
    /// Forwards any message body to the leader and returns the response message.
    /// This avoids unnecessary serialization/deserialization.
    async fn forward_message_to_leader(
        &self,
        leader_node_id: String,
        body: Body,
    ) -> anyhow::Result<Message> {
        let my_node_id = self
            .get_node_id()
            .await
            .ok_or_else(|| anyhow::anyhow!("Node ID not set"))?;

        if leader_node_id == my_node_id {
            return Err(anyhow::anyhow!("Cannot forward to self"));
        }

        let msg_id = self.get_next_forward_msg_id().await;
        let (tx, rx) = oneshot::channel();

        {
            let mut forward_mgr = self.forward_manager.lock().await;
            forward_mgr.response_handlers.insert(msg_id, tx);
        }

        // Update msg_id in the body
        let body_with_msg_id = match body {
            Body::Read(mut read_body) => {
                read_body.msg_id = msg_id;
                Body::Read(read_body)
            }
            Body::Write(mut write_body) => {
                write_body.msg_id = msg_id;
                Body::Write(write_body)
            }
            Body::Cas(mut cas_body) => {
                cas_body.msg_id = msg_id;
                Body::Cas(cas_body)
            }
            _ => {
                return Err(anyhow::anyhow!(
                    "Unsupported body type for forwarding"
                ));
            }
        };

        let msg = Message {
            src: my_node_id.clone(),
            dest: Some(leader_node_id.clone()),
            body: body_with_msg_id,
        };

        self.outgoing_tx
            .send(msg)
            .map_err(|e| anyhow::anyhow!("Failed to forward: {}", e))?;

        let response =
            tokio::time::timeout(std::time::Duration::from_secs(5), rx)
                .await
                .map_err(|_| anyhow::anyhow!("Forward request timed out"))??;

        Ok(response)
    }

    async fn handle_read(
        &self,
        src: String,
        body: ReadBody,
    ) -> anyhow::Result<Option<Message>> {
        if let Some(leader_node_id) = self.should_forward_to_leader().await? {
            // Use generic message forwarding - no serialize/deserialize overhead
            match self
                .forward_message_to_leader(
                    leader_node_id,
                    Body::Read(body.clone()),
                )
                .await
            {
                Ok(mut response) => {
                    // Fix response destination and msg_id
                    response.dest = Some(src);
                    if let Body::ReadOk(ref mut ok_body) = response.body {
                        ok_body.in_reply_to = body.msg_id;
                    } else if let Body::Error(ref mut err_body) = response.body
                    {
                        err_body.in_reply_to = body.msg_id;
                    }
                    return Ok(Some(response));
                }
                Err(e) => {
                    return Ok(Some(Message {
                        src: self.get_node_id().await.unwrap(),
                        dest: Some(src),
                        body: Body::Error(ErrorBody {
                            in_reply_to: body.msg_id,
                            code: ERROR_GENERAL,
                            text: Some(format!("Forward error: {}", e)),
                        }),
                    }));
                }
            }
        }

        let leadership_confirmed = self.check_leadership_confirmed().await?;

        if !leadership_confirmed {
            return Ok(Some(Message {
                src: self.get_node_id().await.unwrap(),
                dest: Some(src),
                body: Body::Error(ErrorBody {
                    in_reply_to: body.msg_id,
                    code: ERROR_GENERAL,
                    text: Some("Not leader and no leader known".to_string()),
                }),
            }));
        }

        let key_str = body.key.to_string().trim_matches('"').to_string();
        let command = KVCommand::Get {
            key: key_str.clone(),
        };

        // Use ReadIndex optimization for reads
        match self.execute_read_command(command).await {
            Ok(KVResponse::Value(Some(value))) => {
                let json_value: serde_json::Value =
                    serde_json::from_str(&value)
                        .unwrap_or(serde_json::Value::String(value));

                Ok(Some(Message {
                    src: self.get_node_id().await.unwrap(),
                    dest: Some(src),
                    body: Body::ReadOk(ReadOkBody {
                        in_reply_to: body.msg_id,
                        value: json_value,
                    }),
                }))
            }
            Ok(KVResponse::Value(None)) => Ok(Some(Message {
                src: self.get_node_id().await.unwrap(),
                dest: Some(src),
                body: Body::Error(ErrorBody {
                    in_reply_to: body.msg_id,
                    code: ERROR_KEY_NOT_EXIST,
                    text: Some(format!("Key {:?} does not exist", key_str)),
                }),
            })),
            Err(e) => Ok(Some(Message {
                src: self.get_node_id().await.unwrap(),
                dest: Some(src),
                body: Body::Error(ErrorBody {
                    in_reply_to: body.msg_id,
                    code: ERROR_GENERAL,
                    text: Some(format!("Error: {}", e)),
                }),
            })),
            _ => unreachable!(),
        }
    }

    async fn handle_write(
        &self,
        src: String,
        body: WriteBody,
    ) -> anyhow::Result<Option<Message>> {
        let key_str = body.key.to_string().trim_matches('"').to_string();
        let value_str = body.value.to_string();

        let command = KVCommand::Set {
            key: key_str,
            value: value_str,
        };

        match self.execute_command(command).await {
            Ok(_) => Ok(Some(Message {
                src: self.get_node_id().await.unwrap(),
                dest: Some(src),
                body: Body::WriteOk(WriteOkBody {
                    in_reply_to: body.msg_id,
                }),
            })),
            Err(e) => Ok(Some(Message {
                src: self.get_node_id().await.unwrap(),
                dest: Some(src),
                body: Body::Error(ErrorBody {
                    in_reply_to: body.msg_id,
                    code: ERROR_GENERAL,
                    text: Some(format!("Error: {}", e)),
                }),
            })),
        }
    }

    async fn handle_cas(
        &self,
        src: String,
        body: CasBody,
    ) -> anyhow::Result<Option<Message>> {
        // First, check if we should forward to leader (same pattern as handle_read)
        if let Some(leader_node_id) = self.should_forward_to_leader().await? {
            // Use generic message forwarding
            return match self
                .forward_message_to_leader(
                    leader_node_id,
                    Body::Cas(body.clone()),
                )
                .await
            {
                Ok(mut response_msg) => {
                    response_msg.dest = Some(src);
                    self.fix_cas_reply_to(&mut response_msg.body, body.msg_id);
                    Ok(Some(response_msg))
                }
                Err(e) => {
                    self.error_response(
                        src,
                        body.msg_id,
                        ERROR_GENERAL,
                        format!("Forward error: {}", e),
                    )
                    .await
                }
            };
        }

        // We are the leader or no leader is known yet
        let leadership_confirmed = self.check_leadership_confirmed().await?;

        if !leadership_confirmed {
            return self
                .error_response(
                    src,
                    body.msg_id,
                    ERROR_GENERAL,
                    "Not leader and no leader known".to_string(),
                )
                .await;
        }

        let key_str = body.key.to_string().trim_matches('"').to_string();
        self.execute_cas_as_leader(src, body, key_str).await
    }

    async fn check_leadership_confirmed(&self) -> anyhow::Result<bool> {
        let context = self.raft_context.lock().await;

        let state = context
            .state
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Node not initialized"))?;
        let state_inner = state.lock().await;

        Ok(state_inner.role.is_leader())
    }

    fn fix_cas_reply_to(&self, body: &mut Body, msg_id: u64) {
        match body {
            Body::CasOk(cas_ok_body) => {
                cas_ok_body.in_reply_to = msg_id;
            }
            Body::Error(error_body) => {
                error_body.in_reply_to = msg_id;
            }
            _ => {}
        }
    }

    async fn error_response(
        &self,
        dest: String,
        in_reply_to: u64,
        code: u32,
        text: String,
    ) -> anyhow::Result<Option<Message>> {
        Ok(Some(Message {
            src: self.get_node_id().await.unwrap_or_default(),
            dest: Some(dest),
            body: Body::Error(ErrorBody {
                in_reply_to,
                code,
                text: Some(text),
            }),
        }))
    }

    async fn execute_cas_as_leader(
        &self,
        src: String,
        body: CasBody,
        key_str: String,
    ) -> anyhow::Result<Option<Message>> {
        let from_str = body.from.to_string().trim_matches('"').to_string();
        let to_str = body.to.to_string().trim_matches('"').to_string();

        let cas_command = KVCommand::CompareAndSet {
            key: key_str.clone(),
            from: from_str.clone(),
            to: to_str,
        };

        match self.execute_command(cas_command).await {
            Ok(KVResponse::Success) => Ok(Some(Message {
                src: self.get_node_id().await.unwrap_or_default(),
                dest: Some(src),
                body: Body::CasOk(CasOkBody {
                    in_reply_to: body.msg_id,
                }),
            })),
            Ok(KVResponse::Value(actual)) => {
                let actual_value = actual.as_ref().map(|v| {
                    serde_json::from_str::<serde_json::Value>(v).unwrap_or_else(
                        |_| serde_json::Value::String(v.clone()),
                    )
                });

                if actual_value.is_none() {
                    return self
                        .error_response(
                            src,
                            body.msg_id,
                            ERROR_KEY_NOT_EXIST,
                            format!("Key {:?} does not exist", key_str),
                        )
                        .await;
                }

                self.error_response(
                    src,
                    body.msg_id,
                    ERROR_CAS_MISMATCH,
                    format!(
                        "Expected {} but found {}",
                        body.from,
                        actual_value.unwrap()
                    ),
                )
                .await
            }
            Err(e) => {
                self.error_response(
                    src,
                    body.msg_id,
                    ERROR_GENERAL,
                    format!("Error: {}", e),
                )
                .await
            }
        }
    }

    async fn handle_append_entries(
        &self,
        src: String,
        body: AppendEntriesBody,
    ) -> anyhow::Result<Option<Message>> {
        let cmd_tx = {
            let context = self.raft_context.lock().await;
            match context.cmd_tx.as_ref() {
                Some(tx) => tx.clone(),
                None => return Ok(None),
            }
        };

        let entries: Vec<LogEntry> = body
            .entries
            .iter()
            .filter_map(|e| {
                let term = e.get("term")?.as_u64()? as u32;
                let command_b64 = e.get("command")?.as_str()?;
                let command = base64::engine::general_purpose::STANDARD
                    .decode(command_b64)
                    .ok()?;
                Some(LogEntry {
                    term,
                    command: std::sync::Arc::from(command.into_boxed_slice()),
                })
            })
            .collect();

        let req = AppendEntriesRequest {
            term: body.term,
            leader_id: body.leader_id,
            prev_log_index: body.prev_log_index,
            prev_log_term: body.prev_log_term,
            entries,
            leader_commit: body.leader_commit,
        };

        let (resp_tx, resp_rx) = oneshot::channel();
        let span = tracing::Span::current();
        cmd_tx
            .send(Command::AppendEntries(req, resp_tx, span))
            .await?;

        let response = resp_rx.await?;

        Ok(Some(Message {
            src: self.get_node_id().await.unwrap(),
            dest: Some(src),
            body: Body::AppendEntriesOk(AppendEntriesOkBody {
                in_reply_to: body.msg_id,
                term: response.term,
                success: response.success,
            }),
        }))
    }

    async fn handle_request_vote(
        &self,
        src: String,
        body: RequestVoteBody,
    ) -> anyhow::Result<Option<Message>> {
        let cmd_tx = {
            let context = self.raft_context.lock().await;
            match context.cmd_tx.as_ref() {
                Some(tx) => tx.clone(),
                None => return Ok(None),
            }
        };

        let req = RequestVoteRequest {
            term: body.term,
            candidate_id: body.candidate_id,
            last_log_index: body.last_log_index,
            last_log_term: body.last_log_term,
        };

        let (resp_tx, resp_rx) = oneshot::channel();
        let span = tracing::Span::current();
        cmd_tx
            .send(Command::RequestVote(req, resp_tx, span))
            .await?;

        let response = resp_rx.await?;

        Ok(Some(Message {
            src: self.get_node_id().await.unwrap(),
            dest: Some(src),
            body: Body::RequestVoteOk(RequestVoteOkBody {
                in_reply_to: body.msg_id,
                term: response.term,
                vote_granted: response.vote_granted,
            }),
        }))
    }
}
