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
    cmd_tx: Option<mpsc::Sender<Command>>,
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

        // Use shorter timeouts for Maelstrom testing
        let timeout_ms = {
            use rand::Rng;
            let base_ms = 500;
            let max_ms = base_ms * 2;
            rand::rng().random_range(base_ms..=max_ms)
        };
        let config = Config {
            heartbeat_interval: tokio::time::Duration::from_millis(50),
            election_timeout: tokio::time::Duration::from_millis(timeout_ms),
            rpc_timeout: std::time::Duration::from_millis(1000),
        };

        let node = Node::new_with_state(config, state, network_factory.clone());

        let task_handle = tokio::spawn(async move {
            let _ = run_raft_node(node, own_port, peers, cmd_rx).await;
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
        let response = self.send_to_raft(cmd.clone()).await?;

        if response.success {
            return self.parse_success_response(response);
        }

        self.handle_command_failure(cmd, response).await
    }

    async fn send_to_raft(
        &self,
        cmd: KVCommand,
    ) -> anyhow::Result<CommandResponse> {
        let cmd_tx = self.get_command_channel().await?;
        let cmd_bytes = bincode::serialize(&cmd)?;
        let (resp_tx, resp_rx) = oneshot::channel();

        cmd_tx
            .send(Command::ClientRequest(
                CommandRequest { command: cmd_bytes },
                resp_tx,
            ))
            .await?;

        Ok(resp_rx.await?)
    }

    async fn get_command_channel(
        &self,
    ) -> anyhow::Result<mpsc::Sender<Command>> {
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
        let (leader_id, is_leader) = self.get_leader_info().await?;

        // If we are the leader, don't forward to ourselves
        if is_leader {
            return Err(anyhow::anyhow!(
                "Command failed on leader: {:?}",
                response.error
            ));
        }

        if let Some(leader_hint) = leader_id
            && let Some(leader_node_id) =
                self.get_node_id_by_hint(leader_hint).await
        {
            return self.forward_to_leader(leader_node_id, cmd).await;
        }

        Err(anyhow::anyhow!("Command failed: {:?}", response.error))
    }

    async fn get_leader_info(&self) -> anyhow::Result<(Option<u32>, bool)> {
        let context = self.raft_context.lock().await;
        let state = context
            .state
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Node not initialized"))?;
        let state_inner = state.lock().await;
        let is_leader = state_inner.role == ikada::raft::Role::Leader;
        Ok((state_inner.leader_id, is_leader))
    }

    async fn get_current_leader_id(&self) -> anyhow::Result<Option<u32>> {
        let (leader_id, _) = self.get_leader_info().await?;
        Ok(leader_id)
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

    /// Forward CAS operation to the leader
    ///
    /// Custom implementation for forwarding compare-and-swap operations.
    async fn forward_cas_to_leader(
        &self,
        leader_node_id: String,
        key: String,
        from: serde_json::Value,
        to: serde_json::Value,
    ) -> anyhow::Result<Message> {
        let my_node_id = self
            .get_node_id()
            .await
            .ok_or_else(|| anyhow::anyhow!("Node ID not set"))?;

        if leader_node_id == my_node_id {
            return Err(anyhow::anyhow!("Cannot forward CAS to self"));
        }

        let msg_id = self.get_next_forward_msg_id().await;
        let (tx, rx) = oneshot::channel();

        {
            let mut forward_mgr = self.forward_manager.lock().await;
            forward_mgr.response_handlers.insert(msg_id, tx);
        }

        let key_value: serde_json::Value = serde_json::from_str(&key)
            .unwrap_or_else(|_| serde_json::Value::String(key.clone()));

        let msg = Message {
            src: my_node_id.clone(),
            dest: Some(leader_node_id.clone()),
            body: Body::Cas(CasBody {
                msg_id,
                key: key_value,
                from,
                to,
            }),
        };

        self.outgoing_tx
            .send(msg)
            .map_err(|e| anyhow::anyhow!("Failed to forward CAS: {}", e))?;

        let response =
            tokio::time::timeout(std::time::Duration::from_secs(5), rx)
                .await
                .map_err(|_| {
                    anyhow::anyhow!("Forward CAS request timed out")
                })??;

        Ok(response)
    }

    async fn handle_read(
        &self,
        src: String,
        body: ReadBody,
    ) -> anyhow::Result<Option<Message>> {
        let key_str = body.key.to_string().trim_matches('"').to_string();
        let command = KVCommand::Get {
            key: key_str.clone(),
        };

        match self.execute_command(command).await {
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
        let key_str = body.key.to_string().trim_matches('"').to_string();

        let is_leader = self.check_is_leader().await?;

        if !is_leader {
            return self.forward_cas_if_possible(src, body, key_str).await;
        }

        self.execute_cas_as_leader(src, body, key_str).await
    }

    async fn check_is_leader(&self) -> anyhow::Result<bool> {
        let context = self.raft_context.lock().await;
        let state = context
            .state
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Node not initialized"))?;
        let state_inner = state.lock().await;
        Ok(state_inner.role == ikada::raft::Role::Leader)
    }

    async fn forward_cas_if_possible(
        &self,
        src: String,
        body: CasBody,
        key_str: String,
    ) -> anyhow::Result<Option<Message>> {
        let leader_id = self.get_current_leader_id().await?;

        if let Some(leader_hint) = leader_id
            && let Some(leader_node_id) =
                self.get_node_id_by_hint(leader_hint).await
        {
            return match self
                .forward_cas_to_leader(
                    leader_node_id,
                    key_str,
                    body.from.clone(),
                    body.to.clone(),
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
                        13,
                        format!("Forward error: {}", e),
                    )
                    .await
                }
            };
        }

        self.error_response(src, body.msg_id, 13, "Not the leader".to_string())
            .await
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
                    serde_json::from_str::<serde_json::Value>(v)
                        .unwrap_or_else(|_| serde_json::Value::String(v.clone()))
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
                self.error_response(src, body.msg_id, ERROR_GENERAL, format!("Error: {}", e))
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
                Some(LogEntry { term, command })
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
        cmd_tx.send(Command::AppendEntries(req, resp_tx)).await?;

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
        cmd_tx.send(Command::RequestVote(req, resp_tx)).await?;

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

/// Run ikada Raft node lifecycle
async fn run_raft_node<NF>(
    mut node: Node<KVCommand, KVStateMachine, NF>,
    _port: u16,
    peers: Vec<SocketAddr>,
    mut cmd_rx: mpsc::Receiver<Command>,
) -> anyhow::Result<()>
where
    NF: ikada::network::NetworkFactory + Clone + Send + 'static,
{
    node.setup(peers).await?;

    use ikada::raft::Role;

    let heartbeat_tx = node.c.heartbeat_tx.clone();
    let client_tx = node.c.client_tx.clone();
    let state = Arc::clone(&node.state);

    tokio::spawn(async move {
        while let Some(cmd) = cmd_rx.recv().await {
            let state_clone = Arc::clone(&state);
            let heartbeat_tx_clone = heartbeat_tx.clone();
            let client_tx_clone = client_tx.clone();

            tokio::spawn(async move {
                use ikada::rpc::*;

                match cmd {
                    Command::AppendEntries(req, resp_tx) => {
                        let resp =
                            ikada::node::handlers::handle_append_entries(
                                &req,
                                state_clone.clone(),
                            )
                            .await
                            .unwrap_or(
                                AppendEntriesResponse {
                                    term: 0,
                                    success: false,
                                },
                            );

                        if resp.success || resp.term > req.term {
                            let _ = heartbeat_tx_clone
                                .send((req.leader_id, req.term));
                        }

                        let _ = resp_tx.send(resp);
                    }
                    Command::RequestVote(req, resp_tx) => {
                        let resp = ikada::node::handlers::handle_request_vote(
                            &req,
                            state_clone.clone(),
                        )
                        .await;

                        let _ = resp_tx.send(resp);
                    }
                    Command::ClientRequest(req, resp_tx) => {
                        let resp =
                            ikada::node::handlers::handle_client_request(
                                &req,
                                state_clone.clone(),
                                client_tx_clone.clone(),
                            )
                            .await;

                        let _ = resp_tx.send(resp);
                    }
                }
            });
        }
    });

    loop {
        let role = {
            let state = node.state.lock().await;
            state.role
        };

        match role {
            Role::Follower => node.run_follower().await?,
            Role::Candidate => node.run_candidate().await?,
            Role::Leader => node.run_leader().await?,
        }
    }
}
