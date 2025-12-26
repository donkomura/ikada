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
    AppendEntriesRequest, CommandRequest, LogEntry, RequestVoteRequest,
};
use ikada::statemachine::{KVCommand, KVResponse, KVStateMachine};

type RaftStateHandle = Arc<Mutex<RaftState<KVCommand, KVStateMachine>>>;

pub struct MaelstromRaftNode {
    node_id: Arc<Mutex<Option<String>>>,
    node_ids: Arc<Mutex<Vec<String>>>,
    state: Arc<Mutex<Option<RaftStateHandle>>>,
    network_factory: Arc<Mutex<Option<MaelstromNetworkFactory>>>,
    outgoing_tx: mpsc::UnboundedSender<Message>,
    cmd_tx: Arc<Mutex<Option<mpsc::Sender<Command>>>>,
    raft_task_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    forward_response_handlers:
        Arc<Mutex<HashMap<u64, oneshot::Sender<Message>>>>,
    next_forward_msg_id: Arc<Mutex<u64>>,
}

impl Clone for MaelstromRaftNode {
    fn clone(&self) -> Self {
        Self {
            node_id: Arc::clone(&self.node_id),
            node_ids: Arc::clone(&self.node_ids),
            state: Arc::clone(&self.state),
            network_factory: Arc::clone(&self.network_factory),
            outgoing_tx: self.outgoing_tx.clone(),
            cmd_tx: Arc::clone(&self.cmd_tx),
            raft_task_handle: Arc::clone(&self.raft_task_handle),
            forward_response_handlers: Arc::clone(
                &self.forward_response_handlers,
            ),
            next_forward_msg_id: Arc::clone(&self.next_forward_msg_id),
        }
    }
}

impl MaelstromRaftNode {
    pub fn new(outgoing_tx: mpsc::UnboundedSender<Message>) -> Self {
        Self {
            node_id: Arc::new(Mutex::new(None)),
            node_ids: Arc::new(Mutex::new(Vec::new())),
            state: Arc::new(Mutex::new(None)),
            network_factory: Arc::new(Mutex::new(None)),
            outgoing_tx,
            cmd_tx: Arc::new(Mutex::new(None)),
            raft_task_handle: Arc::new(Mutex::new(None)),
            forward_response_handlers: Arc::new(Mutex::new(HashMap::new())),
            next_forward_msg_id: Arc::new(Mutex::new(1000000)),
        }
    }

    async fn get_next_forward_msg_id(&self) -> u64 {
        let mut next_id = self.next_forward_msg_id.lock().await;
        let id = *next_id;
        *next_id += 1;
        id
    }

    async fn get_node_id(&self) -> Option<String> {
        let node_id = self.node_id.lock().await;
        node_id.clone()
    }

    pub async fn run(&self) -> anyhow::Result<()> {
        let stdin = io::stdin();
        let reader = BufReader::new(stdin);
        let mut lines = reader.lines();

        while let Some(line) = lines.next_line().await? {
            let msg: Message = serde_json::from_str(&line)?;

            if let Some(response) = self.handle_message(msg).await? {
                self.send_message(response)?;
            }
        }

        Ok(())
    }

    fn send_message(&self, msg: Message) -> anyhow::Result<()> {
        self.outgoing_tx.send(msg)?;
        Ok(())
    }

    async fn handle_message(
        &self,
        msg: Message,
    ) -> anyhow::Result<Option<Message>> {
        // Check if this is a response to a forwarded request
        if let Some(in_reply_to) = self.get_in_reply_to(&msg.body) {
            let mut handlers = self.forward_response_handlers.lock().await;
            if let Some(tx) = handlers.remove(&in_reply_to) {
                let _ = tx.send(msg);
                return Ok(None);
            }
        }

        match msg.body {
            Body::Init(init_body) => self.handle_init(msg.src, init_body).await,

            // Client requests - spawn in separate tasks to avoid blocking
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

            // Raft RPCs - handle immediately to avoid deadlock
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

    async fn handle_init(
        &self,
        src: String,
        body: InitBody,
    ) -> anyhow::Result<Option<Message>> {
        {
            let mut node_id = self.node_id.lock().await;
            *node_id = Some(body.node_id.clone());
        }
        {
            let mut node_ids = self.node_ids.lock().await;
            *node_ids = body.node_ids.clone();
        }

        let network_factory = MaelstromNetworkFactory::new(
            body.node_id.clone(),
            self.outgoing_tx.clone(),
        );

        let mut node_mapping = HashMap::new();
        let mut peers = Vec::new();

        let own_idx = body
            .node_ids
            .iter()
            .position(|n| n == &body.node_id)
            .unwrap_or(0);
        let own_port = 1111 + own_idx as u16;

        for (idx, node_name) in body.node_ids.iter().enumerate() {
            let port = 1111 + idx as u16;
            node_mapping.insert(port as u32, node_name.clone());

            if node_name != &body.node_id {
                peers.push(SocketAddr::from((
                    std::net::Ipv4Addr::LOCALHOST,
                    port,
                )));
            }
        }

        network_factory.set_node_mapping(node_mapping).await;

        use ikada::storage::MemStorage;
        let storage = Box::new(MemStorage::default());
        let state = Arc::new(Mutex::new(RaftState::new(
            own_port as u32,
            storage,
            KVStateMachine::default(),
        )));

        {
            let mut state_lock = self.state.lock().await;
            *state_lock = Some(Arc::clone(&state));
        }

        let (cmd_tx, cmd_rx) = mpsc::channel::<Command>(100);
        {
            let mut cmd_tx_lock = self.cmd_tx.lock().await;
            *cmd_tx_lock = Some(cmd_tx.clone());
        }

        let node = Node::new_with_state(
            Config::default(),
            state,
            network_factory.clone(),
        );

        let handle = tokio::spawn(async move {
            let _ = run_raft_node(node, own_port, peers, cmd_rx).await;
        });

        {
            let mut handle_lock = self.raft_task_handle.lock().await;
            *handle_lock = Some(handle);
        }
        {
            let mut nf_lock = self.network_factory.lock().await;
            *nf_lock = Some(network_factory);
        }

        Ok(Some(Message {
            src: body.node_id,
            dest: Some(src),
            body: Body::InitOk(InitOkBody {
                in_reply_to: body.msg_id,
            }),
        }))
    }

    async fn execute_command(
        &self,
        cmd: KVCommand,
    ) -> anyhow::Result<KVResponse> {
        let cmd_tx = {
            let cmd_tx_lock = self.cmd_tx.lock().await;
            cmd_tx_lock
                .clone()
                .ok_or_else(|| anyhow::anyhow!("Node not initialized"))?
        };

        let cmd_bytes = bincode::serialize(&cmd)?;
        let (resp_tx, resp_rx) = oneshot::channel();

        cmd_tx
            .send(Command::ClientRequest(
                CommandRequest {
                    command: cmd_bytes.clone(),
                },
                resp_tx,
            ))
            .await?;

        let response = resp_rx.await?;

        if !response.success {
            // If not the leader, try to forward to the leader (Maelstrom-specific routing)
            // Get the actual leader_id from state instead of relying on response.leader_hint
            let leader_id = {
                let state_lock = self.state.lock().await;
                let state = state_lock
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("Node not initialized"))?;
                let state_inner = state.lock().await;
                state_inner.leader_id
            };

            if let Some(leader_hint) = leader_id {
                // Get leader's node_id string from the hint
                if let Some(leader_node_id) =
                    self.get_node_id_by_hint(leader_hint).await
                {
                    // Forward the command to the leader via Maelstrom message
                    return self.forward_to_leader(leader_node_id, cmd).await;
                }
            }

            return Err(anyhow::anyhow!(
                "Command failed: {:?}",
                response.error
            ));
        }

        let data = response
            .data
            .ok_or_else(|| anyhow::anyhow!("No response data"))?;
        let kv_response: KVResponse = bincode::deserialize(&data)?;

        Ok(kv_response)
    }

    async fn get_node_id_by_hint(&self, hint: u32) -> Option<String> {
        // Convert port-based hint to node_id string
        // In our setup: port 1111 -> n0, port 1112 -> n1, etc.
        let index = (hint as usize).saturating_sub(1111);
        let node_ids = self.node_ids.lock().await;
        node_ids.get(index).cloned()
    }

    async fn forward_to_leader(
        &self,
        leader_node_id: String,
        cmd: KVCommand,
    ) -> anyhow::Result<KVResponse> {
        // Create a forwarding message based on command type
        let my_node_id = self
            .get_node_id()
            .await
            .ok_or_else(|| anyhow::anyhow!("Node ID not set"))?;

        // Don't forward to ourselves to avoid infinite loop
        if leader_node_id == my_node_id {
            return Err(anyhow::anyhow!("Cannot forward to self"));
        }

        let msg_id = self.get_next_forward_msg_id().await;
        let (tx, rx) = oneshot::channel();

        // Register the response handler
        {
            let mut handlers = self.forward_response_handlers.lock().await;
            handlers.insert(msg_id, tx);
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
                // Delete not supported in Maelstrom lin-kv workload
                return Err(anyhow::anyhow!(
                    "Delete command not supported for forwarding"
                ));
            }
        };

        // Send the message
        self.outgoing_tx
            .send(msg)
            .map_err(|e| anyhow::anyhow!("Failed to forward: {}", e))?;

        // Wait for response with timeout
        let response =
            tokio::time::timeout(std::time::Duration::from_secs(5), rx)
                .await
                .map_err(|_| anyhow::anyhow!("Forward request timed out"))??;

        // Convert the Maelstrom response to KVResponse
        match response.body {
            Body::ReadOk(body) => {
                let value_str = body.value.to_string();
                Ok(KVResponse::Value(Some(value_str)))
            }
            Body::WriteOk(_) => Ok(KVResponse::Ok),
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

        // Don't forward to ourselves to avoid infinite loop
        if leader_node_id == my_node_id {
            return Err(anyhow::anyhow!("Cannot forward CAS to self"));
        }

        let msg_id = self.get_next_forward_msg_id().await;
        let (tx, rx) = oneshot::channel();

        // Register the response handler
        {
            let mut handlers = self.forward_response_handlers.lock().await;
            handlers.insert(msg_id, tx);
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

        // Send the message
        self.outgoing_tx
            .send(msg)
            .map_err(|e| anyhow::anyhow!("Failed to forward CAS: {}", e))?;

        // Wait for response with timeout
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
                    code: 13,
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
                    code: 13,
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

        // Check if we're the leader by inspecting state
        let (is_leader, leader_id) = {
            let state_lock = self.state.lock().await;
            let state = state_lock
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("Node not initialized"))?;
            let state_inner = state.lock().await;
            (
                state_inner.role == ikada::raft::Role::Leader,
                state_inner.leader_id,
            )
        };

        // If not leader, forward the entire CAS operation to the leader
        if !is_leader {
            if let Some(leader_hint) = leader_id
                && let Some(leader_node_id) =
                    self.get_node_id_by_hint(leader_hint).await
            {
                // Forward the CAS request to leader and return its response directly
                match self
                    .forward_cas_to_leader(
                        leader_node_id,
                        key_str,
                        body.from.clone(),
                        body.to.clone(),
                    )
                    .await
                {
                    Ok(mut response_msg) => {
                        // Update the response to be sent to the original source and fix in_reply_to
                        response_msg.dest = Some(src);

                        // Fix in_reply_to field in the body
                        match &mut response_msg.body {
                            Body::CasOk(cas_ok_body) => {
                                cas_ok_body.in_reply_to = body.msg_id;
                            }
                            Body::Error(error_body) => {
                                error_body.in_reply_to = body.msg_id;
                            }
                            _ => {}
                        }

                        return Ok(Some(response_msg));
                    }
                    Err(e) => {
                        return Ok(Some(Message {
                            src: self.get_node_id().await.unwrap(),
                            dest: Some(src),
                            body: Body::Error(ErrorBody {
                                in_reply_to: body.msg_id,
                                code: 13,
                                text: Some(format!("Forward error: {}", e)),
                            }),
                        }));
                    }
                }
            }

            return Ok(Some(Message {
                src: self.get_node_id().await.unwrap(),
                dest: Some(src),
                body: Body::Error(ErrorBody {
                    in_reply_to: body.msg_id,
                    code: 13,
                    text: Some("Not the leader".to_string()),
                }),
            }));
        }

        // We're the leader, execute CAS atomically
        let get_command = KVCommand::Get {
            key: key_str.clone(),
        };
        let current_value_str = match self.execute_command(get_command).await {
            Ok(KVResponse::Value(Some(v))) => v,
            Ok(KVResponse::Value(None)) => {
                return Ok(Some(Message {
                    src: self.get_node_id().await.unwrap(),
                    dest: Some(src),
                    body: Body::Error(ErrorBody {
                        in_reply_to: body.msg_id,
                        code: ERROR_KEY_NOT_EXIST,
                        text: Some(format!("Key {:?} does not exist", key_str)),
                    }),
                }));
            }
            Err(e) => {
                return Ok(Some(Message {
                    src: self.get_node_id().await.unwrap(),
                    dest: Some(src),
                    body: Body::Error(ErrorBody {
                        in_reply_to: body.msg_id,
                        code: 13,
                        text: Some(format!("Error: {}", e)),
                    }),
                }));
            }
            _ => unreachable!(),
        };

        let from_str = body.from.to_string();

        if current_value_str != from_str {
            return Ok(Some(Message {
                src: self.get_node_id().await.unwrap(),
                dest: Some(src),
                body: Body::Error(ErrorBody {
                    in_reply_to: body.msg_id,
                    code: ERROR_CAS_MISMATCH,
                    text: Some(format!(
                        "Expected {} but found {}",
                        from_str, current_value_str
                    )),
                }),
            }));
        }

        let to_str = body.to.to_string();
        let set_command = KVCommand::Set {
            key: key_str,
            value: to_str,
        };

        match self.execute_command(set_command).await {
            Ok(_) => Ok(Some(Message {
                src: self.get_node_id().await.unwrap(),
                dest: Some(src),
                body: Body::CasOk(CasOkBody {
                    in_reply_to: body.msg_id,
                }),
            })),
            Err(e) => Ok(Some(Message {
                src: self.get_node_id().await.unwrap(),
                dest: Some(src),
                body: Body::Error(ErrorBody {
                    in_reply_to: body.msg_id,
                    code: 13,
                    text: Some(format!("Error: {}", e)),
                }),
            })),
        }
    }

    async fn handle_append_entries(
        &self,
        src: String,
        body: AppendEntriesBody,
    ) -> anyhow::Result<Option<Message>> {
        let cmd_tx = {
            let cmd_tx_lock = self.cmd_tx.lock().await;
            match cmd_tx_lock.as_ref() {
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
            let cmd_tx_lock = self.cmd_tx.lock().await;
            match cmd_tx_lock.as_ref() {
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
        let (role, id) = {
            let state = node.state.lock().await;
            (state.role, state.id)
        };

        match role {
            Role::Follower => node.run_follower().await?,
            Role::Candidate => node.run_candidate().await?,
            Role::Leader => node.run_leader().await?,
        }
    }
}
