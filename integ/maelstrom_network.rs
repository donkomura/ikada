use async_trait::async_trait;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tarpc::context;
use tokio::sync::{Mutex, mpsc, oneshot};

use crate::maelstrom::*;
use ikada::network::{NetworkError, NetworkFactory};
use ikada::rpc::*;

pub struct MaelstromNetworkFactory {
    node_id: String,
    node_mapping: Arc<Mutex<HashMap<u32, String>>>,
    outgoing_tx: mpsc::UnboundedSender<Message>,
    rpc_handlers: Arc<Mutex<HashMap<u64, oneshot::Sender<Message>>>>,
    next_msg_id: Arc<Mutex<u64>>,
}

impl MaelstromNetworkFactory {
    pub fn new(
        node_id: String,
        outgoing_tx: mpsc::UnboundedSender<Message>,
    ) -> Self {
        Self {
            node_id,
            node_mapping: Arc::new(Mutex::new(HashMap::new())),
            outgoing_tx,
            rpc_handlers: Arc::new(Mutex::new(HashMap::new())),
            next_msg_id: Arc::new(Mutex::new(1)),
        }
    }

    pub async fn set_node_mapping(&self, mapping: HashMap<u32, String>) {
        let mut node_mapping = self.node_mapping.lock().await;
        *node_mapping = mapping;
    }

    pub async fn handle_incoming_message(&self, msg: Message) {
        let msg_id = match &msg.body {
            Body::AppendEntriesOk(body) => Some(body.in_reply_to),
            Body::RequestVoteOk(body) => Some(body.in_reply_to),
            _ => None,
        };

        if let Some(id) = msg_id {
            let mut handlers = self.rpc_handlers.lock().await;
            if let Some(tx) = handlers.remove(&id) {
                let _ = tx.send(msg);
            }
        }
    }
}

impl Clone for MaelstromNetworkFactory {
    fn clone(&self) -> Self {
        Self {
            node_id: self.node_id.clone(),
            node_mapping: Arc::clone(&self.node_mapping),
            outgoing_tx: self.outgoing_tx.clone(),
            rpc_handlers: Arc::clone(&self.rpc_handlers),
            next_msg_id: Arc::clone(&self.next_msg_id),
        }
    }
}

#[async_trait]
impl NetworkFactory for MaelstromNetworkFactory {
    async fn connect(
        &self,
        _addr: SocketAddr,
    ) -> Result<Arc<dyn RaftRpcTrait>, NetworkError> {
        let port = _addr.port();
        let node_id = port as u32;
        let client = MaelstromRpcClient::new(
            node_id,
            self.node_id.clone(),
            self.node_mapping.clone(),
            self.outgoing_tx.clone(),
            self.rpc_handlers.clone(),
            self.next_msg_id.clone(),
        );
        Ok(Arc::new(client))
    }
}

pub struct MaelstromRpcClient {
    node_id: u32,
    src_node_id: String,
    node_mapping: Arc<Mutex<HashMap<u32, String>>>,
    outgoing_tx: mpsc::UnboundedSender<Message>,
    rpc_handlers: Arc<Mutex<HashMap<u64, oneshot::Sender<Message>>>>,
    next_msg_id: Arc<Mutex<u64>>,
}

impl MaelstromRpcClient {
    pub fn new(
        node_id: u32,
        src_node_id: String,
        node_mapping: Arc<Mutex<HashMap<u32, String>>>,
        outgoing_tx: mpsc::UnboundedSender<Message>,
        rpc_handlers: Arc<Mutex<HashMap<u64, oneshot::Sender<Message>>>>,
        next_msg_id: Arc<Mutex<u64>>,
    ) -> Self {
        Self {
            node_id,
            src_node_id,
            node_mapping,
            outgoing_tx,
            rpc_handlers,
            next_msg_id,
        }
    }

    async fn get_next_msg_id(&self) -> u64 {
        let mut next_id = self.next_msg_id.lock().await;
        let id = *next_id;
        *next_id += 1;
        id
    }

    async fn node_id_to_string(&self, node_id: u32) -> Option<String> {
        let mapping = self.node_mapping.lock().await;
        mapping.get(&node_id).cloned()
    }

    async fn send_and_wait(
        &self,
        msg: Message,
        msg_id: u64,
        timeout: std::time::Duration,
    ) -> Result<Message, NetworkError> {
        let (tx, rx) = oneshot::channel();

        {
            let mut handlers = self.rpc_handlers.lock().await;
            handlers.insert(msg_id, tx);
        }

        self.outgoing_tx.send(msg).map_err(|e| {
            NetworkError::Other(format!("Failed to send: {}", e))
        })?;

        match tokio::time::timeout(timeout, rx).await {
            Ok(result) => result.map_err(|e| {
                NetworkError::Other(format!("Response channel closed: {}", e))
            }),
            Err(_) => {
                let handlers = Arc::clone(&self.rpc_handlers);
                tokio::spawn(async move {
                    let mut handlers = handlers.lock().await;
                    handlers.remove(&msg_id);
                });
                Err(NetworkError::Timeout)
            }
        }
    }
}

#[async_trait]
impl RaftRpcTrait for MaelstromRpcClient {
    async fn append_entries(
        &self,
        ctx: context::Context,
        req: AppendEntriesRequest,
    ) -> anyhow::Result<AppendEntriesResponse> {
        let dest = match self.node_id_to_string(self.node_id).await {
            Some(id) => id,
            None => {
                return Ok(AppendEntriesResponse {
                    term: 0,
                    success: false,
                });
            }
        };

        let msg_id = self.get_next_msg_id().await;

        let entries: Vec<_> = req
            .entries
            .iter()
            .map(|e| {
                use base64::Engine;
                serde_json::json!({
                    "term": e.term,
                    "command": base64::engine::general_purpose::STANDARD.encode(&e.command),
                })
            })
            .collect();

        let msg = Message {
            src: self.src_node_id.clone(),
            dest: Some(dest.clone()),
            body: Body::AppendEntries(AppendEntriesBody {
                msg_id,
                term: req.term,
                leader_id: req.leader_id,
                prev_log_index: req.prev_log_index,
                prev_log_term: req.prev_log_term,
                entries,
                leader_commit: req.leader_commit,
            }),
        };

        let timeout = ctx
            .deadline
            .saturating_duration_since(std::time::Instant::now())
            .max(std::time::Duration::from_secs(3));

        match self.send_and_wait(msg, msg_id, timeout).await {
            Ok(response) => match response.body {
                Body::AppendEntriesOk(body) => Ok(AppendEntriesResponse {
                    term: body.term,
                    success: body.success,
                }),
                _ => Ok(AppendEntriesResponse {
                    term: 0,
                    success: false,
                }),
            },
            Err(e) => {
                Err(anyhow::anyhow!("Failed to send append_entries: {}", e))
            }
        }
    }

    async fn request_vote(
        &self,
        ctx: context::Context,
        req: RequestVoteRequest,
    ) -> anyhow::Result<RequestVoteResponse> {
        let dest = match self.node_id_to_string(self.node_id).await {
            Some(id) => id,
            None => {
                return Ok(RequestVoteResponse {
                    term: 0,
                    vote_granted: false,
                });
            }
        };

        let msg_id = self.get_next_msg_id().await;

        let msg = Message {
            src: self.src_node_id.clone(),
            dest: Some(dest),
            body: Body::RequestVote(RequestVoteBody {
                msg_id,
                term: req.term,
                candidate_id: req.candidate_id,
                last_log_index: req.last_log_index,
                last_log_term: req.last_log_term,
            }),
        };

        let timeout = ctx
            .deadline
            .saturating_duration_since(std::time::Instant::now())
            .max(std::time::Duration::from_secs(3));

        match self.send_and_wait(msg, msg_id, timeout).await {
            Ok(response) => match response.body {
                Body::RequestVoteOk(body) => Ok(RequestVoteResponse {
                    term: body.term,
                    vote_granted: body.vote_granted,
                }),
                _ => Ok(RequestVoteResponse {
                    term: 0,
                    vote_granted: false,
                }),
            },
            Err(e) => {
                Err(anyhow::anyhow!("Failed to send request_vote: {}", e))
            }
        }
    }

    async fn client_request(
        &self,
        _ctx: context::Context,
        _req: CommandRequest,
    ) -> anyhow::Result<CommandResponse> {
        Ok(CommandResponse {
            success: false,
            leader_hint: None,
            data: None,
            error: Some(
                "Not implemented for inter-node communication".to_string(),
            ),
        })
    }
}
