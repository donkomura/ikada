use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[async_trait::async_trait]
pub trait StateMachine: Send + Sync {
    type Command: Send + Sync + Clone;
    type Response: Send + Sync;

    async fn apply(
        &mut self,
        command: &Self::Command,
    ) -> anyhow::Result<Self::Response>;
}

// Simple test state machine that just counts applied commands
#[derive(Default, Debug)]
pub struct NoOpStateMachine {
    pub applied_count: usize,
}

#[async_trait::async_trait]
impl StateMachine for NoOpStateMachine {
    type Command = bytes::Bytes;
    type Response = usize;

    async fn apply(
        &mut self,
        _command: &Self::Command,
    ) -> anyhow::Result<Self::Response> {
        self.applied_count += 1;
        Ok(self.applied_count)
    }
}

#[derive(Default, Debug)]
pub struct KVStateMachine {
    data: HashMap<String, String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum KVCommand {
    Set { key: String, value: String },
    Get { key: String },
    Delete { key: String },
}

#[derive(Clone, Debug)]
pub enum KVResponse {
    Ok,
    Value(Option<String>),
}

#[async_trait::async_trait]
impl StateMachine for KVStateMachine {
    type Command = KVCommand;
    type Response = KVResponse;

    async fn apply(
        &mut self,
        command: &Self::Command,
    ) -> anyhow::Result<Self::Response> {
        match command {
            KVCommand::Set { key, value } => {
                self.data.insert(key.clone(), value.clone());
                Ok(KVResponse::Ok)
            }
            KVCommand::Get { key } => {
                let value = self.data.get(&key.clone());
                Ok(KVResponse::Value(value.cloned()))
            }
            KVCommand::Delete { key } => {
                let value = self.data.remove(key);
                Ok(KVResponse::Value(value))
            }
        }
    }
}
