use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[async_trait::async_trait]
pub trait StateMachine: Send + Sync {
    type Command: Send + Sync + Clone;
    type Response: Send + Sync + Clone + Serialize;

    async fn apply(
        &mut self,
        command: &Self::Command,
    ) -> anyhow::Result<Self::Response>;

    async fn snapshot(&self) -> anyhow::Result<Vec<u8>>;
    async fn restore(&mut self, data: &[u8]) -> anyhow::Result<()>;
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
    async fn snapshot(&self) -> anyhow::Result<Vec<u8>> {
        unimplemented!()
    }
    async fn restore(&mut self, _data: &[u8]) -> anyhow::Result<()> {
        Ok(())
    }
}

#[derive(Default, Debug)]
pub struct KVStateMachine {
    data: HashMap<String, String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum KVCommand {
    Set {
        key: String,
        value: String,
    },
    Get {
        key: String,
    },
    Delete {
        key: String,
    },
    CompareAndSet {
        key: String,
        from: String,
        to: String,
    },
}

impl Default for KVCommand {
    fn default() -> Self {
        KVCommand::Get { key: String::new() }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum KVResponse {
    Success,
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
                Ok(KVResponse::Success)
            }
            KVCommand::Get { key } => {
                let value = self.data.get(&key.clone());
                Ok(KVResponse::Value(value.cloned()))
            }
            KVCommand::Delete { key } => {
                let value = self.data.remove(key);
                Ok(KVResponse::Value(value))
            }
            KVCommand::CompareAndSet { key, from, to } => {
                let current = self.data.get(key);
                let success = current.map(|v| v == from).unwrap_or(false);
                if success {
                    self.data.insert(key.clone(), to.clone());
                    Ok(KVResponse::Success)
                } else {
                    Ok(KVResponse::Value(current.cloned()))
                }
            }
        }
    }

    async fn snapshot(&self) -> anyhow::Result<Vec<u8>> {
        let serialized = bincode::serialize(&self.data).unwrap();
        Ok(serialized)
    }

    async fn restore(&mut self, data: &[u8]) -> anyhow::Result<()> {
        self.data = bincode::deserialize(data)?;
        Ok(())
    }
}
