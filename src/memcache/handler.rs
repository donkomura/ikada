use crate::client::KVStore;
use crate::memcache::error::{MemcacheError, Result};
use crate::memcache::protocol::{MemcacheCommand, MemcacheResponse};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MemcacheValue {
    flags: u32,
    data: Vec<u8>,
}

pub struct MemcacheHandler {
    kv_store: Arc<Mutex<KVStore>>,
}

impl MemcacheHandler {
    pub fn new(kv_store: Arc<Mutex<KVStore>>) -> Self {
        Self { kv_store }
    }

    pub async fn handle_command(
        &self,
        cmd: MemcacheCommand,
    ) -> Result<Vec<MemcacheResponse>> {
        match cmd {
            MemcacheCommand::Set {
                key, flags, data, ..
            } => {
                let value = MemcacheValue { flags, data };
                let value_json = serde_json::to_string(&value)
                    .map_err(|e| MemcacheError::Server(e.to_string()))?;

                let mut store = self.kv_store.lock().await;
                store
                    .set(key, value_json)
                    .await
                    .map_err(|e| MemcacheError::Server(e.to_string()))?;

                Ok(vec![MemcacheResponse::Stored])
            }
            MemcacheCommand::Get { keys } => {
                let mut responses = Vec::new();
                let mut store = self.kv_store.lock().await;

                for key in keys {
                    if let Some(value_json) = store
                        .get(key.clone())
                        .await
                        .map_err(|e| MemcacheError::Server(e.to_string()))?
                    {
                        let value: MemcacheValue =
                            serde_json::from_str(&value_json).map_err(|e| {
                                MemcacheError::Server(e.to_string())
                            })?;

                        responses.push(MemcacheResponse::Value {
                            key,
                            flags: value.flags,
                            data: value.data,
                        });
                    }
                }

                responses.push(MemcacheResponse::End);
                Ok(responses)
            }
            MemcacheCommand::Delete { key } => {
                let mut store = self.kv_store.lock().await;
                let result = store
                    .delete(key)
                    .await
                    .map_err(|e| MemcacheError::Server(e.to_string()))?;

                if result.is_some() {
                    Ok(vec![MemcacheResponse::Deleted])
                } else {
                    Ok(vec![MemcacheResponse::NotFound])
                }
            }
        }
    }
}
