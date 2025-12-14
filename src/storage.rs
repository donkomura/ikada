use crate::raft::PersistentState;
use anyhow::Result;
use std::sync::Arc;
use tokio::sync::Mutex;

#[async_trait::async_trait]
pub trait Storage<T>: Send + Sync + std::fmt::Debug
where
    T: Send + Sync + Clone,
{
    async fn save(&mut self, state: &PersistentState<T>) -> Result<()>;
    async fn load(&self) -> Result<Option<PersistentState<T>>>;
}

pub struct MemStorage<T: Send + Sync> {
    data: Arc<Mutex<Option<PersistentState<T>>>>,
}

impl<T: Send + Sync> std::fmt::Debug for MemStorage<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemStorage").finish()
    }
}

impl<T: Send + Sync> Default for MemStorage<T> {
    fn default() -> Self {
        Self {
            data: Arc::new(Mutex::new(None)),
        }
    }
}

#[async_trait::async_trait]
impl<T> Storage<T> for MemStorage<T>
where
    T: Send + Sync + Clone,
{
    async fn save(&mut self, state: &PersistentState<T>) -> Result<()> {
        let mut d = self.data.lock().await;
        *d = Some(state.clone());
        Ok(())
    }
    async fn load(&self) -> Result<Option<PersistentState<T>>> {
        let d = self.data.lock().await;
        Ok(d.clone())
    }
}
