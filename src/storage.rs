use crate::raft::PersistentState;
use crate::snapshot::SnapshotMetadata;
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
    async fn save_snapshot(
        &self,
        metastate: &SnapshotMetadata,
        state: &[u8],
    ) -> Result<()>;
    async fn load_snapshot(
        &self,
    ) -> Result<Option<(SnapshotMetadata, Vec<u8>)>>;
}

type GuardOption<T> = Arc<Mutex<Option<T>>>;

pub struct MemStorage<T: Send + Sync> {
    state: GuardOption<PersistentState<T>>,
    data: GuardOption<(SnapshotMetadata, Vec<u8>)>,
}

impl<T: Send + Sync> std::fmt::Debug for MemStorage<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemStorage").finish()
    }
}

impl<T: Send + Sync> Default for MemStorage<T> {
    fn default() -> Self {
        Self {
            state: Arc::new(Mutex::new(None)),
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
        let mut s = self.state.lock().await;
        *s = Some(state.clone());
        Ok(())
    }
    async fn load(&self) -> Result<Option<PersistentState<T>>> {
        let s = self.state.lock().await;
        Ok(s.clone())
    }
    async fn save_snapshot(
        &self,
        metadata: &SnapshotMetadata,
        data: &[u8],
    ) -> Result<()> {
        let mut data_guard = self.data.lock().await;
        *data_guard = Some((metadata.clone(), data.to_vec()));
        Ok(())
    }

    async fn load_snapshot(
        &self,
    ) -> Result<Option<(SnapshotMetadata, Vec<u8>)>> {
        let data = self.data.lock().await;
        Ok(data.clone())
    }
}
