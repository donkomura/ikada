use crate::raft::PersistentState;
use crate::snapshot::SnapshotMetadata;
use anyhow::Result;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs;
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

pub struct FileStorage<T: Send + Sync> {
    base_dir: PathBuf,
    _phantom: std::marker::PhantomData<T>,
}

impl<T: Send + Sync> std::fmt::Debug for FileStorage<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileStorage")
            .field("base_dir", &self.base_dir)
            .finish()
    }
}

impl<T: Send + Sync> FileStorage<T> {
    pub fn new(base_dir: PathBuf) -> Self {
        Self {
            base_dir,
            _phantom: std::marker::PhantomData,
        }
    }

    fn state_path(&self) -> PathBuf {
        self.base_dir.join("state.bin")
    }

    fn snapshot_metadata_path(&self) -> PathBuf {
        self.base_dir.join("snapshot_metadata.bin")
    }

    fn snapshot_data_path(&self) -> PathBuf {
        self.base_dir.join("snapshot_data.bin")
    }
}

#[async_trait::async_trait]
impl<T> Storage<T> for FileStorage<T>
where
    T: Send + Sync + Clone + serde::Serialize + serde::de::DeserializeOwned,
{
    async fn save(&mut self, state: &PersistentState<T>) -> Result<()> {
        fs::create_dir_all(&self.base_dir).await?;
        let serialized = bincode::serialize(state)?;
        fs::write(self.state_path(), serialized).await?;
        Ok(())
    }

    async fn load(&self) -> Result<Option<PersistentState<T>>> {
        let path = self.state_path();
        if !path.exists() {
            return Ok(None);
        }
        let bytes = fs::read(path).await?;
        let state: PersistentState<T> = bincode::deserialize(&bytes)?;
        Ok(Some(state))
    }

    async fn save_snapshot(
        &self,
        metadata: &SnapshotMetadata,
        data: &[u8],
    ) -> Result<()> {
        fs::create_dir_all(&self.base_dir).await?;
        let metadata_serialized = bincode::serialize(metadata)?;
        fs::write(self.snapshot_metadata_path(), metadata_serialized).await?;
        fs::write(self.snapshot_data_path(), data).await?;
        Ok(())
    }

    async fn load_snapshot(
        &self,
    ) -> Result<Option<(SnapshotMetadata, Vec<u8>)>> {
        let metadata_path = self.snapshot_metadata_path();
        let data_path = self.snapshot_data_path();

        if !metadata_path.exists() || !data_path.exists() {
            return Ok(None);
        }

        let metadata_bytes = fs::read(metadata_path).await?;
        let metadata: SnapshotMetadata = bincode::deserialize(&metadata_bytes)?;
        let data = fs::read(data_path).await?;

        Ok(Some((metadata, data)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::Entry;

    #[tokio::test]
    async fn test_storage_save_and_load_state() {
        let mut storage = MemStorage::<String>::default();

        let state = PersistentState {
            current_term: 5,
            voted_for: Some(2),
            log: vec![
                Entry {
                    term: 1,
                    command: "cmd1".to_string(),
                },
                Entry {
                    term: 2,
                    command: "cmd2".to_string(),
                },
            ],
        };

        storage.save(&state).await.unwrap();

        let loaded = storage.load().await.unwrap();
        assert!(loaded.is_some());
        let loaded_state = loaded.unwrap();
        assert_eq!(loaded_state.current_term, 5);
        assert_eq!(loaded_state.voted_for, Some(2));
        assert_eq!(loaded_state.log.len(), 2);
        assert_eq!(loaded_state.log[0].command, "cmd1");
        assert_eq!(loaded_state.log[1].command, "cmd2");
    }

    #[tokio::test]
    async fn test_storage_save_and_load_snapshot() {
        let storage = MemStorage::<String>::default();

        let metadata = SnapshotMetadata {
            last_included_index: 100,
            last_included_term: 10,
        };
        let data = b"snapshot data";

        storage.save_snapshot(&metadata, data).await.unwrap();

        let loaded = storage.load_snapshot().await.unwrap();
        assert!(loaded.is_some());
        let (loaded_metadata, loaded_data) = loaded.unwrap();
        assert_eq!(loaded_metadata.last_included_index, 100);
        assert_eq!(loaded_metadata.last_included_term, 10);
        assert_eq!(loaded_data, data);
    }

    #[tokio::test]
    async fn test_storage_load_nonexistent() {
        let storage = MemStorage::<String>::default();

        let loaded = storage.load().await.unwrap();
        assert!(loaded.is_none());

        let loaded_snapshot = storage.load_snapshot().await.unwrap();
        assert!(loaded_snapshot.is_none());
    }

    #[tokio::test]
    async fn test_storage_overwrite() {
        let mut storage = MemStorage::<String>::default();

        let state1 = PersistentState {
            current_term: 1,
            voted_for: None,
            log: vec![],
        };
        storage.save(&state1).await.unwrap();

        let state2 = PersistentState {
            current_term: 2,
            voted_for: Some(1),
            log: vec![Entry {
                term: 2,
                command: "new_cmd".to_string(),
            }],
        };
        storage.save(&state2).await.unwrap();

        let loaded = storage.load().await.unwrap().unwrap();
        assert_eq!(loaded.current_term, 2);
        assert_eq!(loaded.voted_for, Some(1));
        assert_eq!(loaded.log.len(), 1);
    }
}
