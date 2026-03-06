use crate::types::LogIndex;

#[derive(Debug, thiserror::Error)]
pub enum RaftError {
    #[error("not the leader")]
    NotLeader,

    #[error("log index {index} out of bounds (log length: {log_len})")]
    LogIndexOutOfBounds { index: LogIndex, log_len: usize },

    #[error("storage operation failed")]
    Storage(#[from] StorageError),

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("failed to serialize state")]
    Serialize(#[source] Box<bincode::ErrorKind>),

    #[error("failed to deserialize state")]
    Deserialize(#[source] Box<bincode::ErrorKind>),

    #[error("I/O error: {context}")]
    Io {
        #[source]
        source: std::io::Error,
        context: String,
    },
}
