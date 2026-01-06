use thiserror::Error;

#[derive(Error, Debug)]
pub enum MemcacheError {
    #[error("Protocol error: {0}")]
    Protocol(String),

    #[error("Client error: {0}")]
    Client(String),

    #[error("Server error: {0}")]
    Server(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("KVStore error: {0}")]
    KVStore(String),
}

pub type Result<T> = std::result::Result<T, MemcacheError>;
