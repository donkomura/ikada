//! RPC request handlers as pure functions.
//!
//! These functions process Raft RPCs independently of the Node structure,
//! taking Arc<Mutex<RaftState>> directly. This design allows:
//! - Easy unit testing without mocking the entire Node
//! - Clear separation between consensus logic and node lifecycle
//! - Potential reuse in different execution contexts (e.g., embedded scenarios)

mod append_entries;
mod client_command;
mod vote;

pub use append_entries::handle_append_entries;
pub use client_command::{
    handle_client_request_impl, handle_read_index_request,
};
pub use vote::handle_request_vote;
