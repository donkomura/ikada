use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SnapshotMetadata {
    pub last_included_index: u32,
    pub last_included_term: u32,
}
