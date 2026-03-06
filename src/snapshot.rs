use crate::types::{LogIndex, Term};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SnapshotMetadata {
    pub last_included_index: LogIndex,
    pub last_included_term: Term,
}
