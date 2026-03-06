use std::fmt;
use tarpc::serde::{Deserialize, Serialize};

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Default,
    Serialize,
    Deserialize,
)]
#[serde(crate = "tarpc::serde")]
pub struct Term(u32);

impl Term {
    pub fn new(value: u32) -> Self {
        Self(value)
    }

    pub fn as_u32(self) -> u32 {
        self.0
    }

    pub fn increment(&self) -> Self {
        Self(self.0 + 1)
    }
}

impl fmt::Display for Term {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "term-{}", self.0)
    }
}

impl From<u32> for Term {
    fn from(value: u32) -> Self {
        Self(value)
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Default,
    Serialize,
    Deserialize,
)]
#[serde(crate = "tarpc::serde")]
pub struct NodeId(u32);

impl NodeId {
    pub fn new(value: u32) -> Self {
        Self(value)
    }

    pub fn as_u32(self) -> u32 {
        self.0
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "node-{}", self.0)
    }
}

impl From<u32> for NodeId {
    fn from(value: u32) -> Self {
        Self(value)
    }
}

impl From<u16> for NodeId {
    fn from(value: u16) -> Self {
        Self(value as u32)
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Default,
    Serialize,
    Deserialize,
)]
#[serde(crate = "tarpc::serde")]
pub struct LogIndex(u32);

impl LogIndex {
    pub fn new(value: u32) -> Self {
        Self(value)
    }

    pub fn as_u32(self) -> u32 {
        self.0
    }

    pub fn as_usize(self) -> usize {
        self.0 as usize
    }

    pub fn saturating_sub(self, rhs: u32) -> Self {
        Self(self.0.saturating_sub(rhs))
    }

    pub fn saturating_add(self, rhs: u32) -> Self {
        Self(self.0.saturating_add(rhs))
    }

    pub fn checked_prev_usize(self) -> Option<usize> {
        if self.0 > 0 {
            Some((self.0 - 1) as usize)
        } else {
            None
        }
    }
}

impl fmt::Display for LogIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "log-{}", self.0)
    }
}

impl From<u32> for LogIndex {
    fn from(value: u32) -> Self {
        Self(value)
    }
}

impl std::ops::Add<u32> for LogIndex {
    type Output = Self;
    fn add(self, rhs: u32) -> Self {
        Self(self.0 + rhs)
    }
}

impl std::ops::AddAssign<u32> for LogIndex {
    fn add_assign(&mut self, rhs: u32) {
        self.0 += rhs;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn term_ordering() {
        let t1 = Term::new(1);
        let t2 = Term::new(2);
        assert!(t2 > t1);
        assert_eq!(t1.increment(), t2);
    }

    #[test]
    fn term_display() {
        assert_eq!(Term::new(5).to_string(), "term-5");
    }

    #[test]
    fn term_default_is_zero() {
        assert_eq!(Term::default().as_u32(), 0);
    }

    #[test]
    fn node_id_equality() {
        let n1 = NodeId::new(42);
        let n2 = NodeId::new(42);
        let n3 = NodeId::new(99);
        assert_eq!(n1, n2);
        assert_ne!(n1, n3);
    }

    #[test]
    fn node_id_from_u16() {
        let id = NodeId::from(8080u16);
        assert_eq!(id.as_u32(), 8080);
    }

    #[test]
    fn node_id_display() {
        assert_eq!(NodeId::new(3).to_string(), "node-3");
    }

    #[test]
    fn log_index_arithmetic() {
        let idx = LogIndex::new(5);
        assert_eq!(idx + 3, LogIndex::new(8));
        assert_eq!(idx.saturating_sub(2), LogIndex::new(3));
        assert_eq!(idx.saturating_sub(10), LogIndex::new(0));
        assert_eq!(idx.saturating_add(1), LogIndex::new(6));
    }

    #[test]
    fn log_index_add_assign() {
        let mut idx = LogIndex::new(5);
        idx += 3;
        assert_eq!(idx, LogIndex::new(8));
    }

    #[test]
    fn log_index_checked_prev_usize() {
        assert_eq!(LogIndex::new(0).checked_prev_usize(), None);
        assert_eq!(LogIndex::new(1).checked_prev_usize(), Some(0));
        assert_eq!(LogIndex::new(5).checked_prev_usize(), Some(4));
    }

    #[test]
    fn log_index_as_usize() {
        assert_eq!(LogIndex::new(3).as_usize(), 3);
    }

    #[test]
    fn log_index_display() {
        assert_eq!(LogIndex::new(10).to_string(), "log-10");
    }

    #[test]
    fn log_index_ordering() {
        let a = LogIndex::new(1);
        let b = LogIndex::new(5);
        assert!(b > a);
        assert!(a < b);
    }

    #[test]
    fn types_cannot_be_mixed() {
        let _term = Term::new(1);
        let _node = NodeId::new(1);
        let _index = LogIndex::new(1);
        // These are different types despite same inner value.
        // Mixing them requires explicit conversion — compile-time safety.
    }

    #[test]
    fn term_serde_roundtrip() {
        let term = Term::new(42);
        let serialized = bincode::serialize(&term).unwrap();
        let deserialized: Term = bincode::deserialize(&serialized).unwrap();
        assert_eq!(term, deserialized);
    }

    #[test]
    fn node_id_serde_roundtrip() {
        let id = NodeId::new(99);
        let serialized = bincode::serialize(&id).unwrap();
        let deserialized: NodeId = bincode::deserialize(&serialized).unwrap();
        assert_eq!(id, deserialized);
    }

    #[test]
    fn log_index_serde_roundtrip() {
        let idx = LogIndex::new(100);
        let serialized = bincode::serialize(&idx).unwrap();
        let deserialized: LogIndex = bincode::deserialize(&serialized).unwrap();
        assert_eq!(idx, deserialized);
    }

    #[test]
    fn log_index_default_is_zero() {
        assert_eq!(LogIndex::default().as_u32(), 0);
    }

    #[test]
    fn term_hash_usable_in_hashset() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(Term::new(1));
        set.insert(Term::new(2));
        set.insert(Term::new(1));
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn node_id_hash_usable_in_hashmap() {
        use std::collections::HashMap;
        let mut map = HashMap::new();
        map.insert(NodeId::new(1), "leader");
        map.insert(NodeId::new(2), "follower");
        assert_eq!(map.get(&NodeId::new(1)), Some(&"leader"));
    }
}
