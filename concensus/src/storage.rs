use crate::config::NodeId;
use crate::error::StorageError;
use crate::message::LogEntry;
use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};
use std::collections::HashMap;

/// Durable storage for a Multi-Paxos node.
///
/// Implementations persist decided slot-value pairs so a node can recover its
/// state after a restart. The [`Node`](crate::Node) calls
/// [`load_decisions`](PaxosStorage::load_decisions) once at startup and
/// [`save_decision`](PaxosStorage::save_decision) each time a new value is
/// decided.
///
/// For production use, implement this trait with a database or file-backed
/// store. For testing, use [`PaxosMemoryStorage`].
#[async_trait]
pub trait PaxosStorage<V>: Send + 'static
where
    V: Serialize + DeserializeOwned + Clone + Send,
{
    /// Persist a decided value for the given slot.
    ///
    /// Called exactly once per slot when a value reaches consensus.
    async fn save_decision(&mut self, slot: u64, value: V) -> Result<(), StorageError>;

    /// Load all previously persisted decisions.
    ///
    /// Called once during [`Node::run`](crate::Node::run) startup to recover
    /// prior state. Returns `(slot, value)` pairs in any order.
    async fn load_decisions(&self) -> Result<Vec<(u64, V)>, StorageError>;
}

/// Durable storage for a Raft node.
///
/// Covers both the decided-value mapping (mirroring
/// [`PaxosStorage`](PaxosStorage) for parity across algorithms) and the
/// persistent Raft state from the paper: `currentTerm`, `votedFor`, and the
/// replicated log. The [`Node`](crate::Node) constructed via
/// [`Node::with_raft_config`](crate::Node::with_raft_config) requires this
/// trait; it is invoked to flush term, vote, and log entries before sending
/// the corresponding RPC.
///
/// Use [`RaftMemoryStorage`] in tests; production deployments should provide
/// a durable backing store.
#[async_trait]
pub trait RaftStorage<V>: Send + 'static
where
    V: Serialize + DeserializeOwned + Clone + Send,
{
    /// Persist a decided value for the given slot.
    async fn save_decision(&mut self, slot: u64, value: V) -> Result<(), StorageError>;

    /// Load all previously persisted decisions.
    async fn load_decisions(&self) -> Result<Vec<(u64, V)>, StorageError>;

    /// Persist the current Raft term.
    async fn save_term(&mut self, term: u64) -> Result<(), StorageError>;

    /// Load the persisted Raft term, or 0 if no term was ever saved.
    async fn load_term(&self) -> Result<u64, StorageError>;

    /// Persist the candidate this node voted for in the current term, or
    /// `None` to clear the vote (e.g., on term advance).
    async fn save_voted_for(&mut self, voted_for: Option<NodeId>) -> Result<(), StorageError>;

    /// Load the persisted vote, if any.
    async fn load_voted_for(&self) -> Result<Option<NodeId>, StorageError>;

    /// Append entries to the end of the log, in order.
    async fn append_log(&mut self, entries: &[LogEntry<V>]) -> Result<(), StorageError>;

    /// Drop log entries from `index` (inclusive) onward. A no-op if `index` is
    /// past the end of the log.
    async fn truncate_log_from(&mut self, index: u64) -> Result<(), StorageError>;

    /// Load the entire log.
    async fn load_log(&self) -> Result<Vec<LogEntry<V>>, StorageError>;
}

/// In-memory [`PaxosStorage`] implementation backed by a `HashMap`.
///
/// Decisions are lost when the process exits. Suitable for tests and
/// ephemeral deployments where durability is not required.
pub struct PaxosMemoryStorage<V> {
    decisions: HashMap<u64, V>,
}

impl<V> PaxosMemoryStorage<V> {
    /// Creates an empty `PaxosMemoryStorage`.
    pub fn new() -> Self {
        Self {
            decisions: HashMap::new(),
        }
    }
}

impl<V> Default for PaxosMemoryStorage<V> {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl<V> PaxosStorage<V> for PaxosMemoryStorage<V>
where
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    async fn save_decision(&mut self, slot: u64, value: V) -> Result<(), StorageError> {
        self.decisions.insert(slot, value);
        Ok(())
    }

    async fn load_decisions(&self) -> Result<Vec<(u64, V)>, StorageError> {
        Ok(self
            .decisions
            .iter()
            .map(|(&slot, value)| (slot, value.clone()))
            .collect())
    }
}

/// In-memory [`RaftStorage`] implementation.
///
/// Holds the decision map alongside the Raft persistent state (term,
/// votedFor, log). Lost on process exit; suitable for tests and ephemeral
/// deployments.
pub struct RaftMemoryStorage<V> {
    decisions: HashMap<u64, V>,
    term: u64,
    voted_for: Option<NodeId>,
    log: Vec<LogEntry<V>>,
}

impl<V> RaftMemoryStorage<V> {
    /// Creates an empty `RaftMemoryStorage`.
    pub fn new() -> Self {
        Self {
            decisions: HashMap::new(),
            term: 0,
            voted_for: None,
            log: Vec::new(),
        }
    }
}

impl<V> Default for RaftMemoryStorage<V> {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl<V> RaftStorage<V> for RaftMemoryStorage<V>
where
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    async fn save_decision(&mut self, slot: u64, value: V) -> Result<(), StorageError> {
        self.decisions.insert(slot, value);
        Ok(())
    }

    async fn load_decisions(&self) -> Result<Vec<(u64, V)>, StorageError> {
        Ok(self
            .decisions
            .iter()
            .map(|(&slot, value)| (slot, value.clone()))
            .collect())
    }

    async fn save_term(&mut self, term: u64) -> Result<(), StorageError> {
        self.term = term;
        Ok(())
    }

    async fn load_term(&self) -> Result<u64, StorageError> {
        Ok(self.term)
    }

    async fn save_voted_for(&mut self, voted_for: Option<NodeId>) -> Result<(), StorageError> {
        self.voted_for = voted_for;
        Ok(())
    }

    async fn load_voted_for(&self) -> Result<Option<NodeId>, StorageError> {
        Ok(self.voted_for.clone())
    }

    async fn append_log(&mut self, entries: &[LogEntry<V>]) -> Result<(), StorageError> {
        self.log.extend_from_slice(entries);
        Ok(())
    }

    async fn truncate_log_from(&mut self, index: u64) -> Result<(), StorageError> {
        let idx = index as usize;
        if idx < self.log.len() {
            self.log.truncate(idx);
        }
        Ok(())
    }

    async fn load_log(&self) -> Result<Vec<LogEntry<V>>, StorageError> {
        Ok(self.log.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn empty_paxos_storage_returns_no_decisions() {
        let storage = PaxosMemoryStorage::<String>::new();
        let decisions = storage.load_decisions().await.unwrap();
        assert!(decisions.is_empty());
    }

    #[tokio::test]
    async fn save_and_load_paxos_decisions() {
        let mut storage = PaxosMemoryStorage::new();
        storage.save_decision(0, "hello".to_string()).await.unwrap();
        storage.save_decision(2, "world".to_string()).await.unwrap();
        let decisions = storage.load_decisions().await.unwrap();
        assert_eq!(decisions.len(), 2);
        assert!(decisions.contains(&(0, "hello".to_string())));
        assert!(decisions.contains(&(2, "world".to_string())));
    }

    #[tokio::test]
    async fn paxos_save_overwrites_existing_slot() {
        let mut storage = PaxosMemoryStorage::new();
        storage.save_decision(0, "first".to_string()).await.unwrap();
        storage
            .save_decision(0, "second".to_string())
            .await
            .unwrap();
        let decisions = storage.load_decisions().await.unwrap();
        assert_eq!(decisions.len(), 1);
        assert!(decisions.contains(&(0, "second".to_string())));
    }

    #[tokio::test]
    async fn raft_storage_decision_roundtrip() {
        let mut s = RaftMemoryStorage::<String>::new();
        s.save_decision(0, "a".into()).await.unwrap();
        s.save_decision(1, "b".into()).await.unwrap();
        let mut decs = s.load_decisions().await.unwrap();
        decs.sort_by_key(|(slot, _)| *slot);
        assert_eq!(decs, vec![(0, "a".into()), (1, "b".into())]);
    }

    #[tokio::test]
    async fn raft_storage_term_roundtrip() {
        let mut s = RaftMemoryStorage::<String>::new();
        assert_eq!(s.load_term().await.unwrap(), 0);
        s.save_term(7).await.unwrap();
        assert_eq!(s.load_term().await.unwrap(), 7);
    }

    #[tokio::test]
    async fn raft_storage_voted_for_roundtrip() {
        let mut s = RaftMemoryStorage::<String>::new();
        assert!(s.load_voted_for().await.unwrap().is_none());
        let nid = NodeId::new("a", 1);
        s.save_voted_for(Some(nid.clone())).await.unwrap();
        assert_eq!(s.load_voted_for().await.unwrap(), Some(nid));
        s.save_voted_for(None).await.unwrap();
        assert!(s.load_voted_for().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn raft_storage_log_append_load_truncate() {
        let mut s = RaftMemoryStorage::<String>::new();
        assert!(s.load_log().await.unwrap().is_empty());
        s.append_log(&[
            LogEntry {
                term: 1,
                value: "a".into(),
            },
            LogEntry {
                term: 1,
                value: "b".into(),
            },
        ])
        .await
        .unwrap();
        s.append_log(&[LogEntry {
            term: 2,
            value: "c".into(),
        }])
        .await
        .unwrap();
        let log = s.load_log().await.unwrap();
        assert_eq!(log.len(), 3);
        assert_eq!(log[2].term, 2);
        s.truncate_log_from(1).await.unwrap();
        let log = s.load_log().await.unwrap();
        assert_eq!(log.len(), 1);
        assert_eq!(log[0].value, "a");
    }

    #[tokio::test]
    async fn raft_storage_truncate_past_end_is_noop() {
        let mut s = RaftMemoryStorage::<String>::new();
        s.append_log(&[LogEntry {
            term: 1,
            value: "a".into(),
        }])
        .await
        .unwrap();
        s.truncate_log_from(100).await.unwrap();
        assert_eq!(s.load_log().await.unwrap().len(), 1);
    }
}
