use crate::config::NodeId;
use crate::error::StorageError;
use crate::message::LogEntry;
use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Durable storage for consensus decisions.
///
/// Implementations persist decided slot-value pairs so that a node can recover
/// its state after a restart. The [`Node`](crate::Node) calls
/// [`load_decisions`](Storage::load_decisions) once at startup and
/// [`save_decision`](Storage::save_decision) each time a new value is decided.
///
/// For production use, implement this trait with a database or file-backed store.
/// For testing, use [`MemoryStorage`].
#[async_trait]
pub trait Storage<V>: Send + 'static
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

/// Durable storage for Raft-specific state.
///
/// Extends [`Storage`] with the persistent Raft state from the paper:
/// `currentTerm`, `votedFor`, and the replicated log. The [`Node`](crate::Node)
/// constructed via `Node::with_raft_config` requires this trait; it is invoked
/// to flush term, vote, and log entries to durable storage before sending the
/// corresponding RPC.
#[async_trait]
pub trait RaftStorage<V>: Storage<V>
where
    V: Serialize + DeserializeOwned + Clone + Send,
{
    /// Persist the current Raft term.
    async fn save_term(&mut self, term: u64) -> Result<(), StorageError>;

    /// Load the persisted Raft term, or 0 if no term was ever saved.
    async fn load_term(&self) -> Result<u64, StorageError>;

    /// Persist the candidate this node voted for in the current term, or `None`
    /// to clear the vote (e.g., on term advance).
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

/// In-memory [`Storage`] implementation backed by a `HashMap`.
///
/// Decisions are lost when the process exits. Suitable for tests and
/// ephemeral deployments where durability is not required.
pub struct MemoryStorage<V> {
    decisions: HashMap<u64, V>,
    term: u64,
    voted_for: Option<NodeId>,
    log: Vec<LogEntry<V>>,
}

impl<V> MemoryStorage<V> {
    /// Creates an empty `MemoryStorage`.
    pub fn new() -> Self {
        Self {
            decisions: HashMap::new(),
            term: 0,
            voted_for: None,
            log: Vec::new(),
        }
    }
}

impl<V> Default for MemoryStorage<V> {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl<V> Storage<V> for MemoryStorage<V>
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

#[async_trait]
impl<V> RaftStorage<V> for MemoryStorage<V>
where
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
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

/// Adapter that lets one `RaftStorage<V>` instance be shared between
/// the [`Node`](crate::Node) `Storage<V>` and `RaftStorage<V>` requirements.
///
/// `Node::with_raft_config` boxes the user's storage once and gives both
/// roles a clone of this adapter. All operations are serialized through an
/// internal `tokio::sync::Mutex`.
pub(crate) struct SharedRaftStorage<V> {
    inner: Arc<Mutex<Box<dyn RaftStorage<V> + Send>>>,
}

impl<V> SharedRaftStorage<V>
where
    V: Serialize + DeserializeOwned + Clone + Send,
{
    pub(crate) fn new(s: impl RaftStorage<V> + 'static) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Box::new(s))),
        }
    }
}

impl<V> Clone for SharedRaftStorage<V> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

#[async_trait]
impl<V> Storage<V> for SharedRaftStorage<V>
where
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    async fn save_decision(&mut self, slot: u64, value: V) -> Result<(), StorageError> {
        self.inner.lock().await.save_decision(slot, value).await
    }
    async fn load_decisions(&self) -> Result<Vec<(u64, V)>, StorageError> {
        self.inner.lock().await.load_decisions().await
    }
}

#[async_trait]
impl<V> RaftStorage<V> for SharedRaftStorage<V>
where
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    async fn save_term(&mut self, term: u64) -> Result<(), StorageError> {
        self.inner.lock().await.save_term(term).await
    }
    async fn load_term(&self) -> Result<u64, StorageError> {
        self.inner.lock().await.load_term().await
    }
    async fn save_voted_for(&mut self, voted_for: Option<NodeId>) -> Result<(), StorageError> {
        self.inner.lock().await.save_voted_for(voted_for).await
    }
    async fn load_voted_for(&self) -> Result<Option<NodeId>, StorageError> {
        self.inner.lock().await.load_voted_for().await
    }
    async fn append_log(&mut self, entries: &[LogEntry<V>]) -> Result<(), StorageError> {
        self.inner.lock().await.append_log(entries).await
    }
    async fn truncate_log_from(&mut self, index: u64) -> Result<(), StorageError> {
        self.inner.lock().await.truncate_log_from(index).await
    }
    async fn load_log(&self) -> Result<Vec<LogEntry<V>>, StorageError> {
        self.inner.lock().await.load_log().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn empty_storage_returns_no_decisions() {
        let storage = MemoryStorage::<String>::new();
        let decisions = storage.load_decisions().await.unwrap();
        assert!(decisions.is_empty());
    }

    #[tokio::test]
    async fn save_and_load_decisions() {
        let mut storage = MemoryStorage::new();
        storage.save_decision(0, "hello".to_string()).await.unwrap();
        storage.save_decision(2, "world".to_string()).await.unwrap();
        let decisions = storage.load_decisions().await.unwrap();
        assert_eq!(decisions.len(), 2);
        assert!(decisions.contains(&(0, "hello".to_string())));
        assert!(decisions.contains(&(2, "world".to_string())));
    }

    #[tokio::test]
    async fn save_overwrites_existing_slot() {
        let mut storage = MemoryStorage::new();
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
    async fn raft_storage_term_roundtrip() {
        let mut s = MemoryStorage::<String>::new();
        assert_eq!(s.load_term().await.unwrap(), 0);
        s.save_term(7).await.unwrap();
        assert_eq!(s.load_term().await.unwrap(), 7);
    }

    #[tokio::test]
    async fn raft_storage_voted_for_roundtrip() {
        use crate::config::NodeId;
        let mut s = MemoryStorage::<String>::new();
        assert!(s.load_voted_for().await.unwrap().is_none());
        let nid = NodeId::new("a", 1);
        s.save_voted_for(Some(nid.clone())).await.unwrap();
        assert_eq!(s.load_voted_for().await.unwrap(), Some(nid));
        s.save_voted_for(None).await.unwrap();
        assert!(s.load_voted_for().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn raft_storage_log_append_load_truncate() {
        use crate::message::LogEntry;
        let mut s = MemoryStorage::<String>::new();
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
        use crate::message::LogEntry;
        let mut s = MemoryStorage::<String>::new();
        s.append_log(&[LogEntry {
            term: 1,
            value: "a".into(),
        }])
        .await
        .unwrap();
        // Truncate from index well past the end - should be no-op, not panic or error
        s.truncate_log_from(100).await.unwrap();
        assert_eq!(s.load_log().await.unwrap().len(), 1);
    }
}
