use crate::error::StorageError;
use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};
use std::collections::HashMap;

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

/// In-memory [`Storage`] implementation backed by a `HashMap`.
///
/// Decisions are lost when the process exits. Suitable for tests and
/// ephemeral deployments where durability is not required.
pub struct MemoryStorage<V> {
    decisions: HashMap<u64, V>,
}

impl<V> MemoryStorage<V> {
    /// Creates an empty `MemoryStorage`.
    pub fn new() -> Self {
        Self {
            decisions: HashMap::new(),
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
}
