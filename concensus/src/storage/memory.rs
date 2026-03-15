use crate::error::StorageError;
use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};
use std::collections::HashMap;

use super::{AcceptorState, Storage};

/// In-memory [`Storage`] implementation backed by `HashMap`s.
///
/// Decisions and acceptor states are lost when the process exits. Suitable for
/// tests and ephemeral deployments where durability is not required.
pub struct MemoryStorage<V> {
    decisions: HashMap<u64, V>,
    acceptor_states: HashMap<u64, AcceptorState<V>>,
}

impl<V> MemoryStorage<V> {
    /// Creates an empty `MemoryStorage`.
    pub fn new() -> Self {
        Self {
            decisions: HashMap::new(),
            acceptor_states: HashMap::new(),
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
        // Once a slot is decided, acceptor state is no longer needed.
        self.acceptor_states.remove(&slot);
        Ok(())
    }

    async fn load_decisions(&self) -> Result<Vec<(u64, V)>, StorageError> {
        Ok(self
            .decisions
            .iter()
            .map(|(&slot, value)| (slot, value.clone()))
            .collect())
    }

    async fn save_acceptor_state(
        &mut self,
        slot: u64,
        highest_promised: Option<crate::message::ProposalNumber>,
        accepted: Option<(crate::message::ProposalNumber, V)>,
    ) -> Result<(), StorageError> {
        self.acceptor_states.insert(
            slot,
            AcceptorState {
                slot,
                highest_promised,
                accepted,
            },
        );
        Ok(())
    }

    async fn load_acceptor_states(&self) -> Result<Vec<AcceptorState<V>>, StorageError> {
        Ok(self.acceptor_states.values().cloned().collect())
    }

    async fn delete_acceptor_state(&mut self, slot: u64) -> Result<(), StorageError> {
        self.acceptor_states.remove(&slot);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::NodeId;
    use crate::message::ProposalNumber;

    fn test_node_id() -> NodeId {
        NodeId::new("node-1", 1000)
    }

    fn make_pn(round: u64) -> ProposalNumber {
        (round, test_node_id())
    }

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
    async fn save_and_load_acceptor_state() {
        let mut storage = MemoryStorage::<String>::new();
        storage
            .save_acceptor_state(1, Some(make_pn(1)), None)
            .await
            .unwrap();

        let states = storage.load_acceptor_states().await.unwrap();
        assert_eq!(states.len(), 1);
        assert_eq!(states[0].slot, 1);
        assert_eq!(states[0].highest_promised, Some(make_pn(1)));
        assert!(states[0].accepted.is_none());
    }

    #[tokio::test]
    async fn save_decision_cleans_acceptor_state() {
        let mut storage = MemoryStorage::new();
        storage
            .save_acceptor_state(1, Some(make_pn(1)), Some((make_pn(1), "hello".to_string())))
            .await
            .unwrap();
        assert_eq!(storage.load_acceptor_states().await.unwrap().len(), 1);

        storage.save_decision(1, "hello".to_string()).await.unwrap();
        assert!(storage.load_acceptor_states().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn delete_acceptor_state() {
        let mut storage = MemoryStorage::<String>::new();
        storage
            .save_acceptor_state(5, Some(make_pn(2)), None)
            .await
            .unwrap();
        assert_eq!(storage.load_acceptor_states().await.unwrap().len(), 1);

        storage.delete_acceptor_state(5).await.unwrap();
        assert!(storage.load_acceptor_states().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn empty_acceptor_states() {
        let storage = MemoryStorage::<String>::new();
        let states = storage.load_acceptor_states().await.unwrap();
        assert!(states.is_empty());
    }
}
