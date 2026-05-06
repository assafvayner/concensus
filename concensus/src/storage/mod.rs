mod memory;
pub use memory::MemoryStorage;

#[cfg(feature = "duckdb-storage")]
mod duckdb;
#[cfg(feature = "duckdb-storage")]
pub use self::duckdb::DuckDbStorage;

use crate::error::StorageError;
use crate::message::ProposalNumber;
use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};

/// Acceptor state for a single Paxos slot.
///
/// Captures the promises and accepted values that an acceptor must persist
/// to survive crashes. On recovery the node reloads these states to resume
/// participation in any in-progress slots without violating the Paxos safety
/// invariant.
#[derive(Clone, Debug, PartialEq)]
pub struct AcceptorState<V> {
    /// The slot this state belongs to.
    pub slot: u64,
    /// The highest proposal number this acceptor has promised not to accept
    /// proposals below, if any.
    pub highest_promised: Option<ProposalNumber>,
    /// The proposal number and value that this acceptor last accepted, if any.
    pub accepted: Option<(ProposalNumber, V)>,
}

impl<V> AcceptorState<V> {
    pub(crate) fn validation_error(&self) -> Option<&'static str> {
        if self.accepted.is_some() && self.highest_promised.is_none() {
            return Some("accepted value without highest promised proposal");
        }

        if let Some(ref highest_promised) = self.highest_promised {
            if highest_promised.0 == 0 {
                return Some("highest promised proposal round must be greater than zero");
            }
        }
        if let Some((ref accepted_proposal, _)) = self.accepted {
            if accepted_proposal.0 == 0 {
                return Some("accepted proposal round must be greater than zero");
            }
        }

        if let (Some(ref highest_promised), Some((ref accepted_proposal, _))) =
            (&self.highest_promised, &self.accepted)
        {
            if accepted_proposal > highest_promised {
                return Some("accepted proposal must not exceed highest promised proposal");
            }
        }

        None
    }

    /// Returns `true` if this state satisfies Paxos invariants:
    ///
    /// 1. If `accepted` is `Some`, then `highest_promised` must also be `Some`.
    /// 2. All proposal round numbers (first element of the tuple) must be > 0.
    /// 3. The accepted proposal number must be <= `highest_promised`.
    pub fn is_valid(&self) -> bool {
        self.validation_error().is_none()
    }
}

/// Durable storage for consensus decisions and acceptor state.
///
/// Implementations persist decided slot-value pairs and acceptor state so that
/// a node can recover after a restart. The [`Node`](crate::Node) calls
/// [`load_decisions`](Storage::load_decisions) and
/// [`load_acceptor_states`](Storage::load_acceptor_states) once at startup, and
/// the mutation methods each time state changes.
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
    /// Implementations must also remove any acceptor state for this slot in
    /// this method, since [`Node`](crate::Node) relies on `save_decision` for
    /// decision-time cleanup and does not call
    /// [`delete_acceptor_state`](Storage::delete_acceptor_state) separately.
    async fn save_decision(&mut self, slot: u64, value: V) -> Result<(), StorageError>;

    /// Load all previously persisted decisions.
    ///
    /// Called once during [`Node::run`](crate::Node::run) startup to recover
    /// prior state. Returns `(slot, value)` pairs in any order.
    async fn load_decisions(&self) -> Result<Vec<(u64, V)>, StorageError>;

    /// Persist the acceptor state for a slot.
    ///
    /// Called whenever the acceptor's promise or accepted value changes for a
    /// slot that has not yet been decided.
    async fn save_acceptor_state(
        &mut self,
        slot: u64,
        highest_promised: Option<ProposalNumber>,
        accepted: Option<(ProposalNumber, V)>,
    ) -> Result<(), StorageError>;

    /// Load all persisted acceptor states.
    ///
    /// Called once at startup to restore in-progress slot state.
    async fn load_acceptor_states(&self) -> Result<Vec<AcceptorState<V>>, StorageError>;

    /// Delete the acceptor state for a slot.
    ///
    /// This is for explicit garbage collection or test setup. Decision-time
    /// cleanup is part of the [`save_decision`](Storage::save_decision)
    /// contract.
    async fn delete_acceptor_state(&mut self, slot: u64) -> Result<(), StorageError>;
}

#[async_trait]
impl<V> Storage<V> for Box<dyn Storage<V> + Send + Sync>
where
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    async fn save_decision(&mut self, slot: u64, value: V) -> Result<(), StorageError> {
        (**self).save_decision(slot, value).await
    }

    async fn load_decisions(&self) -> Result<Vec<(u64, V)>, StorageError> {
        (**self).load_decisions().await
    }

    async fn save_acceptor_state(
        &mut self,
        slot: u64,
        highest_promised: Option<ProposalNumber>,
        accepted: Option<(ProposalNumber, V)>,
    ) -> Result<(), StorageError> {
        (**self)
            .save_acceptor_state(slot, highest_promised, accepted)
            .await
    }

    async fn load_acceptor_states(&self) -> Result<Vec<AcceptorState<V>>, StorageError> {
        (**self).load_acceptor_states().await
    }

    async fn delete_acceptor_state(&mut self, slot: u64) -> Result<(), StorageError> {
        (**self).delete_acceptor_state(slot).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::NodeId;

    fn test_node_id() -> NodeId {
        NodeId::new("node-1", 1000)
    }

    fn make_pn(round: u64) -> ProposalNumber {
        (round, test_node_id())
    }

    #[test]
    fn valid_state_promised_only() {
        let state: AcceptorState<String> = AcceptorState {
            slot: 0,
            highest_promised: Some(make_pn(1)),
            accepted: None,
        };
        assert!(state.is_valid());
    }

    #[test]
    fn valid_state_promised_and_accepted() {
        let state = AcceptorState {
            slot: 0,
            highest_promised: Some(make_pn(2)),
            accepted: Some((make_pn(1), "hello".to_string())),
        };
        assert!(state.is_valid());
    }

    #[test]
    fn valid_state_accepted_equals_promised() {
        let state = AcceptorState {
            slot: 0,
            highest_promised: Some(make_pn(1)),
            accepted: Some((make_pn(1), "hello".to_string())),
        };
        assert!(state.is_valid());
    }

    #[test]
    fn invalid_accepted_without_promised() {
        let state = AcceptorState {
            slot: 0,
            highest_promised: None,
            accepted: Some((make_pn(1), "hello".to_string())),
        };
        assert!(!state.is_valid());
    }

    #[test]
    fn invalid_zero_round_promised() {
        let state: AcceptorState<String> = AcceptorState {
            slot: 0,
            highest_promised: Some(make_pn(0)),
            accepted: None,
        };
        assert!(!state.is_valid());
    }

    #[test]
    fn invalid_zero_round_accepted() {
        let state = AcceptorState {
            slot: 0,
            highest_promised: Some(make_pn(1)),
            accepted: Some((make_pn(0), "hello".to_string())),
        };
        assert!(!state.is_valid());
    }

    #[test]
    fn invalid_accepted_greater_than_promised() {
        let state = AcceptorState {
            slot: 0,
            highest_promised: Some(make_pn(1)),
            accepted: Some((make_pn(2), "hello".to_string())),
        };
        assert!(!state.is_valid());
    }

    #[test]
    fn empty_state_is_valid() {
        let state: AcceptorState<String> = AcceptorState {
            slot: 0,
            highest_promised: None,
            accepted: None,
        };
        assert!(state.is_valid());
    }
}
