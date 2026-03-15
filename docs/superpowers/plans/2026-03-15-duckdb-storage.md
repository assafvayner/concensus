# DuckDB Storage Backend Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a DuckDB-backed storage implementation with full Paxos crash recovery (decisions + acceptor state).

**Architecture:** Extend `Storage<V>` trait with acceptor state methods. Add dirty-slot tracking to the protocol state machine so the node can persist acceptor state before sending messages. DuckDB implementation wraps a `Connection` in `Arc<Mutex<>>` and uses `spawn_blocking` for all SQL.

**Tech Stack:** Rust, DuckDB (`duckdb` crate), `tokio::task::spawn_blocking`, `serde_json` for value serialization.

**Spec:** `docs/superpowers/specs/2026-03-15-duckdb-storage-design.md`

---

## File Map

| Action | Path | Responsibility |
|--------|------|----------------|
| Delete | `concensus/src/storage.rs` | Replaced by module directory |
| Create | `concensus/src/storage/mod.rs` | `Storage<V>` trait, `AcceptorState<V>`, re-exports |
| Create | `concensus/src/storage/memory.rs` | `MemoryStorage<V>` (moved from old storage.rs) |
| Create | `concensus/src/storage/duckdb.rs` | `DuckDbStorage<V>` (feature-gated) |
| Modify | `concensus/src/message.rs` | Make `ProposalNumber` `pub` |
| Modify | `concensus/src/protocol.rs` | Add dirty tracking, `take_dirty_acceptor_slots`, `initialize_from_acceptor_states` |
| Modify | `concensus/src/node.rs` | Persist-before-send flow, acceptor state recovery on startup |
| Modify | `concensus/src/lib.rs` | Re-export new types |
| Modify | `concensus/src/error.rs` | Add `StorageError::Delete` variant |
| Modify | `concensus/Cargo.toml` | Add `duckdb` optional dep + feature |
| Modify | `concensus-tests/Cargo.toml` | Add `duckdb-storage` feature |
| Modify | `concensus-tests/tests/helpers/mod.rs` | Update `SharedMemoryStorage` for new trait methods |
| Create | `concensus-tests/tests/duckdb_recovery.rs` | Integration test: crash recovery with DuckDB |
| Modify | `concensus-demo/Cargo.toml` | Add `duckdb-storage` feature |
| Modify | `concensus-demo/src/node.rs` | Storage backend config, `Box<dyn Storage>` dispatch |
| Create | `concensus-demo/docker-compose.duckdb.yml` | Docker compose with mounted volumes |

---

## Chunk 1: Storage Trait & Module Reorganization

### Task 1: Reorganize storage into a module directory

**Files:**
- Delete: `concensus/src/storage.rs`
- Create: `concensus/src/storage/mod.rs`
- Create: `concensus/src/storage/memory.rs`

- [ ] **Step 1: Create `concensus/src/storage/memory.rs`**

Copy `MemoryStorage` and its tests from `storage.rs` into this new file:

```rust
use std::collections::HashMap;
use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};
use crate::error::StorageError;
use crate::message::ProposalNumber;
use super::{AcceptorState, Storage};

pub struct MemoryStorage<V> {
    decisions: HashMap<u64, V>,
    acceptor_states: HashMap<u64, AcceptorState<V>>,
}

impl<V> MemoryStorage<V> {
    pub fn new() -> Self {
        Self {
            decisions: HashMap::new(),
            acceptor_states: HashMap::new(),
        }
    }
}

impl<V> Default for MemoryStorage<V> {
    fn default() -> Self { Self::new() }
}

#[async_trait]
impl<V> Storage<V> for MemoryStorage<V>
where
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    async fn save_decision(&mut self, slot: u64, value: V) -> Result<(), StorageError> {
        self.decisions.insert(slot, value);
        self.acceptor_states.remove(&slot);
        Ok(())
    }

    async fn load_decisions(&self) -> Result<Vec<(u64, V)>, StorageError> {
        Ok(self.decisions.iter().map(|(&slot, value)| (slot, value.clone())).collect())
    }

    async fn save_acceptor_state(
        &mut self,
        slot: u64,
        highest_promised: Option<ProposalNumber>,
        accepted: Option<(ProposalNumber, V)>,
    ) -> Result<(), StorageError> {
        self.acceptor_states.insert(slot, AcceptorState { slot, highest_promised, accepted });
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
        storage.save_decision(0, "second".to_string()).await.unwrap();
        let decisions = storage.load_decisions().await.unwrap();
        assert_eq!(decisions.len(), 1);
        assert!(decisions.contains(&(0, "second".to_string())));
    }

    #[tokio::test]
    async fn save_and_load_acceptor_state() {
        let mut storage = MemoryStorage::<String>::new();
        let pn = (1u64, crate::config::NodeId::new("node-1", 1000));
        storage.save_acceptor_state(0, Some(pn.clone()), None).await.unwrap();
        let states = storage.load_acceptor_states().await.unwrap();
        assert_eq!(states.len(), 1);
        assert_eq!(states[0].slot, 0);
        assert_eq!(states[0].highest_promised, Some(pn));
        assert!(states[0].accepted.is_none());
    }

    #[tokio::test]
    async fn save_decision_cleans_up_acceptor_state() {
        let mut storage = MemoryStorage::new();
        let pn = (1u64, crate::config::NodeId::new("node-1", 1000));
        storage.save_acceptor_state(0, Some(pn), None).await.unwrap();
        storage.save_decision(0, "decided".to_string()).await.unwrap();
        let states = storage.load_acceptor_states().await.unwrap();
        assert!(states.is_empty());
    }

    #[tokio::test]
    async fn delete_acceptor_state() {
        let mut storage = MemoryStorage::<String>::new();
        let pn = (1u64, crate::config::NodeId::new("node-1", 1000));
        storage.save_acceptor_state(0, Some(pn), None).await.unwrap();
        storage.delete_acceptor_state(0).await.unwrap();
        let states = storage.load_acceptor_states().await.unwrap();
        assert!(states.is_empty());
    }

    #[tokio::test]
    async fn empty_storage_returns_no_acceptor_states() {
        let storage = MemoryStorage::<String>::new();
        let states = storage.load_acceptor_states().await.unwrap();
        assert!(states.is_empty());
    }
}
```

- [ ] **Step 2: Make `ProposalNumber` public in `message.rs`**

In `concensus/src/message.rs`, change:
```rust
pub(crate) type ProposalNumber = (u64, NodeId);
```
to:
```rust
pub type ProposalNumber = (u64, NodeId);
```

- [ ] **Step 3: Add `StorageError::Delete` variant**

In `concensus/src/error.rs`, add a new variant to `StorageError`:
```rust
#[error("failed to delete acceptor state: {0}")]
Delete(String),
```

- [ ] **Step 4: Create `concensus/src/storage/mod.rs`**

```rust
mod memory;

#[cfg(feature = "duckdb-storage")]
mod duckdb;

use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};
use crate::error::StorageError;
use crate::message::ProposalNumber;

pub use memory::MemoryStorage;

#[cfg(feature = "duckdb-storage")]
pub use self::duckdb::DuckDbStorage;

/// Per-slot acceptor state for crash recovery.
///
/// Contains the Paxos acceptor's promise and accepted value for a given slot.
/// Loaded at startup to restore acceptor invariants after a crash.
#[derive(Clone, Debug)]
pub struct AcceptorState<V> {
    pub slot: u64,
    pub highest_promised: Option<ProposalNumber>,
    pub accepted: Option<(ProposalNumber, V)>,
}

impl<V> AcceptorState<V> {
    /// Validates that this acceptor state satisfies Paxos invariants.
    ///
    /// Returns `true` if valid. Invalid states are logged and skipped during recovery.
    pub fn is_valid(&self) -> bool {
        // Rule 3: if accepted is Some, highest_promised must also be Some
        if self.accepted.is_some() && self.highest_promised.is_none() {
            tracing::warn!(slot = self.slot, "invalid acceptor state: accepted is Some but highest_promised is None");
            return false;
        }

        // Rule 2: proposal number rounds must be > 0
        if let Some(ref hp) = self.highest_promised {
            if hp.0 == 0 {
                tracing::warn!(slot = self.slot, "invalid acceptor state: highest_promised round is 0");
                return false;
            }
        }
        if let Some((ref pn, _)) = self.accepted {
            if pn.0 == 0 {
                tracing::warn!(slot = self.slot, "invalid acceptor state: accepted round is 0");
                return false;
            }
        }

        // Rule 1: if both are Some, accepted pn <= highest_promised
        if let (Some((ref accepted_pn, _)), Some(ref hp)) = (&self.accepted, &self.highest_promised) {
            if accepted_pn > hp {
                tracing::warn!(slot = self.slot, "invalid acceptor state: accepted proposal number exceeds highest_promised");
                return false;
            }
        }

        true
    }
}

/// Durable storage for consensus decisions and acceptor state.
///
/// Implementations persist decided slot-value pairs and per-slot acceptor state
/// so that a node can recover after a restart. The [`Node`](crate::Node) calls
/// [`load_decisions`](Storage::load_decisions) and
/// [`load_acceptor_states`](Storage::load_acceptor_states) once at startup, and
/// the mutation methods during normal operation.
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
    /// Implementations should also clean up any acceptor state for this slot.
    async fn save_decision(&mut self, slot: u64, value: V) -> Result<(), StorageError>;

    /// Load all previously persisted decisions.
    ///
    /// Called once during [`Node::run`](crate::Node::run) startup to recover
    /// prior state. Returns `(slot, value)` pairs in any order.
    async fn load_decisions(&self) -> Result<Vec<(u64, V)>, StorageError>;

    /// Persist acceptor state for a given slot.
    ///
    /// Called when the acceptor updates its promise or accepted value.
    /// Uses upsert semantics — overwrites any existing state for the slot.
    async fn save_acceptor_state(
        &mut self,
        slot: u64,
        highest_promised: Option<ProposalNumber>,
        accepted: Option<(ProposalNumber, V)>,
    ) -> Result<(), StorageError>;

    /// Load all previously persisted acceptor states.
    ///
    /// Called once during startup to restore acceptor invariants after a crash.
    async fn load_acceptor_states(&self) -> Result<Vec<AcceptorState<V>>, StorageError>;

    /// Delete acceptor state for a given slot.
    ///
    /// Note: `save_decision` should also clean up acceptor state for the decided
    /// slot. This method exists for explicit cleanup when needed.
    async fn delete_acceptor_state(&mut self, slot: u64) -> Result<(), StorageError>;
}

/// Blanket impl so `Box<dyn Storage<V> + Send>` can be used where `impl Storage<V>` is expected.
#[async_trait]
impl<V> Storage<V> for Box<dyn Storage<V> + Send>
where
    V: Serialize + DeserializeOwned + Clone + Send,
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
        (**self).save_acceptor_state(slot, highest_promised, accepted).await
    }
    async fn load_acceptor_states(&self) -> Result<Vec<AcceptorState<V>>, StorageError> {
        (**self).load_acceptor_states().await
    }
    async fn delete_acceptor_state(&mut self, slot: u64) -> Result<(), StorageError> {
        (**self).delete_acceptor_state(slot).await
    }
}
```

- [ ] **Step 5: Delete old `concensus/src/storage.rs`**

Remove the file. The module is now `concensus/src/storage/` directory.

- [ ] **Step 6: Update `lib.rs` re-exports**

In `concensus/src/lib.rs`, update the storage re-export line:
```rust
pub use storage::{AcceptorState, MemoryStorage, Storage};
```

Add `ProposalNumber` re-export (from message, which is now pub on that type):
```rust
pub use message::ProposalNumber;
```

Add conditional DuckDB re-export:
```rust
#[cfg(feature = "duckdb-storage")]
pub use storage::DuckDbStorage;
```

- [ ] **Step 7: Run `cargo check -p concensus`**

Expected: Compilation errors from `concensus-tests` and `concensus-demo` because `SharedMemoryStorage` doesn't implement the new trait methods yet. The core `concensus` crate itself should compile cleanly.

Run: `cargo check -p concensus`
Expected: success

- [ ] **Step 8: Update `SharedMemoryStorage` in test helpers**

In `concensus-tests/tests/helpers/mod.rs`, add the three new trait methods to `SharedMemoryStorage`:

```rust
async fn save_acceptor_state(
    &mut self,
    slot: u64,
    highest_promised: Option<concensus::ProposalNumber>,
    accepted: Option<(concensus::ProposalNumber, V)>,
) -> Result<(), StorageError> {
    self.inner.lock().await.save_acceptor_state(slot, highest_promised, accepted).await
}

async fn load_acceptor_states(&self) -> Result<Vec<concensus::AcceptorState<V>>, StorageError> {
    self.inner.lock().await.load_acceptor_states().await
}

async fn delete_acceptor_state(&mut self, slot: u64) -> Result<(), StorageError> {
    self.inner.lock().await.delete_acceptor_state(slot).await
}
```

Also add the necessary imports: `concensus::AcceptorState` and `concensus::ProposalNumber`.

- [ ] **Step 9: Run full workspace check**

Run: `cargo check --workspace`
Expected: success (all crates compile)

- [ ] **Step 10: Run existing tests**

Run: `cargo test --workspace`
Expected: all existing tests pass (no regressions)

- [ ] **Step 11: Commit**

```bash
git add -A
git commit -m "refactor(storage): split into module directory, add acceptor state to Storage trait

Add save_acceptor_state, load_acceptor_states, delete_acceptor_state to
Storage<V>. Add AcceptorState<V> type with validation. Make ProposalNumber
public. Update MemoryStorage and SharedMemoryStorage for new methods."
```

---

## Chunk 2: Protocol Dirty Tracking & Node Persist-Before-Send

### Task 2: Add dirty acceptor slot tracking to ProtocolState

**Files:**
- Modify: `concensus/src/protocol.rs`

- [ ] **Step 1: Write test for dirty slot tracking**

Add to the `tests` module in `protocol.rs`:

```rust
#[test]
fn handle_prepare_marks_dirty_acceptor_slot() {
    let mut proto = make_protocol("a", 3);
    let from = node("b");
    let pn = (1, from.clone());
    proto.handle_message(from, MessageVariant::Prepare { slot: 0, proposal_number: pn });
    let dirty = proto.take_dirty_acceptor_slots();
    assert_eq!(dirty.len(), 1);
    assert_eq!(dirty[0].0, 0); // slot
    assert!(dirty[0].1.is_some()); // highest_promised
}

#[test]
fn handle_accept_marks_dirty_acceptor_slot() {
    let mut proto = make_protocol("a", 3);
    let from = node("b");
    let pn = (1, from.clone());
    proto.handle_message(from, MessageVariant::Accept {
        slot: 0,
        proposal_number: pn,
        value: "hello".to_string(),
    });
    let dirty = proto.take_dirty_acceptor_slots();
    assert_eq!(dirty.len(), 1);
    assert_eq!(dirty[0].0, 0);
    assert!(dirty[0].1.is_some()); // highest_promised
    assert!(dirty[0].2.is_some()); // accepted
}

#[test]
fn take_dirty_clears_dirty_set() {
    let mut proto = make_protocol("a", 3);
    let from = node("b");
    proto.handle_message(from, MessageVariant::Prepare {
        slot: 0,
        proposal_number: (1, node("b")),
    });
    let dirty1 = proto.take_dirty_acceptor_slots();
    assert_eq!(dirty1.len(), 1);
    let dirty2 = proto.take_dirty_acceptor_slots();
    assert!(dirty2.is_empty());
}

#[test]
fn propose_self_vote_marks_dirty() {
    let mut proto = make_protocol("a", 3);
    proto.propose("hello".to_string());
    let dirty = proto.take_dirty_acceptor_slots();
    assert_eq!(dirty.len(), 1);
    assert_eq!(dirty[0].0, 0); // slot
    assert!(dirty[0].1.is_some()); // highest_promised from self-vote
}

#[test]
fn retry_proposal_marks_dirty() {
    let mut proto = make_protocol("a", 3);
    let (_, _) = proto.propose("hello".to_string());
    proto.take_dirty_acceptor_slots(); // clear
    let pn = proto.instances.get(&0).unwrap().proposal_number.clone();
    proto.handle_message(node("b"), MessageVariant::NackPrepare {
        slot: 0,
        proposal_number: pn,
        highest_promised: (5, node("b")),
    });
    proto.take_dirty_acceptor_slots(); // clear nack (no acceptor state change)
    proto.retry_proposal(0);
    let dirty = proto.take_dirty_acceptor_slots();
    assert_eq!(dirty.len(), 1);
    assert_eq!(dirty[0].0, 0);
}

#[test]
fn decided_slot_prepare_does_not_mark_dirty() {
    let mut proto = make_protocol("a", 3);
    // Decide slot 0 first
    proto.handle_message(node("b"), MessageVariant::Decide {
        slot: 0,
        value: "decided".to_string(),
    });
    proto.take_decisions();
    proto.take_dirty_acceptor_slots(); // clear any

    // Late prepare for decided slot
    proto.handle_message(node("c"), MessageVariant::Prepare {
        slot: 0,
        proposal_number: (10, node("c")),
    });
    let dirty = proto.take_dirty_acceptor_slots();
    assert!(dirty.is_empty());
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p concensus -- dirty`
Expected: FAIL — `take_dirty_acceptor_slots` not found

- [ ] **Step 3: Implement dirty tracking**

Add to `ProtocolState`:

```rust
// New field in ProtocolState struct:
dirty_acceptor_slots: Vec<u64>,
```

Initialize to `Vec::new()` in `ProtocolState::new()`.

Add method:
```rust
pub(crate) fn take_dirty_acceptor_slots(&mut self) -> Vec<(u64, Option<ProposalNumber>, Option<(ProposalNumber, V)>)> {
    let slots = std::mem::take(&mut self.dirty_acceptor_slots);
    let mut result = Vec::new();
    for slot in slots {
        if let Some(instance) = self.instances.get(&slot) {
            result.push((
                slot,
                instance.highest_promised.clone(),
                instance.accepted.clone(),
            ));
        }
    }
    result
}
```

In `handle_prepare`, after the acceptor updates `highest_promised` (inside the `if` branch that sends `Promise`), add:
```rust
self.dirty_acceptor_slots.push(slot);
```

In `handle_accept`, after the acceptor updates `highest_promised` and `accepted` (inside the `if` branch that sends `Accepted`), add:
```rust
self.dirty_acceptor_slots.push(slot);
```

In `propose()`, after the self-vote as acceptor (where `highest_promised` is set, around `instance.highest_promised = Some(proposal_number.clone())`), add:
```rust
self.dirty_acceptor_slots.push(slot);
```

In `start_phase2()`, after the self-accept block (where `highest_promised` and `accepted` are set in the `if can_self_accept` block), add inside that block:
```rust
self.dirty_acceptor_slots.push(slot);
```

In `retry_proposal()`, after the self-vote for the new round (where `highest_promised` is set), add:
```rust
self.dirty_acceptor_slots.push(slot);
```

**Important:** Do NOT add dirty tracking in the early-return branches that reply with `Decide` (for already-decided slots) — those don't modify acceptor state.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p concensus -- dirty`
Expected: PASS

- [ ] **Step 5: Run all existing tests to verify no regression**

Run: `cargo test -p concensus`
Expected: all pass

- [ ] **Step 6: Commit**

```bash
git add concensus/src/protocol.rs
git commit -m "feat(protocol): add dirty acceptor slot tracking for persistence"
```

### Task 3: Add `initialize_from_acceptor_states` to ProtocolState

**Files:**
- Modify: `concensus/src/protocol.rs`

- [ ] **Step 1: Write tests**

```rust
#[test]
fn initialize_from_acceptor_states_restores_promise() {
    use crate::storage::AcceptorState;

    let mut proto = make_protocol("a", 3);
    let pn = (5, node("b"));
    let states = vec![AcceptorState {
        slot: 0,
        highest_promised: Some(pn.clone()),
        accepted: None,
    }];
    proto.initialize_from_acceptor_states(states);

    // Prepare with lower number should be nacked
    let responses = proto.handle_message(node("c"), MessageVariant::Prepare {
        slot: 0,
        proposal_number: (3, node("c")),
    });
    assert!(matches!(&responses[0].message, MessageVariant::NackPrepare { .. }));
}

#[test]
fn initialize_from_acceptor_states_restores_accepted() {
    use crate::storage::AcceptorState;

    let mut proto = make_protocol("a", 3);
    let pn = (5, node("b"));
    let states = vec![AcceptorState {
        slot: 0,
        highest_promised: Some(pn.clone()),
        accepted: Some((pn.clone(), "previous".to_string())),
    }];
    proto.initialize_from_acceptor_states(states);

    // Prepare with higher number should return the accepted value
    let responses = proto.handle_message(node("c"), MessageVariant::Prepare {
        slot: 0,
        proposal_number: (10, node("c")),
    });
    match &responses[0].message {
        MessageVariant::Promise { accepted: Some((_, val)), .. } => {
            assert_eq!(val, "previous");
        }
        _ => panic!("expected Promise with accepted value"),
    }
}

#[test]
fn initialize_from_acceptor_states_skips_decided_slots() {
    use crate::storage::AcceptorState;

    let mut proto = make_protocol("a", 3);
    // Decide slot 0
    proto.handle_message(node("b"), MessageVariant::Decide {
        slot: 0,
        value: "done".to_string(),
    });
    proto.take_decisions();

    // Try to restore acceptor state for decided slot
    let states = vec![AcceptorState {
        slot: 0,
        highest_promised: Some((5, node("b"))),
        accepted: None,
    }];
    proto.initialize_from_acceptor_states(states);

    // Slot 0 should still be decided, not have an active instance
    assert!(!proto.instances.contains_key(&0));
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p concensus -- initialize_from_acceptor`
Expected: FAIL — method not found

- [ ] **Step 3: Implement**

Add to `ProtocolState`:

```rust
pub(crate) fn initialize_from_acceptor_states(&mut self, states: Vec<crate::storage::AcceptorState<V>>) {
    for state in states {
        if self.decided_slots.contains_key(&state.slot) {
            continue;
        }
        let instance = self.get_or_create_instance(state.slot);
        instance.highest_promised = state.highest_promised;
        instance.accepted = state.accepted;
    }
}
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p concensus -- initialize_from_acceptor`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add concensus/src/protocol.rs
git commit -m "feat(protocol): add initialize_from_acceptor_states for crash recovery"
```

### Task 4: Update Node for persist-before-send and acceptor recovery

**Files:**
- Modify: `concensus/src/node.rs`

- [ ] **Step 1: Add acceptor state persistence to `handle_incoming_bytes`**

Reorder the flow in `handle_incoming_bytes`. Currently:
1. `handle_message` → `outgoing`
2. `send_outgoing`
3. `process_decisions`

New order:
1. `handle_message` → `outgoing`
2. `persist_dirty_acceptor_slots` (new)
3. `send_outgoing`
4. `process_decisions`

Add a new helper method:

```rust
async fn persist_dirty_acceptor_slots(&mut self) -> Result<(), NodeError> {
    let dirty = self.protocol.take_dirty_acceptor_slots();
    for (slot, highest_promised, accepted) in dirty {
        self.storage
            .save_acceptor_state(slot, highest_promised, accepted)
            .await
            .map_err(NodeError::Storage)?;
    }
    Ok(())
}
```

Update `handle_incoming_bytes`:
```rust
async fn handle_incoming_bytes(
    &mut self,
    data: &[u8],
    senders: &[(NodeId, S)],
) -> Result<(), NodeError> {
    match Message::<V>::from_bytes(data) {
        Ok(msg) => {
            let from = msg.sender;
            let variant = msg.variant;
            let outgoing = self.protocol.handle_message(from, variant);
            self.persist_dirty_acceptor_slots().await?;
            Self::send_outgoing(&self.node_id, &outgoing, senders).await;
            self.process_decisions().await?;

            let lost = self.protocol.take_lost_proposals();
            for value in lost {
                let (_, outgoing) = self.protocol.propose(value);
                self.persist_dirty_acceptor_slots().await?;
                Self::send_outgoing(&self.node_id, &outgoing, senders).await;
                self.process_decisions().await?;
            }
        }
        Err(e) => {
            tracing::warn!(error = %e, "failed to deserialize message");
        }
    }
    Ok(())
}
```

Also update `handle_proposal` to persist dirty slots (proposer self-votes as acceptor in `propose`):
```rust
async fn handle_proposal(&mut self, value: V, senders: &[(NodeId, S)]) -> Result<(), NodeError> {
    let (slot, outgoing) = self.protocol.propose(value);
    tracing::debug!(slot, "new proposal");
    self.persist_dirty_acceptor_slots().await?;
    Self::send_outgoing(&self.node_id, &outgoing, senders).await;
    self.process_decisions().await?;
    Ok(())
}
```

- [ ] **Step 2: Add acceptor state recovery to `Node::run`**

After loading decisions, add acceptor state loading with validation. In the `run` method, after `self.protocol.initialize_from_decisions(decisions)`:

```rust
let acceptor_states = self.storage.load_acceptor_states().await.map_err(NodeError::Storage)?;
let valid_states: Vec<_> = acceptor_states.into_iter().filter(|s| s.is_valid()).collect();
self.protocol.initialize_from_acceptor_states(valid_states);
```

- [ ] **Step 3: Run existing tests**

Run: `cargo test --workspace`
Expected: all pass

- [ ] **Step 4: Commit**

```bash
git add concensus/src/node.rs
git commit -m "feat(node): persist acceptor state before sending, recover on startup"
```

---

## Chunk 3: DuckDB Storage Implementation

### Task 5: Add DuckDB dependency and feature flag

**Files:**
- Modify: `concensus/Cargo.toml`

- [ ] **Step 1: Add to `concensus/Cargo.toml`**

Add feature:
```toml
duckdb-storage = ["dep:duckdb"]
```

Add dependency:
```toml
duckdb = { version = "1", optional = true }
```

- [ ] **Step 2: Verify it compiles without the feature**

Run: `cargo check -p concensus`
Expected: success (duckdb not compiled)

- [ ] **Step 3: Verify it compiles with the feature**

Run: `cargo check -p concensus --features duckdb-storage`
Expected: success (duckdb crate downloads and compiles)

- [ ] **Step 4: Commit**

```bash
git add concensus/Cargo.toml
git commit -m "feat(deps): add optional duckdb dependency behind duckdb-storage feature"
```

### Task 6: Implement DuckDbStorage

**Files:**
- Create: `concensus/src/storage/duckdb.rs`

- [ ] **Step 1: Write tests first**

Create `concensus/src/storage/duckdb.rs` with tests at the bottom:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::NodeId;
    use std::path::PathBuf;

    fn temp_db_path() -> PathBuf {
        let dir = std::env::temp_dir().join(format!("concensus-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        dir.join(format!("{}.db", rand::random::<u64>()))
    }

    #[tokio::test]
    async fn create_new_db() {
        let path = temp_db_path();
        let _storage = DuckDbStorage::<String>::new(&path).unwrap();
        assert!(path.exists());
        std::fs::remove_file(&path).ok();
    }

    #[tokio::test]
    async fn save_and_load_decisions() {
        let path = temp_db_path();
        let mut storage = DuckDbStorage::<String>::new(&path).unwrap();
        storage.save_decision(0, "hello".to_string()).await.unwrap();
        storage.save_decision(2, "world".to_string()).await.unwrap();
        let decisions = storage.load_decisions().await.unwrap();
        assert_eq!(decisions.len(), 2);
        assert!(decisions.contains(&(0, "hello".to_string())));
        assert!(decisions.contains(&(2, "world".to_string())));
        std::fs::remove_file(&path).ok();
    }

    #[tokio::test]
    async fn save_and_load_acceptor_state() {
        let path = temp_db_path();
        let mut storage = DuckDbStorage::<String>::new(&path).unwrap();
        let pn = (1u64, NodeId::new("node-1", 1000));
        storage.save_acceptor_state(0, Some(pn.clone()), None).await.unwrap();
        let states = storage.load_acceptor_states().await.unwrap();
        assert_eq!(states.len(), 1);
        assert_eq!(states[0].slot, 0);
        assert_eq!(states[0].highest_promised, Some(pn));
        assert!(states[0].accepted.is_none());
        std::fs::remove_file(&path).ok();
    }

    #[tokio::test]
    async fn save_acceptor_state_with_accepted_value() {
        let path = temp_db_path();
        let mut storage = DuckDbStorage::<String>::new(&path).unwrap();
        let pn = (3u64, NodeId::new("node-2", 1000));
        storage.save_acceptor_state(
            5,
            Some(pn.clone()),
            Some((pn.clone(), "value".to_string())),
        ).await.unwrap();
        let states = storage.load_acceptor_states().await.unwrap();
        assert_eq!(states.len(), 1);
        assert_eq!(states[0].slot, 5);
        assert_eq!(states[0].accepted.as_ref().unwrap().1, "value");
        std::fs::remove_file(&path).ok();
    }

    #[tokio::test]
    async fn save_decision_cleans_acceptor_state() {
        let path = temp_db_path();
        let mut storage = DuckDbStorage::<String>::new(&path).unwrap();
        let pn = (1u64, NodeId::new("node-1", 1000));
        storage.save_acceptor_state(0, Some(pn), None).await.unwrap();
        storage.save_decision(0, "decided".to_string()).await.unwrap();
        let states = storage.load_acceptor_states().await.unwrap();
        assert!(states.is_empty());
        std::fs::remove_file(&path).ok();
    }

    #[tokio::test]
    async fn delete_acceptor_state() {
        let path = temp_db_path();
        let mut storage = DuckDbStorage::<String>::new(&path).unwrap();
        let pn = (1u64, NodeId::new("node-1", 1000));
        storage.save_acceptor_state(0, Some(pn), None).await.unwrap();
        storage.delete_acceptor_state(0).await.unwrap();
        let states = storage.load_acceptor_states().await.unwrap();
        assert!(states.is_empty());
        std::fs::remove_file(&path).ok();
    }

    #[tokio::test]
    async fn save_acceptor_state_upserts() {
        let path = temp_db_path();
        let mut storage = DuckDbStorage::<String>::new(&path).unwrap();
        let pn1 = (1u64, NodeId::new("node-1", 1000));
        let pn2 = (5u64, NodeId::new("node-2", 1000));
        storage.save_acceptor_state(0, Some(pn1), None).await.unwrap();
        storage.save_acceptor_state(0, Some(pn2.clone()), None).await.unwrap();
        let states = storage.load_acceptor_states().await.unwrap();
        assert_eq!(states.len(), 1);
        assert_eq!(states[0].highest_promised, Some(pn2));
        std::fs::remove_file(&path).ok();
    }

    #[tokio::test]
    async fn persistence_survives_reopen() {
        let path = temp_db_path();
        let pn = (1u64, NodeId::new("node-1", 1000));

        // First session: write data
        {
            let mut storage = DuckDbStorage::<String>::new(&path).unwrap();
            storage.save_decision(0, "hello".to_string()).await.unwrap();
            storage.save_acceptor_state(1, Some(pn.clone()), None).await.unwrap();
        }

        // Second session: read back
        {
            let storage = DuckDbStorage::<String>::new(&path).unwrap();
            let decisions = storage.load_decisions().await.unwrap();
            assert_eq!(decisions.len(), 1);
            assert!(decisions.contains(&(0, "hello".to_string())));
            let states = storage.load_acceptor_states().await.unwrap();
            assert_eq!(states.len(), 1);
            assert_eq!(states[0].slot, 1);
        }

        std::fs::remove_file(&path).ok();
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p concensus --features duckdb-storage -- duckdb`
Expected: FAIL — `DuckDbStorage` struct not defined

- [ ] **Step 3: Implement `DuckDbStorage`**

Add the implementation above the tests in `concensus/src/storage/duckdb.rs`:

```rust
use std::marker::PhantomData;
use std::path::Path;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use duckdb::params;
use serde::{de::DeserializeOwned, Serialize};

use crate::error::StorageError;
use crate::message::ProposalNumber;
use super::{AcceptorState, Storage};

/// DuckDB-backed [`Storage`] implementation for durable crash recovery.
///
/// Stores decisions and acceptor state in two tables. The database file is
/// created automatically on first use. All SQL is executed via
/// `tokio::task::spawn_blocking` to avoid blocking the async runtime.
pub struct DuckDbStorage<V> {
    conn: Arc<Mutex<duckdb::Connection>>,
    _phantom: PhantomData<V>,
}

impl<V> DuckDbStorage<V> {
    /// Opens or creates a DuckDB database at the given path.
    ///
    /// Creates `decisions` and `acceptor_state` tables if they don't exist.
    pub fn new(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        let conn = duckdb::Connection::open(path.as_ref())
            .map_err(|e| StorageError::Load(format!("failed to open DuckDB: {e}")))?;

        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS decisions (
                slot UBIGINT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS acceptor_state (
                slot UBIGINT PRIMARY KEY,
                highest_promised TEXT,
                accepted TEXT
            );",
        )
        .map_err(|e| StorageError::Load(format!("failed to create tables: {e}")))?;

        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            _phantom: PhantomData,
        })
    }
}

#[async_trait]
impl<V> Storage<V> for DuckDbStorage<V>
where
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    async fn save_decision(&mut self, slot: u64, value: V) -> Result<(), StorageError> {
        let conn = Arc::clone(&self.conn);
        let value_json = serde_json::to_string(&value)
            .map_err(|e| StorageError::Persist(format!("failed to serialize value: {e}")))?;

        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().map_err(|e| StorageError::Persist(format!("mutex poisoned: {e}")))?;
            conn.execute(
                "INSERT OR REPLACE INTO decisions (slot, value) VALUES (?, ?)",
                params![slot, value_json],
            )
            .map_err(|e| StorageError::Persist(format!("failed to insert decision: {e}")))?;

            conn.execute(
                "DELETE FROM acceptor_state WHERE slot = ?",
                params![slot],
            )
            .map_err(|e| StorageError::Persist(format!("failed to clean acceptor state: {e}")))?;

            Ok(())
        })
        .await
        .map_err(|e| StorageError::Persist(format!("blocking task failed: {e}")))?
    }

    async fn load_decisions(&self) -> Result<Vec<(u64, V)>, StorageError> {
        let conn = Arc::clone(&self.conn);

        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().map_err(|e| StorageError::Load(format!("mutex poisoned: {e}")))?;
            let mut stmt = conn.prepare("SELECT slot, value FROM decisions")
                .map_err(|e| StorageError::Load(format!("failed to prepare query: {e}")))?;

            let rows = stmt.query_map([], |row| {
                let slot: u64 = row.get(0)?;
                let value_json: String = row.get(1)?;
                Ok((slot, value_json))
            })
            .map_err(|e| StorageError::Load(format!("failed to query decisions: {e}")))?;

            let mut decisions = Vec::new();
            for row in rows {
                let (slot, value_json) = row.map_err(|e| StorageError::Load(format!("failed to read row: {e}")))?;
                match serde_json::from_str::<V>(&value_json) {
                    Ok(value) => decisions.push((slot, value)),
                    Err(e) => {
                        tracing::warn!(slot, error = %e, "skipping decision with invalid value");
                    }
                }
            }
            Ok(decisions)
        })
        .await
        .map_err(|e| StorageError::Load(format!("blocking task failed: {e}")))?
    }

    async fn save_acceptor_state(
        &mut self,
        slot: u64,
        highest_promised: Option<ProposalNumber>,
        accepted: Option<(ProposalNumber, V)>,
    ) -> Result<(), StorageError> {
        let conn = Arc::clone(&self.conn);
        let hp_json = serde_json::to_string(&highest_promised)
            .map_err(|e| StorageError::Persist(format!("failed to serialize highest_promised: {e}")))?;
        let acc_json = serde_json::to_string(&accepted)
            .map_err(|e| StorageError::Persist(format!("failed to serialize accepted: {e}")))?;

        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().map_err(|e| StorageError::Persist(format!("mutex poisoned: {e}")))?;
            conn.execute(
                "INSERT OR REPLACE INTO acceptor_state (slot, highest_promised, accepted) VALUES (?, ?, ?)",
                params![slot, hp_json, acc_json],
            )
            .map_err(|e| StorageError::Persist(format!("failed to save acceptor state: {e}")))?;
            Ok(())
        })
        .await
        .map_err(|e| StorageError::Persist(format!("blocking task failed: {e}")))?
    }

    async fn load_acceptor_states(&self) -> Result<Vec<AcceptorState<V>>, StorageError> {
        let conn = Arc::clone(&self.conn);

        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().map_err(|e| StorageError::Load(format!("mutex poisoned: {e}")))?;
            let mut stmt = conn.prepare("SELECT slot, highest_promised, accepted FROM acceptor_state")
                .map_err(|e| StorageError::Load(format!("failed to prepare query: {e}")))?;

            let rows = stmt.query_map([], |row| {
                let slot: u64 = row.get(0)?;
                let hp_json: Option<String> = row.get(1)?;
                let acc_json: Option<String> = row.get(2)?;
                Ok((slot, hp_json, acc_json))
            })
            .map_err(|e| StorageError::Load(format!("failed to query acceptor states: {e}")))?;

            let mut states = Vec::new();
            for row in rows {
                let (slot, hp_json, acc_json) = row.map_err(|e| StorageError::Load(format!("failed to read row: {e}")))?;

                let highest_promised: Option<ProposalNumber> = match hp_json {
                    Some(json) => match serde_json::from_str(&json) {
                        Ok(v) => v,
                        Err(e) => {
                            tracing::warn!(slot, error = %e, "skipping acceptor state with invalid highest_promised");
                            continue;
                        }
                    },
                    None => None,
                };

                let accepted: Option<(ProposalNumber, V)> = match acc_json {
                    Some(json) => match serde_json::from_str(&json) {
                        Ok(v) => v,
                        Err(e) => {
                            tracing::warn!(slot, error = %e, "skipping acceptor state with invalid accepted");
                            continue;
                        }
                    },
                    None => None,
                };

                states.push(AcceptorState { slot, highest_promised, accepted });
            }
            Ok(states)
        })
        .await
        .map_err(|e| StorageError::Load(format!("blocking task failed: {e}")))?
    }

    async fn delete_acceptor_state(&mut self, slot: u64) -> Result<(), StorageError> {
        let conn = Arc::clone(&self.conn);

        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().map_err(|e| StorageError::Delete(format!("mutex poisoned: {e}")))?;
            conn.execute(
                "DELETE FROM acceptor_state WHERE slot = ?",
                params![slot],
            )
            .map_err(|e| StorageError::Delete(format!("failed to delete acceptor state: {e}")))?;
            Ok(())
        })
        .await
        .map_err(|e| StorageError::Delete(format!("blocking task failed: {e}")))?
    }
}
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p concensus --features duckdb-storage -- duckdb`
Expected: all PASS

- [ ] **Step 5: Run full workspace tests**

Run: `cargo test --workspace`
Expected: all pass

- [ ] **Step 6: Commit**

```bash
git add concensus/src/storage/duckdb.rs concensus/Cargo.toml
git commit -m "feat(storage): implement DuckDbStorage with full crash recovery support"
```

---

## Chunk 4: Integration Tests

### Task 7: DuckDB recovery integration test

**Files:**
- Modify: `concensus-tests/Cargo.toml`
- Create: `concensus-tests/tests/duckdb_recovery.rs`

- [ ] **Step 1: Add `duckdb-storage` feature to test crate**

In `concensus-tests/Cargo.toml`, update the concensus dependency:

```toml
concensus = { path = "../concensus", features = ["channel-transport", "test-support", "duckdb-storage"] }
```

- [ ] **Step 2: Write the integration test**

Create `concensus-tests/tests/duckdb_recovery.rs`:

```rust
mod helpers;

use concensus::{
    channel, ChannelSender, Decided, DuckDbStorage, Node, NodeId, PeerInfo, Storage,
};
use std::path::PathBuf;
use tokio::time::{timeout, Duration};

fn temp_db_path(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("concensus-integ-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir.join(format!("{}.db", name))
}

fn build_cluster_with_duckdb(
    ids: &[NodeId; 3],
    db_paths: &[PathBuf; 3],
) -> (
    Vec<concensus::NodeHandle<String>>,
    Vec<concensus::DecisionReceiver<String>>,
    Vec<tokio::task::JoinHandle<Result<(), concensus::NodeError>>>,
) {
    let (tx0, rx0) = channel(64);
    let (tx1, rx1) = channel(64);
    let (tx2, rx2) = channel(64);

    let txs = [&tx0, &tx1, &tx2];
    let mut rxs = vec![Some(rx0), Some(rx1), Some(rx2)];

    let mut handles = Vec::new();
    let mut decision_rxs = Vec::new();
    let mut run_handles = Vec::new();

    for i in 0..3 {
        let peers: Vec<PeerInfo<ChannelSender>> = (0..3)
            .filter(|&j| j != i)
            .map(|j| PeerInfo {
                id: ids[j].clone(),
                sender: txs[j].clone(),
            })
            .collect();

        let receiver = rxs[i].take().unwrap();
        let storage = DuckDbStorage::<String>::new(&db_paths[i]).unwrap();

        let (node, handle, dec_rx) = Node::with_id(ids[i].clone(), peers, receiver, storage);
        let rh = tokio::spawn(node.run());

        handles.push(handle);
        decision_rxs.push(dec_rx);
        run_handles.push(rh);
    }

    (handles, decision_rxs, run_handles)
}

fn teardown(
    handles: Vec<concensus::NodeHandle<String>>,
    decisions: Vec<concensus::DecisionReceiver<String>>,
    run_handles: Vec<tokio::task::JoinHandle<Result<(), concensus::NodeError>>>,
) {
    for h in handles { drop(h); }
    for d in decisions { drop(d); }
    for rh in run_handles { rh.abort(); }
}

async fn collect_until_value(
    rx: &mut concensus::DecisionReceiver<String>,
    target: &str,
    deadline: Duration,
) -> Vec<Decided<String>> {
    let mut collected = Vec::new();
    let start = tokio::time::Instant::now();
    loop {
        let remaining = deadline.saturating_sub(start.elapsed());
        if remaining.is_zero() {
            panic!("timed out waiting for value {:?}; got: {:?}", target,
                collected.iter().map(|d: &Decided<String>| (&d.value, d.slot)).collect::<Vec<_>>());
        }
        let decided = timeout(remaining, rx.recv())
            .await.expect("timed out").expect("channel closed");
        let found = decided.value == target;
        collected.push(decided);
        if found { return collected; }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duckdb_node_recovers_decisions_after_crash() {
    let ids: [NodeId; 3] = [
        NodeId::new("node-0", 1000),
        NodeId::new("node-1", 1000),
        NodeId::new("node-2", 1000),
    ];
    let db_paths: [PathBuf; 3] = [
        temp_db_path("node0-recov"),
        temp_db_path("node1-recov"),
        temp_db_path("node2-recov"),
    ];

    // Phase 1: decide "value-a"
    let (handles, mut decisions, run_handles) = build_cluster_with_duckdb(&ids, &db_paths);
    handles[0].propose("value-a".to_string()).await.unwrap();
    for dec in &mut decisions {
        let decided = timeout(Duration::from_secs(5), dec.recv())
            .await.unwrap().unwrap();
        assert_eq!(decided.value, "value-a");
    }
    teardown(handles, decisions, run_handles);

    // Phase 2: rebuild from same DB files, propose "value-b"
    let (handles2, mut decisions2, run_handles2) = build_cluster_with_duckdb(&ids, &db_paths);
    handles2[1].propose("value-b".to_string()).await.unwrap();
    for dec in &mut decisions2 {
        let collected = collect_until_value(dec, "value-b", Duration::from_secs(5)).await;
        assert_eq!(collected.last().unwrap().value, "value-b");
    }
    teardown(handles2, decisions2, run_handles2);

    // Cleanup
    for p in &db_paths { std::fs::remove_file(p).ok(); }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duckdb_recovered_node_does_not_replay_old_decisions() {
    let ids: [NodeId; 3] = [
        NodeId::new("node-0", 1000),
        NodeId::new("node-1", 1000),
        NodeId::new("node-2", 1000),
    ];
    let db_paths: [PathBuf; 3] = [
        temp_db_path("node0-norep"),
        temp_db_path("node1-norep"),
        temp_db_path("node2-norep"),
    ];

    // Phase 1: decide "value-a"
    let (handles, mut decisions, run_handles) = build_cluster_with_duckdb(&ids, &db_paths);
    handles[0].propose("value-a".to_string()).await.unwrap();
    for dec in &mut decisions {
        timeout(Duration::from_secs(5), dec.recv()).await.unwrap().unwrap();
    }
    teardown(handles, decisions, run_handles);

    // Phase 2: rebuild, propose "value-b"
    let (handles2, mut decisions2, run_handles2) = build_cluster_with_duckdb(&ids, &db_paths);
    handles2[1].propose("value-b".to_string()).await.unwrap();

    // Node-0 should NOT replay "value-a" from storage
    let collected = collect_until_value(&mut decisions2[0], "value-b", Duration::from_secs(5)).await;
    assert!(
        !collected.iter().any(|d| d.value == "value-a"),
        "recovered node-0 should not replay old decisions; got: {:?}",
        collected.iter().map(|d| (&d.value, d.slot)).collect::<Vec<_>>()
    );
    assert!(collected.last().unwrap().slot >= 1);

    teardown(handles2, decisions2, run_handles2);
    for p in &db_paths { std::fs::remove_file(p).ok(); }
}
```

- [ ] **Step 3: Run the integration tests**

Run: `cargo test -p concensus-tests -- duckdb`
Expected: all PASS

- [ ] **Step 4: Run full workspace tests**

Run: `cargo test --workspace`
Expected: all pass

- [ ] **Step 5: Commit**

```bash
git add concensus-tests/Cargo.toml concensus-tests/tests/duckdb_recovery.rs
git commit -m "test: add DuckDB crash recovery integration tests"
```

---

## Chunk 5: Demo Integration

### Task 8: Add storage config to demo node

**Files:**
- Modify: `concensus-demo/Cargo.toml`
- Modify: `concensus-demo/src/node.rs`

- [ ] **Step 1: Add `duckdb-storage` feature to demo**

In `concensus-demo/Cargo.toml`, update the concensus dependency:

```toml
concensus = { path = "../concensus", features = ["tcp-transport", "uds-transport", "test-support", "duckdb-storage"] }
```

- [ ] **Step 2: Add `StorageBackend` enum and config parsing**

In `concensus-demo/src/node.rs`, add the enum after the `Transport` enum:

```rust
#[derive(Debug)]
enum StorageBackend {
    Memory,
    DuckDb { path: PathBuf },
}
```

Add `storage` field to `Config`:
```rust
#[derive(Debug)]
struct Config {
    node_name: String,
    transport: Transport,
    storage: StorageBackend,
    grpc_port: u16,
}
```

In `parse_config()`, add storage parsing before the `Config` return:
```rust
let storage = match std::env::var("STORAGE").unwrap_or_else(|_| "memory".to_string()).as_str() {
    "memory" => StorageBackend::Memory,
    "duckdb" => {
        let path = std::env::var("DUCKDB_PATH")
            .expect("DUCKDB_PATH required when STORAGE=duckdb");
        StorageBackend::DuckDb { path: PathBuf::from(path) }
    }
    other => panic!("STORAGE must be 'memory' or 'duckdb', got '{}'", other),
};
```

- [ ] **Step 3: Update `start_node_tcp` and `start_node_uds` to accept `Box<dyn Storage<String> + Send>`**

Change both functions to accept storage as a parameter:

```rust
async fn start_node_tcp(
    node_name: &str,
    bind_addr: SocketAddr,
    peers: Vec<(NodeId, String)>,
    storage: Box<dyn Storage<String> + Send>,
) -> (NodeHandle<String>, DecisionReceiver<String>) {
    let peers = resolve_tcp_peers(peers).await;
    let (peer_infos, receiver) = TcpTransport::create(bind_addr, peers)
        .await
        .expect("failed to bind TCP transport");

    let node_id = NodeId::new(node_name, 0);
    let (node, handle, decision_rx) = Node::with_id(node_id, peer_infos, receiver, storage);

    let name = node_name.to_string();
    tokio::spawn(async move {
        if let Err(e) = node.run().await {
            tracing::error!(node_name = %name, error = %e, "node task exited with error");
            std::process::exit(1);
        }
    });

    (handle, decision_rx)
}
```

Same change for `start_node_uds`:
```rust
async fn start_node_uds(
    node_name: &str,
    bind_path: PathBuf,
    peers: Vec<(NodeId, PathBuf)>,
    storage: Box<dyn Storage<String> + Send>,
) -> (NodeHandle<String>, DecisionReceiver<String>) {
    let (peer_infos, receiver) = UdsTransport::create(bind_path, peers)
        .await
        .expect("failed to bind UDS transport");

    let node_id = NodeId::new(node_name, 0);
    let (node, handle, decision_rx) = Node::with_id(node_id, peer_infos, receiver, storage);

    let name = node_name.to_string();
    tokio::spawn(async move {
        if let Err(e) = node.run().await {
            tracing::error!(node_name = %name, error = %e, "node task exited with error");
            std::process::exit(1);
        }
    });

    (handle, decision_rx)
}
```

- [ ] **Step 4: Update `main()` to construct storage and pass it**

Add imports at the top:
```rust
use concensus::{DuckDbStorage, Storage};
```

In `main()`, construct storage before the transport match:
```rust
let storage: Box<dyn Storage<String> + Send> = match &config.storage {
    StorageBackend::Memory => {
        tracing::info!("using in-memory storage");
        Box::new(MemoryStorage::<String>::new())
    }
    StorageBackend::DuckDb { path } => {
        tracing::info!(path = ?path, "using DuckDB storage");
        Box::new(DuckDbStorage::<String>::new(path).expect("failed to open DuckDB"))
    }
};
```

Update the transport match arms to pass `storage`:
```rust
let (handle, decision_rx) = match config.transport {
    Transport::Tcp { bind_addr, peers } => {
        tracing::info!(
            node_name = %config.node_name,
            transport = "tcp",
            bind = %bind_addr,
            grpc_port = config.grpc_port,
            "node started"
        );
        start_node_tcp(&config.node_name, bind_addr, peers, storage).await
    }
    Transport::Uds { bind_path, peers } => {
        tracing::info!(
            node_name = %config.node_name,
            transport = "uds",
            bind = ?bind_path,
            grpc_port = config.grpc_port,
            "node started"
        );
        start_node_uds(&config.node_name, bind_path, peers, storage).await
    }
};
```

- [ ] **Step 5: Verify it compiles**

Run: `cargo check -p concensus-demo`
Expected: success

- [ ] **Step 6: Commit**

```bash
git add concensus-demo/Cargo.toml concensus-demo/src/node.rs
git commit -m "feat(demo): add configurable storage backend (memory or duckdb)"
```

### Task 9: Add Docker Compose file for DuckDB

**Files:**
- Create: `concensus-demo/docker-compose.duckdb.yml`

- [ ] **Step 1: Create the compose file**

Create `concensus-demo/docker-compose.duckdb.yml`:

```yaml
x-node: &node-base
  build:
    context: ..
    dockerfile: concensus-demo/Dockerfile
  environment: &node-env
    TRANSPORT: tcp
    BIND_ADDR: "0.0.0.0:9000"
    PEERS: "node-1=node-1:9000,node-2=node-2:9000,node-3=node-3:9000"
    GRPC_PORT: "50051"
    STORAGE: duckdb
    DUCKDB_PATH: "/data/consensus.db"
    RUST_LOG: info
  healthcheck:
    test: ["CMD", "concensus-cli", "--addr", "localhost:50051", "health"]
    interval: 5s
    timeout: 3s
    retries: 10
    start_period: 10s
  networks:
    - consensus-net

services:
  node-1:
    <<: *node-base
    hostname: node-1
    ports:
      - "50051:50051"
    volumes:
      - node1-data:/data
  node-2:
    <<: *node-base
    hostname: node-2
    ports:
      - "50052:50051"
    volumes:
      - node2-data:/data
  node-3:
    <<: *node-base
    hostname: node-3
    ports:
      - "50053:50051"
    volumes:
      - node3-data:/data

volumes:
  node1-data:
  node2-data:
  node3-data:

networks:
  consensus-net:
    driver: bridge
```

Note: All three gRPC ports are exposed (50051, 50052, 50053) so crash recovery can be verified from any node.

- [ ] **Step 2: Commit**

```bash
git add concensus-demo/docker-compose.duckdb.yml
git commit -m "feat(demo): add docker-compose.duckdb.yml with persistent storage volumes"
```

### Task 10: Verify full workspace and run lints

- [ ] **Step 1: Format**

Run: `cargo +nightly fmt --all`

- [ ] **Step 2: Clippy**

Run: `cargo clippy --workspace --all-features -- -D warnings`
Expected: no warnings

- [ ] **Step 3: Full test suite**

Run: `cargo test --workspace --all-features`
Expected: all pass

- [ ] **Step 4: Fix any issues found and commit**

If any issues, fix and commit with message:
```bash
git commit -m "fix: address clippy/fmt issues"
```

- [ ] **Step 5: Final commit if fmt produced changes**

```bash
git add -A
git commit -m "style: format with cargo +nightly fmt"
```
