//! redb-backed state machine for the KV demo.
//!
//! [`KvState`] persists the application key/value store plus a single
//! `last_applied_slot` watermark inside one redb database. Each applied op
//! writes its result and bumps the watermark in the same write transaction,
//! so the materialised state and the watermark cannot diverge.
//!
//! Cold-start dedupe: daccord's [`DecisionReceiver`](daccord::DecisionReceiver)
//! is at-least-once across a restart. [`KvState::apply`] checks the persisted
//! watermark before mutating and returns [`ApplyOutcome::Skipped`] for any
//! slot at or below it.
//!
//! The state machine is owned exclusively by the applier task — concurrent
//! callers go through the consensus log, not through this struct.

use std::path::Path;

use bytes::Bytes;
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};

use crate::error::KvResult;
use crate::kv::{KvOp, KvOpKind, KvOpResult};

const KV_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("kv");
const META_TABLE: TableDefinition<&str, u64> = TableDefinition::new("meta");
const META_WATERMARK: &str = "last_applied_slot";

/// Outcome of applying a decision to the state machine.
#[derive(Debug, Clone, PartialEq)]
pub enum ApplyOutcome {
    /// The op was applied; the slot watermark advanced.
    Applied(KvOpResult),
    /// The slot is at or below the persisted watermark and was skipped. The
    /// op had already been applied in a prior incarnation of the process.
    Skipped,
}

/// Persistent state machine for the KV demo.
pub struct KvState {
    db: Database,
}

impl KvState {
    /// Open or create a redb database at `path` and prepare the `kv` and
    /// `meta` tables.
    pub fn open(path: impl AsRef<Path>) -> KvResult<Self> {
        let db = Database::create(path.as_ref())?;
        let txn = db.begin_write()?;
        {
            let _ = txn.open_table(KV_TABLE)?;
            let _ = txn.open_table(META_TABLE)?;
        }
        txn.commit()?;
        Ok(Self { db })
    }

    /// Return the persisted watermark, or `None` if no op has been applied yet.
    pub fn last_applied_slot(&self) -> KvResult<Option<u64>> {
        let txn = self.db.begin_read()?;
        let meta = txn.open_table(META_TABLE)?;
        Ok(meta.get(META_WATERMARK)?.map(|v| v.value()))
    }

    /// Apply a decision in a single write transaction.
    ///
    /// If `slot` is at or below the persisted watermark, the op is skipped and
    /// no state change occurs. Otherwise the value table is mutated (for Put)
    /// or read (for Get), the watermark is bumped to `slot`, and the result
    /// is returned.
    pub fn apply(&self, slot: u64, op: &KvOp) -> KvResult<ApplyOutcome> {
        let txn = self.db.begin_write()?;
        let outcome = {
            let mut meta = txn.open_table(META_TABLE)?;
            let current = meta.get(META_WATERMARK)?.map(|v| v.value());
            if let Some(w) = current {
                if slot <= w {
                    return Ok(ApplyOutcome::Skipped);
                }
            }
            let mut kv = txn.open_table(KV_TABLE)?;
            let result = match &op.kind {
                KvOpKind::Put { key, value } => {
                    let prior = kv.insert(key.as_str(), value.as_ref())?;
                    KvOpResult::Put(prior.is_none())
                }
                KvOpKind::Get { key } => {
                    let value = kv
                        .get(key.as_str())?
                        .map(|v| Bytes::copy_from_slice(v.value()));
                    KvOpResult::Get(value)
                }
            };
            meta.insert(META_WATERMARK, slot)?;
            ApplyOutcome::Applied(result)
        };
        txn.commit()?;
        Ok(outcome)
    }
}

impl std::fmt::Debug for KvState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KvState").finish_non_exhaustive()
    }
}

// The kv table stores raw bytes; redb's value-as-bytes encoding is what we
// want here. Errors map through `?` into [`KvError::Storage`] via the
// `From` impls in [`crate::error`].
#[allow(dead_code)]
fn _assert_send_sync() {
    fn assert_send<T: Send>() {}
    fn assert_sync<T: Sync>() {}
    assert_send::<KvState>();
    assert_sync::<KvState>();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kv::{KvOp, KvOpKind, KvOpResult};
    use bytes::Bytes;
    use tempfile::TempDir;

    fn op_put(id: u128, key: &str, value: &[u8]) -> KvOp {
        KvOp {
            id,
            kind: KvOpKind::Put {
                key: key.to_string(),
                value: Bytes::copy_from_slice(value),
            },
        }
    }

    fn op_get(id: u128, key: &str) -> KvOp {
        KvOp {
            id,
            kind: KvOpKind::Get {
                key: key.to_string(),
            },
        }
    }

    fn fresh_state() -> (TempDir, KvState) {
        let dir = TempDir::new().unwrap();
        let state = KvState::open(dir.path().join("kv.redb")).unwrap();
        (dir, state)
    }

    #[test]
    fn put_then_get_returns_value() {
        let (_dir, state) = fresh_state();
        let put = state.apply(0, &op_put(1, "alice", b"v1")).unwrap();
        assert!(matches!(put, ApplyOutcome::Applied(KvOpResult::Put(true))));

        let get = state.apply(1, &op_get(2, "alice")).unwrap();
        match get {
            ApplyOutcome::Applied(KvOpResult::Get(Some(v))) => assert_eq!(v.as_ref(), b"v1"),
            other => panic!("expected Get(Some(b\"v1\")), got {:?}", other),
        }
    }

    #[test]
    fn put_replace_returns_was_new_false() {
        let (_dir, state) = fresh_state();
        let first = state.apply(0, &op_put(1, "k", b"v1")).unwrap();
        assert!(matches!(
            first,
            ApplyOutcome::Applied(KvOpResult::Put(true))
        ));

        let second = state.apply(1, &op_put(2, "k", b"v2")).unwrap();
        assert!(matches!(
            second,
            ApplyOutcome::Applied(KvOpResult::Put(false))
        ));

        let get = state.apply(2, &op_get(3, "k")).unwrap();
        match get {
            ApplyOutcome::Applied(KvOpResult::Get(Some(v))) => assert_eq!(v.as_ref(), b"v2"),
            other => panic!("expected v2, got {:?}", other),
        }
    }

    #[test]
    fn get_missing_key_returns_none() {
        let (_dir, state) = fresh_state();
        let get = state.apply(0, &op_get(1, "absent")).unwrap();
        assert!(matches!(get, ApplyOutcome::Applied(KvOpResult::Get(None))));
    }

    #[test]
    fn replay_below_watermark_is_skipped() {
        let (_dir, state) = fresh_state();
        state.apply(0, &op_put(1, "k", b"v1")).unwrap();
        state.apply(1, &op_put(2, "k", b"v2")).unwrap();

        // Re-applying slot 0 should be skipped: state already advanced to 1.
        let replay = state.apply(0, &op_put(99, "k", b"junk")).unwrap();
        assert!(matches!(replay, ApplyOutcome::Skipped));

        // And the existing value is untouched.
        let get = state.apply(2, &op_get(4, "k")).unwrap();
        match get {
            ApplyOutcome::Applied(KvOpResult::Get(Some(v))) => assert_eq!(v.as_ref(), b"v2"),
            other => panic!("expected v2, got {:?}", other),
        }
    }

    #[test]
    fn replay_at_watermark_is_skipped() {
        let (_dir, state) = fresh_state();
        state.apply(0, &op_put(1, "k", b"v1")).unwrap();
        let replay = state.apply(0, &op_put(2, "k", b"v2")).unwrap();
        assert!(matches!(replay, ApplyOutcome::Skipped));
    }

    #[test]
    fn state_persists_across_reopen() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("kv.redb");
        {
            let state = KvState::open(&path).unwrap();
            state.apply(0, &op_put(1, "alice", b"42")).unwrap();
            state.apply(1, &op_put(2, "bob", b"7")).unwrap();
            assert_eq!(state.last_applied_slot().unwrap(), Some(1));
        }
        let state = KvState::open(&path).unwrap();
        assert_eq!(state.last_applied_slot().unwrap(), Some(1));

        // Re-applying slots that were already applied must be no-ops.
        let replay = state.apply(0, &op_put(99, "alice", b"x")).unwrap();
        assert!(matches!(replay, ApplyOutcome::Skipped));

        let get = state.apply(2, &op_get(3, "alice")).unwrap();
        match get {
            ApplyOutcome::Applied(KvOpResult::Get(Some(v))) => assert_eq!(v.as_ref(), b"42"),
            other => panic!("expected 42, got {:?}", other),
        }
    }
}
