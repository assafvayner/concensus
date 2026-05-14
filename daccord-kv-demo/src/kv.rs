//! Public KV interface ([`KvNode`]) plus the op type that flows through the
//! consensus log.
//!
//! Every `put` and every `get` on a [`KvNode`] is serialised as a [`KvOp`]
//! and submitted to the underlying daccord node. A single applier task per
//! node consumes [`Decided<KvOp>`](daccord::Decided) values in slot order
//! and feeds them into [`KvState::apply`](crate::state::KvState::apply); the
//! op's `id` field is then used to route the computed [`KvOpResult`] back to
//! the originating gRPC handler via a oneshot in a shared pending map.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

use daccord::{DecisionReceiver, NodeHandle, NodeState};

use crate::error::{KvError, KvResult};
use crate::state::{ApplyOutcome, KvState};

/// A KV operation submitted to the consensus log. The `id` field is the only
/// way the originating node correlates a decided op back to the handler that
/// is awaiting its result; it is generated randomly per request.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct KvOp {
    pub id: u128,
    pub kind: KvOpKind,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum KvOpKind {
    Put { key: String, value: Bytes },
    Get { key: String },
}

/// Result of applying a [`KvOp`] to the state machine.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KvOpResult {
    /// `true` if the key was previously absent (i.e. this put was a first
    /// write), `false` if it replaced an existing value.
    Put(bool),
    /// `Some(value)` if the key existed at apply time, `None` otherwise.
    Get(Option<Bytes>),
}

type PendingMap = Arc<Mutex<HashMap<u128, oneshot::Sender<KvOpResult>>>>;

/// A replicated KV node.
///
/// Wraps a running daccord [`Node`](daccord::Node) (via its
/// [`NodeHandle`](daccord::NodeHandle) and
/// [`DecisionReceiver`](daccord::DecisionReceiver)) with a redb-backed state
/// machine and a single applier task. Cloning is intentionally not provided;
/// the gRPC service and tests hold the `KvNode` directly and share it via
/// `Arc<KvNode>` when needed.
pub struct KvNode {
    handle: NodeHandle<KvOp>,
    pending: PendingMap,
    /// Held to keep the applier task alive for the lifetime of the `KvNode`.
    /// On drop, the task observes the closed channel and exits, firing
    /// `KvError::Cancelled` on any remaining outstanding oneshots via the
    /// pending-map drop guard.
    _applier: ApplierHandle,
}

/// JoinHandle wrapper that aborts the applier task on drop. Without this, a
/// dropped `KvNode` would leave the task running until the decision channel
/// closed on its own, which only happens when the daccord node is also gone.
struct ApplierHandle(tokio::task::JoinHandle<()>);

impl Drop for ApplierHandle {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl KvNode {
    /// Wrap a running daccord node with a KV state machine. Opens (or creates)
    /// the redb state database at `state_path` and spawns the applier task.
    ///
    /// `state_path` must be a file path inside a directory that already
    /// exists — `KvState::open` does not `mkdir -p`.
    pub fn spawn(
        handle: NodeHandle<KvOp>,
        decisions: DecisionReceiver<KvOp>,
        state_path: impl AsRef<Path>,
    ) -> KvResult<Self> {
        let state = KvState::open(state_path)?;
        let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
        let applier_pending = pending.clone();
        let task = tokio::spawn(run_applier(state, decisions, applier_pending));
        Ok(Self {
            handle,
            pending,
            _applier: ApplierHandle(task),
        })
    }

    /// Insert or replace the value for a key. Returns `true` if the key was
    /// previously absent, `false` if an existing value was replaced.
    pub async fn put(&self, key: String, value: Bytes) -> KvResult<bool> {
        let id = random_op_id();
        let op = KvOp {
            id,
            kind: KvOpKind::Put { key, value },
        };
        match self.submit(op).await? {
            KvOpResult::Put(was_new) => Ok(was_new),
            KvOpResult::Get(_) => Err(KvError::ApplierGone),
        }
    }

    /// Read the current value for a key. Returns `None` if the key is absent.
    pub async fn get(&self, key: String) -> KvResult<Option<Bytes>> {
        let id = random_op_id();
        let op = KvOp {
            id,
            kind: KvOpKind::Get { key },
        };
        match self.submit(op).await? {
            KvOpResult::Get(value) => Ok(value),
            KvOpResult::Put(_) => Err(KvError::ApplierGone),
        }
    }

    /// Snapshot of the underlying daccord node's state.
    pub fn status(&self) -> NodeState {
        self.handle.status()
    }

    async fn submit(&self, op: KvOp) -> KvResult<KvOpResult> {
        let id = op.id;
        let (tx, rx) = oneshot::channel();
        {
            let mut guard = self.pending.lock().map_err(|_| KvError::ApplierGone)?;
            guard.insert(id, tx);
        }

        if let Err(e) = self.handle.propose(op).await {
            // Propose failed before consensus accepted the op; drop the
            // pending entry so it doesn't leak.
            if let Ok(mut guard) = self.pending.lock() {
                guard.remove(&id);
            }
            return Err(KvError::from(e));
        }

        match rx.await {
            Ok(result) => Ok(result),
            Err(_) => Err(KvError::Cancelled),
        }
    }
}

fn random_op_id() -> u128 {
    // u128 is large enough that collision probability is negligible even
    // across the whole demo cluster's lifetime; no need for a separate
    // process-local counter.
    rand::random()
}

async fn run_applier(state: KvState, mut decisions: DecisionReceiver<KvOp>, pending: PendingMap) {
    while let Some(decided) = decisions.recv().await {
        let slot = decided.slot;
        let op = decided.value;
        let id = op.id;
        let outcome = match state.apply(slot, &op) {
            Ok(outcome) => outcome,
            Err(e) => {
                tracing::error!(slot, error = %e, "kv applier: state-store error, exiting");
                // Drop the pending map so every outstanding oneshot fires
                // Cancelled — callers will surface KvError::Cancelled.
                if let Ok(mut guard) = pending.lock() {
                    guard.clear();
                }
                return;
            }
        };
        match outcome {
            ApplyOutcome::Applied(result) => {
                let waiter = pending.lock().ok().and_then(|mut g| g.remove(&id));
                if let Some(tx) = waiter {
                    // If the receiver is gone (caller dropped), that's fine.
                    let _ = tx.send(result);
                }
            }
            ApplyOutcome::Skipped => {
                // Slot was already applied across a restart; the originator
                // is gone. Nothing to do.
                tracing::debug!(slot, "kv applier: skipped already-applied slot");
            }
        }
    }
    tracing::info!("kv applier: decision channel closed, exiting");
}
