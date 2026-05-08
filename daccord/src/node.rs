//! Core consensus node and its associated handle types.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use serde::{de::DeserializeOwned, Serialize};
use tokio::sync::{mpsc, oneshot, watch};

use crate::config::{NodeId, PeerInfo};
use crate::error::{NodeError, ProposeError};
use crate::message::{Message, WireVariant};
use crate::proposal::{Pending, PendingPaxosStorage, PendingRaftStorage, Submission};
use crate::protocol::{Outgoing, PaxosProtocol, ProtocolImpl, SendTarget};
use crate::storage::{PaxosStorage, RaftStorage};
use crate::transport::{MessageReceiver, MessageSender};

/// Public mirror of the internal Raft role. Exposed via [`NodeHandle::status`]
/// and [`Node::peek_state`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NodeRole {
    Follower,
    Candidate,
    Leader,
}

/// Identifies which consensus algorithm a [`Node`] is running. Reported by
/// [`NodeHandle::status`] / [`Node::peek_state`] as part of [`NodeState`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NodeAlgorithm {
    Paxos,
    Raft,
}

/// Snapshot of a node's protocol state, for tests and observability.
///
/// Returned by [`NodeHandle::status`]. Fields that don't apply to the active
/// algorithm carry sentinel values: a Paxos node always reports
/// `role = None`, `term = 0`, `voted_for = None`.
#[derive(Clone, Debug)]
pub struct NodeState {
    pub node_id: NodeId,
    pub algorithm: NodeAlgorithm,
    pub role: Option<NodeRole>,
    pub term: u64,
    pub leader: Option<NodeId>,
    pub voted_for: Option<NodeId>,
    pub log_len: u64,
    pub commit_index: Option<u64>,
    pub last_applied: Option<u64>,
}

/// Channel receiver for consensus decisions.
///
/// Yields [`Decided`] values in the order they are finalized by the consensus
/// protocol. Obtain one from [`Node::paxos`] / [`Node::raft`].
pub type DecisionReceiver<V> = mpsc::Receiver<Decided<V>>;

/// A value that has reached consensus, paired with its slot number.
///
/// All nodes in a healthy cluster will produce the same `Decided` value for
/// each slot. Slots are assigned sequentially starting from 0.
///
/// # Idempotency
///
/// Consumers must treat `Decided` delivery as **at-least-once** and dedupe by
/// slot. The same `Decided { slot, value }` may be delivered more than once
/// across a node restart: when a node crashes between persisting a log entry
/// and persisting the corresponding decision, recovery sees the entry as
/// uncommitted, and the next leader's heartbeat re-applies it. The slot and
/// value will be identical to the prior delivery — the cluster never disagrees
/// about a slot's value — but a consumer that performs side effects on each
/// `Decided` must keep its own `last_processed_slot` watermark.
#[derive(Clone, Debug)]
pub struct Decided<V> {
    /// The slot number this value was decided in.
    pub slot: u64,
    /// The decided value.
    pub value: V,
}

const PROPOSAL_CHANNEL_CAPACITY: usize = 1024;
const DECISION_CHANNEL_CAPACITY: usize = 1024;

/// A consensus node.
///
/// `Node` is the central type in this crate. It runs a consensus protocol
/// (Multi-Paxos or Raft) over a set of peers using pluggable transport and
/// storage backends.
///
/// # Type Parameters
///
/// - `V` — the value type being decided. Must be serializable, cloneable, and
///   comparable for equality (used to detect lost proposals).
/// - `S` — the [`MessageSender`] implementation used to reach peers.
/// - `R` — the [`MessageReceiver`] implementation for incoming messages.
///
/// # Lifecycle
///
/// 1. **Construct** via [`Node::paxos`] / [`Node::raft`] (production) or
///    [`Node::paxos_with_id`] / [`Node::raft_with_id`] (testing).
///    This returns `(Node, NodeHandle, DecisionReceiver)`.
/// 2. **Spawn** the node's event loop with [`Node::run`] on a tokio task.
/// 3. **Propose** values through the [`NodeHandle`].
/// 4. **Consume** decided values from the [`DecisionReceiver`].
/// 5. **Shut down** by dropping all [`NodeHandle`] clones — the event loop exits
///    gracefully once the proposal channel closes.
pub struct Node<V, S: MessageSender, R: MessageReceiver> {
    node_id: NodeId,
    peers: Vec<PeerInfo<S>>,
    receiver: Option<R>,
    protocol: ProtocolImpl<Pending<V>>,
    proposal_rx: mpsc::Receiver<Submission<V>>,
    decision_tx: mpsc::Sender<Decided<V>>,
    state_tx: Arc<watch::Sender<NodeState>>,
    algorithm: NodeAlgorithm,
}

/// A cloneable handle for submitting proposals to a running [`Node`].
///
/// Obtain a `NodeHandle` from [`Node::paxos`] / [`Node::raft`]. Cloning is
/// cheap (wraps a tokio mpsc sender + a watch receiver) and allows multiple
/// producers to submit proposals concurrently.
///
/// The node shuts down gracefully when all `NodeHandle` clones are dropped.
pub struct NodeHandle<V> {
    proposal_tx: mpsc::Sender<Submission<V>>,
    state_rx: watch::Receiver<NodeState>,
}

impl<V> Clone for NodeHandle<V> {
    fn clone(&self) -> Self {
        Self {
            proposal_tx: self.proposal_tx.clone(),
            state_rx: self.state_rx.clone(),
        }
    }
}

/// Holds outstanding proposal completion senders, firing
/// [`ProposeError::Cancelled`] on every remaining oneshot when dropped.
///
/// Wrapping the pending map in a guard ensures cancellation fires on panic-
/// induced unwind as well as normal shutdown.
struct PendingMap<V> {
    inner: HashMap<u64, oneshot::Sender<Result<Decided<V>, ProposeError>>>,
}

impl<V> PendingMap<V> {
    fn new() -> Self {
        Self {
            inner: HashMap::new(),
        }
    }

    fn insert(&mut self, nonce: u64, tx: oneshot::Sender<Result<Decided<V>, ProposeError>>) {
        self.inner.insert(nonce, tx);
    }

    fn remove(&mut self, nonce: u64) -> Option<oneshot::Sender<Result<Decided<V>, ProposeError>>> {
        self.inner.remove(&nonce)
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.inner.len()
    }
}

impl<V> Drop for PendingMap<V> {
    fn drop(&mut self) {
        for (_, tx) in self.inner.drain() {
            let _ = tx.send(Err(ProposeError::Cancelled));
        }
    }
}

/// Returns a random nonce suitable for a live proposal.
///
/// `nonce = 0` is reserved as the sentinel for values recovered from storage,
/// so live proposals must never use it. This function loops until it draws a
/// non-zero value; with a 64-bit RNG the loop body runs exactly once with
/// overwhelming probability.
fn live_nonce() -> u64 {
    loop {
        let n = rand::random::<u64>();
        if n != 0 {
            return n;
        }
    }
}

fn initial_state(node_id: NodeId, algorithm: NodeAlgorithm) -> NodeState {
    NodeState {
        node_id,
        algorithm,
        role: match algorithm {
            NodeAlgorithm::Raft => Some(NodeRole::Follower),
            NodeAlgorithm::Paxos => None,
        },
        term: 0,
        leader: None,
        voted_for: None,
        log_len: 0,
        commit_index: None,
        last_applied: None,
    }
}

impl<V, S, R> Node<V, S, R>
where
    V: Serialize + DeserializeOwned + Clone + Send + Sync + PartialEq + 'static,
    S: MessageSender,
    R: MessageReceiver,
{
    /// Creates a Multi-Paxos consensus node with an auto-generated [`NodeId`].
    ///
    /// The node ID is formed from the given `name` and the current UNIX timestamp
    /// as the incarnation number, ensuring uniqueness across restarts.
    /// `config` is plumbed through to the underlying Paxos protocol; pass
    /// [`PaxosConfig::default`](crate::PaxosConfig::default) for defaults.
    ///
    /// Returns `(node, handle, decision_rx)`:
    /// - `node` — call [`Node::run`] to start the event loop
    /// - `handle` — use [`NodeHandle::propose`] to submit values
    /// - `decision_rx` — receives [`Decided`] values as consensus is reached
    pub fn paxos(
        name: impl Into<Arc<str>>,
        config: crate::config::PaxosConfig,
        peers: Vec<PeerInfo<S>>,
        receiver: R,
        storage: impl PaxosStorage<V> + 'static,
    ) -> (Self, NodeHandle<V>, DecisionReceiver<V>) {
        let incarnation = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before UNIX epoch")
            .as_secs();
        let node_id = NodeId::new(name, incarnation);
        Self::paxos_with_id_inner(node_id, config, peers, receiver, storage)
    }

    /// Creates a Multi-Paxos consensus node with an explicit [`NodeId`].
    ///
    /// Useful in tests where peers need matching deterministic identities; in
    /// production prefer [`Node::paxos`].
    ///
    /// Requires the `test-support` feature flag (always available in `#[cfg(test)]`).
    #[cfg(any(test, feature = "test-support"))]
    pub fn paxos_with_id(
        node_id: NodeId,
        config: crate::config::PaxosConfig,
        peers: Vec<PeerInfo<S>>,
        receiver: R,
        storage: impl PaxosStorage<V> + 'static,
    ) -> (Self, NodeHandle<V>, DecisionReceiver<V>) {
        Self::paxos_with_id_inner(node_id, config, peers, receiver, storage)
    }

    fn paxos_with_id_inner(
        node_id: NodeId,
        config: crate::config::PaxosConfig,
        peers: Vec<PeerInfo<S>>,
        receiver: R,
        storage: impl PaxosStorage<V> + 'static,
    ) -> (Self, NodeHandle<V>, DecisionReceiver<V>) {
        let total_nodes = peers.len() + 1;
        let adapted = PendingPaxosStorage::<V, _>::new(storage);
        let protocol = ProtocolImpl::Paxos(PaxosProtocol::new_with_config(
            node_id.clone(),
            total_nodes,
            config,
            Box::new(adapted),
        ));
        let (proposal_tx, proposal_rx) = mpsc::channel(PROPOSAL_CHANNEL_CAPACITY);
        let (decision_tx, decision_rx) = mpsc::channel(DECISION_CHANNEL_CAPACITY);
        let initial = initial_state(node_id.clone(), NodeAlgorithm::Paxos);
        let (state_tx, state_rx) = watch::channel(initial);
        let state_tx = Arc::new(state_tx);

        let node = Self {
            node_id,
            peers,
            receiver: Some(receiver),
            protocol,
            proposal_rx,
            decision_tx,
            state_tx,
            algorithm: NodeAlgorithm::Paxos,
        };

        (
            node,
            NodeHandle {
                proposal_tx,
                state_rx,
            },
            decision_rx,
        )
    }

    /// Creates a Raft consensus node with an auto-generated [`NodeId`].
    ///
    /// Requires storage that implements [`RaftStorage`](crate::RaftStorage) so
    /// the node can persist `currentTerm`, `votedFor`, and the replicated log.
    /// [`RaftMemoryStorage`](crate::RaftMemoryStorage) satisfies this in
    /// tests; production deployments should provide a durable backing store.
    pub fn raft(
        name: impl Into<Arc<str>>,
        config: crate::config::RaftConfig,
        peers: Vec<PeerInfo<S>>,
        receiver: R,
        storage: impl RaftStorage<V> + 'static,
    ) -> (Self, NodeHandle<V>, DecisionReceiver<V>) {
        let incarnation = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before UNIX epoch")
            .as_secs();
        let node_id = NodeId::new(name, incarnation);
        Self::raft_with_id_inner(node_id, config, peers, receiver, storage)
    }

    /// Creates a Raft consensus node with an explicit [`NodeId`].
    ///
    /// Useful in tests for deterministic IDs.
    #[cfg(any(test, feature = "test-support"))]
    pub fn raft_with_id(
        node_id: NodeId,
        config: crate::config::RaftConfig,
        peers: Vec<PeerInfo<S>>,
        receiver: R,
        storage: impl RaftStorage<V> + 'static,
    ) -> (Self, NodeHandle<V>, DecisionReceiver<V>) {
        Self::raft_with_id_inner(node_id, config, peers, receiver, storage)
    }

    fn raft_with_id_inner(
        node_id: NodeId,
        config: crate::config::RaftConfig,
        peers: Vec<PeerInfo<S>>,
        receiver: R,
        storage: impl RaftStorage<V> + 'static,
    ) -> (Self, NodeHandle<V>, DecisionReceiver<V>) {
        let total_nodes = peers.len() + 1;
        let adapted = PendingRaftStorage::<V, _>::new(storage);
        let protocol = ProtocolImpl::Raft(crate::protocol::RaftProtocol::new(
            node_id.clone(),
            total_nodes,
            config,
            Box::new(adapted),
        ));
        let (proposal_tx, proposal_rx) = mpsc::channel(PROPOSAL_CHANNEL_CAPACITY);
        let (decision_tx, decision_rx) = mpsc::channel(DECISION_CHANNEL_CAPACITY);
        let initial = initial_state(node_id.clone(), NodeAlgorithm::Raft);
        let (state_tx, state_rx) = watch::channel(initial);
        let state_tx = Arc::new(state_tx);
        let node = Self {
            node_id,
            peers,
            receiver: Some(receiver),
            protocol,
            proposal_rx,
            decision_tx,
            state_tx,
            algorithm: NodeAlgorithm::Raft,
        };
        (
            node,
            NodeHandle {
                proposal_tx,
                state_rx,
            },
            decision_rx,
        )
    }

    /// Snapshot of the node's current protocol state.
    ///
    /// Backwards-compat thin wrapper around the watch-channel state. Tests may
    /// also poll [`NodeHandle::status`] for the same information.
    #[cfg(any(test, feature = "test-support"))]
    pub fn peek_state(&self) -> NodeState {
        self.compute_state()
    }

    fn compute_state(&self) -> NodeState {
        let snap = self.protocol.peek_state();
        NodeState {
            node_id: self.node_id.clone(),
            algorithm: self.algorithm,
            role: snap.role,
            term: snap.term,
            leader: snap.leader,
            voted_for: snap.voted_for,
            log_len: snap.log_len,
            commit_index: snap.commit_index,
            last_applied: snap.last_applied,
        }
    }

    fn publish_state(&self) {
        let new_state = self.compute_state();
        self.state_tx.send_replace(new_state);
    }

    /// Runs the consensus event loop until shutdown or fatal error.
    ///
    /// This method consumes the `Node` and drives the consensus protocol:
    /// - Loads previously decided values from storage
    /// - Listens for proposals via the internal channel (from [`NodeHandle`])
    /// - Processes incoming messages from peers
    /// - Periodically retries stalled proposals with exponential backoff
    /// - Re-broadcasts recent decisions so late peers can catch up
    ///
    /// # Shutdown
    ///
    /// The event loop exits when all [`NodeHandle`] clones are dropped (the
    /// proposal channel closes). It returns `Ok(())` in this case. Any
    /// proposals still pending at shutdown have their oneshots fired with
    /// [`ProposeError::Cancelled`] via the [`PendingMap`] drop guard.
    ///
    /// # Errors
    ///
    /// - [`NodeError::NoQuorum`] — the transport receiver closed while the node
    ///   had outstanding proposals and needs a quorum to make progress.
    /// - [`NodeError::Storage`] — a storage operation failed.
    pub async fn run(mut self) -> Result<(), NodeError> {
        tracing::info!(node_id = %self.node_id, "node starting");

        self.protocol.recover().await.map_err(NodeError::Storage)?;
        self.publish_state();

        // Build senders list
        let mut senders: Vec<(NodeId, S)> = Vec::new();
        let peers = std::mem::take(&mut self.peers);
        let has_peers = !peers.is_empty();

        for peer in peers {
            senders.push((peer.id, peer.sender));
        }

        // Take receiver out of Option
        let mut receiver = self.receiver.take().unwrap();

        let total_cluster = senders.len() + 1; // including self
        let quorum = (total_cluster / 2) + 1;

        // Retry check interval
        let mut retry_interval = tokio::time::interval(std::time::Duration::from_millis(50));
        retry_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        // Track whether the receiver is still usable.
        let mut receiver_alive = has_peers;

        // Pending proposal completions, keyed by nonce. Wrapped in a guard so
        // every outstanding oneshot is fired with `Cancelled` on panic-induced
        // unwind as well as normal shutdown.
        let mut pending: PendingMap<V> = PendingMap::new();

        loop {
            if receiver_alive {
                tokio::select! {
                    proposal = self.proposal_rx.recv() => {
                        match proposal {
                            Some(submission) => self.handle_submission(submission, &mut pending, &senders).await?,
                            None => {
                                tracing::info!("all proposal handles dropped, shutting down");
                                return Ok(());
                            }
                        }
                    }
                    result = receiver.recv() => {
                        match result {
                            Ok(data) => {
                                self.handle_incoming_bytes(&data, &mut pending, &senders).await?;
                            }
                            Err(e) => {
                                tracing::warn!(error = %e, "receiver error");
                                if 1 < quorum && !self.protocol.is_idle() {
                                    return Err(NodeError::NoQuorum);
                                }
                                receiver_alive = false;
                            }
                        }
                    }
                    _ = retry_interval.tick() => {
                        self.handle_retries(&mut pending, &senders).await?;
                    }
                }
            } else {
                tokio::select! {
                    proposal = self.proposal_rx.recv() => {
                        match proposal {
                            Some(submission) => self.handle_submission(submission, &mut pending, &senders).await?,
                            None => {
                                tracing::info!("all proposal handles dropped, shutting down");
                                return Ok(());
                            }
                        }
                    }
                    _ = retry_interval.tick() => {
                        self.handle_retries(&mut pending, &senders).await?;
                    }
                }
            }
        }
    }

    async fn handle_submission(
        &mut self,
        submission: Submission<V>,
        pending: &mut PendingMap<V>,
        senders: &[(NodeId, S)],
    ) -> Result<(), NodeError> {
        let Submission {
            pending: pending_value,
            completion,
        } = submission;
        pending.insert(pending_value.nonce, completion);
        let outgoing = self.protocol.propose(pending_value);
        self.protocol
            .flush_persist()
            .await
            .map_err(NodeError::Storage)?;
        Self::send_outgoing(&self.node_id, &outgoing, senders).await;
        self.process_decisions(pending).await?;
        self.publish_state();
        Ok(())
    }

    async fn handle_incoming_bytes(
        &mut self,
        data: &[u8],
        pending: &mut PendingMap<V>,
        senders: &[(NodeId, S)],
    ) -> Result<(), NodeError> {
        match Message::<Pending<V>>::from_bytes(data) {
            Ok(msg) => {
                let from = msg.sender;
                let outgoing = self.protocol.handle_wire_message(from, msg.variant);
                self.protocol
                    .flush_persist()
                    .await
                    .map_err(NodeError::Storage)?;
                Self::send_outgoing(&self.node_id, &outgoing, senders).await;
                self.process_decisions(pending).await?;

                // Re-propose any lost proposals
                let lost = self.protocol.take_lost_proposals();
                for value in lost {
                    let outgoing = self.protocol.propose(value);
                    self.protocol
                        .flush_persist()
                        .await
                        .map_err(NodeError::Storage)?;
                    Self::send_outgoing(&self.node_id, &outgoing, senders).await;
                    self.process_decisions(pending).await?;
                }
                self.publish_state();
            }
            Err(e) => {
                tracing::warn!(error = %e, "failed to deserialize message");
            }
        }
        Ok(())
    }

    async fn handle_retries(
        &mut self,
        pending: &mut PendingMap<V>,
        senders: &[(NodeId, S)],
    ) -> Result<(), NodeError> {
        let outgoing = self.protocol.on_tick(Instant::now());
        self.protocol
            .flush_persist()
            .await
            .map_err(NodeError::Storage)?;
        Self::send_outgoing(&self.node_id, &outgoing, senders).await;
        self.process_decisions(pending).await?;
        self.publish_state();
        Ok(())
    }

    async fn send_outgoing(
        node_id: &NodeId,
        outgoing: &[Outgoing<WireVariant<Pending<V>>>],
        senders: &[(NodeId, S)],
    ) {
        for out in outgoing {
            let msg = Message {
                sender: node_id.clone(),
                variant: out.message.clone(),
            };
            let bytes = match msg.to_bytes() {
                Ok(b) => b,
                Err(e) => {
                    tracing::error!(error = %e, "failed to serialize message");
                    continue;
                }
            };
            match &out.target {
                SendTarget::Peer(target) => {
                    if let Some((_, sender)) = senders.iter().find(|(id, _)| id == target) {
                        let _ = sender.send(bytes).await;
                    }
                }
                SendTarget::Broadcast => {
                    for (_, sender) in senders {
                        let _ = sender.send(bytes.clone()).await;
                    }
                }
            }
        }
    }

    async fn process_decisions(&mut self, pending: &mut PendingMap<V>) -> Result<(), NodeError> {
        let decisions = self
            .protocol
            .drain_decisions()
            .await
            .map_err(NodeError::Storage)?;
        for decision in decisions {
            let slot = decision.slot;
            let Pending { nonce, value } = decision.value;
            tracing::info!(slot, "value decided");

            let public = Decided {
                slot,
                value: value.clone(),
            };

            if self.decision_tx.send(public.clone()).await.is_err() {
                tracing::warn!("decision receiver dropped, decisions will not be delivered");
            }

            if let Some(tx) = pending.remove(nonce) {
                // The receiver may have been dropped (caller no longer cares),
                // in which case the send simply fails — that's fine.
                let _ = tx.send(Ok(public));
            }
        }

        Ok(())
    }
}

impl<V> NodeHandle<V>
where
    V: Serialize + DeserializeOwned + Clone + Send + 'static,
{
    /// Submit a value for consensus and await its commit.
    ///
    /// Returns when the value has been decided by the cluster or an error
    /// prevents progress. The returned [`Decided`] carries the slot the value
    /// was assigned and (currently) the original value passed in.
    ///
    /// # Errors
    ///
    /// - [`ProposeError::ChannelFull`] — the internal proposal queue (capacity
    ///   1024) is full. Back off and retry.
    /// - [`ProposeError::NotRunning`] — the node's event loop has stopped.
    /// - [`ProposeError::Cancelled`] — the node shut down before the proposal
    ///   was decided.
    pub async fn propose(&self, value: V) -> Result<Decided<V>, ProposeError> {
        let nonce = live_nonce();
        let (tx, rx) = oneshot::channel();
        let submission = Submission {
            pending: Pending { nonce, value },
            completion: tx,
        };
        self.proposal_tx.try_send(submission).map_err(|e| match e {
            mpsc::error::TrySendError::Full(_) => ProposeError::ChannelFull,
            mpsc::error::TrySendError::Closed(_) => ProposeError::NotRunning,
        })?;
        match rx.await {
            Ok(result) => result,
            // The event-loop dropped the sender without firing it (e.g., panic
            // in the loop that bypassed the PendingMap drop guard). Surface as
            // Cancelled so callers don't wait forever.
            Err(_) => Err(ProposeError::Cancelled),
        }
    }

    /// Returns the latest observed [`NodeState`] for this node.
    ///
    /// The returned snapshot is updated every time the event loop processes a
    /// proposal, an incoming message, or a periodic tick. May briefly lag the
    /// truly-current protocol state by one event-loop iteration.
    pub fn status(&self) -> NodeState {
        self.state_rx.borrow().clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::PeerInfo;
    use crate::error::TransportError;
    use crate::storage::{PaxosMemoryStorage, RaftMemoryStorage};
    use bytes::Bytes;

    struct DummySender;
    #[async_trait::async_trait]
    impl MessageSender for DummySender {
        async fn send(&self, _data: Bytes) -> Result<(), TransportError> {
            Ok(())
        }
    }

    struct DummyReceiver;
    #[async_trait::async_trait]
    impl MessageReceiver for DummyReceiver {
        async fn recv(&mut self) -> Result<Bytes, TransportError> {
            std::future::pending().await
        }
    }

    #[test]
    fn node_paxos_returns_node_handle_and_receiver() {
        use crate::config::PaxosConfig;
        let (_node, _handle, _decision_rx) = Node::<String, DummySender, DummyReceiver>::paxos(
            "test-node",
            PaxosConfig::default(),
            vec![],
            DummyReceiver,
            PaxosMemoryStorage::new(),
        );
    }

    #[tokio::test]
    async fn paxos_constructor_works() {
        use crate::config::PaxosConfig;
        let (_node, _handle, _rx) = Node::<String, DummySender, DummyReceiver>::paxos(
            "test",
            PaxosConfig::default(),
            vec![],
            DummyReceiver,
            PaxosMemoryStorage::new(),
        );
    }

    #[tokio::test]
    async fn raft_constructor_works() {
        use crate::config::RaftConfig;
        let (_node, _handle, _rx) = Node::<String, DummySender, DummyReceiver>::raft(
            "test",
            RaftConfig::default(),
            vec![],
            DummyReceiver,
            RaftMemoryStorage::new(),
        );
    }

    #[tokio::test]
    async fn node_handle_is_cloneable() {
        use crate::config::PaxosConfig;
        let (_node, handle, _rx) = Node::<String, DummySender, DummyReceiver>::paxos(
            "test-node",
            PaxosConfig::default(),
            vec![],
            DummyReceiver,
            PaxosMemoryStorage::new(),
        );
        let _handle2 = handle.clone();
    }

    #[tokio::test]
    async fn propose_returns_channel_full_when_full() {
        use crate::config::PaxosConfig;
        // Construct a node but never spawn the event loop, so the proposal
        // channel fills up and stays full.
        let (_node, handle, _rx) = Node::<String, DummySender, DummyReceiver>::paxos(
            "test-node",
            PaxosConfig::default(),
            vec![],
            DummyReceiver,
            PaxosMemoryStorage::new(),
        );
        for i in 0..1024 {
            let h = handle.clone();
            // Spawn so the await on the oneshot doesn't block this test —
            // we only care that try_send succeeds. The spawned future lives
            // forever (no event loop) but the test doesn't wait on it.
            tokio::spawn(async move {
                let _ = h.propose(format!("msg-{}", i)).await;
            });
            tokio::task::yield_now().await;
        }
        // Give spawned tasks a moment to enqueue.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        // The next propose should be rejected with ChannelFull.
        let result = handle.propose("overflow".to_string()).await;
        assert!(matches!(result, Err(ProposeError::ChannelFull)));
    }

    #[tokio::test]
    async fn propose_channel_full() {
        // Distinct test: fill the proposal channel without spawning the event
        // loop and assert ChannelFull. Mirrors the test above but uses the
        // explicit name from the task plan.
        use crate::config::PaxosConfig;
        let (_node, handle, _rx) = Node::<String, DummySender, DummyReceiver>::paxos(
            "test-node",
            PaxosConfig::default(),
            vec![],
            DummyReceiver,
            PaxosMemoryStorage::new(),
        );
        for i in 0..1024 {
            let h = handle.clone();
            tokio::spawn(async move {
                let _ = h.propose(format!("msg-{}", i)).await;
            });
            tokio::task::yield_now().await;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let result = handle.propose("overflow".to_string()).await;
        assert!(matches!(result, Err(ProposeError::ChannelFull)));
    }

    #[tokio::test]
    async fn propose_returns_decided_value() {
        use crate::config::PaxosConfig;
        let (node, handle, _decision_rx) = Node::<String, DummySender, DummyReceiver>::paxos(
            "solo",
            PaxosConfig::default(),
            vec![],
            DummyReceiver,
            PaxosMemoryStorage::new(),
        );
        let _run_handle = tokio::spawn(node.run());

        let decided = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            handle.propose("hello".to_string()),
        )
        .await
        .expect("timed out waiting for propose to resolve")
        .expect("propose returned an error");

        assert_eq!(decided.slot, 0);
        assert_eq!(decided.value, "hello");
    }

    #[tokio::test]
    async fn propose_cancelled_on_drop() {
        use crate::config::PaxosConfig;
        // Build a Paxos node with a single peer it can never reach: any
        // proposal needs 2-of-2 quorum so it will sit pending. The receiver
        // blocks forever. We spawn the event loop, kick off a propose, then
        // abort the run task so the Node (and its PendingMap drop guard) is
        // dropped while the proposal is still pending.
        let peer_id = NodeId::new("never", 1);
        let (node, handle, _rx) = Node::<String, DummySender, DummyReceiver>::paxos_with_id(
            NodeId::new("solo", 1),
            PaxosConfig::default(),
            vec![PeerInfo {
                id: peer_id,
                sender: DummySender,
            }],
            DummyReceiver,
            PaxosMemoryStorage::new(),
        );
        let run_handle = tokio::spawn(node.run());

        // Spawn the propose. The future will sit awaiting the oneshot until
        // the PendingMap drop guard fires Cancelled.
        let h = handle.clone();
        let propose_handle =
            tokio::spawn(async move { h.propose("never-decides".to_string()).await });

        // Give the event loop a moment to register the proposal.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Aborting the run task drops the Node (and the PendingMap inside),
        // which fires Cancelled on every outstanding oneshot.
        run_handle.abort();
        let _ = run_handle.await;

        let result = tokio::time::timeout(std::time::Duration::from_secs(5), propose_handle)
            .await
            .expect("propose did not resolve")
            .expect("task panicked");
        assert!(matches!(result, Err(ProposeError::Cancelled)));

        drop(handle);
    }

    #[tokio::test]
    async fn pending_does_not_leak() {
        use crate::config::PaxosConfig;
        // Single-node Paxos: every proposal commits immediately, so the
        // pending map should always end up empty after process_decisions
        // resolves the oneshot. We can't directly inspect the pending map from
        // outside the loop, but we can use a stand-in: drive several proposals
        // in sequence and make sure each completes successfully.
        let (node, handle, _decision_rx) = Node::<String, DummySender, DummyReceiver>::paxos(
            "solo",
            PaxosConfig::default(),
            vec![],
            DummyReceiver,
            PaxosMemoryStorage::new(),
        );
        let _run_handle = tokio::spawn(node.run());

        for i in 0..5 {
            let decided = tokio::time::timeout(
                std::time::Duration::from_secs(2),
                handle.propose(format!("v-{}", i)),
            )
            .await
            .expect("timed out")
            .expect("propose error");
            assert_eq!(decided.slot, i as u64);
            assert_eq!(decided.value, format!("v-{}", i));
        }
    }

    #[tokio::test]
    async fn pending_map_drop_fires_cancelled() {
        // White-box: dropping the PendingMap directly should send Cancelled
        // on every contained oneshot.
        let mut pending: PendingMap<String> = PendingMap::new();
        let (tx0, rx0) = oneshot::channel();
        let (tx1, rx1) = oneshot::channel();
        pending.insert(1, tx0);
        pending.insert(2, tx1);
        assert_eq!(pending.len(), 2);
        drop(pending);
        assert!(matches!(rx0.await.unwrap(), Err(ProposeError::Cancelled)));
        assert!(matches!(rx1.await.unwrap(), Err(ProposeError::Cancelled)));
    }

    #[tokio::test]
    async fn single_node_consensus() {
        use crate::config::PaxosConfig;
        let (node, handle, mut decision_rx) = Node::<String, DummySender, DummyReceiver>::paxos(
            "solo",
            PaxosConfig::default(),
            vec![],
            DummyReceiver,
            PaxosMemoryStorage::new(),
        );

        let run_handle = tokio::spawn(node.run());

        let decided = handle.propose("hello".to_string()).await.unwrap();
        assert_eq!(decided.slot, 0);
        assert_eq!(decided.value, "hello");

        let from_channel =
            tokio::time::timeout(std::time::Duration::from_secs(1), decision_rx.recv())
                .await
                .expect("timed out")
                .expect("channel closed");

        assert_eq!(from_channel.slot, 0);
        assert_eq!(from_channel.value, "hello");

        drop(handle);
        let _ = tokio::time::timeout(std::time::Duration::from_secs(1), run_handle).await;
    }

    #[tokio::test]
    async fn three_node_consensus() {
        use tokio::sync::mpsc as tokio_mpsc;

        struct ChannelSender(tokio_mpsc::Sender<Bytes>);
        #[async_trait::async_trait]
        impl MessageSender for ChannelSender {
            async fn send(&self, data: Bytes) -> Result<(), TransportError> {
                self.0.send(data).await.map_err(|_| TransportError::Closed)
            }
        }

        struct ChannelReceiver(tokio_mpsc::Receiver<Bytes>);
        #[async_trait::async_trait]
        impl MessageReceiver for ChannelReceiver {
            async fn recv(&mut self) -> Result<Bytes, TransportError> {
                self.0.recv().await.ok_or(TransportError::Closed)
            }
        }

        // One channel per node for receiving (all senders write to target node's channel)
        let (a_tx, a_rx) = tokio_mpsc::channel::<Bytes>(64);
        let (b_tx, b_rx) = tokio_mpsc::channel::<Bytes>(64);
        let (c_tx, c_rx) = tokio_mpsc::channel::<Bytes>(64);

        let id_a = NodeId::new("a", 1000);
        let id_b = NodeId::new("b", 1000);
        let id_c = NodeId::new("c", 1000);

        let (node_a, handle_a, mut rx_a) = Node::paxos_with_id(
            id_a.clone(),
            crate::config::PaxosConfig::default(),
            vec![
                PeerInfo {
                    id: id_b.clone(),
                    sender: ChannelSender(b_tx.clone()),
                },
                PeerInfo {
                    id: id_c.clone(),
                    sender: ChannelSender(c_tx.clone()),
                },
            ],
            ChannelReceiver(a_rx),
            PaxosMemoryStorage::<String>::new(),
        );
        let (node_b, _handle_b, mut rx_b) = Node::paxos_with_id(
            id_b.clone(),
            crate::config::PaxosConfig::default(),
            vec![
                PeerInfo {
                    id: id_a.clone(),
                    sender: ChannelSender(a_tx.clone()),
                },
                PeerInfo {
                    id: id_c.clone(),
                    sender: ChannelSender(c_tx.clone()),
                },
            ],
            ChannelReceiver(b_rx),
            PaxosMemoryStorage::<String>::new(),
        );
        let (node_c, _handle_c, mut rx_c) = Node::paxos_with_id(
            id_c.clone(),
            crate::config::PaxosConfig::default(),
            vec![
                PeerInfo {
                    id: id_a.clone(),
                    sender: ChannelSender(a_tx.clone()),
                },
                PeerInfo {
                    id: id_b.clone(),
                    sender: ChannelSender(b_tx.clone()),
                },
            ],
            ChannelReceiver(c_rx),
            PaxosMemoryStorage::<String>::new(),
        );

        tokio::spawn(node_a.run());
        tokio::spawn(node_b.run());
        tokio::spawn(node_c.run());

        // Propose from node A
        handle_a.propose("hello".to_string()).await.unwrap();

        let timeout = std::time::Duration::from_secs(5);
        let da = tokio::time::timeout(timeout, rx_a.recv())
            .await
            .unwrap()
            .unwrap();
        let db = tokio::time::timeout(timeout, rx_b.recv())
            .await
            .unwrap()
            .unwrap();
        let dc = tokio::time::timeout(timeout, rx_c.recv())
            .await
            .unwrap()
            .unwrap();

        assert_eq!(da.value, "hello");
        assert_eq!(db.value, "hello");
        assert_eq!(dc.value, "hello");
        assert_eq!(da.slot, db.slot);
        assert_eq!(db.slot, dc.slot);
    }

    #[tokio::test]
    async fn paxos_node_drops_raft_messages_silently() {
        use crate::config::{NodeId, PeerInfo};
        use crate::message::{Message, RaftMessage, WireVariant};
        use bytes::Bytes;
        use std::sync::Arc;
        use tokio::sync::Mutex;

        // A receiver that yields one synthetic Raft-formatted message, then pends.
        struct OneShot {
            once: Arc<Mutex<Option<Bytes>>>,
        }
        #[async_trait::async_trait]
        impl MessageReceiver for OneShot {
            async fn recv(&mut self) -> Result<Bytes, crate::error::TransportError> {
                let next = {
                    let mut g = self.once.lock().await;
                    g.take()
                };
                match next {
                    Some(b) => Ok(b),
                    None => std::future::pending().await,
                }
            }
        }

        let raft_msg: Message<Pending<String>> = Message {
            sender: NodeId::new("attacker", 1),
            variant: WireVariant::Raft(RaftMessage::RequestVote {
                term: 99,
                candidate: NodeId::new("attacker", 1),
                last_log_index: None,
                last_log_term: 0,
            }),
        };
        let bytes = raft_msg.to_bytes().unwrap();
        let one_shot = OneShot {
            once: Arc::new(Mutex::new(Some(bytes))),
        };

        // 2-node Paxos cluster — multi-peer mode polls the receiver, so the
        // injected Raft message will be dispatched through ProtocolImpl::handle_wire_message
        // and hit the cross-algorithm rejection arm.
        let id = NodeId::new("paxos-node", 1);
        let peer_id = NodeId::new("dummy-peer", 1);
        let (node, handle, mut decisions) = Node::<String, DummySender, OneShot>::paxos_with_id(
            id,
            crate::config::PaxosConfig::default(),
            vec![PeerInfo {
                id: peer_id,
                sender: DummySender,
            }],
            one_shot,
            PaxosMemoryStorage::new(),
        );
        let run_handle = tokio::spawn(node.run());

        // Wait for the receiver to be polled and the Raft bytes to be processed.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        // No decision should have been delivered.
        let decision =
            tokio::time::timeout(std::time::Duration::from_millis(100), decisions.recv()).await;
        assert!(decision.is_err(), "no decision should have been delivered");

        // Run task is still alive (no panic).
        assert!(!run_handle.is_finished(), "node should still be running");

        // Cluster shutdown
        drop(handle);
        let _ = tokio::time::timeout(std::time::Duration::from_secs(1), run_handle).await;
    }

    #[test]
    fn live_nonce_is_never_zero() {
        for _ in 0..10_000 {
            assert_ne!(live_nonce(), 0);
        }
    }

    #[test]
    fn node_state_is_constructible() {
        let s = NodeState {
            node_id: NodeId::new("a", 1),
            algorithm: NodeAlgorithm::Raft,
            role: Some(NodeRole::Follower),
            term: 3,
            leader: None,
            voted_for: None,
            log_len: 0,
            commit_index: None,
            last_applied: None,
        };
        assert_eq!(s.term, 3);
        assert!(matches!(s.role, Some(NodeRole::Follower)));
    }

    #[tokio::test]
    async fn node_peek_state_returns_snapshot() {
        use crate::config::RaftConfig;
        let id = NodeId::new("solo", 1);
        let (node, _handle, _rx) = Node::<String, DummySender, DummyReceiver>::raft_with_id(
            id.clone(),
            RaftConfig::default(),
            vec![],
            DummyReceiver,
            RaftMemoryStorage::new(),
        );
        let s = node.peek_state();
        assert_eq!(s.node_id, id);
        assert_eq!(s.algorithm, NodeAlgorithm::Raft);
        // Before `run()`, Raft is a follower at term 0 (paper-aligned bootstrap).
        assert_eq!(s.term, 0);
        assert!(matches!(s.role, Some(NodeRole::Follower)));
    }

    #[tokio::test]
    async fn node_handle_status_returns_initial_state() {
        use crate::config::RaftConfig;
        let id = NodeId::new("solo", 1);
        let (_node, handle, _rx) = Node::<String, DummySender, DummyReceiver>::raft_with_id(
            id.clone(),
            RaftConfig::default(),
            vec![],
            DummyReceiver,
            RaftMemoryStorage::new(),
        );
        let s = handle.status();
        assert_eq!(s.node_id, id);
        assert_eq!(s.algorithm, NodeAlgorithm::Raft);
        assert_eq!(s.term, 0);
        assert!(matches!(s.role, Some(NodeRole::Follower)));
    }
}
