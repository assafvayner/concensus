//! Core consensus node and its associated handle types.

use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use serde::{de::DeserializeOwned, Serialize};
use tokio::sync::mpsc;

use crate::config::{NodeId, PeerInfo};
use crate::error::{NodeError, ProposeError};
use crate::message::{Message, WireVariant};
use crate::protocol::{Outgoing, PaxosProtocol, ProtocolImpl, SendTarget};
use crate::storage::{PaxosStorage, RaftStorage};
use crate::transport::{MessageReceiver, MessageSender};

/// Public mirror of the internal Raft role. Used by the test-support
/// observability hook [`Node::peek_state`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NodeRole {
    Follower,
    Candidate,
    Leader,
}

/// Identifies which consensus algorithm a [`Node`] is running. Reported by
/// [`Node::peek_state`] as part of [`NodeState`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NodeAlgorithm {
    Paxos,
    Raft,
}

/// Snapshot of a node's protocol state, for tests and observability.
///
/// Returned by [`Node::peek_state`] (test-support feature only). Fields that
/// don't apply to the active algorithm carry sentinel values: a Paxos node
/// always reports `role = None`, `term = 0`, `voted_for = None`.
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
/// Yields [`Decided`] values in the order they are finalized by the Paxos protocol.
/// Obtain one from [`Node::paxos`] / [`Node::raft`].
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

/// A Paxos consensus node.
///
/// `Node` is the central type in this crate. It runs the Multi-Paxos protocol
/// over a set of peers using pluggable transport and storage backends.
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
///
/// # Example
///
/// ```rust,no_run
/// # use concensus::*;
/// # async fn example<S: MessageSender + Sync, R: MessageReceiver>(
/// #     peers: Vec<PeerInfo<S>>, receiver: R,
/// # ) -> Result<(), Box<dyn std::error::Error>> {
/// let storage = PaxosMemoryStorage::<String>::new();
/// let (node, handle, mut decisions) = Node::paxos("my-node", PaxosConfig::default(), peers, receiver, storage);
///
/// // Run the event loop
/// tokio::spawn(async move {
///     if let Err(e) = node.run().await {
///         eprintln!("node error: {e}");
///     }
/// });
///
/// // Propose a value
/// handle.propose("hello".into()).await?;
///
/// // Wait for the decision
/// if let Some(decided) = decisions.recv().await {
///     println!("slot {}: {}", decided.slot, decided.value);
/// }
/// # Ok(())
/// # }
/// ```
///
/// # Single-Node Mode
///
/// When constructed with an empty peer list, the node acts as a single-node
/// cluster. Multi-Paxos decides values as soon as the leader proposes. Raft
/// starts in term 0; the event loop runs periodic ticks so the peer can complete
/// its one-vote election before replication commits with quorum 1.
pub struct Node<V, S: MessageSender, R: MessageReceiver> {
    node_id: NodeId,
    peers: Vec<PeerInfo<S>>,
    receiver: Option<R>,
    protocol: ProtocolImpl<V>,
    proposal_rx: mpsc::Receiver<V>,
    decision_tx: mpsc::Sender<Decided<V>>,
}

/// A cloneable handle for submitting proposals to a running [`Node`].
///
/// Obtain a `NodeHandle` from [`Node::paxos`] / [`Node::raft`]. Cloning is
/// cheap (wraps a tokio mpsc sender) and allows multiple producers to submit
/// proposals concurrently.
///
/// The node shuts down gracefully when all `NodeHandle` clones are dropped.
pub struct NodeHandle<V> {
    proposal_tx: mpsc::Sender<V>,
}

impl<V> Clone for NodeHandle<V> {
    fn clone(&self) -> Self {
        Self {
            proposal_tx: self.proposal_tx.clone(),
        }
    }
}

impl<V, S, R> Node<V, S, R>
where
    V: Serialize + DeserializeOwned + Clone + Send + PartialEq + 'static,
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
        let protocol = ProtocolImpl::Paxos(PaxosProtocol::new_with_config(
            node_id.clone(),
            total_nodes,
            config,
            Box::new(storage),
        ));
        let (proposal_tx, proposal_rx) = mpsc::channel(PROPOSAL_CHANNEL_CAPACITY);
        let (decision_tx, decision_rx) = mpsc::channel(DECISION_CHANNEL_CAPACITY);

        let node = Self {
            node_id,
            peers,
            receiver: Some(receiver),
            protocol,
            proposal_rx,
            decision_tx,
        };

        (node, NodeHandle { proposal_tx }, decision_rx)
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
    ) -> (Self, NodeHandle<V>, DecisionReceiver<V>)
    where
        V: Sync,
    {
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
    ) -> (Self, NodeHandle<V>, DecisionReceiver<V>)
    where
        V: Sync,
    {
        Self::raft_with_id_inner(node_id, config, peers, receiver, storage)
    }

    fn raft_with_id_inner(
        node_id: NodeId,
        config: crate::config::RaftConfig,
        peers: Vec<PeerInfo<S>>,
        receiver: R,
        storage: impl RaftStorage<V> + 'static,
    ) -> (Self, NodeHandle<V>, DecisionReceiver<V>)
    where
        V: Sync,
    {
        let total_nodes = peers.len() + 1;
        let protocol = ProtocolImpl::Raft(crate::protocol::RaftProtocol::new(
            node_id.clone(),
            total_nodes,
            config,
            Box::new(storage),
        ));
        let (proposal_tx, proposal_rx) = mpsc::channel(PROPOSAL_CHANNEL_CAPACITY);
        let (decision_tx, decision_rx) = mpsc::channel(DECISION_CHANNEL_CAPACITY);
        let node = Self {
            node_id,
            peers,
            receiver: Some(receiver),
            protocol,
            proposal_rx,
            decision_tx,
        };
        (node, NodeHandle { proposal_tx }, decision_rx)
    }

    /// Test-only snapshot of the node's current protocol state.
    ///
    /// Useful for integration tests that need to observe per-node state
    /// (current term, role, log length, commit index) without coupling to
    /// internal types. Returns sentinel values for fields that don't apply
    /// to the active algorithm (Paxos always reports `role = None`).
    #[cfg(any(test, feature = "test-support"))]
    pub fn peek_state(&self) -> NodeState {
        let snap = self.protocol.peek_state();
        let algorithm = match &self.protocol {
            crate::protocol::ProtocolImpl::Paxos(_) => NodeAlgorithm::Paxos,
            crate::protocol::ProtocolImpl::Raft(_) => NodeAlgorithm::Raft,
        };
        NodeState {
            node_id: self.node_id.clone(),
            algorithm,
            role: snap.role,
            term: snap.term,
            leader: snap.leader,
            voted_for: snap.voted_for,
            log_len: snap.log_len,
            commit_index: snap.commit_index,
            last_applied: snap.last_applied,
        }
    }

    /// Runs the Paxos event loop until shutdown or fatal error.
    ///
    /// This method consumes the `Node` and drives the consensus protocol:
    /// - Loads previously decided values from storage
    /// - Listens for proposals via the internal channel (from [`NodeHandle`])
    /// - Processes incoming Paxos messages from peers
    /// - Periodically retries stalled proposals with exponential backoff
    /// - Re-broadcasts recent decisions so late peers can catch up
    ///
    /// # Shutdown
    ///
    /// The event loop exits when all [`NodeHandle`] clones are dropped (the
    /// proposal channel closes). It returns `Ok(())` in this case.
    ///
    /// # Errors
    ///
    /// - [`NodeError::NoQuorum`] — the transport receiver closed while the node
    ///   had outstanding proposals and needs a quorum to make progress.
    /// - [`NodeError::Storage`] — a storage operation failed.
    pub async fn run(mut self) -> Result<(), NodeError> {
        tracing::info!(node_id = %self.node_id, "node starting");

        self.protocol.recover().await.map_err(NodeError::Storage)?;

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

        loop {
            if has_peers {
                tokio::select! {
                    proposal = self.proposal_rx.recv() => {
                        match proposal {
                            Some(value) => self.handle_proposal(value, &senders).await?,
                            None => {
                                tracing::info!("all proposal handles dropped, shutting down");
                                return Ok(());
                            }
                        }
                    }
                    result = receiver.recv() => {
                        match result {
                            Ok(data) => {
                                self.handle_incoming_bytes(&data, &senders).await?;
                            }
                            Err(e) => {
                                tracing::warn!(error = %e, "receiver error");
                                if 1 < quorum && !self.protocol.is_idle() {
                                    return Err(NodeError::NoQuorum);
                                }
                            }
                        }
                    }
                    _ = retry_interval.tick() => {
                        self.handle_retries(&senders).await?;
                    }
                }
            } else {
                // No peers — single-node cluster. Still run `on_tick` so Raft can
                // complete an initial election and send leader heartbeats; Paxos
                // uses ticks for its leader-side timers as well.
                tokio::select! {
                    proposal = self.proposal_rx.recv() => {
                        match proposal {
                            Some(value) => self.handle_proposal(value, &senders).await?,
                            None => {
                                tracing::info!("all proposal handles dropped, shutting down");
                                return Ok(());
                            }
                        }
                    }
                    _ = retry_interval.tick() => {
                        self.handle_retries(&senders).await?;
                    }
                }
            }
        }
    }

    async fn handle_proposal(
        &mut self,
        value: V,
        senders: &[(NodeId, S)],
    ) -> Result<(), NodeError> {
        let outgoing = self.protocol.propose(value);
        self.protocol
            .flush_persist()
            .await
            .map_err(NodeError::Storage)?;
        Self::send_outgoing(&self.node_id, &outgoing, senders).await;
        self.process_decisions().await?;
        Ok(())
    }

    async fn handle_incoming_bytes(
        &mut self,
        data: &[u8],
        senders: &[(NodeId, S)],
    ) -> Result<(), NodeError> {
        match Message::<V>::from_bytes(data) {
            Ok(msg) => {
                let from = msg.sender;
                let outgoing = self.protocol.handle_wire_message(from, msg.variant);
                self.protocol
                    .flush_persist()
                    .await
                    .map_err(NodeError::Storage)?;
                Self::send_outgoing(&self.node_id, &outgoing, senders).await;
                self.process_decisions().await?;

                // Re-propose any lost proposals
                let lost = self.protocol.take_lost_proposals();
                for value in lost {
                    let outgoing = self.protocol.propose(value);
                    self.protocol
                        .flush_persist()
                        .await
                        .map_err(NodeError::Storage)?;
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

    async fn handle_retries(&mut self, senders: &[(NodeId, S)]) -> Result<(), NodeError> {
        let outgoing = self.protocol.on_tick(Instant::now());
        self.protocol
            .flush_persist()
            .await
            .map_err(NodeError::Storage)?;
        Self::send_outgoing(&self.node_id, &outgoing, senders).await;
        self.process_decisions().await?;
        Ok(())
    }

    async fn send_outgoing(
        node_id: &NodeId,
        outgoing: &[Outgoing<WireVariant<V>>],
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

    async fn process_decisions(&mut self) -> Result<(), NodeError> {
        let decisions = self
            .protocol
            .drain_decisions()
            .await
            .map_err(NodeError::Storage)?;
        for decision in &decisions {
            tracing::info!(slot = decision.slot, "value decided");

            if self
                .decision_tx
                .send(Decided {
                    slot: decision.slot,
                    value: decision.value.clone(),
                })
                .await
                .is_err()
            {
                tracing::warn!("decision receiver dropped, decisions will not be delivered");
            }
        }

        Ok(())
    }
}

impl<V> NodeHandle<V>
where
    V: Serialize + DeserializeOwned + Clone + Send + 'static,
{
    /// Submit a value for consensus.
    ///
    /// The value is enqueued for the node's event loop to process. It will
    /// eventually be assigned a slot and decided by the cluster, or
    /// re-proposed if another value wins the slot.
    ///
    /// This method uses non-blocking `try_send` semantics internally, so it
    /// returns immediately.
    ///
    /// # Errors
    ///
    /// - [`ProposeError::ChannelFull`] — the internal proposal queue (capacity
    ///   1024) is full. Back off and retry.
    /// - [`ProposeError::NotRunning`] — the node's event loop has stopped.
    pub async fn propose(&self, value: V) -> Result<(), ProposeError> {
        self.proposal_tx.try_send(value).map_err(|e| match e {
            mpsc::error::TrySendError::Full(_) => ProposeError::ChannelFull,
            mpsc::error::TrySendError::Closed(_) => ProposeError::NotRunning,
        })
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
        let (_node, handle, _rx) = Node::<String, DummySender, DummyReceiver>::paxos(
            "test-node",
            PaxosConfig::default(),
            vec![],
            DummyReceiver,
            PaxosMemoryStorage::new(),
        );
        // Fill the channel (capacity 1024)
        for i in 0..1024 {
            handle.propose(format!("msg-{}", i)).await.unwrap();
        }
        // Next one should fail
        let result = handle.propose("overflow".to_string()).await;
        assert!(matches!(result, Err(ProposeError::ChannelFull)));
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

        handle.propose("hello".to_string()).await.unwrap();

        let decided = tokio::time::timeout(std::time::Duration::from_secs(1), decision_rx.recv())
            .await
            .expect("timed out")
            .expect("channel closed");

        assert_eq!(decided.slot, 0);
        assert_eq!(decided.value, "hello");

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

        let raft_msg: Message<String> = Message {
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
}
