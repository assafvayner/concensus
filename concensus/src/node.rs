//! Core consensus node and its associated handle types.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{de::DeserializeOwned, Serialize};
use tokio::sync::mpsc;

use crate::config::{NodeId, PeerInfo};
use crate::error::{NodeError, ProposeError};
use crate::message::Message;
#[cfg(feature = "multi-paxos")]
use crate::message::MessageVariant;
use crate::protocol::{Outgoing, ProtocolState, SendTarget};
use crate::storage::Storage;
use crate::transport::{MessageReceiver, MessageSender};

/// Channel receiver for consensus decisions.
///
/// Yields [`Decided`] values in the order they are finalized by the Paxos protocol.
/// Obtain one from [`Node::new`] or [`Node::with_id`].
pub type DecisionReceiver<V> = mpsc::Receiver<Decided<V>>;

/// A value that has reached consensus, paired with its slot number.
///
/// All nodes in a healthy cluster will produce the same `Decided` value for each
/// slot. Slots are assigned sequentially starting from 0.
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
/// 1. **Construct** via [`Node::new`] (production) or [`Node::with_id`] (testing).
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
/// let storage = MemoryStorage::<String>::new();
/// let (node, handle, mut decisions) = Node::new("my-node", peers, receiver, storage);
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
/// cluster and decides values immediately without network communication.
pub struct Node<V, S: MessageSender, R: MessageReceiver> {
    node_id: NodeId,
    peers: Vec<PeerInfo<S>>,
    receiver: Option<R>,
    storage: Box<dyn Storage<V> + Send + Sync>,
    protocol: ProtocolState<V>,
    proposal_rx: mpsc::Receiver<V>,
    decision_tx: mpsc::Sender<Decided<V>>,
    #[cfg(feature = "multi-paxos")]
    forwarded_proposals: Vec<(u64, std::time::Instant, V)>,
    #[cfg(feature = "multi-paxos")]
    next_forward_id: u64,
}

/// A cloneable handle for submitting proposals to a running [`Node`].
///
/// Obtain a `NodeHandle` from [`Node::new`] or [`Node::with_id`]. Cloning is
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
    /// Creates a new consensus node with an auto-generated [`NodeId`].
    ///
    /// The node ID is formed from the given `name` and the current UNIX timestamp
    /// as the incarnation number, ensuring uniqueness across restarts.
    ///
    /// Returns `(node, handle, decision_rx)`:
    /// - `node` — call [`Node::run`] to start the event loop
    /// - `handle` — use [`NodeHandle::propose`] to submit values
    /// - `decision_rx` — receives [`Decided`] values as consensus is reached
    pub fn new(
        name: impl Into<Arc<str>>,
        peers: Vec<PeerInfo<S>>,
        receiver: R,
        storage: impl Storage<V> + Sync + 'static,
    ) -> (Self, NodeHandle<V>, DecisionReceiver<V>) {
        let incarnation = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before UNIX epoch")
            .as_secs();
        let node_id = NodeId::new(name, incarnation);
        Self::with_id_inner(node_id, peers, receiver, storage)
    }

    /// Creates a new consensus node with an explicit [`NodeId`].
    ///
    /// This is useful in tests where you need deterministic, matching node
    /// identities across peers. In production, prefer [`Node::new`] which
    /// generates the incarnation automatically.
    ///
    /// Requires the `test-support` feature flag (always available in `#[cfg(test)]`).
    #[cfg(any(test, feature = "test-support"))]
    pub fn with_id(
        node_id: NodeId,
        peers: Vec<PeerInfo<S>>,
        receiver: R,
        storage: impl Storage<V> + Sync + 'static,
    ) -> (Self, NodeHandle<V>, DecisionReceiver<V>) {
        Self::with_id_inner(node_id, peers, receiver, storage)
    }

    fn with_id_inner(
        node_id: NodeId,
        peers: Vec<PeerInfo<S>>,
        receiver: R,
        storage: impl Storage<V> + Sync + 'static,
    ) -> (Self, NodeHandle<V>, DecisionReceiver<V>) {
        let total_nodes = peers.len() + 1;
        let protocol = ProtocolState::new(node_id.clone(), total_nodes);
        let (proposal_tx, proposal_rx) = mpsc::channel(PROPOSAL_CHANNEL_CAPACITY);
        let (decision_tx, decision_rx) = mpsc::channel(DECISION_CHANNEL_CAPACITY);

        let node = Self {
            node_id,
            peers,
            receiver: Some(receiver),
            storage: Box::new(storage),
            protocol,
            proposal_rx,
            decision_tx,
            #[cfg(feature = "multi-paxos")]
            forwarded_proposals: Vec::new(),
            #[cfg(feature = "multi-paxos")]
            next_forward_id: 0,
        };

        (node, NodeHandle { proposal_tx }, decision_rx)
    }

    /// Runs the Paxos event loop until shutdown or fatal error.
    ///
    /// This method consumes the `Node` and drives the consensus protocol:
    /// - Loads previously decided values from [`Storage`]
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

        // Load existing decisions
        let decisions = self
            .storage
            .load_decisions()
            .await
            .map_err(NodeError::Storage)?;
        self.protocol.initialize_from_decisions(decisions);

        // Load and restore acceptor state for crash recovery
        let acceptor_states = self
            .storage
            .load_acceptor_states()
            .await
            .map_err(NodeError::Storage)?;
        let mut valid_states = Vec::new();
        for state in acceptor_states {
            if let Some(reason) = state.validation_error() {
                tracing::warn!(
                    slot = state.slot,
                    reason,
                    "skipping invalid acceptor state during recovery"
                );
                continue;
            }
            valid_states.push(state);
        }
        self.protocol.initialize_from_acceptor_states(valid_states);

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
                // No peers — single node, only listen for proposals
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
                }
            }
        }
    }

    async fn persist_dirty_acceptor_slots(&mut self) -> Result<(), NodeError> {
        let dirty = self.protocol.take_dirty_acceptor_slots();
        for state in dirty {
            self.storage
                .save_acceptor_state(state.slot, state.highest_promised, state.accepted)
                .await
                .map_err(NodeError::Storage)?;
        }
        Ok(())
    }

    async fn handle_proposal(
        &mut self,
        value: V,
        senders: &[(NodeId, S)],
    ) -> Result<(), NodeError> {
        #[cfg(feature = "multi-paxos")]
        {
            if let Some(leader_id) = self.protocol.get_leader() {
                if leader_id != self.node_id {
                    tracing::debug!(leader = %leader_id, "forwarding proposal to leader");
                    let outgoing = vec![Outgoing {
                        target: SendTarget::Peer(leader_id),
                        message: MessageVariant::Forward {
                            value: value.clone(),
                        },
                    }];
                    Self::send_outgoing(&self.node_id, &outgoing, senders).await;
                    let fwd_id = self.next_forward_id;
                    self.next_forward_id += 1;
                    self.forwarded_proposals
                        .push((fwd_id, std::time::Instant::now(), value));
                    return Ok(());
                }
            }
        }

        let (slot, outgoing) = self.protocol.propose(value);
        tracing::debug!(slot, "new proposal");
        self.persist_dirty_acceptor_slots().await?;
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

    async fn handle_retries(&mut self, senders: &[(NodeId, S)]) -> Result<(), NodeError> {
        let slots = self.protocol.get_retryable_proposals();
        for slot in slots {
            tracing::debug!(slot, "retrying proposal");
            let outgoing = self.protocol.retry_proposal(slot);
            self.persist_dirty_acceptor_slots().await?;
            Self::send_outgoing(&self.node_id, &outgoing, senders).await;
        }

        // Re-broadcast recent decisions so peers that missed the original
        // Decide message can learn the outcome.
        let rebroadcasts = self.protocol.get_decision_rebroadcasts();
        if !rebroadcasts.is_empty() {
            Self::send_outgoing(&self.node_id, &rebroadcasts, senders).await;
        }

        #[cfg(feature = "multi-paxos")]
        {
            // Send heartbeat if we're the leader and haven't sent a Decide recently
            if self.protocol.should_send_heartbeat() {
                let heartbeat = self.protocol.make_heartbeat();
                Self::send_outgoing(&self.node_id, &heartbeat, senders).await;
            }

            // Check for leader timeout — start election if leader is unresponsive
            if self.protocol.check_leader_timeout() {
                let outgoing = self.protocol.start_election();
                Self::send_outgoing(&self.node_id, &outgoing, senders).await;
            }

            // Check forwarded proposal timeouts (1 second)
            let forward_timeout = std::time::Duration::from_secs(1);
            let now = std::time::Instant::now();
            let timed_out: Vec<V> = self
                .forwarded_proposals
                .iter()
                .filter(|(_, t, _)| now.duration_since(*t) >= forward_timeout)
                .map(|(_, _, v)| v.clone())
                .collect();
            self.forwarded_proposals
                .retain(|(_, t, _)| now.duration_since(*t) < forward_timeout);

            for value in timed_out {
                tracing::debug!("forwarded proposal timed out, proposing directly");
                let (_, outgoing) = self.protocol.propose(value);
                self.persist_dirty_acceptor_slots().await?;
                Self::send_outgoing(&self.node_id, &outgoing, senders).await;
                self.process_decisions().await?;
            }
        }

        Ok(())
    }

    async fn send_outgoing(node_id: &NodeId, outgoing: &[Outgoing<V>], senders: &[(NodeId, S)]) {
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
        let decisions = self.protocol.take_decisions();
        for decision in &decisions {
            self.storage
                .save_decision(decision.slot, decision.value.clone())
                .await
                .map_err(NodeError::Storage)?;

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

        // Remove at most one forwarded proposal per decided value. Using per-request
        // IDs ensures that if the same value is forwarded twice, only one entry is
        // cleared per decision — the other stays and will either get its own decision
        // or time out and fall back to direct proposal.
        #[cfg(feature = "multi-paxos")]
        {
            for decision in &decisions {
                if let Some(pos) = self
                    .forwarded_proposals
                    .iter()
                    .position(|(_, _, v)| *v == decision.value)
                {
                    self.forwarded_proposals.remove(pos);
                }
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
    use crate::storage::MemoryStorage;
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
    fn node_new_returns_node_handle_and_receiver() {
        let (_node, _handle, _decision_rx) = Node::<String, DummySender, DummyReceiver>::new(
            "test-node",
            vec![],
            DummyReceiver,
            MemoryStorage::new(),
        );
    }

    #[tokio::test]
    async fn node_handle_is_cloneable() {
        let (_node, handle, _rx) = Node::<String, DummySender, DummyReceiver>::new(
            "test-node",
            vec![],
            DummyReceiver,
            MemoryStorage::new(),
        );
        let _handle2 = handle.clone();
    }

    #[tokio::test]
    async fn propose_returns_channel_full_when_full() {
        let (_node, handle, _rx) = Node::<String, DummySender, DummyReceiver>::new(
            "test-node",
            vec![],
            DummyReceiver,
            MemoryStorage::new(),
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
        let (node, handle, mut decision_rx) = Node::<String, DummySender, DummyReceiver>::new(
            "solo",
            vec![],
            DummyReceiver,
            MemoryStorage::new(),
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

        let (node_a, handle_a, mut rx_a) = Node::with_id(
            id_a.clone(),
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
            MemoryStorage::<String>::new(),
        );
        let (node_b, _handle_b, mut rx_b) = Node::with_id(
            id_b.clone(),
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
            MemoryStorage::<String>::new(),
        );
        let (node_c, _handle_c, mut rx_c) = Node::with_id(
            id_c.clone(),
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
            MemoryStorage::<String>::new(),
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
}
