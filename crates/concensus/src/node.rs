use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use serde::{de::DeserializeOwned, Serialize};
use tokio::sync::mpsc;

use crate::config::{NodeId, PeerConfig};
use crate::error::{NodeError, ProposeError, TransportError};
use crate::message::Message;
use crate::protocol::{Outgoing, ProtocolState, SendTarget};
use crate::storage::Storage;
use crate::transport::{MessageReceiver, MessageSender};

pub type DecisionReceiver<V> = mpsc::Receiver<Decided<V>>;

#[derive(Clone, Debug)]
pub struct Decided<V> {
    pub slot: u64,
    pub value: V,
}

const PROPOSAL_CHANNEL_CAPACITY: usize = 1024;
const DECISION_CHANNEL_CAPACITY: usize = 1024;

pub struct Node<V, S: MessageSender, R: MessageReceiver> {
    node_id: NodeId,
    peers: Vec<PeerConfig<S, R>>,
    storage: Box<dyn Storage<V> + Send>,
    protocol: ProtocolState<V>,
    proposal_rx: mpsc::Receiver<V>,
    decision_tx: mpsc::Sender<Decided<V>>,
}

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
    V: Serialize + DeserializeOwned + Clone + Send + 'static,
    S: MessageSender,
    R: MessageReceiver,
{
    pub fn new(
        name: impl Into<Arc<str>>,
        peers: Vec<PeerConfig<S, R>>,
        storage: impl Storage<V> + Send + 'static,
    ) -> (Self, NodeHandle<V>, DecisionReceiver<V>) {
        let incarnation = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before UNIX epoch")
            .as_secs();
        let node_id = NodeId::new(name, incarnation);
        Self::with_id_inner(node_id, peers, storage)
    }

    #[cfg(test)]
    pub fn with_id(
        node_id: NodeId,
        peers: Vec<PeerConfig<S, R>>,
        storage: impl Storage<V> + Send + 'static,
    ) -> (Self, NodeHandle<V>, DecisionReceiver<V>) {
        Self::with_id_inner(node_id, peers, storage)
    }

    fn with_id_inner(
        node_id: NodeId,
        peers: Vec<PeerConfig<S, R>>,
        storage: impl Storage<V> + Send + 'static,
    ) -> (Self, NodeHandle<V>, DecisionReceiver<V>) {
        let total_nodes = peers.len() + 1;
        let protocol = ProtocolState::new(node_id.clone(), total_nodes);
        let (proposal_tx, proposal_rx) = mpsc::channel(PROPOSAL_CHANNEL_CAPACITY);
        let (decision_tx, decision_rx) = mpsc::channel(DECISION_CHANNEL_CAPACITY);

        let node = Self {
            node_id,
            peers,
            storage: Box::new(storage),
            protocol,
            proposal_rx,
            decision_tx,
        };

        (node, NodeHandle { proposal_tx }, decision_rx)
    }

    pub async fn run(mut self) -> Result<(), NodeError> {
        tracing::info!(node_id = %self.node_id, "node starting");

        // Load existing decisions
        let decisions = self.storage.load_decisions().await.map_err(NodeError::Storage)?;
        self.protocol.initialize_from_decisions(decisions);

        // Split peers into senders and receivers
        let mut senders: Vec<(NodeId, S)> = Vec::new();
        let (incoming_tx, mut incoming_rx) =
            mpsc::channel::<(NodeId, Result<Bytes, TransportError>)>(1024);

        let peers = std::mem::take(&mut self.peers);
        let has_peers = !peers.is_empty();

        for peer in peers {
            senders.push((peer.id.clone(), peer.sender));
            let peer_id = peer.id;
            let tx = incoming_tx.clone();
            let mut receiver = peer.receiver;
            tokio::spawn(async move {
                loop {
                    let result = receiver.recv().await;
                    let is_err = result.is_err();
                    if tx.send((peer_id.clone(), result)).await.is_err() {
                        break; // Node dropped
                    }
                    if is_err {
                        break; // Peer disconnected
                    }
                }
            });
        }
        drop(incoming_tx); // Only spawned tasks hold senders now

        let mut active_peers = senders.len();
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
                    incoming = incoming_rx.recv() => {
                        match incoming {
                            Some((from, Ok(data))) => {
                                self.handle_incoming_message(from, &data, &senders).await?;
                            }
                            Some((from, Err(e))) => {
                                tracing::warn!(peer = %from, error = %e, "peer disconnected");
                                active_peers -= 1;
                                if active_peers + 1 < quorum && !self.protocol.is_idle() {
                                    return Err(NodeError::NoQuorum);
                                }
                            }
                            None => {
                                // All receiver tasks exited
                                if 1 < quorum && !self.protocol.is_idle() {
                                    return Err(NodeError::NoQuorum);
                                }
                            }
                        }
                    }
                    _ = retry_interval.tick() => {
                        self.handle_retries(&senders).await;
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

    async fn handle_proposal(&mut self, value: V, senders: &[(NodeId, S)]) -> Result<(), NodeError> {
        let (slot, outgoing) = self.protocol.propose(value);
        tracing::debug!(slot, "new proposal");
        Self::send_outgoing(&outgoing, senders).await;
        self.process_decisions().await?;
        Ok(())
    }

    async fn handle_incoming_message(
        &mut self,
        from: NodeId,
        data: &[u8],
        senders: &[(NodeId, S)],
    ) -> Result<(), NodeError> {
        match Message::<V>::from_bytes(data) {
            Ok(msg) => {
                let outgoing = self.protocol.handle_message(from, msg);
                Self::send_outgoing(&outgoing, senders).await;
                self.process_decisions().await?;

                // Re-propose any lost proposals
                let lost = self.protocol.take_lost_proposals();
                for value in lost {
                    let (_, outgoing) = self.protocol.propose(value);
                    Self::send_outgoing(&outgoing, senders).await;
                    self.process_decisions().await?;
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "failed to deserialize message");
            }
        }
        Ok(())
    }

    async fn handle_retries(&mut self, senders: &[(NodeId, S)]) {
        let slots = self.protocol.get_retryable_proposals();
        for slot in slots {
            tracing::debug!(slot, "retrying proposal");
            let outgoing = self.protocol.retry_proposal(slot);
            Self::send_outgoing(&outgoing, senders).await;
        }
    }

    async fn send_outgoing(outgoing: &[Outgoing<V>], senders: &[(NodeId, S)]) {
        for out in outgoing {
            let bytes = match out.message.to_bytes() {
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
        for decision in decisions {
            self.storage
                .save_decision(decision.slot, decision.value.clone())
                .await
                .map_err(NodeError::Storage)?;

            tracing::info!(slot = decision.slot, "value decided");

            if self
                .decision_tx
                .send(Decided {
                    slot: decision.slot,
                    value: decision.value,
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
    pub async fn propose(&self, value: V) -> Result<(), ProposeError> {
        self.proposal_tx
            .try_send(value)
            .map_err(|e| match e {
                mpsc::error::TrySendError::Full(_) => ProposeError::ChannelFull,
                mpsc::error::TrySendError::Closed(_) => ProposeError::NotRunning,
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::PeerConfig;
    use crate::error::TransportError;
    use crate::storage::MemoryStorage;

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
            MemoryStorage::new(),
        );
    }

    #[tokio::test]
    async fn node_handle_is_cloneable() {
        let (_node, handle, _rx) = Node::<String, DummySender, DummyReceiver>::new(
            "test-node",
            vec![],
            MemoryStorage::new(),
        );
        let _handle2 = handle.clone();
    }

    #[tokio::test]
    async fn propose_returns_channel_full_when_full() {
        let (_node, handle, _rx) = Node::<String, DummySender, DummyReceiver>::new(
            "test-node",
            vec![],
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
            MemoryStorage::new(),
        );

        let run_handle = tokio::spawn(node.run());

        handle.propose("hello".to_string()).await.unwrap();

        let decided = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            decision_rx.recv(),
        )
        .await
        .expect("timed out")
        .expect("channel closed");

        assert_eq!(decided.slot, 0);
        assert_eq!(decided.value, "hello");

        drop(handle);
        let _ = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            run_handle,
        )
        .await;
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

        // Bidirectional channels: A<->B, A<->C, B<->C
        let (ab_tx, ab_rx) = tokio_mpsc::channel(64);
        let (ba_tx, ba_rx) = tokio_mpsc::channel(64);
        let (ac_tx, ac_rx) = tokio_mpsc::channel(64);
        let (ca_tx, ca_rx) = tokio_mpsc::channel(64);
        let (bc_tx, bc_rx) = tokio_mpsc::channel(64);
        let (cb_tx, cb_rx) = tokio_mpsc::channel(64);

        let id_a = NodeId::new("a", 1000);
        let id_b = NodeId::new("b", 1000);
        let id_c = NodeId::new("c", 1000);

        let (node_a, handle_a, mut rx_a) = Node::with_id(
            id_a.clone(),
            vec![
                PeerConfig {
                    id: id_b.clone(),
                    sender: ChannelSender(ab_tx),
                    receiver: ChannelReceiver(ba_rx),
                },
                PeerConfig {
                    id: id_c.clone(),
                    sender: ChannelSender(ac_tx),
                    receiver: ChannelReceiver(ca_rx),
                },
            ],
            MemoryStorage::<String>::new(),
        );
        let (node_b, _handle_b, mut rx_b) = Node::with_id(
            id_b.clone(),
            vec![
                PeerConfig {
                    id: id_a.clone(),
                    sender: ChannelSender(ba_tx),
                    receiver: ChannelReceiver(ab_rx),
                },
                PeerConfig {
                    id: id_c.clone(),
                    sender: ChannelSender(bc_tx),
                    receiver: ChannelReceiver(cb_rx),
                },
            ],
            MemoryStorage::<String>::new(),
        );
        let (node_c, _handle_c, mut rx_c) = Node::with_id(
            id_c.clone(),
            vec![
                PeerConfig {
                    id: id_a.clone(),
                    sender: ChannelSender(ca_tx),
                    receiver: ChannelReceiver(ac_rx),
                },
                PeerConfig {
                    id: id_b.clone(),
                    sender: ChannelSender(cb_tx),
                    receiver: ChannelReceiver(bc_rx),
                },
            ],
            MemoryStorage::<String>::new(),
        );

        tokio::spawn(node_a.run());
        tokio::spawn(node_b.run());
        tokio::spawn(node_c.run());

        // Propose from node A
        handle_a.propose("hello".to_string()).await.unwrap();

        let timeout = std::time::Duration::from_secs(5);
        let da = tokio::time::timeout(timeout, rx_a.recv()).await.unwrap().unwrap();
        let db = tokio::time::timeout(timeout, rx_b.recv()).await.unwrap().unwrap();
        let dc = tokio::time::timeout(timeout, rx_c.recv()).await.unwrap().unwrap();

        assert_eq!(da.value, "hello");
        assert_eq!(db.value, "hello");
        assert_eq!(dc.value, "hello");
        assert_eq!(da.slot, db.slot);
        assert_eq!(db.slot, dc.slot);
    }
}
