pub mod transport_filters;

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use concensus::{
    channel, unbounded_channel, AcceptorState, ChannelReceiver, ChannelSender, Decided,
    DecisionReceiver, MemoryStorage, Node, NodeHandle, NodeId, PeerInfo, ProposalNumber, Storage,
    StorageError,
};
use serde::{de::DeserializeOwned, Serialize};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time::{timeout, Duration};
use transport_filters::{
    DelayedSender, LossyReceiver, LossySender, ReorderingReceiver, ReorderingSender,
};

// ---------------------------------------------------------------------------
// Cluster node types
// ---------------------------------------------------------------------------

pub struct ClusterNode {
    pub handle: NodeHandle<String>,
    pub decisions: DecisionReceiver<String>,
    pub run_handle: JoinHandle<Result<(), concensus::NodeError>>,
    pub id: NodeId,
}

pub struct ClusterNodeTyped<V> {
    pub handle: NodeHandle<V>,
    pub decisions: DecisionReceiver<V>,
    pub run_handle: JoinHandle<Result<(), concensus::NodeError>>,
    pub id: NodeId,
}

// ---------------------------------------------------------------------------
// Shared storage for recovery tests
// ---------------------------------------------------------------------------

pub struct SharedMemoryStorage<V> {
    inner: Arc<Mutex<MemoryStorage<V>>>,
}

impl<V> SharedMemoryStorage<V> {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(MemoryStorage::new())),
        }
    }
}

impl<V> Clone for SharedMemoryStorage<V> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

#[async_trait]
impl<V> Storage<V> for SharedMemoryStorage<V>
where
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    async fn save_decision(&mut self, slot: u64, value: V) -> Result<(), StorageError> {
        self.inner.lock().await.save_decision(slot, value).await
    }

    async fn load_decisions(&self) -> Result<Vec<(u64, V)>, StorageError> {
        self.inner.lock().await.load_decisions().await
    }

    async fn save_acceptor_state(
        &mut self,
        slot: u64,
        highest_promised: Option<concensus::ProposalNumber>,
        accepted: Option<(concensus::ProposalNumber, V)>,
    ) -> Result<(), StorageError> {
        self.inner
            .lock()
            .await
            .save_acceptor_state(slot, highest_promised, accepted)
            .await
    }

    async fn load_acceptor_states(&self) -> Result<Vec<AcceptorState<V>>, StorageError> {
        self.inner.lock().await.load_acceptor_states().await
    }

    async fn delete_acceptor_state(&mut self, slot: u64) -> Result<(), StorageError> {
        self.inner.lock().await.delete_acceptor_state(slot).await
    }
}

// ---------------------------------------------------------------------------
// Basic cluster creation
// ---------------------------------------------------------------------------

pub fn create_cluster(n: usize) -> Vec<ClusterNode> {
    create_cluster_inner(n, false, 0)
}

pub fn create_unbounded_cluster(n: usize) -> Vec<ClusterNode> {
    create_cluster_inner(n, true, 0)
}

pub fn create_cluster_with_dead_node(n: usize) -> Vec<ClusterNode> {
    create_cluster_inner(n, false, 1)
}

pub fn create_cluster_with_dead_nodes(n: usize, dead_count: usize) -> Vec<ClusterNode> {
    assert!(dead_count < n, "dead_count must be less than n");
    create_cluster_inner(n, false, dead_count)
}

fn create_cluster_inner(n: usize, unbounded: bool, dead_count: usize) -> Vec<ClusterNode> {
    assert!(n > 0);

    let ids: Vec<NodeId> = (0..n)
        .map(|i| NodeId::new(format!("node-{}", i), 1000))
        .collect();

    let mut senders: Vec<ChannelSender> = Vec::new();
    let mut receivers: Vec<ChannelReceiver> = Vec::new();
    for _ in 0..n {
        let (tx, rx) = if unbounded {
            unbounded_channel()
        } else {
            channel(64)
        };
        senders.push(tx);
        receivers.push(rx);
    }

    let mut cluster_nodes = Vec::new();
    for i in 0..n {
        let is_dead = i >= n - dead_count;

        let peers: Vec<PeerInfo<ChannelSender>> = (0..n)
            .filter(|&j| j != i)
            .map(|j| PeerInfo {
                id: ids[j].clone(),
                sender: senders[j].clone(),
            })
            .collect();

        let receiver = receivers.remove(0);
        let storage = MemoryStorage::<String>::new();

        let (node, handle, decisions) = Node::with_id(ids[i].clone(), peers, receiver, storage);

        let run_handle = if is_dead {
            tokio::spawn(async move {
                drop(node);
                futures_never().await
            })
        } else {
            tokio::spawn(node.run())
        };

        cluster_nodes.push(ClusterNode {
            handle,
            decisions,
            run_handle,
            id: ids[i].clone(),
        });
    }

    cluster_nodes
}

// ---------------------------------------------------------------------------
// Generic cluster creation (for non-String value types)
// ---------------------------------------------------------------------------

pub fn create_cluster_typed<V>(n: usize) -> Vec<ClusterNodeTyped<V>>
where
    V: Serialize + DeserializeOwned + Clone + Send + Sync + PartialEq + 'static,
{
    assert!(n > 0);

    let ids: Vec<NodeId> = (0..n)
        .map(|i| NodeId::new(format!("node-{}", i), 1000))
        .collect();

    let mut senders: Vec<ChannelSender> = Vec::new();
    let mut receivers: Vec<ChannelReceiver> = Vec::new();
    for _ in 0..n {
        let (tx, rx) = channel(64);
        senders.push(tx);
        receivers.push(rx);
    }

    let mut cluster_nodes = Vec::new();
    for i in 0..n {
        let peers: Vec<PeerInfo<ChannelSender>> = (0..n)
            .filter(|&j| j != i)
            .map(|j| PeerInfo {
                id: ids[j].clone(),
                sender: senders[j].clone(),
            })
            .collect();

        let receiver = receivers.remove(0);
        let storage = MemoryStorage::<V>::new();

        let (node, handle, decisions) = Node::with_id(ids[i].clone(), peers, receiver, storage);

        let run_handle = tokio::spawn(node.run());

        cluster_nodes.push(ClusterNodeTyped {
            handle,
            decisions,
            run_handle,
            id: ids[i].clone(),
        });
    }

    cluster_nodes
}

// ---------------------------------------------------------------------------
// Lossy cluster creation
// ---------------------------------------------------------------------------

pub fn create_lossy_cluster(n: usize, drop_rate: f64) -> Vec<ClusterNode> {
    assert!(n > 0);

    let ids: Vec<NodeId> = (0..n)
        .map(|i| NodeId::new(format!("node-{}", i), 1000))
        .collect();

    let mut senders: Vec<LossySender<ChannelSender>> = Vec::new();
    let mut receivers: Vec<ChannelReceiver> = Vec::new();
    for _ in 0..n {
        let (tx, rx) = channel(64);
        senders.push(LossySender::with_drop_rate(tx, drop_rate));
        receivers.push(rx);
    }

    let mut cluster_nodes = Vec::new();
    for i in 0..n {
        let peers: Vec<PeerInfo<LossySender<ChannelSender>>> = (0..n)
            .filter(|&j| j != i)
            .map(|j| PeerInfo {
                id: ids[j].clone(),
                sender: senders[j].clone(),
            })
            .collect();

        let receiver = receivers.remove(0);
        let storage = MemoryStorage::<String>::new();

        let (node, handle, decisions) = Node::with_id(ids[i].clone(), peers, receiver, storage);

        let run_handle = tokio::spawn(node.run());

        cluster_nodes.push(ClusterNode {
            handle,
            decisions,
            run_handle,
            id: ids[i].clone(),
        });
    }

    cluster_nodes
}

// ---------------------------------------------------------------------------
// Lossy cluster with unbounded channels (avoids backpressure deadlocks)
// ---------------------------------------------------------------------------

pub fn create_lossy_unbounded_cluster(n: usize, drop_rate: f64) -> Vec<ClusterNode> {
    assert!(n > 0);

    let ids: Vec<NodeId> = (0..n)
        .map(|i| NodeId::new(format!("node-{}", i), 1000))
        .collect();

    let mut senders: Vec<LossySender<ChannelSender>> = Vec::new();
    let mut receivers: Vec<ChannelReceiver> = Vec::new();
    for _ in 0..n {
        let (tx, rx) = unbounded_channel();
        senders.push(LossySender::with_drop_rate(tx, drop_rate));
        receivers.push(rx);
    }

    let mut cluster_nodes = Vec::new();
    for i in 0..n {
        let peers: Vec<PeerInfo<LossySender<ChannelSender>>> = (0..n)
            .filter(|&j| j != i)
            .map(|j| PeerInfo {
                id: ids[j].clone(),
                sender: senders[j].clone(),
            })
            .collect();

        let receiver = receivers.remove(0);
        let storage = MemoryStorage::<String>::new();

        let (node, handle, decisions) = Node::with_id(ids[i].clone(), peers, receiver, storage);

        let run_handle = tokio::spawn(node.run());

        cluster_nodes.push(ClusterNode {
            handle,
            decisions,
            run_handle,
            id: ids[i].clone(),
        });
    }

    cluster_nodes
}

// ---------------------------------------------------------------------------
// Delayed cluster creation
// ---------------------------------------------------------------------------

pub fn create_delayed_cluster(n: usize, min_ms: u64, max_ms: u64) -> Vec<ClusterNode> {
    assert!(n > 0);

    let ids: Vec<NodeId> = (0..n)
        .map(|i| NodeId::new(format!("node-{}", i), 1000))
        .collect();

    // Use unbounded channels to avoid backpressure deadlocks: the DelayedSender
    // blocks the node event loop during the sleep, and a bounded channel can
    // cause the sender to also block waiting for capacity, creating a deadlock.
    let mut senders: Vec<DelayedSender<ChannelSender>> = Vec::new();
    let mut receivers: Vec<ChannelReceiver> = Vec::new();
    for _ in 0..n {
        let (tx, rx) = unbounded_channel();
        senders.push(DelayedSender::with_range(
            tx,
            Duration::from_millis(min_ms),
            Duration::from_millis(max_ms),
        ));
        receivers.push(rx);
    }

    let mut cluster_nodes = Vec::new();
    for i in 0..n {
        let peers: Vec<PeerInfo<DelayedSender<ChannelSender>>> = (0..n)
            .filter(|&j| j != i)
            .map(|j| PeerInfo {
                id: ids[j].clone(),
                sender: senders[j].clone(),
            })
            .collect();

        let receiver = receivers.remove(0);
        let storage = MemoryStorage::<String>::new();

        let (node, handle, decisions) = Node::with_id(ids[i].clone(), peers, receiver, storage);

        let run_handle = tokio::spawn(node.run());

        cluster_nodes.push(ClusterNode {
            handle,
            decisions,
            run_handle,
            id: ids[i].clone(),
        });
    }

    cluster_nodes
}

// ---------------------------------------------------------------------------
// Reordering cluster creation
// ---------------------------------------------------------------------------

pub fn create_reordering_cluster(n: usize, window_ms: u64, batch_size: usize) -> Vec<ClusterNode> {
    assert!(n > 0);

    let ids: Vec<NodeId> = (0..n)
        .map(|i| NodeId::new(format!("node-{}", i), 1000))
        .collect();

    let mut senders: Vec<ReorderingSender<ChannelSender>> = Vec::new();
    let mut receivers: Vec<ReorderingReceiver<ChannelReceiver>> = Vec::new();
    for _ in 0..n {
        let (tx, rx) = channel(64);
        senders.push(ReorderingSender::new(tx));
        receivers.push(ReorderingReceiver::with_params(
            rx,
            Duration::from_millis(window_ms),
            batch_size,
        ));
    }

    let mut cluster_nodes = Vec::new();
    for i in 0..n {
        let peers: Vec<PeerInfo<ReorderingSender<ChannelSender>>> = (0..n)
            .filter(|&j| j != i)
            .map(|j| PeerInfo {
                id: ids[j].clone(),
                sender: senders[j].clone(),
            })
            .collect();

        let receiver = receivers.remove(0);
        let storage = MemoryStorage::<String>::new();

        let (node, handle, decisions) = Node::with_id(ids[i].clone(), peers, receiver, storage);

        let run_handle = tokio::spawn(node.run());

        cluster_nodes.push(ClusterNode {
            handle,
            decisions,
            run_handle,
            id: ids[i].clone(),
        });
    }

    cluster_nodes
}

// ---------------------------------------------------------------------------
// Combined lossy + delayed cluster creation
// ---------------------------------------------------------------------------

pub fn create_lossy_delayed_cluster(
    n: usize,
    drop_rate: f64,
    min_delay_ms: u64,
    max_delay_ms: u64,
) -> Vec<ClusterNode> {
    assert!(n > 0);

    let ids: Vec<NodeId> = (0..n)
        .map(|i| NodeId::new(format!("node-{}", i), 1000))
        .collect();

    // Wrap: DelayedSender<LossySender<ChannelSender>>
    // Loss check happens first (inner), then delay (outer).
    // Use unbounded channels to avoid backpressure deadlocks with delayed sends.
    let mut senders: Vec<DelayedSender<LossySender<ChannelSender>>> = Vec::new();
    let mut receivers: Vec<ChannelReceiver> = Vec::new();
    for _ in 0..n {
        let (tx, rx) = unbounded_channel();
        let lossy = LossySender::with_drop_rate(tx, drop_rate);
        senders.push(DelayedSender::with_range(
            lossy,
            Duration::from_millis(min_delay_ms),
            Duration::from_millis(max_delay_ms),
        ));
        receivers.push(rx);
    }

    let mut cluster_nodes = Vec::new();
    for i in 0..n {
        let peers: Vec<PeerInfo<DelayedSender<LossySender<ChannelSender>>>> = (0..n)
            .filter(|&j| j != i)
            .map(|j| PeerInfo {
                id: ids[j].clone(),
                sender: senders[j].clone(),
            })
            .collect();

        let receiver = receivers.remove(0);
        let storage = MemoryStorage::<String>::new();

        let (node, handle, decisions) = Node::with_id(ids[i].clone(), peers, receiver, storage);

        let run_handle = tokio::spawn(node.run());

        cluster_nodes.push(ClusterNode {
            handle,
            decisions,
            run_handle,
            id: ids[i].clone(),
        });
    }

    cluster_nodes
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async fn futures_never() -> Result<(), concensus::NodeError> {
    loop {
        tokio::time::sleep(Duration::from_secs(3600)).await;
    }
}

pub async fn collect_decisions(
    rx: &mut DecisionReceiver<String>,
    count: usize,
) -> Vec<Decided<String>> {
    collect_decisions_with_timeout(rx, count, Duration::from_secs(5)).await
}

pub async fn collect_decisions_with_timeout(
    rx: &mut DecisionReceiver<String>,
    count: usize,
    per_decision_timeout: Duration,
) -> Vec<Decided<String>> {
    let mut results = Vec::new();
    for _ in 0..count {
        let decided = timeout(per_decision_timeout, rx.recv())
            .await
            .expect("timed out waiting for decision")
            .expect("decision channel closed");
        results.push(decided);
    }
    results
}

// ---------------------------------------------------------------------------
// Assertions
// ---------------------------------------------------------------------------

/// Assert all nodes agree on the same slot-to-value mapping.
pub fn assert_consistent_decisions(all_decisions: &[Vec<Decided<String>>]) {
    if all_decisions.is_empty() {
        return;
    }

    let reference: HashMap<u64, String> = all_decisions[0]
        .iter()
        .map(|d| (d.slot, d.value.clone()))
        .collect();

    for (i, decisions) in all_decisions.iter().enumerate() {
        let node_map: HashMap<u64, String> = decisions
            .iter()
            .map(|d| (d.slot, d.value.clone()))
            .collect();

        assert_eq!(
            decisions.len(),
            node_map.len(),
            "node {} has duplicate slots",
            i
        );

        for (slot, value) in &node_map {
            if let Some(ref_value) = reference.get(slot) {
                assert_eq!(
                    value, ref_value,
                    "node {} disagrees on slot {}: got {}, expected {}",
                    i, slot, value, ref_value
                );
            }
        }
    }
}

/// The core Paxos safety invariant: for any slot, all nodes that decided
/// that slot must have decided the SAME value. Unlike assert_consistent_decisions
/// which requires all nodes to have the same number of decisions, this check
/// tolerates partial sets — it only checks slots that multiple nodes decided.
pub fn assert_safety_invariant(all_decisions: &[Vec<Decided<String>>]) {
    let mut slot_values: HashMap<u64, String> = HashMap::new();
    for (node_idx, decisions) in all_decisions.iter().enumerate() {
        for d in decisions {
            if let Some(existing) = slot_values.get(&d.slot) {
                assert_eq!(
                    &d.value, existing,
                    "SAFETY VIOLATION: node {} decided slot {} = {:?}, \
                     but another node decided slot {} = {:?}",
                    node_idx, d.slot, d.value, d.slot, existing
                );
            } else {
                slot_values.insert(d.slot, d.value.clone());
            }
        }
    }
}
