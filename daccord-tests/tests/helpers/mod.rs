// Test helpers are shared across many integration-test binaries. Each binary
// pulls in this module independently and only uses the subset it needs, which
// means cargo emits dead_code/unused_imports warnings for the rest. Allow them
// at the module level so the workspace stays clippy-clean.
#![allow(dead_code, unused_imports)]

pub mod raft_invariants;
pub mod transport_filters;

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use daccord::{
    channel, unbounded_channel, ChannelReceiver, ChannelSender, Decided, DecisionReceiver,
    LogEntry, Node, NodeHandle, NodeId, PaxosConfig, PaxosMemoryStorage, PaxosStorage, PeerInfo,
    RaftMemoryStorage, RaftStorage, StorageError,
};
use serde::{de::DeserializeOwned, Serialize};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time::{timeout, Duration};
use transport_filters::{
    DelayedSender, LossyReceiver, LossySender, ReorderingReceiver, ReorderingSender,
    ToggleDropSender,
};

// ---------------------------------------------------------------------------
// Cluster node types
// ---------------------------------------------------------------------------

pub struct ClusterNode {
    pub handle: NodeHandle<String>,
    pub decisions: DecisionReceiver<String>,
    pub run_handle: JoinHandle<Result<(), daccord::NodeError>>,
    pub id: NodeId,
}

pub struct ClusterNodeTyped<V> {
    pub handle: NodeHandle<V>,
    pub decisions: DecisionReceiver<V>,
    pub run_handle: JoinHandle<Result<(), daccord::NodeError>>,
    pub id: NodeId,
}

// ---------------------------------------------------------------------------
// Shared storage for recovery tests
// ---------------------------------------------------------------------------

/// Shared `PaxosStorage` for restart-recovery tests.
///
/// Wraps a `PaxosMemoryStorage` in `Arc<Mutex<...>>` so a node can be dropped
/// and a new one constructed against the same underlying state, modeling
/// process restart with persistent disk.
pub struct SharedMemoryStorage<V> {
    inner: Arc<Mutex<PaxosMemoryStorage<V>>>,
}

impl<V> SharedMemoryStorage<V> {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(PaxosMemoryStorage::new())),
        }
    }
}

impl<V> Default for SharedMemoryStorage<V> {
    fn default() -> Self {
        Self::new()
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
impl<V> PaxosStorage<V> for SharedMemoryStorage<V>
where
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    async fn save_decision(&mut self, slot: u64, value: V) -> Result<(), StorageError> {
        self.inner.lock().await.save_decision(slot, value).await
    }

    async fn load_decisions(&self) -> Result<Vec<(u64, V)>, StorageError> {
        self.inner.lock().await.load_decisions().await
    }
}

/// Shared `RaftStorage` for restart-recovery tests. Mirrors
/// `SharedMemoryStorage` for the Raft side.
pub struct SharedRaftStorage<V> {
    inner: Arc<Mutex<RaftMemoryStorage<V>>>,
}

impl<V> SharedRaftStorage<V> {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(RaftMemoryStorage::new())),
        }
    }
}

impl<V> Default for SharedRaftStorage<V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<V> Clone for SharedRaftStorage<V> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

#[async_trait]
impl<V> RaftStorage<V> for SharedRaftStorage<V>
where
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    async fn save_decision(&mut self, slot: u64, value: V) -> Result<(), StorageError> {
        self.inner.lock().await.save_decision(slot, value).await
    }

    async fn load_decisions(&self) -> Result<Vec<(u64, V)>, StorageError> {
        self.inner.lock().await.load_decisions().await
    }

    async fn save_term(&mut self, term: u64) -> Result<(), StorageError> {
        self.inner.lock().await.save_term(term).await
    }

    async fn load_term(&self) -> Result<u64, StorageError> {
        self.inner.lock().await.load_term().await
    }

    async fn save_voted_for(&mut self, voted_for: Option<NodeId>) -> Result<(), StorageError> {
        self.inner.lock().await.save_voted_for(voted_for).await
    }

    async fn load_voted_for(&self) -> Result<Option<NodeId>, StorageError> {
        self.inner.lock().await.load_voted_for().await
    }

    async fn append_log(&mut self, entries: &[LogEntry<V>]) -> Result<(), StorageError> {
        self.inner.lock().await.append_log(entries).await
    }

    async fn truncate_log_from(&mut self, index: u64) -> Result<(), StorageError> {
        self.inner.lock().await.truncate_log_from(index).await
    }

    async fn load_log(&self) -> Result<Vec<LogEntry<V>>, StorageError> {
        self.inner.lock().await.load_log().await
    }

    async fn save_commit_index(&mut self, commit_index: Option<u64>) -> Result<(), StorageError> {
        self.inner
            .lock()
            .await
            .save_commit_index(commit_index)
            .await
    }

    async fn load_commit_index(&self) -> Result<Option<u64>, StorageError> {
        self.inner.lock().await.load_commit_index().await
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
        let storage = PaxosMemoryStorage::<String>::new();

        let (node, handle, decisions) = Node::paxos_with_id(
            ids[i].clone(),
            PaxosConfig::default(),
            peers,
            receiver,
            storage,
        );

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
        let storage = PaxosMemoryStorage::<V>::new();

        let (node, handle, decisions) = Node::paxos_with_id(
            ids[i].clone(),
            PaxosConfig::default(),
            peers,
            receiver,
            storage,
        );

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
// Raft cluster creation
// ---------------------------------------------------------------------------

/// Default `RaftConfig` for tests. Wider timeouts than production (150-300ms
/// election window, 50ms heartbeat) to absorb scheduler jitter without
/// destabilizing convergence.
fn test_raft_config() -> daccord::RaftConfig {
    daccord::RaftConfig {
        election_timeout_min: std::time::Duration::from_millis(150),
        election_timeout_max: std::time::Duration::from_millis(300),
        heartbeat_interval: std::time::Duration::from_millis(50),
    }
}

pub fn create_raft_cluster(n: usize) -> Vec<ClusterNode> {
    create_raft_cluster_inner(n, daccord::RaftConfig::default())
}

pub fn create_raft_cluster_with_config(n: usize, config: daccord::RaftConfig) -> Vec<ClusterNode> {
    create_raft_cluster_inner(n, config)
}

pub fn create_raft_lossy_cluster(n: usize, drop_rate: f64) -> Vec<ClusterNode> {
    create_raft_lossy_cluster_inner(n, drop_rate, false, test_raft_config())
}

pub fn create_raft_lossy_unbounded_cluster(n: usize, drop_rate: f64) -> Vec<ClusterNode> {
    create_raft_lossy_cluster_inner(n, drop_rate, true, test_raft_config())
}

pub fn create_raft_lossy_cluster_with_config(
    n: usize,
    drop_rate: f64,
    config: daccord::RaftConfig,
) -> Vec<ClusterNode> {
    create_raft_lossy_cluster_inner(n, drop_rate, false, config)
}

fn create_raft_lossy_cluster_inner(
    n: usize,
    drop_rate: f64,
    unbounded: bool,
    config: daccord::RaftConfig,
) -> Vec<ClusterNode> {
    assert!(n > 0);
    let ids: Vec<NodeId> = (0..n)
        .map(|i| NodeId::new(format!("node-{i}"), 1000))
        .collect();
    let mut tx_for: HashMap<NodeId, LossySender<ChannelSender>> = HashMap::new();
    let mut rx_for: HashMap<NodeId, ChannelReceiver> = HashMap::new();
    for id in &ids {
        let (tx, rx) = if unbounded {
            unbounded_channel()
        } else {
            channel(64)
        };
        tx_for.insert(id.clone(), LossySender::with_drop_rate(tx, drop_rate));
        rx_for.insert(id.clone(), rx);
    }
    let mut out = Vec::with_capacity(n);
    for me in &ids {
        let peers: Vec<PeerInfo<LossySender<ChannelSender>>> = ids
            .iter()
            .filter(|other| *other != me)
            .map(|other| PeerInfo {
                id: other.clone(),
                sender: tx_for[other].clone(),
            })
            .collect();
        let recv = rx_for.remove(me).unwrap();
        let (node, handle, decisions) = Node::raft_with_id(
            me.clone(),
            config.clone(),
            peers,
            recv,
            RaftMemoryStorage::<String>::new(),
        );
        let run_handle = tokio::spawn(node.run());
        out.push(ClusterNode {
            handle,
            decisions,
            run_handle,
            id: me.clone(),
        });
    }
    out
}

pub fn create_raft_delayed_cluster(n: usize, min_ms: u64, max_ms: u64) -> Vec<ClusterNode> {
    assert!(n > 0);
    let cfg = test_raft_config();
    let ids: Vec<NodeId> = (0..n)
        .map(|i| NodeId::new(format!("node-{i}"), 1000))
        .collect();
    let mut tx_for: HashMap<NodeId, DelayedSender<ChannelSender>> = HashMap::new();
    let mut rx_for: HashMap<NodeId, ChannelReceiver> = HashMap::new();
    for id in &ids {
        let (tx, rx) = unbounded_channel();
        tx_for.insert(
            id.clone(),
            DelayedSender::with_range(
                tx,
                Duration::from_millis(min_ms),
                Duration::from_millis(max_ms),
            ),
        );
        rx_for.insert(id.clone(), rx);
    }
    let mut out = Vec::with_capacity(n);
    for me in &ids {
        let peers: Vec<PeerInfo<DelayedSender<ChannelSender>>> = ids
            .iter()
            .filter(|other| *other != me)
            .map(|other| PeerInfo {
                id: other.clone(),
                sender: tx_for[other].clone(),
            })
            .collect();
        let recv = rx_for.remove(me).unwrap();
        let (node, handle, decisions) = Node::raft_with_id(
            me.clone(),
            cfg.clone(),
            peers,
            recv,
            RaftMemoryStorage::<String>::new(),
        );
        let run_handle = tokio::spawn(node.run());
        out.push(ClusterNode {
            handle,
            decisions,
            run_handle,
            id: me.clone(),
        });
    }
    out
}

pub fn create_raft_reordering_cluster(
    n: usize,
    window_ms: u64,
    batch_size: usize,
) -> Vec<ClusterNode> {
    assert!(n > 0);
    let cfg = test_raft_config();
    let ids: Vec<NodeId> = (0..n)
        .map(|i| NodeId::new(format!("node-{i}"), 1000))
        .collect();
    let mut tx_for: HashMap<NodeId, ReorderingSender<ChannelSender>> = HashMap::new();
    let mut rx_for: HashMap<NodeId, ReorderingReceiver<ChannelReceiver>> = HashMap::new();
    for id in &ids {
        let (tx, rx) = channel(64);
        tx_for.insert(id.clone(), ReorderingSender::new(tx));
        rx_for.insert(
            id.clone(),
            ReorderingReceiver::with_params(rx, Duration::from_millis(window_ms), batch_size),
        );
    }
    let mut out = Vec::with_capacity(n);
    for me in &ids {
        let peers: Vec<PeerInfo<ReorderingSender<ChannelSender>>> = ids
            .iter()
            .filter(|other| *other != me)
            .map(|other| PeerInfo {
                id: other.clone(),
                sender: tx_for[other].clone(),
            })
            .collect();
        let recv = rx_for.remove(me).unwrap();
        let (node, handle, decisions) = Node::raft_with_id(
            me.clone(),
            cfg.clone(),
            peers,
            recv,
            RaftMemoryStorage::<String>::new(),
        );
        let run_handle = tokio::spawn(node.run());
        out.push(ClusterNode {
            handle,
            decisions,
            run_handle,
            id: me.clone(),
        });
    }
    out
}

pub fn create_raft_lossy_delayed_cluster(
    n: usize,
    drop_rate: f64,
    min_delay_ms: u64,
    max_delay_ms: u64,
) -> Vec<ClusterNode> {
    assert!(n > 0);
    let cfg = test_raft_config();
    let ids: Vec<NodeId> = (0..n)
        .map(|i| NodeId::new(format!("node-{i}"), 1000))
        .collect();
    let mut tx_for: HashMap<NodeId, DelayedSender<LossySender<ChannelSender>>> = HashMap::new();
    let mut rx_for: HashMap<NodeId, ChannelReceiver> = HashMap::new();
    for id in &ids {
        let (tx, rx) = unbounded_channel();
        let lossy = LossySender::with_drop_rate(tx, drop_rate);
        tx_for.insert(
            id.clone(),
            DelayedSender::with_range(
                lossy,
                Duration::from_millis(min_delay_ms),
                Duration::from_millis(max_delay_ms),
            ),
        );
        rx_for.insert(id.clone(), rx);
    }
    let mut out = Vec::with_capacity(n);
    for me in &ids {
        let peers: Vec<PeerInfo<DelayedSender<LossySender<ChannelSender>>>> = ids
            .iter()
            .filter(|other| *other != me)
            .map(|other| PeerInfo {
                id: other.clone(),
                sender: tx_for[other].clone(),
            })
            .collect();
        let recv = rx_for.remove(me).unwrap();
        let (node, handle, decisions) = Node::raft_with_id(
            me.clone(),
            cfg.clone(),
            peers,
            recv,
            RaftMemoryStorage::<String>::new(),
        );
        let run_handle = tokio::spawn(node.run());
        out.push(ClusterNode {
            handle,
            decisions,
            run_handle,
            id: me.clone(),
        });
    }
    out
}

pub type EdgeFlags = HashMap<(NodeId, NodeId), std::sync::Arc<std::sync::atomic::AtomicBool>>;

pub fn create_raft_cluster_with_edge_filters(n: usize) -> (Vec<ClusterNode>, EdgeFlags) {
    assert!(n > 0);
    let cfg = test_raft_config();
    let ids: Vec<NodeId> = (0..n)
        .map(|i| NodeId::new(format!("node-{i}"), 1000))
        .collect();
    // Each peer has its own incoming receiver. Per-edge senders wrap the
    // receiver's tx with a ToggleDropSender — one per (from, to) pair.
    let mut rx_for: HashMap<NodeId, ChannelReceiver> = HashMap::new();
    let mut base_tx: HashMap<NodeId, ChannelSender> = HashMap::new();
    for id in &ids {
        let (tx, rx) = unbounded_channel();
        base_tx.insert(id.clone(), tx);
        rx_for.insert(id.clone(), rx);
    }
    let mut edges: EdgeFlags = HashMap::new();
    let mut edge_senders: HashMap<(NodeId, NodeId), ToggleDropSender<ChannelSender>> =
        HashMap::new();
    for from in &ids {
        for to in &ids {
            if from == to {
                continue;
            }
            let (sender, flag) = ToggleDropSender::new(base_tx[to].clone());
            edge_senders.insert((from.clone(), to.clone()), sender);
            edges.insert((from.clone(), to.clone()), flag);
        }
    }
    let mut out = Vec::with_capacity(n);
    for me in &ids {
        let peers: Vec<PeerInfo<ToggleDropSender<ChannelSender>>> = ids
            .iter()
            .filter(|other| *other != me)
            .map(|other| PeerInfo {
                id: other.clone(),
                sender: edge_senders[&(me.clone(), other.clone())].clone(),
            })
            .collect();
        let recv = rx_for.remove(me).unwrap();
        let (node, handle, decisions) = Node::raft_with_id(
            me.clone(),
            cfg.clone(),
            peers,
            recv,
            RaftMemoryStorage::<String>::new(),
        );
        let run_handle = tokio::spawn(node.run());
        out.push(ClusterNode {
            handle,
            decisions,
            run_handle,
            id: me.clone(),
        });
    }
    (out, edges)
}

fn create_raft_cluster_inner(n: usize, config: daccord::RaftConfig) -> Vec<ClusterNode> {
    assert!(n > 0);

    let ids: Vec<NodeId> = (0..n)
        .map(|i| NodeId::new(format!("node-{}", i), 1000))
        .collect();

    let mut tx_for: HashMap<NodeId, ChannelSender> = HashMap::new();
    let mut rx_for: HashMap<NodeId, ChannelReceiver> = HashMap::new();
    for id in &ids {
        let (tx, rx) = channel(64);
        tx_for.insert(id.clone(), tx);
        rx_for.insert(id.clone(), rx);
    }

    let mut out = Vec::with_capacity(n);
    for me in &ids {
        let peers: Vec<PeerInfo<ChannelSender>> = ids
            .iter()
            .filter(|other| *other != me)
            .map(|other| PeerInfo {
                id: other.clone(),
                sender: tx_for[other].clone(),
            })
            .collect();
        let recv = rx_for.remove(me).unwrap();
        let (node, handle, decisions) = Node::raft_with_id(
            me.clone(),
            config.clone(),
            peers,
            recv,
            RaftMemoryStorage::<String>::new(),
        );
        let run_handle = tokio::spawn(node.run());
        out.push(ClusterNode {
            handle,
            decisions,
            run_handle,
            id: me.clone(),
        });
    }
    out
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
        let storage = PaxosMemoryStorage::<String>::new();

        let (node, handle, decisions) = Node::paxos_with_id(
            ids[i].clone(),
            PaxosConfig::default(),
            peers,
            receiver,
            storage,
        );

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
        let storage = PaxosMemoryStorage::<String>::new();

        let (node, handle, decisions) = Node::paxos_with_id(
            ids[i].clone(),
            PaxosConfig::default(),
            peers,
            receiver,
            storage,
        );

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
        let storage = PaxosMemoryStorage::<String>::new();

        let (node, handle, decisions) = Node::paxos_with_id(
            ids[i].clone(),
            PaxosConfig::default(),
            peers,
            receiver,
            storage,
        );

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
        let storage = PaxosMemoryStorage::<String>::new();

        let (node, handle, decisions) = Node::paxos_with_id(
            ids[i].clone(),
            PaxosConfig::default(),
            peers,
            receiver,
            storage,
        );

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
        let storage = PaxosMemoryStorage::<String>::new();

        let (node, handle, decisions) = Node::paxos_with_id(
            ids[i].clone(),
            PaxosConfig::default(),
            peers,
            receiver,
            storage,
        );

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

async fn futures_never() -> Result<(), daccord::NodeError> {
    loop {
        tokio::time::sleep(Duration::from_secs(3600)).await;
    }
}

pub async fn collect_decisions(
    rx: &mut DecisionReceiver<String>,
    count: usize,
) -> Vec<Decided<String>> {
    collect_decisions_with_timeout(rx, count, Duration::from_secs(30)).await
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

#[derive(Clone, Copy, Debug)]
pub enum Algorithm {
    Paxos,
    Raft,
}

pub fn create_cluster_with_algorithm(n: usize, alg: Algorithm) -> Vec<ClusterNode> {
    match alg {
        Algorithm::Paxos => create_cluster(n),
        Algorithm::Raft => create_raft_cluster(n),
    }
}

pub fn create_lossy_with_algorithm(n: usize, drop_rate: f64, alg: Algorithm) -> Vec<ClusterNode> {
    match alg {
        Algorithm::Paxos => create_lossy_cluster(n, drop_rate),
        Algorithm::Raft => create_raft_lossy_cluster(n, drop_rate),
    }
}

pub fn create_lossy_unbounded_with_algorithm(
    n: usize,
    drop_rate: f64,
    alg: Algorithm,
) -> Vec<ClusterNode> {
    match alg {
        Algorithm::Paxos => create_lossy_unbounded_cluster(n, drop_rate),
        Algorithm::Raft => create_raft_lossy_unbounded_cluster(n, drop_rate),
    }
}

pub fn create_delayed_with_algorithm(
    n: usize,
    min_ms: u64,
    max_ms: u64,
    alg: Algorithm,
) -> Vec<ClusterNode> {
    match alg {
        Algorithm::Paxos => create_delayed_cluster(n, min_ms, max_ms),
        Algorithm::Raft => create_raft_delayed_cluster(n, min_ms, max_ms),
    }
}

pub fn create_reordering_with_algorithm(
    n: usize,
    window_ms: u64,
    batch_size: usize,
    alg: Algorithm,
) -> Vec<ClusterNode> {
    match alg {
        Algorithm::Paxos => create_reordering_cluster(n, window_ms, batch_size),
        Algorithm::Raft => create_raft_reordering_cluster(n, window_ms, batch_size),
    }
}

pub fn create_lossy_delayed_with_algorithm(
    n: usize,
    drop_rate: f64,
    min_ms: u64,
    max_ms: u64,
    alg: Algorithm,
) -> Vec<ClusterNode> {
    match alg {
        Algorithm::Paxos => create_lossy_delayed_cluster(n, drop_rate, min_ms, max_ms),
        Algorithm::Raft => create_raft_lossy_delayed_cluster(n, drop_rate, min_ms, max_ms),
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
