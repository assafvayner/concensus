pub mod lossy_transport;

use std::collections::HashMap;

use concensus::{
    channel, unbounded_channel, ChannelReceiver, ChannelSender, Decided, DecisionReceiver,
    MemoryStorage, Node, NodeHandle, NodeId, PeerInfo,
};
use lossy_transport::{LossyReceiver, LossySender};
use tokio::task::JoinHandle;
use tokio::time::{timeout, Duration};

pub struct ClusterNode {
    pub handle: NodeHandle<String>,
    pub decisions: DecisionReceiver<String>,
    pub run_handle: JoinHandle<Result<(), concensus::NodeError>>,
    pub id: NodeId,
}

/// Creates a cluster where senders drop messages at the given rate.
///
/// Only senders are lossy (not receivers), because wrapping the receiver
/// would block the node's `tokio::select!` retry arm — the LossyReceiver's
/// internal loop would prevent retry timers from firing.
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

        let (node, handle, decisions) =
            Node::with_id(ids[i].clone(), peers, receiver, storage);

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

pub fn create_cluster(n: usize) -> Vec<ClusterNode> {
    create_cluster_inner(n, false, false)
}

pub fn create_unbounded_cluster(n: usize) -> Vec<ClusterNode> {
    create_cluster_inner(n, true, false)
}

pub fn create_cluster_with_dead_node(n: usize) -> Vec<ClusterNode> {
    create_cluster_inner(n, false, true)
}

fn create_cluster_inner(n: usize, unbounded: bool, last_dead: bool) -> Vec<ClusterNode> {
    assert!(n > 0);

    // Create node IDs
    let ids: Vec<NodeId> = (0..n)
        .map(|i| NodeId::new(format!("node-{}", i), 1000))
        .collect();

    // Create channels: one (sender, receiver) per node
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

    // Build nodes
    let mut cluster_nodes = Vec::new();
    for i in 0..n {
        let is_dead = last_dead && i == n - 1;

        // Build peer list: all nodes except self
        let peers: Vec<PeerInfo<ChannelSender>> = (0..n)
            .filter(|&j| j != i)
            .map(|j| PeerInfo {
                id: ids[j].clone(),
                sender: senders[j].clone(),
            })
            .collect();

        let receiver = receivers.remove(0);
        let storage = MemoryStorage::<String>::new();

        let (node, handle, decisions) =
            Node::with_id(ids[i].clone(), peers, receiver, storage);

        let run_handle = if is_dead {
            // Spawn a task that just drops the node immediately (simulates unreachable peer)
            tokio::spawn(async move {
                drop(node);
                // Keep future alive so JoinHandle doesn't complete immediately
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

/// Assert all nodes agree on the same slot-to-value mapping.
pub fn assert_consistent_decisions(all_decisions: &[Vec<Decided<String>>]) {
    if all_decisions.is_empty() {
        return;
    }

    // Build slot->value map from first node
    let reference: HashMap<u64, String> = all_decisions[0]
        .iter()
        .map(|d| (d.slot, d.value.clone()))
        .collect();

    // Check each node agrees
    for (i, decisions) in all_decisions.iter().enumerate() {
        let node_map: HashMap<u64, String> = decisions
            .iter()
            .map(|d| (d.slot, d.value.clone()))
            .collect();

        // Slots must be unique per node
        assert_eq!(
            decisions.len(),
            node_map.len(),
            "node {} has duplicate slots",
            i
        );

        // Every decision must match reference
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
