//! In-process integration tests for `daccord-kv-demo`.
//!
//! Builds 1- and 3-node clusters using daccord's in-memory `channel`
//! transport. Each `KvNode` is given its own redb file in a `TempDir`.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tempfile::TempDir;
use tokio::time::timeout;

use daccord::{
    channel, ChannelReceiver, ChannelSender, Node, NodeId, PaxosConfig, PeerInfo, RedbPaxosStorage,
};
use daccord_kv_demo::kv::{KvNode, KvOp};

const CLUSTER_TIMEOUT: Duration = Duration::from_secs(5);

struct ClusterNode {
    kv: Arc<KvNode>,
    #[allow(dead_code)]
    state_dir: TempDir,
}

async fn build_paxos_cluster(n: usize) -> Vec<ClusterNode> {
    assert!(n > 0);

    let ids: Vec<NodeId> = (0..n)
        .map(|i| NodeId::new(format!("kv-{}", i), 1000))
        .collect();

    let mut senders: Vec<ChannelSender> = Vec::new();
    let mut receivers: Vec<ChannelReceiver> = Vec::new();
    for _ in 0..n {
        let (tx, rx) = channel(256);
        senders.push(tx);
        receivers.push(rx);
    }

    let mut cluster = Vec::new();
    for i in 0..n {
        let peers: Vec<PeerInfo<ChannelSender>> = (0..n)
            .filter(|&j| j != i)
            .map(|j| PeerInfo {
                id: ids[j].clone(),
                sender: senders[j].clone(),
            })
            .collect();

        let receiver = receivers.remove(0);
        let state_dir = TempDir::new().unwrap();
        let consensus_path = state_dir.path().join("consensus.redb");
        let kv_state_path = state_dir.path().join("kv-state.redb");

        let storage = RedbPaxosStorage::<KvOp>::open(&consensus_path).unwrap();

        let (node, handle, decisions) = Node::paxos_with_id(
            ids[i].clone(),
            PaxosConfig::default(),
            peers,
            receiver,
            storage,
        );

        tokio::spawn(node.run());

        let kv = KvNode::spawn(handle, decisions, &kv_state_path).unwrap();
        cluster.push(ClusterNode {
            kv: Arc::new(kv),
            state_dir,
        });
    }
    cluster
}

#[tokio::test]
async fn single_node_put_get() {
    let cluster = build_paxos_cluster(1).await;
    let node = cluster[0].kv.clone();

    let was_new = timeout(
        CLUSTER_TIMEOUT,
        node.put("alice".into(), Bytes::from_static(b"v1")),
    )
    .await
    .expect("timed out")
    .expect("put failed");
    assert!(was_new);

    let value = timeout(CLUSTER_TIMEOUT, node.get("alice".into()))
        .await
        .expect("timed out")
        .expect("get failed");
    assert_eq!(value.as_deref(), Some(&b"v1"[..]));
}

#[tokio::test]
async fn cross_node_reads_see_writes() {
    let cluster = build_paxos_cluster(3).await;

    let was_new = timeout(
        CLUSTER_TIMEOUT,
        cluster[0].kv.put("alice".into(), Bytes::from_static(b"42")),
    )
    .await
    .expect("timed out")
    .expect("put failed");
    assert!(was_new);

    for (i, node) in cluster.iter().enumerate() {
        let value = timeout(CLUSTER_TIMEOUT, node.kv.get("alice".into()))
            .await
            .unwrap_or_else(|_| panic!("get on node {} timed out", i))
            .unwrap_or_else(|e| panic!("get on node {} failed: {}", i, e));
        assert_eq!(
            value.as_deref(),
            Some(&b"42"[..]),
            "node {} did not see the write",
            i
        );
    }
}

#[tokio::test]
async fn replace_returns_was_new_false() {
    let cluster = build_paxos_cluster(3).await;

    let first = timeout(
        CLUSTER_TIMEOUT,
        cluster[0].kv.put("k".into(), Bytes::from_static(b"v1")),
    )
    .await
    .expect("timed out")
    .expect("first put failed");
    assert!(first, "first put should report was_new=true");

    let second = timeout(
        CLUSTER_TIMEOUT,
        cluster[1].kv.put("k".into(), Bytes::from_static(b"v2")),
    )
    .await
    .expect("timed out")
    .expect("second put failed");
    assert!(!second, "second put should report was_new=false");

    let value = timeout(CLUSTER_TIMEOUT, cluster[2].kv.get("k".into()))
        .await
        .expect("timed out")
        .expect("get failed");
    assert_eq!(value.as_deref(), Some(&b"v2"[..]));
}

#[tokio::test]
async fn get_missing_key_returns_none() {
    let cluster = build_paxos_cluster(3).await;
    let value = timeout(CLUSTER_TIMEOUT, cluster[0].kv.get("ghost".into()))
        .await
        .expect("timed out")
        .expect("get failed");
    assert!(value.is_none());
}

#[tokio::test]
async fn concurrent_puts_from_different_nodes_all_commit() {
    let cluster = build_paxos_cluster(3).await;

    // Three concurrent puts to the same key, one per node. All must commit;
    // the final value on every node must be one of the three proposed values,
    // and the same value on every node (consensus invariant).
    let candidates: Vec<&[u8]> = vec![b"from-0", b"from-1", b"from-2"];
    let mut handles = Vec::new();
    for (i, node) in cluster.iter().enumerate() {
        let kv = node.kv.clone();
        let v = Bytes::copy_from_slice(candidates[i]);
        handles.push(tokio::spawn(
            async move { kv.put("contended".into(), v).await },
        ));
    }
    for h in handles {
        timeout(CLUSTER_TIMEOUT, h)
            .await
            .expect("propose timed out")
            .expect("task panicked")
            .expect("put failed");
    }

    // Read back from every node and assert agreement + that the winning value
    // is one of the candidates.
    let mut seen: Vec<Bytes> = Vec::new();
    for (i, node) in cluster.iter().enumerate() {
        let value = timeout(CLUSTER_TIMEOUT, node.kv.get("contended".into()))
            .await
            .unwrap_or_else(|_| panic!("get on node {} timed out", i))
            .unwrap_or_else(|e| panic!("get on node {} failed: {}", i, e))
            .expect("contended key should be set");
        seen.push(value);
    }
    let first = seen[0].clone();
    for (i, v) in seen.iter().enumerate().skip(1) {
        assert_eq!(v, &first, "node {} disagrees with node 0", i);
    }
    let final_bytes: &[u8] = first.as_ref();
    assert!(
        candidates.contains(&final_bytes),
        "final value {:?} is not one of the proposed values",
        first
    );
}

#[tokio::test]
async fn state_persists_across_kv_node_restart() {
    // Single-node Paxos cluster (no peers), backed by a redb file in a TempDir
    // that survives across the KvNode being dropped. The daccord node itself
    // is dropped along with the KvNode, so we re-create both against the same
    // on-disk consensus.redb + kv-state.redb.
    let dir = TempDir::new().unwrap();
    let consensus_path = dir.path().join("consensus.redb");
    let kv_state_path = dir.path().join("kv-state.redb");
    let id = NodeId::new("kv-solo", 1000);

    // ── First incarnation: write some values, then drop everything.
    {
        let (_dummy_tx, dummy_rx) = channel(64);
        let storage = RedbPaxosStorage::<KvOp>::open(&consensus_path).unwrap();
        let (node, handle, decisions) = Node::paxos_with_id(
            id.clone(),
            PaxosConfig::default(),
            Vec::<PeerInfo<ChannelSender>>::new(),
            dummy_rx,
            storage,
        );
        let run_handle = tokio::spawn(node.run());
        let kv = KvNode::spawn(handle, decisions, &kv_state_path).unwrap();

        timeout(
            CLUSTER_TIMEOUT,
            kv.put("alice".into(), Bytes::from_static(b"42")),
        )
        .await
        .unwrap()
        .unwrap();
        timeout(
            CLUSTER_TIMEOUT,
            kv.put("bob".into(), Bytes::from_static(b"7")),
        )
        .await
        .unwrap()
        .unwrap();

        drop(kv);
        run_handle.abort();
        let _ = run_handle.await;
    }

    // ── Second incarnation: reopen the same files, get the values back.
    {
        let (_dummy_tx, dummy_rx) = channel(64);
        let storage = RedbPaxosStorage::<KvOp>::open(&consensus_path).unwrap();
        let (node, handle, decisions) = Node::paxos_with_id(
            id.clone(),
            PaxosConfig::default(),
            Vec::<PeerInfo<ChannelSender>>::new(),
            dummy_rx,
            storage,
        );
        tokio::spawn(node.run());
        let kv = KvNode::spawn(handle, decisions, &kv_state_path).unwrap();

        // daccord re-delivers persisted decisions on startup; give the
        // applier a moment to catch up before reading.
        tokio::time::sleep(Duration::from_millis(100)).await;

        let alice = timeout(CLUSTER_TIMEOUT, kv.get("alice".into()))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(alice.as_deref(), Some(&b"42"[..]));

        let bob = timeout(CLUSTER_TIMEOUT, kv.get("bob".into()))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(bob.as_deref(), Some(&b"7"[..]));
    }
}
