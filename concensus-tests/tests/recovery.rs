mod helpers;

use concensus::{channel, ChannelSender, Decided, MemoryStorage, Node, NodeId, PeerInfo};
use helpers::SharedMemoryStorage;
use tokio::time::{timeout, Duration};

/// Helper: build a 3-node cluster where node-0 uses the given `SharedMemoryStorage`
/// and the other two use fresh `MemoryStorage`.
#[allow(clippy::type_complexity)]
fn build_cluster(
    ids: &[NodeId; 3],
    storage0: SharedMemoryStorage<String>,
) -> (
    Vec<concensus::NodeHandle<String>>,
    Vec<concensus::DecisionReceiver<String>>,
    Vec<tokio::task::JoinHandle<Result<(), concensus::NodeError>>>,
) {
    let (tx0, rx0) = channel(64);
    let (tx1, rx1) = channel(64);
    let (tx2, rx2) = channel(64);

    let peers_for_0 = vec![
        PeerInfo {
            id: ids[1].clone(),
            sender: tx1.clone(),
        },
        PeerInfo {
            id: ids[2].clone(),
            sender: tx2.clone(),
        },
    ];
    let peers_for_1: Vec<PeerInfo<ChannelSender>> = vec![
        PeerInfo {
            id: ids[0].clone(),
            sender: tx0.clone(),
        },
        PeerInfo {
            id: ids[2].clone(),
            sender: tx2.clone(),
        },
    ];
    let peers_for_2: Vec<PeerInfo<ChannelSender>> = vec![
        PeerInfo {
            id: ids[0].clone(),
            sender: tx0.clone(),
        },
        PeerInfo {
            id: ids[1].clone(),
            sender: tx1.clone(),
        },
    ];

    let (node0, handle0, dec0) = Node::with_id(ids[0].clone(), peers_for_0, rx0, storage0);
    let (node1, handle1, dec1) = Node::with_id(
        ids[1].clone(),
        peers_for_1,
        rx1,
        MemoryStorage::<String>::new(),
    );
    let (node2, handle2, dec2) = Node::with_id(
        ids[2].clone(),
        peers_for_2,
        rx2,
        MemoryStorage::<String>::new(),
    );

    let rh0 = tokio::spawn(node0.run());
    let rh1 = tokio::spawn(node1.run());
    let rh2 = tokio::spawn(node2.run());

    (
        vec![handle0, handle1, handle2],
        vec![dec0, dec1, dec2],
        vec![rh0, rh1, rh2],
    )
}

/// Tear down every node: drop handles and abort run tasks.
fn teardown(
    handles: Vec<concensus::NodeHandle<String>>,
    decisions: Vec<concensus::DecisionReceiver<String>>,
    run_handles: Vec<tokio::task::JoinHandle<Result<(), concensus::NodeError>>>,
) {
    for h in handles {
        drop(h);
    }
    for d in decisions {
        drop(d);
    }
    for rh in run_handles {
        rh.abort();
    }
}

/// Collect decisions until we see one with the given value, or timeout.
async fn collect_until_value(
    rx: &mut concensus::DecisionReceiver<String>,
    target: &str,
    deadline: Duration,
) -> Vec<Decided<String>> {
    let mut collected = Vec::new();
    let start = tokio::time::Instant::now();
    loop {
        let remaining = deadline.saturating_sub(start.elapsed());
        if remaining.is_zero() {
            panic!(
                "timed out waiting for value {:?}; collected so far: {:?}",
                target,
                collected
                    .iter()
                    .map(|d: &Decided<String>| (&d.value, d.slot))
                    .collect::<Vec<_>>()
            );
        }
        let decided = timeout(remaining, rx.recv())
            .await
            .expect("timed out waiting for decision")
            .expect("decision channel closed");
        let found = decided.value == target;
        collected.push(decided);
        if found {
            return collected;
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn node_recovers_from_storage_and_continues() {
    let ids: [NodeId; 3] = [
        NodeId::new("node-0", 1000),
        NodeId::new("node-1", 1000),
        NodeId::new("node-2", 1000),
    ];

    // Shared storage for node-0 that survives restarts.
    let storage0 = SharedMemoryStorage::<String>::new();

    // --- Phase 1: initial cluster, propose "value-a" ---
    let (handles, mut decisions, run_handles) = build_cluster(&ids, storage0.clone());

    handles[0].propose("value-a".to_string()).await.unwrap();

    for dec in &mut decisions {
        let decided = timeout(Duration::from_secs(5), dec.recv())
            .await
            .expect("timed out waiting for decision")
            .expect("decision channel closed");
        assert_eq!(decided.value, "value-a");
    }

    // --- Simulate crash: tear down entire cluster ---
    teardown(handles, decisions, run_handles);

    // --- Phase 2: rebuild cluster, reuse storage0 for node-0 ---
    let (handles2, mut decisions2, run_handles2) = build_cluster(&ids, storage0.clone());

    // Propose a new value from node-1.
    handles2[1].propose("value-b".to_string()).await.unwrap();

    // After restart, node-0 already knows about slot 0 from storage, but nodes 1 and 2
    // are fresh. They may re-decide slot 0 before getting to "value-b". Collect decisions
    // until we see "value-b" from each node.
    for dec in &mut decisions2 {
        let collected = collect_until_value(dec, "value-b", Duration::from_secs(5)).await;
        let target = collected.last().unwrap();
        assert_eq!(target.value, "value-b");
    }

    teardown(handles2, decisions2, run_handles2);
}

#[tokio::test]
async fn shared_raft_storage_handoff_preserves_term_and_log() {
    use concensus::{LogEntry, RaftStorage};
    use helpers::SharedRaftStorage;
    let mut s1 = SharedRaftStorage::<String>::new();
    s1.save_term(7).await.unwrap();
    s1.append_log(&[LogEntry {
        term: 7,
        value: "x".into(),
    }])
    .await
    .unwrap();
    let s2 = s1.clone();
    assert_eq!(s2.load_term().await.unwrap(), 7);
    assert_eq!(s2.load_log().await.unwrap().len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recovered_node_skips_decided_slots() {
    let ids: [NodeId; 3] = [
        NodeId::new("node-0", 1000),
        NodeId::new("node-1", 1000),
        NodeId::new("node-2", 1000),
    ];

    let storage0 = SharedMemoryStorage::<String>::new();

    // --- Phase 1: decide "value-a" ---
    let (handles, mut decisions, run_handles) = build_cluster(&ids, storage0.clone());

    handles[0].propose("value-a".to_string()).await.unwrap();

    for dec in &mut decisions {
        let decided = timeout(Duration::from_secs(5), dec.recv())
            .await
            .expect("timed out waiting for decision")
            .expect("decision channel closed");
        assert_eq!(decided.value, "value-a");
    }

    teardown(handles, decisions, run_handles);

    // --- Phase 2: rebuild, propose "value-b" ---
    let (handles2, mut decisions2, run_handles2) = build_cluster(&ids, storage0.clone());

    handles2[1].propose("value-b".to_string()).await.unwrap();

    // Node-0 recovered slot 0 from storage, so its decision receiver should NOT
    // replay "value-a". The first decision it emits should be for a new slot.
    let collected_node0 =
        collect_until_value(&mut decisions2[0], "value-b", Duration::from_secs(5)).await;

    // Verify that node-0 never re-emitted "value-a" — it was already in storage.
    assert!(
        !collected_node0.iter().any(|d| d.value == "value-a"),
        "recovered node-0 should not replay old decisions from storage; got: {:?}",
        collected_node0
            .iter()
            .map(|d| (&d.value, d.slot))
            .collect::<Vec<_>>()
    );

    let decided_node0 = collected_node0.last().unwrap();
    assert_eq!(decided_node0.value, "value-b");
    assert!(
        decided_node0.slot >= 1,
        "recovered node-0 should decide at slot >= 1, got slot {}",
        decided_node0.slot
    );

    // Verify nodes 1 and 2 also decided "value-b" (they may decide "value-a" first
    // since they have fresh storage and re-learn slot 0).
    for decisions in decisions2.iter_mut().take(3).skip(1) {
        let collected = collect_until_value(decisions, "value-b", Duration::from_secs(5)).await;
        assert_eq!(collected.last().unwrap().value, "value-b");
    }

    teardown(handles2, decisions2, run_handles2);
}
