mod helpers;

use std::path::PathBuf;

use concensus::{channel, ChannelSender, Decided, DuckDbStorage, Node, NodeId, PeerInfo};
use tokio::time::{timeout, Duration};

/// Create a temp directory and return a path like `{tmp}/concensus-integ-{pid}/{name}.db`.
fn temp_db_path(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("concensus-integ-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("failed to create temp dir");
    dir.join(format!("{}.db", name))
}

/// Build a 3-node cluster using channel transport and DuckDbStorage backed by the given paths.
fn build_cluster_with_duckdb(
    ids: &[NodeId; 3],
    db_paths: &[PathBuf; 3],
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

    let storage0 =
        DuckDbStorage::<String>::new(&db_paths[0]).expect("failed to open DuckDB for node-0");
    let storage1 =
        DuckDbStorage::<String>::new(&db_paths[1]).expect("failed to open DuckDB for node-1");
    let storage2 =
        DuckDbStorage::<String>::new(&db_paths[2]).expect("failed to open DuckDB for node-2");

    let (node0, handle0, dec0) = Node::with_id(ids[0].clone(), peers_for_0, rx0, storage0);
    let (node1, handle1, dec1) = Node::with_id(ids[1].clone(), peers_for_1, rx1, storage1);
    let (node2, handle2, dec2) = Node::with_id(ids[2].clone(), peers_for_2, rx2, storage2);

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

/// Clean up temp DB files for this process.
fn cleanup_temp_dir() {
    let dir = std::env::temp_dir().join(format!("concensus-integ-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duckdb_node_recovers_decisions_after_crash() {
    let ids: [NodeId; 3] = [
        NodeId::new("node-0", 1000),
        NodeId::new("node-1", 1000),
        NodeId::new("node-2", 1000),
    ];

    let db_paths: [PathBuf; 3] = [
        temp_db_path("recover-node-0"),
        temp_db_path("recover-node-1"),
        temp_db_path("recover-node-2"),
    ];

    // --- Phase 1: initial cluster, propose "value-a" ---
    let (handles, mut decisions, run_handles) = build_cluster_with_duckdb(&ids, &db_paths);

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

    // --- Phase 2: rebuild cluster from same DB files ---
    let (handles2, mut decisions2, run_handles2) = build_cluster_with_duckdb(&ids, &db_paths);

    // Propose a new value from node-1.
    handles2[1].propose("value-b".to_string()).await.unwrap();

    // After restart, all nodes recover from DuckDB. Collect decisions until we see "value-b".
    for dec in &mut decisions2 {
        let collected = collect_until_value(dec, "value-b", Duration::from_secs(5)).await;
        let target = collected.last().unwrap();
        assert_eq!(target.value, "value-b");
    }

    teardown(handles2, decisions2, run_handles2);
    cleanup_temp_dir();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duckdb_recovered_node_does_not_replay_old_decisions() {
    let ids: [NodeId; 3] = [
        NodeId::new("node-0", 1000),
        NodeId::new("node-1", 1000),
        NodeId::new("node-2", 1000),
    ];

    let db_paths: [PathBuf; 3] = [
        temp_db_path("noreplay-node-0"),
        temp_db_path("noreplay-node-1"),
        temp_db_path("noreplay-node-2"),
    ];

    // --- Phase 1: decide "value-a" ---
    let (handles, mut decisions, run_handles) = build_cluster_with_duckdb(&ids, &db_paths);

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
    let (handles2, mut decisions2, run_handles2) = build_cluster_with_duckdb(&ids, &db_paths);

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

    // Verify nodes 1 and 2 also decided "value-b".
    for i in 1..3 {
        let collected =
            collect_until_value(&mut decisions2[i], "value-b", Duration::from_secs(5)).await;
        assert_eq!(collected.last().unwrap().value, "value-b");
    }

    teardown(handles2, decisions2, run_handles2);
    cleanup_temp_dir();
}
