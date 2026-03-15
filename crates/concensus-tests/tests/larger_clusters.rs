mod helpers;

use std::collections::HashSet;

use helpers::{
    assert_consistent_decisions, collect_decisions, collect_decisions_with_timeout, create_cluster,
    create_cluster_with_dead_nodes, create_lossy_cluster,
};
use tokio::time::Duration;

/// Generous timeout for lossy and contention tests.
const TIMEOUT: Duration = Duration::from_secs(30);

// ===========================================================================
// 4-NODE CLUSTER (quorum = 3)
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn four_node_single_value() {
    let mut cluster = create_cluster(4);

    cluster[0]
        .handle
        .propose("hello".to_string())
        .await
        .unwrap();

    let mut all = Vec::new();
    for node in &mut cluster {
        let decisions = collect_decisions(&mut node.decisions, 1).await;
        assert_eq!(decisions.len(), 1);
        assert_eq!(decisions[0].value, "hello");
        all.push(decisions);
    }
    assert_consistent_decisions(&all);

    for node in cluster {
        drop(node.handle);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn four_node_multiple_proposals() {
    let mut cluster = create_cluster(4);

    let expected: HashSet<String> = (0..10).map(|i| format!("v-{}", i)).collect();
    for v in &expected {
        cluster[0].handle.propose(v.clone()).await.unwrap();
    }

    let mut all = Vec::new();
    for node in &mut cluster {
        let decisions = collect_decisions(&mut node.decisions, 10).await;
        let values: HashSet<String> = decisions.iter().map(|d| d.value.clone()).collect();
        assert_eq!(values, expected);
        all.push(decisions);
    }
    assert_consistent_decisions(&all);

    for node in cluster {
        drop(node.handle);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn four_node_proposals_from_different_nodes() {
    let mut cluster = create_cluster(4);

    for (i, node) in cluster.iter().enumerate() {
        node.handle.propose(format!("from-{}", i)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let expected: HashSet<String> = (0..4).map(|i| format!("from-{}", i)).collect();

    let mut all = Vec::new();
    for node in &mut cluster {
        let decisions = collect_decisions(&mut node.decisions, 4).await;
        let values: HashSet<String> = decisions.iter().map(|d| d.value.clone()).collect();
        assert_eq!(values, expected);
        all.push(decisions);
    }
    assert_consistent_decisions(&all);

    for node in cluster {
        drop(node.handle);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn four_node_one_dead_still_decides() {
    let mut cluster = create_cluster_with_dead_nodes(4, 1);

    cluster[0]
        .handle
        .propose("despite-failure".to_string())
        .await
        .unwrap();

    // Only check the first 3 nodes (node 3 is dead)
    let mut all = Vec::new();
    for node in cluster.iter_mut().take(3) {
        let decisions = collect_decisions(&mut node.decisions, 1).await;
        assert_eq!(decisions.len(), 1);
        assert_eq!(decisions[0].value, "despite-failure");
        all.push(decisions);
    }
    assert_consistent_decisions(&all);

    for node in cluster {
        drop(node.handle);
        node.run_handle.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn four_node_lossy_10pct_multiple_proposals() {
    let mut cluster = create_lossy_cluster(4, 0.10);

    let expected: HashSet<String> = (0..10).map(|i| format!("v-{}", i)).collect();
    for v in &expected {
        cluster[0].handle.propose(v.clone()).await.unwrap();
    }

    let mut all = Vec::new();
    for node in &mut cluster {
        let decisions = collect_decisions_with_timeout(&mut node.decisions, 10, TIMEOUT).await;
        let values: HashSet<String> = decisions.iter().map(|d| d.value.clone()).collect();
        assert_eq!(values, expected);
        all.push(decisions);
    }
    assert_consistent_decisions(&all);

    for node in cluster {
        drop(node.handle);
    }
}

// ===========================================================================
// 5-NODE CLUSTER (quorum = 3)
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn five_node_single_value() {
    let mut cluster = create_cluster(5);

    cluster[0]
        .handle
        .propose("hello".to_string())
        .await
        .unwrap();

    let mut all = Vec::new();
    for node in &mut cluster {
        let decisions = collect_decisions(&mut node.decisions, 1).await;
        assert_eq!(decisions.len(), 1);
        assert_eq!(decisions[0].value, "hello");
        all.push(decisions);
    }
    assert_consistent_decisions(&all);

    for node in cluster {
        drop(node.handle);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn five_node_multiple_proposals() {
    let mut cluster = create_cluster(5);

    let expected: HashSet<String> = (0..10).map(|i| format!("v-{}", i)).collect();
    for v in &expected {
        cluster[0].handle.propose(v.clone()).await.unwrap();
    }

    let mut all = Vec::new();
    for node in &mut cluster {
        let decisions = collect_decisions(&mut node.decisions, 10).await;
        let values: HashSet<String> = decisions.iter().map(|d| d.value.clone()).collect();
        assert_eq!(values, expected);
        all.push(decisions);
    }
    assert_consistent_decisions(&all);

    for node in cluster {
        drop(node.handle);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn five_node_proposals_from_different_nodes() {
    let mut cluster = create_cluster(5);

    for (i, node) in cluster.iter().enumerate() {
        node.handle.propose(format!("from-{}", i)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let expected: HashSet<String> = (0..5).map(|i| format!("from-{}", i)).collect();

    let mut all = Vec::new();
    for node in &mut cluster {
        let decisions = collect_decisions(&mut node.decisions, 5).await;
        let values: HashSet<String> = decisions.iter().map(|d| d.value.clone()).collect();
        assert_eq!(values, expected);
        all.push(decisions);
    }
    assert_consistent_decisions(&all);

    for node in cluster {
        drop(node.handle);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn five_node_two_dead_still_decides() {
    let mut cluster = create_cluster_with_dead_nodes(5, 2);

    cluster[0]
        .handle
        .propose("despite-failure".to_string())
        .await
        .unwrap();

    // Only check the first 3 nodes (nodes 3 and 4 are dead)
    let mut all = Vec::new();
    for node in cluster.iter_mut().take(3) {
        let decisions = collect_decisions(&mut node.decisions, 1).await;
        assert_eq!(decisions.len(), 1);
        assert_eq!(decisions[0].value, "despite-failure");
        all.push(decisions);
    }
    assert_consistent_decisions(&all);

    for node in cluster {
        drop(node.handle);
        node.run_handle.abort();
    }
}

// ===========================================================================
// 7-NODE CLUSTER (quorum = 4)
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn seven_node_single_value() {
    let mut cluster = create_cluster(7);

    cluster[0]
        .handle
        .propose("hello".to_string())
        .await
        .unwrap();

    let mut all = Vec::new();
    for node in &mut cluster {
        let decisions = collect_decisions(&mut node.decisions, 1).await;
        assert_eq!(decisions.len(), 1);
        assert_eq!(decisions[0].value, "hello");
        all.push(decisions);
    }
    assert_consistent_decisions(&all);

    for node in cluster {
        drop(node.handle);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn seven_node_multiple_proposals() {
    let mut cluster = create_cluster(7);

    let expected: HashSet<String> = (0..10).map(|i| format!("v-{}", i)).collect();
    for v in &expected {
        cluster[0].handle.propose(v.clone()).await.unwrap();
    }

    let mut all = Vec::new();
    for node in &mut cluster {
        let decisions = collect_decisions(&mut node.decisions, 10).await;
        let values: HashSet<String> = decisions.iter().map(|d| d.value.clone()).collect();
        assert_eq!(values, expected);
        all.push(decisions);
    }
    assert_consistent_decisions(&all);

    for node in cluster {
        drop(node.handle);
    }
}
