mod helpers;

use helpers::{assert_consistent_decisions, collect_decisions, create_raft_cluster};

#[tokio::test]
async fn raft_cluster_starts_without_panic() {
    let cluster = create_raft_cluster(3);
    // Let the cluster run for ~500ms — at least one election should complete.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    for node in cluster {
        drop(node.handle);
        let _ = tokio::time::timeout(std::time::Duration::from_secs(1), node.run_handle).await;
    }
}

#[tokio::test]
async fn raft_three_node_consensus() {
    let mut cluster = create_raft_cluster(3);
    // Wait for an election to settle.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Try to propose from each node — at least one is the leader.
    for node in &cluster {
        let _ = node.handle.propose("hello".into()).await;
    }

    let mut all = Vec::new();
    for node in &mut cluster {
        let decisions = collect_decisions(&mut node.decisions, 1).await;
        all.push(decisions);
    }
    for d in &all {
        assert_eq!(d.len(), 1);
        assert_eq!(d[0].value, "hello");
    }
    assert_consistent_decisions(&all);
    for node in cluster {
        drop(node.handle);
    }
}
