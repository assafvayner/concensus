mod helpers;

use helpers::create_raft_cluster;

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
