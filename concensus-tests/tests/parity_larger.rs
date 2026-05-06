mod helpers;

use helpers::{
    assert_consistent_decisions, collect_decisions, create_cluster_with_algorithm, Algorithm,
};
use tokio::time::Duration;

async fn larger_cluster_for(alg: Algorithm, n: usize) {
    let mut cluster = create_cluster_with_algorithm(n, alg);
    tokio::time::sleep(Duration::from_millis(500)).await;
    cluster[0].handle.propose("alpha".into()).await.unwrap();
    cluster[0].handle.propose("beta".into()).await.unwrap();
    cluster[0].handle.propose("gamma".into()).await.unwrap();
    let mut all = Vec::new();
    for node in &mut cluster {
        let d = collect_decisions(&mut node.decisions, 3).await;
        all.push(d);
    }
    assert_consistent_decisions(&all);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn paxos_5_node_consensus() {
    larger_cluster_for(Algorithm::Paxos, 5).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn raft_5_node_consensus() {
    larger_cluster_for(Algorithm::Raft, 5).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn paxos_7_node_consensus() {
    larger_cluster_for(Algorithm::Paxos, 7).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn raft_7_node_consensus() {
    larger_cluster_for(Algorithm::Raft, 7).await;
}
