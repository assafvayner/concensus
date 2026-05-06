mod helpers;

use helpers::{assert_safety_invariant, create_cluster_with_algorithm, Algorithm};
use tokio::time::Duration;

async fn stress_concurrent_proposals_for(alg: Algorithm) {
    let mut cluster = create_cluster_with_algorithm(3, alg);
    tokio::time::sleep(Duration::from_millis(500)).await;
    for (i, node) in cluster.iter().enumerate() {
        for j in 0..50 {
            node.handle.propose(format!("n{i}-v{j}")).await.unwrap();
        }
    }
    let mut all = Vec::new();
    for node in &mut cluster {
        let mut d = Vec::new();
        for _ in 0..150 {
            match tokio::time::timeout(Duration::from_secs(20), node.decisions.recv()).await {
                Ok(Some(x)) => d.push(x),
                _ => break,
            }
        }
        all.push(d);
    }
    assert_safety_invariant(&all);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn paxos_stress_50_concurrent() {
    stress_concurrent_proposals_for(Algorithm::Paxos).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn raft_stress_50_concurrent() {
    stress_concurrent_proposals_for(Algorithm::Raft).await;
}
