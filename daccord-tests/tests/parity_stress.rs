mod helpers;

use helpers::{assert_safety_invariant, create_cluster_with_algorithm, Algorithm};
use tokio::time::Duration;

async fn stress_concurrent_proposals_for(alg: Algorithm) {
    let mut cluster = create_cluster_with_algorithm(3, alg);
    tokio::time::sleep(Duration::from_millis(500)).await;
    let propose_handles: Vec<_> = cluster
        .iter()
        .enumerate()
        .map(|(i, node)| {
            let h = node.handle.clone();
            tokio::spawn(async move {
                for j in 0..50 {
                    let _ = h.propose(format!("n{i}-v{j}")).await;
                }
            })
        })
        .collect();
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
    use helpers::raft_invariants::RaftClusterInvariantChecker;
    let mut checker = RaftClusterInvariantChecker::new(cluster.len());
    checker.poll(&mut cluster);
    let _ = checker.total_decided();
    assert_safety_invariant(&all);
    for h in propose_handles {
        h.abort();
    }
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
