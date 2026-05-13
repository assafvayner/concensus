mod helpers;

use helpers::{assert_safety_invariant, create_lossy_unbounded_with_algorithm, Algorithm};
use tokio::time::Duration;

async fn lossy_single_value_for(alg: Algorithm, drop_rate: f64) {
    let mut cluster = create_lossy_unbounded_with_algorithm(3, drop_rate, alg);
    tokio::time::sleep(Duration::from_millis(800)).await;
    // Raft followers don't retry dropped Forward messages, so under heavy
    // loss a propose at a non-leader can hang. Spawn so the test can still
    // collect whatever decisions arrive.
    let h = cluster[0].handle.clone();
    let propose_handle = tokio::spawn(async move { h.propose("hello".into()).await });
    let mut all = Vec::new();
    for node in &mut cluster {
        let mut d = Vec::new();
        if let Ok(Some(x)) =
            tokio::time::timeout(Duration::from_secs(15), node.decisions.recv()).await
        {
            d.push(x);
        }
        all.push(d);
    }
    assert_safety_invariant(&all);
    propose_handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn paxos_lossy_1pct_single_value() {
    lossy_single_value_for(Algorithm::Paxos, 0.01).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn paxos_lossy_10pct_single_value() {
    lossy_single_value_for(Algorithm::Paxos, 0.10).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn paxos_lossy_20pct_single_value() {
    lossy_single_value_for(Algorithm::Paxos, 0.20).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn paxos_lossy_30pct_single_value() {
    lossy_single_value_for(Algorithm::Paxos, 0.30).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn raft_lossy_1pct_single_value() {
    lossy_single_value_for(Algorithm::Raft, 0.01).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn raft_lossy_10pct_single_value() {
    lossy_single_value_for(Algorithm::Raft, 0.10).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn raft_lossy_20pct_single_value() {
    lossy_single_value_for(Algorithm::Raft, 0.20).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn raft_lossy_30pct_single_value() {
    lossy_single_value_for(Algorithm::Raft, 0.30).await;
}

async fn lossy_concurrent_proposals_for(alg: Algorithm, drop_rate: f64) {
    let mut cluster = create_lossy_unbounded_with_algorithm(5, drop_rate, alg);
    tokio::time::sleep(Duration::from_millis(800)).await;
    let propose_handles: Vec<_> = cluster
        .iter()
        .enumerate()
        .map(|(i, node)| {
            let h = node.handle.clone();
            tokio::spawn(async move { h.propose(format!("v-{i}")).await })
        })
        .collect();
    let mut all = Vec::new();
    for node in &mut cluster {
        let mut d = Vec::new();
        for _ in 0..5 {
            match tokio::time::timeout(Duration::from_secs(15), node.decisions.recv()).await {
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
async fn paxos_lossy_15pct_concurrent_5() {
    lossy_concurrent_proposals_for(Algorithm::Paxos, 0.15).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn raft_lossy_15pct_concurrent_5() {
    lossy_concurrent_proposals_for(Algorithm::Raft, 0.15).await;
}
