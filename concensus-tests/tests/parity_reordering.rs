mod helpers;

use helpers::{assert_safety_invariant, create_reordering_with_algorithm, Algorithm};
use tokio::time::Duration;

async fn reordering_single_value_for(alg: Algorithm, window_ms: u64, batch_size: usize) {
    let mut cluster = create_reordering_with_algorithm(3, window_ms, batch_size, alg);
    tokio::time::sleep(Duration::from_millis(800)).await;
    cluster[0].handle.propose("hello".into()).await.unwrap();
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
}

async fn reordering_concurrent_for(alg: Algorithm, window_ms: u64, batch_size: usize) {
    let mut cluster = create_reordering_with_algorithm(5, window_ms, batch_size, alg);
    tokio::time::sleep(Duration::from_millis(800)).await;
    for (i, node) in cluster.iter().enumerate() {
        node.handle.propose(format!("v-{i}")).await.unwrap();
    }
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
    assert_safety_invariant(&all);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn paxos_reordering_small_single() {
    reordering_single_value_for(Algorithm::Paxos, 10, 5).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn raft_reordering_small_single() {
    reordering_single_value_for(Algorithm::Raft, 10, 5).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn paxos_reordering_large_concurrent() {
    reordering_concurrent_for(Algorithm::Paxos, 50, 10).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn raft_reordering_large_concurrent() {
    reordering_concurrent_for(Algorithm::Raft, 50, 10).await;
}
