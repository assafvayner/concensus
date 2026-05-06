mod helpers;

use helpers::{assert_safety_invariant, create_delayed_with_algorithm, Algorithm};
use tokio::time::Duration;

async fn delayed_single_value_for(alg: Algorithm, min_ms: u64, max_ms: u64) {
    let mut cluster = create_delayed_with_algorithm(3, min_ms, max_ms, alg);
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn paxos_delayed_50ms_single_value() {
    delayed_single_value_for(Algorithm::Paxos, 0, 50).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn paxos_delayed_200ms_single_value() {
    delayed_single_value_for(Algorithm::Paxos, 0, 200).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn raft_delayed_50ms_single_value() {
    delayed_single_value_for(Algorithm::Raft, 0, 50).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn raft_delayed_200ms_single_value() {
    delayed_single_value_for(Algorithm::Raft, 0, 200).await;
}

async fn delayed_concurrent_for(alg: Algorithm, min_ms: u64, max_ms: u64) {
    let mut cluster = create_delayed_with_algorithm(5, min_ms, max_ms, alg);
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
async fn paxos_delayed_50ms_concurrent_5() {
    delayed_concurrent_for(Algorithm::Paxos, 0, 50).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn raft_delayed_50ms_concurrent_5() {
    delayed_concurrent_for(Algorithm::Raft, 0, 50).await;
}
