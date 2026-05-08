mod helpers;

use helpers::{assert_safety_invariant, create_lossy_delayed_with_algorithm, Algorithm};
use tokio::time::Duration;

async fn adversarial_concurrent_for(alg: Algorithm) {
    let mut cluster = create_lossy_delayed_with_algorithm(5, 0.10, 0, 50, alg);
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
    assert_safety_invariant(&all);
    for h in propose_handles {
        h.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn paxos_adversarial_lossy_delayed() {
    adversarial_concurrent_for(Algorithm::Paxos).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn raft_adversarial_lossy_delayed() {
    adversarial_concurrent_for(Algorithm::Raft).await;
}
