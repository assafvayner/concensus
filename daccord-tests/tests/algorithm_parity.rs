mod helpers;

use helpers::{
    assert_consistent_decisions, collect_decisions, create_cluster_with_algorithm, Algorithm,
};

async fn single_value_for(alg: Algorithm) {
    let mut cluster = create_cluster_with_algorithm(3, alg);
    // Allow election (Raft) / leader establishment (multi-paxos with multi-paxos feature) to settle.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    cluster[0].handle.propose("hello".into()).await.unwrap();

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

#[tokio::test]
async fn paxos_single_value() {
    single_value_for(Algorithm::Paxos).await;
}

#[tokio::test]
async fn raft_single_value() {
    single_value_for(Algorithm::Raft).await;
}

async fn sequential_proposals_for(alg: Algorithm) {
    let mut cluster = create_cluster_with_algorithm(3, alg);
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let values = vec!["alpha", "beta", "gamma"];
    for v in &values {
        cluster[0].handle.propose(v.to_string()).await.unwrap();
        // Small delay between proposals — both algorithms tolerate concurrent submission,
        // but a small gap reduces flakiness from network reordering in the channel transport.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    let mut all = Vec::new();
    for node in &mut cluster {
        let decisions = collect_decisions(&mut node.decisions, 3).await;
        all.push(decisions);
    }
    let expected: std::collections::HashSet<String> =
        values.iter().map(|s| s.to_string()).collect();
    for d in &all {
        let got: std::collections::HashSet<String> = d.iter().map(|x| x.value.clone()).collect();
        assert_eq!(got, expected);
    }
    assert_consistent_decisions(&all);
    for node in cluster {
        drop(node.handle);
    }
}

#[tokio::test]
async fn paxos_sequential_proposals() {
    sequential_proposals_for(Algorithm::Paxos).await;
}

#[tokio::test]
async fn raft_sequential_proposals() {
    sequential_proposals_for(Algorithm::Raft).await;
}
