mod helpers;

use std::collections::HashSet;

use helpers::{
    assert_consistent_decisions, collect_decisions_with_timeout, create_lossy_unbounded_cluster,
    create_unbounded_cluster,
};
use tokio::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hundred_proposals_lossless() {
    let mut cluster = create_unbounded_cluster(3);

    for i in 0..100 {
        cluster[0].handle.propose(format!("v-{}", i)).await.unwrap();
    }

    let expected: HashSet<String> = (0..100).map(|i| format!("v-{}", i)).collect();

    let mut all_decisions = Vec::new();
    for node in &mut cluster {
        let decisions =
            collect_decisions_with_timeout(&mut node.decisions, 100, Duration::from_secs(30)).await;
        let decided_values: HashSet<String> = decisions.iter().map(|d| d.value.clone()).collect();
        assert_eq!(decided_values, expected);
        all_decisions.push(decisions);
    }

    assert_consistent_decisions(&all_decisions);

    for node in cluster {
        drop(node.handle);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hundred_proposals_lossy_5pct() {
    let mut cluster = create_lossy_unbounded_cluster(3, 0.05);

    for i in 0..100 {
        cluster[0].handle.propose(format!("v-{}", i)).await.unwrap();
    }

    let expected: HashSet<String> = (0..100).map(|i| format!("v-{}", i)).collect();

    let mut all_decisions = Vec::new();
    for node in &mut cluster {
        let decisions =
            collect_decisions_with_timeout(&mut node.decisions, 100, Duration::from_secs(60)).await;
        let decided_values: HashSet<String> = decisions.iter().map(|d| d.value.clone()).collect();
        assert_eq!(decided_values, expected);
        all_decisions.push(decisions);
    }

    assert_consistent_decisions(&all_decisions);

    for node in cluster {
        drop(node.handle);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn five_node_fifty_proposals_lossy() {
    let mut cluster = create_lossy_unbounded_cluster(5, 0.05);

    for i in 0..50 {
        cluster[0].handle.propose(format!("v-{}", i)).await.unwrap();
    }

    let expected: HashSet<String> = (0..50).map(|i| format!("v-{}", i)).collect();

    let mut all_decisions = Vec::new();
    for node in &mut cluster {
        let decisions =
            collect_decisions_with_timeout(&mut node.decisions, 50, Duration::from_secs(60)).await;
        let decided_values: HashSet<String> = decisions.iter().map(|d| d.value.clone()).collect();
        assert_eq!(decided_values, expected);
        all_decisions.push(decisions);
    }

    assert_consistent_decisions(&all_decisions);

    for node in cluster {
        drop(node.handle);
    }
}
