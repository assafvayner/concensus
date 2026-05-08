mod helpers;

use std::collections::HashSet;

use helpers::{assert_consistent_decisions, collect_decisions_with_timeout, create_lossy_cluster};
use tokio::time::Duration;

/// With message loss, Paxos retries take longer. Use generous timeouts.
/// At high loss rates with contention, multiple retry rounds with exponential
/// backoff can add up significantly.
const TIMEOUT_PER_DECISION: Duration = Duration::from_secs(60);

// ---------------------------------------------------------------------------
// 1% drop rate
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lossy_1pct_single_value() {
    let mut cluster = create_lossy_cluster(3, 0.01);

    cluster[0]
        .handle
        .propose("hello".to_string())
        .await
        .unwrap();

    let mut all = Vec::new();
    for node in &mut cluster {
        let decisions =
            collect_decisions_with_timeout(&mut node.decisions, 1, TIMEOUT_PER_DECISION).await;
        assert_eq!(decisions[0].value, "hello");
        all.push(decisions);
    }
    assert_consistent_decisions(&all);

    for node in cluster {
        drop(node.handle);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lossy_1pct_multiple_proposals() {
    let mut cluster = create_lossy_cluster(3, 0.01);

    let expected: HashSet<String> = (0..10).map(|i| format!("v-{}", i)).collect();
    for v in &expected {
        cluster[0].handle.propose(v.clone()).await.unwrap();
    }

    let mut all = Vec::new();
    for node in &mut cluster {
        let decisions =
            collect_decisions_with_timeout(&mut node.decisions, 10, TIMEOUT_PER_DECISION).await;
        let values: HashSet<String> = decisions.iter().map(|d| d.value.clone()).collect();
        assert_eq!(values, expected);
        all.push(decisions);
    }
    assert_consistent_decisions(&all);

    for node in cluster {
        drop(node.handle);
    }
}

// ---------------------------------------------------------------------------
// 10% drop rate
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lossy_10pct_single_value() {
    let mut cluster = create_lossy_cluster(3, 0.10);

    cluster[0]
        .handle
        .propose("hello".to_string())
        .await
        .unwrap();

    let mut all = Vec::new();
    for node in &mut cluster {
        let decisions =
            collect_decisions_with_timeout(&mut node.decisions, 1, TIMEOUT_PER_DECISION).await;
        assert_eq!(decisions[0].value, "hello");
        all.push(decisions);
    }
    assert_consistent_decisions(&all);

    for node in cluster {
        drop(node.handle);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lossy_10pct_multiple_proposals() {
    let mut cluster = create_lossy_cluster(3, 0.10);

    let expected: HashSet<String> = (0..10).map(|i| format!("v-{}", i)).collect();
    for v in &expected {
        cluster[0].handle.propose(v.clone()).await.unwrap();
    }

    let mut all = Vec::new();
    for node in &mut cluster {
        let decisions =
            collect_decisions_with_timeout(&mut node.decisions, 10, TIMEOUT_PER_DECISION).await;
        let values: HashSet<String> = decisions.iter().map(|d| d.value.clone()).collect();
        assert_eq!(values, expected);
        all.push(decisions);
    }
    assert_consistent_decisions(&all);

    for node in cluster {
        drop(node.handle);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lossy_10pct_proposals_from_different_nodes() {
    let mut cluster = create_lossy_cluster(3, 0.10);

    let propose_handles: Vec<_> = cluster
        .iter()
        .enumerate()
        .map(|(i, node)| {
            let h = node.handle.clone();
            tokio::spawn(async move {
                // Small stagger to reduce initial slot contention between proposers
                tokio::time::sleep(Duration::from_millis(10 * i as u64)).await;
                h.propose(format!("from-{}", i)).await
            })
        })
        .collect();

    let expected: HashSet<String> = (0..3).map(|i| format!("from-{}", i)).collect();

    let mut all = Vec::new();
    for node in &mut cluster {
        let decisions =
            collect_decisions_with_timeout(&mut node.decisions, 3, TIMEOUT_PER_DECISION).await;
        let values: HashSet<String> = decisions.iter().map(|d| d.value.clone()).collect();
        assert_eq!(values, expected);
        all.push(decisions);
    }
    assert_consistent_decisions(&all);

    for h in propose_handles {
        h.abort();
    }
    for node in cluster {
        drop(node.handle);
    }
}

// ---------------------------------------------------------------------------
// 20% drop rate
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lossy_20pct_single_value() {
    let mut cluster = create_lossy_cluster(3, 0.20);

    cluster[0]
        .handle
        .propose("hello".to_string())
        .await
        .unwrap();

    let mut all = Vec::new();
    for node in &mut cluster {
        let decisions =
            collect_decisions_with_timeout(&mut node.decisions, 1, TIMEOUT_PER_DECISION).await;
        assert_eq!(decisions[0].value, "hello");
        all.push(decisions);
    }
    assert_consistent_decisions(&all);

    for node in cluster {
        drop(node.handle);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lossy_20pct_multiple_proposals() {
    let mut cluster = create_lossy_cluster(3, 0.20);

    let expected: HashSet<String> = (0..10).map(|i| format!("v-{}", i)).collect();
    for v in &expected {
        cluster[0].handle.propose(v.clone()).await.unwrap();
    }

    let mut all = Vec::new();
    for node in &mut cluster {
        let decisions =
            collect_decisions_with_timeout(&mut node.decisions, 10, TIMEOUT_PER_DECISION).await;
        let values: HashSet<String> = decisions.iter().map(|d| d.value.clone()).collect();
        assert_eq!(values, expected);
        all.push(decisions);
    }
    assert_consistent_decisions(&all);

    for node in cluster {
        drop(node.handle);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lossy_20pct_proposals_from_different_nodes() {
    let mut cluster = create_lossy_cluster(3, 0.20);

    let propose_handles: Vec<_> = cluster
        .iter()
        .enumerate()
        .map(|(i, node)| {
            let h = node.handle.clone();
            tokio::spawn(async move {
                // Small stagger to reduce initial slot contention between proposers
                tokio::time::sleep(Duration::from_millis(10 * i as u64)).await;
                h.propose(format!("from-{}", i)).await
            })
        })
        .collect();

    let expected: HashSet<String> = (0..3).map(|i| format!("from-{}", i)).collect();

    let mut all = Vec::new();
    for node in &mut cluster {
        let decisions =
            collect_decisions_with_timeout(&mut node.decisions, 3, TIMEOUT_PER_DECISION).await;
        let values: HashSet<String> = decisions.iter().map(|d| d.value.clone()).collect();
        assert_eq!(values, expected);
        all.push(decisions);
    }
    assert_consistent_decisions(&all);

    for h in propose_handles {
        h.abort();
    }
    for node in cluster {
        drop(node.handle);
    }
}

// ---------------------------------------------------------------------------
// 30% drop rate
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lossy_30pct_single_value() {
    let mut cluster = create_lossy_cluster(3, 0.30);

    cluster[0]
        .handle
        .propose("hello".to_string())
        .await
        .unwrap();

    let mut all = Vec::new();
    for node in &mut cluster {
        let decisions =
            collect_decisions_with_timeout(&mut node.decisions, 1, TIMEOUT_PER_DECISION).await;
        assert_eq!(decisions[0].value, "hello");
        all.push(decisions);
    }
    assert_consistent_decisions(&all);

    for node in cluster {
        drop(node.handle);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lossy_30pct_multiple_proposals() {
    let mut cluster = create_lossy_cluster(3, 0.30);

    let expected: HashSet<String> = (0..10).map(|i| format!("v-{}", i)).collect();
    for v in &expected {
        cluster[0].handle.propose(v.clone()).await.unwrap();
    }

    let mut all = Vec::new();
    for node in &mut cluster {
        let decisions =
            collect_decisions_with_timeout(&mut node.decisions, 10, TIMEOUT_PER_DECISION).await;
        let values: HashSet<String> = decisions.iter().map(|d| d.value.clone()).collect();
        assert_eq!(values, expected);
        all.push(decisions);
    }
    assert_consistent_decisions(&all);

    for node in cluster {
        drop(node.handle);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lossy_30pct_proposals_from_different_nodes() {
    let mut cluster = create_lossy_cluster(3, 0.30);

    let propose_handles: Vec<_> = cluster
        .iter()
        .enumerate()
        .map(|(i, node)| {
            let h = node.handle.clone();
            tokio::spawn(async move {
                // Small stagger to reduce initial slot contention between proposers
                tokio::time::sleep(Duration::from_millis(10 * i as u64)).await;
                h.propose(format!("from-{}", i)).await
            })
        })
        .collect();

    let expected: HashSet<String> = (0..3).map(|i| format!("from-{}", i)).collect();

    let mut all = Vec::new();
    for node in &mut cluster {
        let decisions =
            collect_decisions_with_timeout(&mut node.decisions, 3, TIMEOUT_PER_DECISION).await;
        let values: HashSet<String> = decisions.iter().map(|d| d.value.clone()).collect();
        assert_eq!(values, expected);
        all.push(decisions);
    }
    assert_consistent_decisions(&all);

    for h in propose_handles {
        h.abort();
    }
    for node in cluster {
        drop(node.handle);
    }
}
