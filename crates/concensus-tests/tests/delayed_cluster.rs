mod helpers;

use std::collections::HashSet;

use helpers::{
    assert_consistent_decisions, collect_decisions_with_timeout, create_delayed_cluster,
    create_lossy_delayed_cluster, create_reordering_cluster,
};
use tokio::time::Duration;

const TIMEOUT: Duration = Duration::from_secs(60);

// ---------------------------------------------------------------------------
// Delayed (0-50 ms)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delayed_0_50ms_single_value() {
    let mut cluster = create_delayed_cluster(3, 0, 50);

    cluster[0]
        .handle
        .propose("hello".to_string())
        .await
        .unwrap();

    let mut all = Vec::new();
    for node in &mut cluster {
        let decisions = collect_decisions_with_timeout(&mut node.decisions, 1, TIMEOUT).await;
        assert_eq!(decisions[0].value, "hello");
        all.push(decisions);
    }
    assert_consistent_decisions(&all);

    for node in cluster {
        drop(node.handle);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delayed_0_50ms_multiple_proposals() {
    let mut cluster = create_delayed_cluster(3, 0, 50);

    let handle = cluster[0].handle.clone();
    // Spawn proposal submission in background so it doesn't block decision
    // collection; the delayed transport makes inline sends slow.
    let proposer = tokio::spawn(async move {
        for i in 0..10 {
            handle.propose(format!("v-{}", i)).await.unwrap();
            // Stagger to reduce contention under delayed transport
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });

    let expected: HashSet<String> = (0..10).map(|i| format!("v-{}", i)).collect();

    // Destructure to get separate mutable borrows for concurrent collection.
    let mut iter = cluster.iter_mut();
    let n0 = iter.next().unwrap();
    let n1 = iter.next().unwrap();
    let n2 = iter.next().unwrap();

    let (d0, d1, d2) = tokio::join!(
        collect_decisions_with_timeout(&mut n0.decisions, 10, TIMEOUT),
        collect_decisions_with_timeout(&mut n1.decisions, 10, TIMEOUT),
        collect_decisions_with_timeout(&mut n2.decisions, 10, TIMEOUT),
    );

    proposer.await.unwrap();

    for decisions in [&d0, &d1, &d2] {
        let values: HashSet<String> = decisions.iter().map(|d| d.value.clone()).collect();
        assert_eq!(values, expected);
    }
    assert_consistent_decisions(&[d0, d1, d2]);

    for node in cluster {
        drop(node.handle);
    }
}

// ---------------------------------------------------------------------------
// Reordering (window 10 ms, batch size 5)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reordered_single_value() {
    let mut cluster = create_reordering_cluster(3, 10, 5);

    cluster[0]
        .handle
        .propose("hello".to_string())
        .await
        .unwrap();

    let mut all = Vec::new();
    for node in &mut cluster {
        let decisions = collect_decisions_with_timeout(&mut node.decisions, 1, TIMEOUT).await;
        assert_eq!(decisions[0].value, "hello");
        all.push(decisions);
    }
    assert_consistent_decisions(&all);

    for node in cluster {
        drop(node.handle);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reordered_multiple_proposals() {
    let mut cluster = create_reordering_cluster(3, 10, 5);

    let expected: HashSet<String> = (0..10).map(|i| format!("v-{}", i)).collect();
    for v in &expected {
        cluster[0].handle.propose(v.clone()).await.unwrap();
    }

    let mut all = Vec::new();
    for node in &mut cluster {
        let decisions = collect_decisions_with_timeout(&mut node.decisions, 10, TIMEOUT).await;
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
// Combined lossy (10%) + delayed (0-30 ms)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lossy_10pct_delayed_0_30ms_single_value() {
    let mut cluster = create_lossy_delayed_cluster(3, 0.10, 0, 30);

    cluster[0]
        .handle
        .propose("hello".to_string())
        .await
        .unwrap();

    let mut all = Vec::new();
    for node in &mut cluster {
        let decisions = collect_decisions_with_timeout(&mut node.decisions, 1, TIMEOUT).await;
        assert_eq!(decisions[0].value, "hello");
        all.push(decisions);
    }
    assert_consistent_decisions(&all);

    for node in cluster {
        drop(node.handle);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lossy_10pct_delayed_0_30ms_multiple_proposals() {
    let mut cluster = create_lossy_delayed_cluster(3, 0.10, 0, 30);

    let handle = cluster[0].handle.clone();
    let proposer = tokio::spawn(async move {
        for i in 0..10 {
            handle.propose(format!("v-{}", i)).await.unwrap();
            // Stagger to reduce contention under lossy+delayed transport
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });

    let expected: HashSet<String> = (0..10).map(|i| format!("v-{}", i)).collect();

    let mut iter = cluster.iter_mut();
    let n0 = iter.next().unwrap();
    let n1 = iter.next().unwrap();
    let n2 = iter.next().unwrap();

    let (d0, d1, d2) = tokio::join!(
        collect_decisions_with_timeout(&mut n0.decisions, 10, TIMEOUT),
        collect_decisions_with_timeout(&mut n1.decisions, 10, TIMEOUT),
        collect_decisions_with_timeout(&mut n2.decisions, 10, TIMEOUT),
    );

    proposer.await.unwrap();

    for decisions in [&d0, &d1, &d2] {
        let values: HashSet<String> = decisions.iter().map(|d| d.value.clone()).collect();
        assert_eq!(values, expected);
    }
    assert_consistent_decisions(&[d0, d1, d2]);

    for node in cluster {
        drop(node.handle);
    }
}
