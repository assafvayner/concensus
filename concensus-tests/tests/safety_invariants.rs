mod helpers;

use std::collections::HashMap;

use helpers::{assert_safety_invariant, create_lossy_cluster, create_lossy_delayed_cluster};
use tokio::time::{timeout, Duration};

// ---------------------------------------------------------------------------
// 1. safety_under_loss_with_concurrent_proposers
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn safety_under_loss_with_concurrent_proposers() {
    let mut cluster = create_lossy_cluster(5, 0.15);

    // All 5 nodes propose simultaneously.
    for (i, node) in cluster.iter().enumerate().take(5) {
        node.handle.propose(format!("val-{}", i)).await.unwrap();
    }

    // Collect up to 5 decisions per node, tolerating fewer.
    let mut all_decisions = Vec::new();
    for node in &mut cluster {
        let mut decisions = Vec::new();
        for _ in 0..5 {
            match timeout(Duration::from_secs(10), node.decisions.recv()).await {
                Ok(Some(d)) => decisions.push(d),
                _ => break,
            }
        }
        all_decisions.push(decisions);
    }

    assert_safety_invariant(&all_decisions);

    for node in cluster {
        drop(node.handle);
    }
}

// ---------------------------------------------------------------------------
// 2. safety_four_node_under_loss_with_concurrent_proposers
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn safety_four_node_under_loss_with_concurrent_proposers() {
    let mut cluster = create_lossy_cluster(4, 0.15);

    // All 4 nodes propose simultaneously.
    for (i, node) in cluster.iter().enumerate().take(4) {
        node.handle.propose(format!("val-{}", i)).await.unwrap();
    }

    // Collect up to 4 decisions per node, tolerating fewer.
    let mut all_decisions = Vec::new();
    for node in &mut cluster {
        let mut decisions = Vec::new();
        for _ in 0..4 {
            match timeout(Duration::from_secs(10), node.decisions.recv()).await {
                Ok(Some(d)) => decisions.push(d),
                _ => break,
            }
        }
        all_decisions.push(decisions);
    }

    assert_safety_invariant(&all_decisions);

    for node in cluster {
        drop(node.handle);
    }
}

// ---------------------------------------------------------------------------
// 3. safety_under_rapid_concurrent_proposals
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn safety_under_rapid_concurrent_proposals() {
    let mut cluster = create_lossy_cluster(3, 0.10);

    // Each node proposes 10 values.
    for (i, node) in cluster.iter().enumerate().take(3) {
        for j in 0..10 {
            node.handle.propose(format!("n{}-v{}", i, j)).await.unwrap();
        }
    }

    // Collect up to 30 decisions per node.
    let mut all_decisions = Vec::new();
    for node in &mut cluster {
        let mut decisions = Vec::new();
        for _ in 0..30 {
            match timeout(Duration::from_secs(10), node.decisions.recv()).await {
                Ok(Some(d)) => decisions.push(d),
                _ => break,
            }
        }
        all_decisions.push(decisions);
    }

    assert_safety_invariant(&all_decisions);

    let total: usize = all_decisions.iter().map(|d| d.len()).sum();
    assert!(total > 0, "expected at least some decisions, got none");

    for node in cluster {
        drop(node.handle);
    }
}

// ---------------------------------------------------------------------------
// 4. safety_under_loss_and_delay
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn safety_under_loss_and_delay() {
    let mut cluster = create_lossy_delayed_cluster(5, 0.10, 0, 30);

    // First 3 nodes propose 5 values each.
    for (i, node) in cluster.iter().enumerate().take(3) {
        for j in 0..5 {
            node.handle.propose(format!("n{}-v{}", i, j)).await.unwrap();
        }
    }

    // Collect up to 15 decisions per node.
    let mut all_decisions = Vec::new();
    for node in &mut cluster {
        let mut decisions = Vec::new();
        for _ in 0..15 {
            match timeout(Duration::from_secs(15), node.decisions.recv()).await {
                Ok(Some(d)) => decisions.push(d),
                _ => break,
            }
        }
        all_decisions.push(decisions);
    }

    assert_safety_invariant(&all_decisions);

    for node in cluster {
        drop(node.handle);
    }
}

// ---------------------------------------------------------------------------
// 5. no_duplicate_slots_per_node
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_duplicate_slots_per_node() {
    let mut cluster = create_lossy_cluster(3, 0.10);

    // Propose 20 values from node 0.
    for j in 0..20 {
        cluster[0]
            .handle
            .propose(format!("val-{}", j))
            .await
            .unwrap();
    }

    // Collect all 20 decisions from each node.
    for node in &mut cluster {
        let mut slot_map: HashMap<u64, String> = HashMap::new();
        for _ in 0..20 {
            match timeout(Duration::from_secs(30), node.decisions.recv()).await {
                Ok(Some(d)) => {
                    if let Some(existing) = slot_map.get(&d.slot) {
                        panic!(
                            "node {} got duplicate slot {}: first={:?}, second={:?}",
                            node.id, d.slot, existing, d.value
                        );
                    }
                    slot_map.insert(d.slot, d.value);
                }
                _ => break,
            }
        }
    }

    for node in cluster {
        drop(node.handle);
    }
}
