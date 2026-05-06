#![cfg(feature = "multi-paxos")]

mod helpers;

use helpers::{
    assert_consistent_decisions, assert_safety_invariant, collect_decisions,
    collect_decisions_with_timeout, create_cluster, create_lossy_cluster,
};
use tokio::time::{timeout, Duration};

// ---------------------------------------------------------------------------
// 1. leader_emerges_on_first_proposal
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn leader_emerges_on_first_proposal() {
    let mut cluster = create_cluster(3);

    cluster[0]
        .handle
        .propose("first".to_string())
        .await
        .unwrap();

    let mut all_decisions = Vec::new();
    for node in &mut cluster {
        let decisions = collect_decisions(&mut node.decisions, 1).await;
        assert_eq!(decisions.len(), 1);
        assert_eq!(decisions[0].value, "first");
        all_decisions.push(decisions);
    }

    assert_consistent_decisions(&all_decisions);

    for node in cluster {
        drop(node.handle);
    }
}

// ---------------------------------------------------------------------------
// 2. leader_fast_path_multiple_proposals
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn leader_fast_path_multiple_proposals() {
    let mut cluster = create_cluster(3);

    // Establish leadership with a first proposal.
    cluster[0]
        .handle
        .propose("first".to_string())
        .await
        .unwrap();

    let mut all_decisions = Vec::new();
    for node in &mut cluster {
        let decisions = collect_decisions(&mut node.decisions, 1).await;
        assert_eq!(decisions[0].value, "first");
        all_decisions.push(decisions);
    }
    assert_consistent_decisions(&all_decisions);

    // Now propose 10 more values via the established leader (fast path).
    for i in 0..10 {
        cluster[0]
            .handle
            .propose(format!("val-{}", i))
            .await
            .unwrap();
    }

    let mut all_decisions = Vec::new();
    for node in &mut cluster {
        let decisions = collect_decisions(&mut node.decisions, 10).await;
        assert_eq!(decisions.len(), 10);
        all_decisions.push(decisions);
    }

    assert_consistent_decisions(&all_decisions);

    for node in cluster {
        drop(node.handle);
    }
}

// ---------------------------------------------------------------------------
// 3. follower_forwards_to_leader
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn follower_forwards_to_leader() {
    let mut cluster = create_cluster(3);

    // Establish node 0 as leader.
    cluster[0]
        .handle
        .propose("leader-init".to_string())
        .await
        .unwrap();

    for node in &mut cluster {
        let _ = collect_decisions(&mut node.decisions, 1).await;
    }

    // Propose from node 1 (a follower); should forward to leader.
    cluster[1]
        .handle
        .propose("from-follower".to_string())
        .await
        .unwrap();

    let mut all_decisions = Vec::new();
    for node in &mut cluster {
        let decisions = collect_decisions(&mut node.decisions, 1).await;
        assert_eq!(decisions.len(), 1);
        assert_eq!(decisions[0].value, "from-follower");
        all_decisions.push(decisions);
    }

    assert_consistent_decisions(&all_decisions);

    for node in cluster {
        drop(node.handle);
    }
}

// ---------------------------------------------------------------------------
// 4. leader_crash_and_failover
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn leader_crash_and_failover() {
    let mut cluster = create_cluster(3);

    // Establish node 0 as leader.
    cluster[0]
        .handle
        .propose("before-crash".to_string())
        .await
        .unwrap();

    for node in &mut cluster {
        let _ = collect_decisions(&mut node.decisions, 1).await;
    }

    // Crash node 0: abort its run task and drop a clone of its handle.
    cluster[0].run_handle.abort();
    drop(cluster[0].handle.clone());

    // Wait for election timeout to trigger failover.
    tokio::time::sleep(Duration::from_millis(800)).await;

    // Propose from node 1 after leader crash.
    cluster[1]
        .handle
        .propose("after-crash".to_string())
        .await
        .unwrap();

    // Collect from surviving nodes (1 and 2) with extended timeout.
    for node in cluster.iter_mut().take(3).skip(1) {
        let decisions =
            collect_decisions_with_timeout(&mut node.decisions, 1, Duration::from_secs(10)).await;
        assert_eq!(decisions.len(), 1);
        assert_eq!(decisions[0].value, "after-crash");
    }

    for node in cluster {
        drop(node.handle);
        node.run_handle.abort();
    }
}

// ---------------------------------------------------------------------------
// 5. forwarding_fallback_no_leader
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn forwarding_fallback_no_leader() {
    let mut cluster = create_cluster(3);

    // No leader established yet; propose from node 1.
    // Should fall back to full Paxos.
    cluster[1]
        .handle
        .propose("no-leader".to_string())
        .await
        .unwrap();

    let mut all_decisions = Vec::new();
    for node in &mut cluster {
        let decisions = collect_decisions(&mut node.decisions, 1).await;
        assert_eq!(decisions.len(), 1);
        assert_eq!(decisions[0].value, "no-leader");
        all_decisions.push(decisions);
    }

    assert_consistent_decisions(&all_decisions);

    for node in cluster {
        drop(node.handle);
    }
}

// ---------------------------------------------------------------------------
// 6. heartbeat_keeps_leadership
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn heartbeat_keeps_leadership() {
    let mut cluster = create_cluster(3);

    // Establish node 0 as leader.
    cluster[0]
        .handle
        .propose("establish-leader".to_string())
        .await
        .unwrap();

    for node in &mut cluster {
        let _ = collect_decisions(&mut node.decisions, 1).await;
    }

    // Sleep beyond the 500ms heartbeat/election timeout threshold.
    tokio::time::sleep(Duration::from_millis(800)).await;

    // Propose from node 1; should still forward to the original leader
    // because heartbeats keep leadership alive.
    cluster[1]
        .handle
        .propose("still-alive".to_string())
        .await
        .unwrap();

    let mut all_decisions = Vec::new();
    for node in &mut cluster {
        let decisions = collect_decisions(&mut node.decisions, 1).await;
        assert_eq!(decisions.len(), 1);
        assert_eq!(decisions[0].value, "still-alive");
        all_decisions.push(decisions);
    }

    assert_consistent_decisions(&all_decisions);

    for node in cluster {
        drop(node.handle);
    }
}

// ---------------------------------------------------------------------------
// 7. safety_under_leader_transition
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn safety_under_leader_transition() {
    let mut cluster = create_lossy_cluster(3, 0.10);

    // Each of 3 nodes proposes 5 values.
    for (i, node) in cluster.iter().enumerate().take(3) {
        for j in 0..5 {
            node.handle.propose(format!("n{}-v{}", i, j)).await.unwrap();
        }
    }

    // Collect up to 15 decisions per node with 10s timeout; don't fail if fewer.
    let mut all_decisions = Vec::new();
    for node in &mut cluster {
        let mut decisions = Vec::new();
        for _ in 0..15 {
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
// 8. lossy_network_with_leader
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lossy_network_with_leader() {
    let mut cluster = create_lossy_cluster(3, 0.10);

    // Propose 5 values from node 0 with 50ms stagger.
    for i in 0..5 {
        cluster[0]
            .handle
            .propose(format!("lossy-{}", i))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Collect all 5 from each node with 30s timeout.
    let mut all_decisions = Vec::new();
    for node in &mut cluster {
        let decisions =
            collect_decisions_with_timeout(&mut node.decisions, 5, Duration::from_secs(30)).await;
        assert_eq!(decisions.len(), 5);
        all_decisions.push(decisions);
    }

    assert_consistent_decisions(&all_decisions);

    for node in cluster {
        drop(node.handle);
    }
}

// ---------------------------------------------------------------------------
// 9. Duplicate forwarded values are each independently tracked
// ---------------------------------------------------------------------------

/// Verifies that forwarding the same value twice doesn't cause one of them
/// to be silently lost. Each forward should be tracked independently so that
/// if the first one is decided, the second still gets its own slot (or times
/// out and is re-proposed directly).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duplicate_forwarded_values_both_decide() {
    let mut cluster = create_cluster(3);

    // Establish node-0 as leader
    cluster[0]
        .handle
        .propose("establish".to_string())
        .await
        .unwrap();
    for node in &mut cluster {
        collect_decisions(&mut node.decisions, 1).await;
    }

    // Propose the same value twice from a follower
    cluster[1]
        .handle
        .propose("duplicate".to_string())
        .await
        .unwrap();
    cluster[1]
        .handle
        .propose("duplicate".to_string())
        .await
        .unwrap();

    // Both should be decided (in separate slots)
    let mut all = Vec::new();
    for node in &mut cluster {
        let decisions =
            collect_decisions_with_timeout(&mut node.decisions, 2, Duration::from_secs(10)).await;
        assert_eq!(decisions.len(), 2);
        assert_eq!(decisions[0].value, "duplicate");
        assert_eq!(decisions[1].value, "duplicate");
        assert_ne!(decisions[0].slot, decisions[1].slot);
        all.push(decisions);
    }
    assert_consistent_decisions(&all);

    for node in cluster {
        drop(node.handle);
    }
}

// ---------------------------------------------------------------------------
// 10. Late-joining follower discovers leader via heartbeat
// ---------------------------------------------------------------------------

/// After a leader is established, a follower should learn its identity via
/// heartbeat messages and then forward subsequent proposals instead of
/// running full Paxos.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn follower_discovers_leader_via_heartbeat() {
    let mut cluster = create_cluster(3);

    // Establish node-0 as leader
    cluster[0]
        .handle
        .propose("establish".to_string())
        .await
        .unwrap();
    for node in &mut cluster {
        collect_decisions(&mut node.decisions, 1).await;
    }

    // Wait for at least one heartbeat (>100ms interval)
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Propose from node-2 — should have learned leader via heartbeat
    cluster[2]
        .handle
        .propose("from-follower-2".to_string())
        .await
        .unwrap();

    let mut all = Vec::new();
    for node in &mut cluster {
        let decisions = collect_decisions(&mut node.decisions, 1).await;
        assert_eq!(decisions[0].value, "from-follower-2");
        all.push(decisions);
    }
    assert_consistent_decisions(&all);

    for node in cluster {
        drop(node.handle);
    }
}
