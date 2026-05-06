mod helpers;

use std::time::Duration;

use helpers::{
    assert_safety_invariant, collect_decisions_with_timeout, create_raft_cluster, ClusterNode,
};

/// Detects which node is most likely the current leader by proposing a probe
/// at every node and observing which decisions channel emits first. With the
/// channel transport, the leader's own commit reaches its decisions channel
/// fastest. Returns `Some(idx)` if any node decided within 2s, else `None`.
async fn detect_active_leader_index(cluster: &mut [ClusterNode]) -> Option<usize> {
    let probe = format!(
        "probe-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    for node in cluster.iter() {
        let _ = node.handle.propose(probe.clone()).await;
    }
    // Race the per-node decision recv timeouts.
    let mut futs: Vec<_> = cluster
        .iter_mut()
        .enumerate()
        .map(|(i, n)| {
            Box::pin(async move {
                let r = tokio::time::timeout(Duration::from_secs(2), n.decisions.recv()).await;
                (i, r.is_ok() && r.unwrap().is_some())
            })
        })
        .collect();
    while !futs.is_empty() {
        let (result, _, remaining) = futures::future::select_all(futs).await;
        if result.1 {
            return Some(result.0);
        }
        futs = remaining;
    }
    None
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leader_failover_resumes_progress() {
    let mut cluster = create_raft_cluster(3);
    tokio::time::sleep(Duration::from_millis(800)).await;
    let leader_idx = detect_active_leader_index(&mut cluster)
        .await
        .expect("a leader should be elected");
    cluster[leader_idx].run_handle.abort();
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let survivor = (leader_idx + 1) % cluster.len();
    cluster[survivor]
        .handle
        .propose("after-failover".into())
        .await
        .unwrap();
    let mut decisions_seen = 0;
    for (i, node) in cluster.iter_mut().enumerate() {
        if i == leader_idx {
            continue;
        }
        for _ in 0..3 {
            match tokio::time::timeout(Duration::from_secs(5), node.decisions.recv()).await {
                Ok(Some(_)) => decisions_seen += 1,
                _ => break,
            }
        }
    }
    assert!(
        decisions_seen >= 1,
        "expected survivors to commit at least one post-failover decision"
    );
    let _ = cluster;
}
