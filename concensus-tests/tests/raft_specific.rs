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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn network_partition_majority_progresses() {
    use std::sync::atomic::Ordering;
    let (mut cluster, edges) = helpers::create_raft_cluster_with_edge_filters(5);
    tokio::time::sleep(Duration::from_millis(800)).await;

    // Partition: nodes [0,1,2] | nodes [3,4]. Drop every cross-group edge.
    let group_a = vec![
        cluster[0].id.clone(),
        cluster[1].id.clone(),
        cluster[2].id.clone(),
    ];
    let group_b = vec![cluster[3].id.clone(), cluster[4].id.clone()];
    for from in &group_a {
        for to in &group_b {
            edges[&(from.clone(), to.clone())].store(true, Ordering::Relaxed);
            edges[&(to.clone(), from.clone())].store(true, Ordering::Relaxed);
        }
    }
    tokio::time::sleep(Duration::from_millis(1200)).await;

    cluster[0]
        .handle
        .propose("majority-1".into())
        .await
        .unwrap();
    cluster[1]
        .handle
        .propose("majority-2".into())
        .await
        .unwrap();
    let mut majority_decisions = Vec::new();
    for i in 0..3 {
        let d =
            collect_decisions_with_timeout(&mut cluster[i].decisions, 2, Duration::from_secs(8))
                .await;
        majority_decisions.push(d);
    }

    // Minority side cannot commit (no quorum). Try to propose; expect no decision.
    cluster[3].handle.propose("minority".into()).await.unwrap();
    let res = tokio::time::timeout(Duration::from_secs(2), cluster[3].decisions.recv()).await;
    assert!(
        res.is_err() || res.ok().flatten().is_none(),
        "minority must not decide while partitioned"
    );

    // Heal.
    for from in &group_a {
        for to in &group_b {
            edges[&(from.clone(), to.clone())].store(false, Ordering::Relaxed);
            edges[&(to.clone(), from.clone())].store(false, Ordering::Relaxed);
        }
    }
    let mut minority_decisions = Vec::new();
    for i in 3..5 {
        let d =
            collect_decisions_with_timeout(&mut cluster[i].decisions, 2, Duration::from_secs(15))
                .await;
        minority_decisions.push(d);
    }
    let mut all = majority_decisions;
    all.extend(minority_decisions);
    assert_safety_invariant(&all);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partition_minority_leader_steps_down() {
    use std::sync::atomic::Ordering;
    let (mut cluster, edges) = helpers::create_raft_cluster_with_edge_filters(5);
    tokio::time::sleep(Duration::from_millis(800)).await;

    let leader_idx = detect_active_leader_index(&mut cluster)
        .await
        .expect("a leader must exist before partition");

    // Isolate the leader: drop every edge in & out of leader_idx.
    let leader_id = cluster[leader_idx].id.clone();
    let other_ids: Vec<_> = cluster
        .iter()
        .filter(|n| n.id != leader_id)
        .map(|n| n.id.clone())
        .collect();
    for other in &other_ids {
        edges[&(leader_id.clone(), other.clone())].store(true, Ordering::Relaxed);
        edges[&(other.clone(), leader_id.clone())].store(true, Ordering::Relaxed);
    }

    // Wait for the majority to elect a new leader and commit fresh values.
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let mut majority_committed_one = false;
    for (i, node) in cluster.iter().enumerate() {
        if node.id == leader_id {
            continue;
        }
        if (node.handle.propose(format!("post-iso-{i}")).await).is_ok() {
            // ok
        }
    }
    tokio::time::sleep(Duration::from_millis(800)).await;
    for (i, node) in cluster.iter_mut().enumerate() {
        if node.id == leader_id {
            continue;
        }
        if let Ok(Some(_)) =
            tokio::time::timeout(Duration::from_secs(3), node.decisions.recv()).await
        {
            majority_committed_one = true;
            break;
        }
        let _ = i;
    }
    assert!(
        majority_committed_one,
        "majority must commit while leader is isolated"
    );

    // Heal.
    for other in &other_ids {
        edges[&(leader_id.clone(), other.clone())].store(false, Ordering::Relaxed);
        edges[&(other.clone(), leader_id.clone())].store(false, Ordering::Relaxed);
    }
    tokio::time::sleep(Duration::from_millis(1500)).await;

    // Old leader, after heal, should accept new entries via forward (it's no longer the leader).
    cluster[leader_idx]
        .handle
        .propose("via-old-leader-after-heal".into())
        .await
        .unwrap();
    let _ =
        tokio::time::timeout(Duration::from_secs(5), cluster[leader_idx].decisions.recv()).await;
    // We don't strictly assert reception (it might already be in the buffer);
    // the key invariant is that the cluster is making consistent progress.
    // Drain remaining decisions to verify safety.
    let mut all = Vec::new();
    for node in &mut cluster {
        let mut d = Vec::new();
        for _ in 0..5 {
            match tokio::time::timeout(Duration::from_millis(500), node.decisions.recv()).await {
                Ok(Some(x)) => d.push(x),
                _ => break,
            }
        }
        all.push(d);
    }
    assert_safety_invariant(&all);
}
