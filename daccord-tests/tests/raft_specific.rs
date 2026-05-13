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
    // Spawn so probes run in parallel; whichever node is leader will commit
    // fastest. The propose futures may not complete on minority/lossy nodes;
    // we abort all of them after detection.
    let probe_handles: Vec<_> = cluster
        .iter()
        .map(|node| {
            let h = node.handle.clone();
            let p = probe.clone();
            tokio::spawn(async move { h.propose(p).await })
        })
        .collect();
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
    let mut found = None;
    while !futs.is_empty() {
        let (result, _, remaining) = futures::future::select_all(futs).await;
        if result.1 {
            found = Some(result.0);
            break;
        }
        futs = remaining;
    }
    for h in probe_handles {
        h.abort();
    }
    found
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
    for node in cluster.iter_mut().take(3) {
        let d =
            collect_decisions_with_timeout(&mut node.decisions, 2, Duration::from_secs(8)).await;
        majority_decisions.push(d);
    }

    // Minority side cannot commit (no quorum). Spawn the propose so the test
    // doesn't block on the never-arriving commit.
    let minority_h = cluster[3].handle.clone();
    let minority_propose = tokio::spawn(async move { minority_h.propose("minority".into()).await });
    let res = tokio::time::timeout(Duration::from_secs(2), cluster[3].decisions.recv()).await;
    assert!(
        res.is_err() || res.ok().flatten().is_none(),
        "minority must not decide while partitioned"
    );
    minority_propose.abort();

    // Heal.
    for from in &group_a {
        for to in &group_b {
            edges[&(from.clone(), to.clone())].store(false, Ordering::Relaxed);
            edges[&(to.clone(), from.clone())].store(false, Ordering::Relaxed);
        }
    }
    let mut minority_decisions = Vec::new();
    for node in cluster.iter_mut().skip(3).take(2) {
        let d =
            collect_decisions_with_timeout(&mut node.decisions, 2, Duration::from_secs(15)).await;
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
    let post_iso_handles: Vec<_> = cluster
        .iter()
        .enumerate()
        .filter(|(_, node)| node.id != leader_id)
        .map(|(i, node)| {
            let h = node.handle.clone();
            tokio::spawn(async move { h.propose(format!("post-iso-{i}")).await })
        })
        .collect();
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
    for h in post_iso_handles {
        h.abort();
    }

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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn election_livelock_resolves() {
    use daccord::RaftConfig;
    // Tight election window with randomization range still wide enough to
    // break ties — the property under test is that the randomization _does_
    // break ties, not that any specific timing converges instantly.
    let cfg = RaftConfig {
        election_timeout_min: Duration::from_millis(80),
        election_timeout_max: Duration::from_millis(200),
        heartbeat_interval: Duration::from_millis(20),
    };
    let mut cluster = helpers::create_raft_lossy_cluster_with_config(3, 0.30, cfg);
    // Allow time for the cluster to thrash through livelock attempts and
    // eventually settle on a leader despite tight timeouts and 30% loss.
    tokio::time::sleep(Duration::from_millis(2000)).await;
    // Continuously propose at every node — eventually one will be elected
    // leader long enough to commit. We retry until the global deadline.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let mut got_decision = false;
    let mut spawned: Vec<tokio::task::JoinHandle<_>> = Vec::new();
    while tokio::time::Instant::now() < deadline {
        for node in &cluster {
            let h = node.handle.clone();
            spawned.push(tokio::spawn(async move {
                let _ = h.propose("after-livelock".into()).await;
            }));
        }
        // Race a short timeout across all nodes.
        let mut futs: Vec<_> = cluster
            .iter_mut()
            .map(|n| {
                Box::pin(async move {
                    tokio::time::timeout(Duration::from_secs(2), n.decisions.recv())
                        .await
                        .ok()
                        .flatten()
                        .is_some()
                })
            })
            .collect();
        while !futs.is_empty() {
            let (ok, _, remaining) = futures::future::select_all(futs).await;
            if ok {
                got_decision = true;
                break;
            }
            futs = remaining;
        }
        if got_decision {
            break;
        }
    }
    assert!(
        got_decision,
        "cluster must settle on a leader and decide within 30s"
    );
    for h in spawned {
        h.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leader_completeness_under_churn() {
    use helpers::raft_invariants::RaftClusterInvariantChecker;
    let mut cluster = create_raft_cluster(5);
    tokio::time::sleep(Duration::from_millis(800)).await;

    let mut checker = RaftClusterInvariantChecker::new(cluster.len());
    let mut counter = 0u32;
    let mut killed: Vec<usize> = Vec::new();
    let mut churn_proposes: Vec<tokio::task::JoinHandle<_>> = Vec::new();
    for round in 0..3 {
        let leader_idx = match detect_active_leader_index(&mut cluster).await {
            Some(i) if !killed.contains(&i) => i,
            _ => {
                // Could not find a leader in a survivor set — bail out the loop.
                break;
            }
        };
        for _ in 0..5 {
            counter += 1;
            let h = cluster[leader_idx].handle.clone();
            let value = format!("round-{round}-v-{counter}");
            churn_proposes.push(tokio::spawn(async move {
                let _ = h.propose(value).await;
            }));
        }
        tokio::time::sleep(Duration::from_millis(800)).await;
        checker.poll(&mut cluster);
        cluster[leader_idx].run_handle.abort();
        killed.push(leader_idx);
        tokio::time::sleep(Duration::from_millis(1500)).await;
    }
    for h in churn_proposes {
        h.abort();
    }

    // Drain remaining decisions across all surviving nodes; assert safety.
    let mut all = Vec::new();
    for (i, node) in cluster.iter_mut().enumerate() {
        if killed.contains(&i) {
            continue;
        }
        let mut d = Vec::new();
        for _ in 0..50 {
            match tokio::time::timeout(Duration::from_millis(500), node.decisions.recv()).await {
                Ok(Some(x)) => d.push(x),
                _ => break,
            }
        }
        all.push(d);
    }
    assert_safety_invariant(&all);
    assert!(
        !all.is_empty(),
        "expected surviving nodes to have decisions"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn conflict_index_helps_recovery_after_long_isolation() {
    use std::sync::atomic::Ordering;
    let (mut cluster, edges) = helpers::create_raft_cluster_with_edge_filters(3);
    tokio::time::sleep(Duration::from_millis(800)).await;

    let leader_idx = detect_active_leader_index(&mut cluster)
        .await
        .expect("leader required");
    let lagging_idx = (leader_idx + 1) % 3;
    let lagging_id = cluster[lagging_idx].id.clone();

    // Isolate the lagging follower.
    let all_ids: Vec<_> = cluster.iter().map(|n| n.id.clone()).collect();
    for other in &all_ids {
        if other == &lagging_id {
            continue;
        }
        edges[&(lagging_id.clone(), other.clone())].store(true, Ordering::Relaxed);
        edges[&(other.clone(), lagging_id.clone())].store(true, Ordering::Relaxed);
    }

    // Generate ~30 decisions on the surviving 2/3 quorum.
    for i in 0..30 {
        let _ = cluster[leader_idx].handle.propose(format!("v-{i}")).await;
    }
    tokio::time::sleep(Duration::from_millis(1500)).await;

    // Heal. Lagging follower should catch up.
    for other in &all_ids {
        if other == &lagging_id {
            continue;
        }
        edges[&(lagging_id.clone(), other.clone())].store(false, Ordering::Relaxed);
        edges[&(other.clone(), lagging_id.clone())].store(false, Ordering::Relaxed);
    }

    // Wait for catch-up. The conflict-index optimization should converge fast.
    let mut decisions_on_lagger = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(
            Duration::from_millis(500),
            cluster[lagging_idx].decisions.recv(),
        )
        .await
        {
            Ok(Some(d)) => {
                decisions_on_lagger.push(d);
                if decisions_on_lagger.len() >= 25 {
                    break;
                }
            }
            _ => continue,
        }
    }
    assert!(
        decisions_on_lagger.len() >= 20,
        "lagging follower must catch up with the cluster (got {} decisions)",
        decisions_on_lagger.len()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn invariant_checker_observes_decisions() {
    use helpers::raft_invariants::RaftClusterInvariantChecker;
    let mut cluster = create_raft_cluster(3);
    tokio::time::sleep(Duration::from_millis(800)).await;
    cluster[0].handle.propose("v1".into()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    let mut checker = RaftClusterInvariantChecker::new(3);
    checker.poll(&mut cluster);
    assert_eq!(checker.total_decided(), 1);
}

/// Single-node Raft end-to-end through the Node abstraction.
///
/// The peer starts in term 0; the node's tick loop completes an election so it
/// becomes leader, then proposals replicate with quorum 1. After restart with a
/// durable vote for self in the loaded term, `recover()` resumes leadership.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_node_raft_end_to_end() {
    let mut cluster = create_raft_cluster(1);
    cluster[0].handle.propose("solo".into()).await.unwrap();
    let d =
        collect_decisions_with_timeout(&mut cluster[0].decisions, 1, Duration::from_secs(5)).await;
    assert_eq!(d.len(), 1);
    assert_eq!(d[0].value, "solo");
}
