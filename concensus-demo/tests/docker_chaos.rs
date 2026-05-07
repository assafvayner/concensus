mod chaos_harness;

use chaos_harness::{fetch_decisions, propose, DockerCluster};
use std::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn raft_chaos_kill_node_and_partition() {
    let cluster = DockerCluster::up_3node_raft();
    cluster.wait_healthy(Duration::from_secs(60)).await;

    let entry = cluster.nodes[0].grpc.clone();

    // Phase 1: baseline load.
    for i in 0..30 {
        propose(&entry, &format!("v-{i}")).await.expect("propose");
    }
    tokio::time::sleep(Duration::from_secs(2)).await;

    let baseline = fetch_decisions(&entry).await.expect("fetch");
    assert!(
        baseline.len() >= 25,
        "expected ~30 decisions, got {}",
        baseline.len()
    );

    for (i, (slot, _)) in baseline.iter().enumerate() {
        assert_eq!(*slot, i as u64, "baseline slots must be contiguous");
    }

    // Phase 2: stop node-2.
    cluster.stop_node(1);
    tokio::time::sleep(Duration::from_secs(3)).await;

    for i in 30..50 {
        let _ = propose(&entry, &format!("v-{i}")).await;
    }
    tokio::time::sleep(Duration::from_secs(2)).await;

    let after_stop = fetch_decisions(&entry).await.expect("fetch");
    assert!(
        after_stop.len() >= baseline.len() + 15,
        "after stopping a follower, cluster should keep deciding (got {} -> {})",
        baseline.len(),
        after_stop.len()
    );

    // Phase 3: restart and observe catch-up.
    cluster.start_node(1);
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Phase 4: partition node-3.
    cluster.disconnect_node(2);
    tokio::time::sleep(Duration::from_secs(2)).await;
    for i in 50..70 {
        let _ = propose(&entry, &format!("v-{i}")).await;
    }
    tokio::time::sleep(Duration::from_secs(2)).await;

    let after_partition = fetch_decisions(&entry).await.expect("fetch");
    assert!(
        after_partition.len() > after_stop.len(),
        "majority side must keep deciding"
    );

    cluster.connect_node(2);
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Phase 5: final liveness check.
    for i in 70..75 {
        propose(&entry, &format!("final-{i}"))
            .await
            .expect("propose");
    }
    tokio::time::sleep(Duration::from_secs(2)).await;

    let final_decisions = fetch_decisions(&entry).await.expect("fetch");
    assert!(
        final_decisions.len() >= after_partition.len() + 4,
        "post-heal cluster must complete final batch"
    );

    for (i, (slot, _)) in final_decisions.iter().enumerate() {
        assert_eq!(*slot, i as u64, "slot gaps detected at position {i}");
    }
}
