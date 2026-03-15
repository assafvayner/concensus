mod helpers;

use std::collections::HashSet;

use helpers::{
    assert_consistent_decisions, collect_decisions, create_cluster, create_cluster_with_dead_node,
    create_unbounded_cluster,
};

#[tokio::test]
async fn single_value_consensus() {
    let mut cluster = create_cluster(3);

    cluster[0]
        .handle
        .propose("hello".to_string())
        .await
        .unwrap();

    let mut all_decisions = Vec::new();
    for node in &mut cluster {
        let decisions = collect_decisions(&mut node.decisions, 1).await;
        assert_eq!(decisions.len(), 1);
        assert_eq!(decisions[0].value, "hello");
        all_decisions.push(decisions);
    }

    assert_consistent_decisions(&all_decisions);

    // Shut down by dropping handles
    for node in cluster {
        drop(node.handle);
        // Don't wait for run_handle — dropping handle causes graceful shutdown
    }
}

#[tokio::test]
async fn multiple_sequential_proposals() {
    let mut cluster = create_cluster(3);

    let values = vec!["alpha", "beta", "gamma"];
    for v in &values {
        cluster[0].handle.propose(v.to_string()).await.unwrap();
        // Small delay to ensure sequential processing
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    let expected: HashSet<String> = values.iter().map(|v| v.to_string()).collect();

    let mut all_decisions = Vec::new();
    for node in &mut cluster {
        let decisions = collect_decisions(&mut node.decisions, 3).await;
        let decided_values: HashSet<String> = decisions.iter().map(|d| d.value.clone()).collect();
        assert_eq!(decided_values, expected);
        all_decisions.push(decisions);
    }

    assert_consistent_decisions(&all_decisions);

    for node in cluster {
        drop(node.handle);
    }
}

#[tokio::test]
async fn proposals_from_different_nodes() {
    let mut cluster = create_cluster(3);

    cluster[0]
        .handle
        .propose("from-0".to_string())
        .await
        .unwrap();
    cluster[1]
        .handle
        .propose("from-1".to_string())
        .await
        .unwrap();
    cluster[2]
        .handle
        .propose("from-2".to_string())
        .await
        .unwrap();

    let expected: HashSet<String> = ["from-0", "from-1", "from-2"]
        .iter()
        .map(|v| v.to_string())
        .collect();

    let mut all_decisions = Vec::new();
    for node in &mut cluster {
        let decisions = collect_decisions(&mut node.decisions, 3).await;
        let decided_values: HashSet<String> = decisions.iter().map(|d| d.value.clone()).collect();
        assert_eq!(decided_values, expected);
        all_decisions.push(decisions);
    }

    assert_consistent_decisions(&all_decisions);

    for node in cluster {
        drop(node.handle);
    }
}

#[tokio::test]
async fn single_node_cluster() {
    let mut cluster = create_cluster(1);

    cluster[0].handle.propose("solo".to_string()).await.unwrap();

    let decisions = collect_decisions(&mut cluster[0].decisions, 1).await;
    assert_eq!(decisions.len(), 1);
    assert_eq!(decisions[0].value, "solo");
    assert_eq!(decisions[0].slot, 0);

    drop(cluster[0].handle.clone());
}

#[tokio::test]
async fn quorum_with_dead_node() {
    let mut cluster = create_cluster_with_dead_node(3);

    cluster[0]
        .handle
        .propose("despite-failure".to_string())
        .await
        .unwrap();

    // Only check nodes 0 and 1 (node 2 is dead)
    let mut all_decisions = Vec::new();
    for node in cluster.iter_mut().take(2) {
        let decisions = collect_decisions(&mut node.decisions, 1).await;
        assert_eq!(decisions.len(), 1);
        assert_eq!(decisions[0].value, "despite-failure");
        all_decisions.push(decisions);
    }

    assert_consistent_decisions(&all_decisions);

    for node in cluster {
        drop(node.handle);
        node.run_handle.abort();
    }
}

#[tokio::test]
async fn rapid_concurrent_proposals() {
    let mut cluster = create_unbounded_cluster(3);

    let num_proposals = 50;
    let expected: HashSet<String> = (0..num_proposals).map(|i| format!("value-{}", i)).collect();

    for i in 0..num_proposals {
        cluster[0]
            .handle
            .propose(format!("value-{}", i))
            .await
            .unwrap();
    }

    let mut all_decisions = Vec::new();
    for node in &mut cluster {
        let decisions = collect_decisions(&mut node.decisions, num_proposals).await;
        let decided_values: HashSet<String> = decisions.iter().map(|d| d.value.clone()).collect();
        assert_eq!(decided_values, expected);
        all_decisions.push(decisions);
    }

    assert_consistent_decisions(&all_decisions);

    for node in cluster {
        drop(node.handle);
    }
}
