mod helpers;

use helpers::create_cluster;
use tokio::time::{timeout, Duration};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropping_all_handles_causes_clean_shutdown() {
    let cluster = create_cluster(3);

    let mut run_handles = Vec::new();
    for node in cluster {
        drop(node.handle);
        drop(node.decisions);
        run_handles.push(node.run_handle);
    }

    for rh in run_handles {
        let result = timeout(Duration::from_secs(5), rh)
            .await
            .expect("node did not shut down within 5 seconds")
            .expect("task panicked");
        assert!(result.is_ok(), "node.run() returned an error: {:?}", result);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_during_active_proposal() {
    let cluster = create_cluster(3);

    // Propose a value without waiting on the result. The propose future will
    // be cancelled when the cluster shuts down — but the spawned task holds
    // a NodeHandle clone, so abort it to release that handle before waiting
    // for the event loops to exit.
    let h = cluster[0].handle.clone();
    let propose_handle = tokio::spawn(async move { h.propose("inflight".to_string()).await });

    // Give the event loop a moment to register the proposal.
    tokio::time::sleep(Duration::from_millis(50)).await;
    propose_handle.abort();
    let _ = propose_handle.await;

    let mut run_handles = Vec::new();
    for node in cluster {
        drop(node.handle);
        drop(node.decisions);
        run_handles.push(node.run_handle);
    }

    for rh in run_handles {
        let _ = timeout(Duration::from_secs(5), rh)
            .await
            .expect("node did not shut down within 5 seconds");
    }
}

#[tokio::test]
async fn single_node_shuts_down_on_handle_drop() {
    let cluster = create_cluster(1);
    let node = cluster.into_iter().next().unwrap();

    drop(node.handle);
    drop(node.decisions);

    let result = timeout(Duration::from_secs(5), node.run_handle)
        .await
        .expect("node did not shut down within 5 seconds")
        .expect("task panicked");
    assert!(result.is_ok(), "node.run() returned an error: {:?}", result);
}
