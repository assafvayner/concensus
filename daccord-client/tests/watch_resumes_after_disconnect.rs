//! Verifies the SDK's `watch()` stream auto-reconnects when the server is
//! restarted on the same port, and resumes from `last_yielded_slot + 1`
//! without gaps or duplicates.

mod common;

use std::time::Duration;

use bytes::Bytes;
use common::{restart_server_on_addr, TestServer};
use daccord_client::{Client, Decision};
use futures::StreamExt;
use tokio::sync::mpsc;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn watch_resumes_after_server_restart() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_test_writer()
        .try_init();

    let mut server = TestServer::start().await;
    let addr = server.addr();
    let (handle, decisions, algorithm) = server.split_node();

    let client = Client::connect(vec![server.endpoint()])
        .await
        .expect("client connect");

    // Drive the watch in a background task and forward decisions on a
    // channel so the main test can drive proposes without fighting the
    // stream's `&mut` borrow.
    let (decisions_tx, mut decisions_rx) = mpsc::unbounded_channel::<Decision>();
    let watch_client = client.clone();
    let watch_task = tokio::spawn(async move {
        let stream = watch_client.watch(0);
        tokio::pin!(stream);
        while let Some(item) = stream.next().await {
            match item {
                Ok(d) => {
                    if decisions_tx.send(d).is_err() {
                        break;
                    }
                }
                Err(e) => {
                    tracing::error!(error = ?e, "watch stream surfaced terminal error");
                    break;
                }
            }
        }
    });

    // Phase 1: propose three values pre-restart and confirm Watch yields them.
    for payload in [b"a".as_ref(), b"b", b"c"] {
        client
            .propose(Bytes::copy_from_slice(payload))
            .await
            .unwrap_or_else(|e| panic!("propose {:?}: {e}", payload));
    }

    let phase1 = collect_decisions_with_timeout(&mut decisions_rx, 3, Duration::from_secs(5)).await;
    assert_eq!(phase1.len(), 3, "expected 3 phase-1 decisions");
    assert_eq!(phase1[0].slot, 0);
    assert_eq!(phase1[0].payload.as_ref(), b"a");
    assert_eq!(phase1[1].slot, 1);
    assert_eq!(phase1[1].payload.as_ref(), b"b");
    assert_eq!(phase1[2].slot, 2);
    assert_eq!(phase1[2].payload.as_ref(), b"c");

    // Trigger reconnect: gracefully shut down the gRPC server. The Node and
    // DecisionLog are owned via Arc/clones outside the server task, so they
    // survive.
    server.shutdown_server().await;

    // Phase 2: propose two more values directly via the NodeHandle while the
    // gRPC server is down. They land in the DecisionLog and will be visible
    // once we restart the server.
    handle
        .propose(b"d".to_vec())
        .await
        .expect("propose d via handle");
    handle
        .propose(b"e".to_vec())
        .await
        .expect("propose e via handle");

    // Restart on the same port. The SDK's auto-reconnecting watch should
    // attempt to reopen the stream until this listener is up.
    let _restarted =
        restart_server_on_addr(addr, handle.clone(), decisions.clone(), algorithm).await;

    // Phase 3: collect the two new decisions delivered through the
    // reconnected watch.
    let phase2 =
        collect_decisions_with_timeout(&mut decisions_rx, 2, Duration::from_secs(15)).await;
    assert_eq!(phase2.len(), 2, "expected 2 phase-2 decisions");
    assert_eq!(phase2[0].slot, 3);
    assert_eq!(phase2[0].payload.as_ref(), b"d");
    assert_eq!(phase2[1].slot, 4);
    assert_eq!(phase2[1].payload.as_ref(), b"e");

    // Phase 2 saw slots 3 and 4. Confirm no straggler arrives in a small grace window.
    tokio::select! {
        _ = tokio::time::sleep(Duration::from_millis(50)) => {},
        msg = decisions_rx.recv() => panic!("unexpected extra decision: {msg:?}"),
    }

    drop(client);
    drop(_restarted);
    watch_task.abort();
}

async fn collect_decisions_with_timeout(
    rx: &mut mpsc::UnboundedReceiver<Decision>,
    count: usize,
    timeout: Duration,
) -> Vec<Decision> {
    let mut out = Vec::with_capacity(count);
    let deadline = tokio::time::Instant::now() + timeout;
    while out.len() < count {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        match tokio::time::timeout(remaining, rx.recv()).await {
            Ok(Some(d)) => out.push(d),
            Ok(None) => panic!("watch channel closed before {count} decisions arrived"),
            Err(_) => panic!(
                "timed out collecting {count} decisions; got {} so far: {out:?}",
                out.len()
            ),
        }
    }
    out
}
