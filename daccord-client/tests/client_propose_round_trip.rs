//! End-to-end SDK ↔ demo gRPC round-trip test.
//!
//! Exercises [`daccord_client::Client`] talking to a `daccord_demo`
//! gRPC service backed by a real single-node Paxos `Node` over channel
//! transport. Verifies propose, get_decisions, and status all behave on the
//! happy path.

mod common;

use bytes::Bytes;
use common::TestServer;
use daccord_client::{Algorithm, Client};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn propose_then_inspect_via_get_decisions_and_status() {
    let server = TestServer::start().await;

    let client = Client::connect(vec![server.endpoint()])
        .await
        .expect("client connect");

    // First proposal lands at slot 0.
    let d0 = client
        .propose(Bytes::from_static(b"hello"))
        .await
        .expect("propose hello");
    assert_eq!(d0.slot, 0, "first decision should be slot 0");
    assert_eq!(d0.payload.as_ref(), b"hello");

    // Second proposal lands at slot 1.
    let d1 = client
        .propose(Bytes::from_static(b"world"))
        .await
        .expect("propose world");
    assert_eq!(d1.slot, 1, "second decision should be slot 1");
    assert_eq!(d1.payload.as_ref(), b"world");

    // GetDecisions reflects both, in slot order.
    let (decisions, next_index) = client.get_decisions(0, None).await.expect("get_decisions");
    assert_eq!(
        decisions.len(),
        2,
        "expected 2 decisions, got {decisions:?}"
    );
    assert_eq!(decisions[0].slot, 0);
    assert_eq!(decisions[0].payload.as_ref(), b"hello");
    assert_eq!(decisions[1].slot, 1);
    assert_eq!(decisions[1].payload.as_ref(), b"world");
    assert_eq!(next_index, 2);

    // Status reflects the configured algorithm. Paxos reports
    // `log_len = 0` and `commit_index = None` by design (those are
    // Raft-specific fields exposed in the same struct), so we just assert
    // the call succeeds and the algorithm field round-trips correctly.
    let status = client.status().await.expect("status");
    assert_eq!(status.algorithm, Algorithm::Paxos);

    drop(client);
    drop(server);
}
