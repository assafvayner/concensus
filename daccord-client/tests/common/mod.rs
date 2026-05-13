//! Shared in-process gRPC test harness for the SDK integration tests.
//!
//! Each test file picks up only the parts it needs, so cargo will warn about
//! unused items when a single binary doesn't use them all.
#![allow(dead_code)]

use std::net::SocketAddr;
use std::sync::Arc;

use daccord_demo::consensus_proto::consensus_service_server::ConsensusServiceServer;
use daccord_demo::service::{Algorithm, ConsensusServiceImpl, DecisionLog};
use daccord_demo::test_support::{start_in_process_node, InProcessNode};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

/// In-process gRPC server backed by a single-node Paxos cluster over channel
/// transport.
///
/// Drops cleanly: dropping the `TestServer` aborts the gRPC and node tasks.
pub struct TestServer {
    addr: SocketAddr,
    node: InProcessNode,
    server_task: JoinHandle<()>,
    shutdown: Option<oneshot::Sender<()>>,
}

impl TestServer {
    /// Start a gRPC service against a fresh single-node Paxos cluster bound
    /// to `127.0.0.1:0` (OS-assigned port).
    pub async fn start() -> Self {
        let node = start_in_process_node("test-node");
        let (addr, server_task, shutdown) = spawn_server(
            SocketAddr::from(([127, 0, 0, 1], 0)),
            node.handle.clone(),
            node.decisions.clone(),
            node.algorithm,
        )
        .await;
        Self {
            addr,
            node,
            server_task,
            shutdown: Some(shutdown),
        }
    }

    /// Endpoint URL the SDK should connect to.
    pub fn endpoint(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// Bound socket address (useful for restarting the server on the same
    /// port).
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Detach the in-process node, leaving the server-side handles in this
    /// `TestServer` only. Used by the watch-reconnect test to keep the node
    /// alive across server-only restarts.
    ///
    /// Returns clones of the `NodeHandle` and the `Arc<DecisionLog>` so the
    /// caller can both propose and inspect the log directly while the server
    /// is down.
    pub fn split_node(&self) -> (daccord::NodeHandle<Vec<u8>>, Arc<DecisionLog>, Algorithm) {
        (
            self.node.handle.clone(),
            self.node.decisions.clone(),
            self.node.algorithm,
        )
    }

    /// Forcibly shut down the gRPC server (aborts the serve task).
    ///
    /// We use abort rather than tonic's graceful shutdown because tests want
    /// to simulate an abrupt connection drop: graceful shutdown waits for
    /// active server-streaming RPCs (e.g. `Watch`) to complete on their own,
    /// which they never do under normal operation.
    ///
    /// The underlying node and decision log are left alive; only the gRPC
    /// surface is torn down. After this returns, the bound port is released
    /// and ready to be rebound.
    pub async fn shutdown_server(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        self.server_task.abort();
        let _ = (&mut self.server_task).await;
        // Give the OS a brief moment to release the bound port. macOS in
        // particular sometimes needs a tick before the listening socket is
        // gone from the kernel's bookkeeping.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        self.server_task.abort();
        self.node.shutdown();
    }
}

/// Wrap an already-bound `TcpListener` in a tonic gRPC server, spawn the
/// serve loop, and return the actual bound address plus task and shutdown
/// handles. Both `spawn_server` and `restart_server_on_addr` delegate here.
fn bind_and_serve(
    listener: TcpListener,
    handle: daccord::NodeHandle<Vec<u8>>,
    decisions: Arc<DecisionLog>,
    algorithm: Algorithm,
    shutdown: oneshot::Receiver<()>,
) -> (SocketAddr, JoinHandle<()>) {
    let local_addr = listener.local_addr().expect("local_addr");
    let stream = TcpListenerStream::new(listener);
    let svc = ConsensusServiceImpl::new(handle, decisions, algorithm);
    let server = Server::builder()
        .add_service(ConsensusServiceServer::new(svc))
        .serve_with_incoming_shutdown(stream, async move {
            let _ = shutdown.await;
        });
    let join = tokio::spawn(async move {
        if let Err(e) = server.await {
            tracing::error!(error = %e, "test gRPC server error");
        }
    });
    (local_addr, join)
}

/// Bind the gRPC server to `bind_addr` (use port 0 for OS-assigned), spawn
/// the serve loop, and return the actual bound address plus shutdown handles.
async fn spawn_server(
    bind_addr: SocketAddr,
    handle: daccord::NodeHandle<Vec<u8>>,
    decisions: Arc<DecisionLog>,
    algorithm: Algorithm,
) -> (SocketAddr, JoinHandle<()>, oneshot::Sender<()>) {
    let listener = bind_with_retry(bind_addr).await;
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let (addr, server_task) = bind_and_serve(listener, handle, decisions, algorithm, shutdown_rx);
    (addr, server_task, shutdown_tx)
}

async fn bind_with_retry(addr: SocketAddr) -> TcpListener {
    let mut attempts = 0;
    loop {
        match TcpListener::bind(addr).await {
            Ok(listener) => return listener,
            Err(e) if attempts < 50 => {
                attempts += 1;
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                tracing::debug!(error = %e, attempt = attempts, "rebind retry");
            }
            Err(e) => panic!("could not bind {addr}: {e}"),
        }
    }
}

/// Restart the gRPC server bound to a specific socket address against an
/// already-running node + decision log. The bound address is typically the
/// same `SocketAddr` the previous `TestServer` exposed so existing client
/// connections reconnect transparently.
pub async fn restart_server_on_addr(
    addr: SocketAddr,
    handle: daccord::NodeHandle<Vec<u8>>,
    decisions: Arc<DecisionLog>,
    algorithm: Algorithm,
) -> RestartedServer {
    let listener = bind_with_retry(addr).await;
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let (local_addr, server_task) =
        bind_and_serve(listener, handle, decisions, algorithm, shutdown_rx);
    assert_eq!(local_addr, addr, "rebound to a different address");

    RestartedServer {
        addr,
        server_task: Some(server_task),
        shutdown: Some(shutdown_tx),
    }
}

/// Handle for a server that was restarted via [`restart_server_on_addr`].
/// Aborts cleanly on drop.
pub struct RestartedServer {
    addr: SocketAddr,
    server_task: Option<JoinHandle<()>>,
    shutdown: Option<oneshot::Sender<()>>,
}

impl RestartedServer {
    pub fn endpoint(&self) -> String {
        format!("http://{}", self.addr)
    }
}

impl Drop for RestartedServer {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        if let Some(task) = self.server_task.take() {
            task.abort();
        }
    }
}
