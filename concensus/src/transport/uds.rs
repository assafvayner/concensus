//! Unix domain socket transport with length-prefixed framing.
//!
//! Identical wire protocol to the [`tcp`](super::tcp) transport (4-byte BE
//! length prefix + payload) but communicates over Unix sockets instead of TCP.
//! Useful when all nodes run on the same host or share a filesystem (e.g. a
//! Docker shared volume).
//!
//! Stale socket files are automatically removed on bind and rebind.

use std::path::{Path, PathBuf};

use async_trait::async_trait;
use bytes::Bytes;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::unix::OwnedWriteHalf;
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{mpsc, Mutex};

use crate::config::{NodeId, PeerInfo};
use crate::error::TransportError;
use crate::transport::{MessageReceiver, MessageSender};

const MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024; // 16 MiB
const LISTENER_CHANNEL_CAPACITY: usize = 1024;
const INITIAL_REBIND_DELAY_MS: u64 = 100;
const MAX_REBIND_DELAY_MS: u64 = 5000;

// ─── UdsSender ───────────────────────────────────────────────────────────────

/// Sends length-prefixed messages to a single Unix domain socket peer.
///
/// Behaves identically to [`TcpSender`](super::tcp::TcpSender) but connects
/// to a filesystem socket path instead of a TCP address.
pub struct UdsSender {
    path: PathBuf,
    conn: Mutex<Option<OwnedWriteHalf>>,
}

impl UdsSender {
    /// Creates a new sender targeting the given socket path. No connection is
    /// opened until the first message is sent.
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            conn: Mutex::new(None),
        }
    }
}

#[async_trait]
impl MessageSender for UdsSender {
    async fn send(&self, data: Bytes) -> Result<(), TransportError> {
        let mut guard = self.conn.lock().await;

        // Lazy connect
        if guard.is_none() {
            let stream = UnixStream::connect(&self.path)
                .await
                .map_err(|e| TransportError::Other(Box::new(e)))?;
            let (_read_half, write_half) = stream.into_split();
            *guard = Some(write_half);
        }

        let writer = guard.as_mut().unwrap();

        // Write 4-byte BE length prefix
        let len = data.len() as u32;
        if writer.write_all(&len.to_be_bytes()).await.is_err() {
            *guard = None;
            return Err(TransportError::Closed);
        }

        // Write payload
        if writer.write_all(&data).await.is_err() {
            *guard = None;
            return Err(TransportError::Closed);
        }

        // Flush
        if writer.flush().await.is_err() {
            *guard = None;
            return Err(TransportError::Closed);
        }

        Ok(())
    }
}

// ─── UdsReceiver ─────────────────────────────────────────────────────────────

/// Listens for incoming Unix socket connections and demultiplexes received
/// messages into a single stream.
///
/// Behaves identically to [`TcpReceiver`](super::tcp::TcpReceiver) but
/// listens on a filesystem socket path.
pub struct UdsReceiver {
    incoming_rx: mpsc::Receiver<Bytes>,
}

impl UdsReceiver {
    /// Binds a Unix listener at `path` and starts accepting connections.
    /// Any existing socket file at `path` is removed first.
    pub async fn bind(path: impl AsRef<Path>) -> Result<Self, TransportError> {
        let path = path.as_ref().to_path_buf();

        // Remove stale socket file if it exists
        let _ = std::fs::remove_file(&path);

        let listener = UnixListener::bind(&path).map_err(|e| TransportError::Other(Box::new(e)))?;

        let (incoming_tx, incoming_rx) = mpsc::channel(LISTENER_CHANNEL_CAPACITY);

        tokio::spawn(Self::accept_loop(listener, path, incoming_tx));

        Ok(Self { incoming_rx })
    }

    pub(crate) async fn accept_loop(
        mut listener: UnixListener,
        path: PathBuf,
        incoming_tx: mpsc::Sender<Bytes>,
    ) {
        loop {
            match listener.accept().await {
                Ok((stream, _addr)) => {
                    tracing::debug!(?path, "accepted UDS connection");
                    let tx = incoming_tx.clone();
                    tokio::spawn(Self::reader_task(stream, tx));
                }
                Err(e) => {
                    tracing::warn!(error = %e, "UDS listener error, attempting rebind");

                    let mut delay_ms = INITIAL_REBIND_DELAY_MS;
                    loop {
                        if incoming_tx.is_closed() {
                            return; // Receiver dropped, shut down
                        }

                        // Add jitter: ±25%
                        let jitter =
                            (delay_ms as f64 * 0.25 * (2.0 * rand::random::<f64>() - 1.0)) as i64;
                        let actual_delay = (delay_ms as i64 + jitter).max(10) as u64;
                        tokio::time::sleep(std::time::Duration::from_millis(actual_delay)).await;

                        // Remove stale socket before rebind
                        let _ = std::fs::remove_file(&path);

                        match UnixListener::bind(&path) {
                            Ok(new_listener) => {
                                tracing::info!("UDS listener rebound successfully");
                                listener = new_listener;
                                break;
                            }
                            Err(e) => {
                                tracing::warn!(error = %e, delay_ms, "rebind failed, retrying");
                                delay_ms = (delay_ms * 2).min(MAX_REBIND_DELAY_MS);
                            }
                        }
                    }
                }
            }
        }
    }

    async fn reader_task(stream: UnixStream, incoming_tx: mpsc::Sender<Bytes>) {
        let (mut reader, _writer) = stream.into_split();

        loop {
            // Read 4-byte length prefix
            let mut len_buf = [0u8; 4];
            if reader.read_exact(&mut len_buf).await.is_err() {
                break;
            }
            let len = u32::from_be_bytes(len_buf) as usize;

            // Validate size
            if len > MAX_MESSAGE_SIZE {
                tracing::warn!(len, "message exceeds max size, dropping connection");
                break;
            }

            // Read payload
            let mut payload = vec![0u8; len];
            if reader.read_exact(&mut payload).await.is_err() {
                break;
            }

            if incoming_tx.send(Bytes::from(payload)).await.is_err() {
                break; // Receiver dropped
            }
        }
    }
}

#[async_trait]
impl MessageReceiver for UdsReceiver {
    async fn recv(&mut self) -> Result<Bytes, TransportError> {
        self.incoming_rx.recv().await.ok_or(TransportError::Closed)
    }
}

// ─── UdsTransport factory ────────────────────────────────────────────────────

/// Factory for creating a matched set of UDS senders and a receiver.
pub struct UdsTransport;

impl UdsTransport {
    /// Creates [`PeerInfo`] entries for all peers and binds a [`UdsReceiver`]
    /// at `bind_path`.
    ///
    /// This is the recommended way to set up UDS transport. It returns
    /// everything needed to construct a [`Node`](crate::Node).
    pub async fn create(
        bind_path: impl AsRef<Path>,
        peers: Vec<(NodeId, PathBuf)>,
    ) -> Result<(Vec<PeerInfo<UdsSender>>, UdsReceiver), TransportError> {
        let receiver = UdsReceiver::bind(bind_path).await?;
        let peers = peers
            .into_iter()
            .map(|(id, path)| PeerInfo {
                id,
                sender: UdsSender::new(path),
            })
            .collect();
        Ok((peers, receiver))
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_socket_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("concensus-test-{}-{}", name, std::process::id()))
    }

    #[tokio::test]
    async fn uds_sender_receiver_roundtrip() {
        let sock_path = temp_socket_path("roundtrip");
        let _ = std::fs::remove_file(&sock_path);

        let listener = UnixListener::bind(&sock_path).unwrap();

        let (tx, rx) = mpsc::channel(16);
        tokio::spawn(UdsReceiver::accept_loop(listener, sock_path.clone(), tx));
        let mut receiver = UdsReceiver { incoming_rx: rx };

        let sender = UdsSender::new(&sock_path);

        let payload = Bytes::from("hello-uds");
        sender.send(payload.clone()).await.unwrap();

        let received = tokio::time::timeout(std::time::Duration::from_secs(2), receiver.recv())
            .await
            .expect("timed out")
            .expect("recv failed");

        assert_eq!(received, payload);

        let _ = std::fs::remove_file(&sock_path);
    }

    #[tokio::test]
    async fn uds_sender_reconnects_after_failure() {
        let sock_path = temp_socket_path("reconnect");
        let _ = std::fs::remove_file(&sock_path);

        let listener = UnixListener::bind(&sock_path).unwrap();

        let (tx, rx) = mpsc::channel(16);
        tokio::spawn(UdsReceiver::accept_loop(listener, sock_path.clone(), tx));
        let mut receiver = UdsReceiver { incoming_rx: rx };

        let sender = UdsSender::new(&sock_path);

        // First send — establishes connection
        sender.send(Bytes::from("msg-1")).await.unwrap();
        let r1 = tokio::time::timeout(std::time::Duration::from_secs(2), receiver.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(r1, Bytes::from("msg-1"));

        // Force-close the connection by clearing it
        {
            let mut guard = sender.conn.lock().await;
            *guard = None;
        }

        // Second send — should reconnect
        sender.send(Bytes::from("msg-2")).await.unwrap();
        let r2 = tokio::time::timeout(std::time::Duration::from_secs(2), receiver.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(r2, Bytes::from("msg-2"));

        let _ = std::fs::remove_file(&sock_path);
    }

    #[tokio::test]
    async fn uds_rejects_oversized_message() {
        let sock_path = temp_socket_path("oversize");
        let _ = std::fs::remove_file(&sock_path);

        let listener = UnixListener::bind(&sock_path).unwrap();

        let (tx, rx) = mpsc::channel(16);
        tokio::spawn(UdsReceiver::accept_loop(listener, sock_path.clone(), tx));
        let mut receiver = UdsReceiver { incoming_rx: rx };

        // Manually connect and send an oversized length prefix
        let mut stream = UnixStream::connect(&sock_path).await.unwrap();
        let fake_len = (MAX_MESSAGE_SIZE as u32) + 1;
        stream.write_all(&fake_len.to_be_bytes()).await.unwrap();
        stream.flush().await.unwrap();
        drop(stream);

        // The reader task should drop this connection.
        // A subsequent valid message from a new connection should still work.
        let sender = UdsSender::new(&sock_path);
        sender.send(Bytes::from("valid")).await.unwrap();

        let received = tokio::time::timeout(std::time::Duration::from_secs(2), receiver.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(received, Bytes::from("valid"));

        let _ = std::fs::remove_file(&sock_path);
    }

    #[tokio::test]
    async fn uds_transport_factory() {
        let sock_path = temp_socket_path("factory");
        let _ = std::fs::remove_file(&sock_path);

        let peer_path = temp_socket_path("factory-peer");
        let id_peer = NodeId::new("peer", 1000);

        let result = UdsTransport::create(&sock_path, vec![(id_peer.clone(), peer_path)]).await;

        assert!(result.is_ok());
        let (peers, _receiver) = result.unwrap();
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].id, id_peer);

        let _ = std::fs::remove_file(&sock_path);
    }
}
