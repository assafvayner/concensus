use std::net::SocketAddr;

use async_trait::async_trait;
use bytes::Bytes;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Mutex};

use crate::config::{NodeId, PeerInfo};
use crate::error::TransportError;
use crate::transport::{MessageReceiver, MessageSender};

const MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024; // 16 MiB
const LISTENER_CHANNEL_CAPACITY: usize = 1024;
const INITIAL_REBIND_DELAY_MS: u64 = 100;
const MAX_REBIND_DELAY_MS: u64 = 5000;

// ─── TcpSender ───────────────────────────────────────────────────────────────

pub struct TcpSender {
    addr: SocketAddr,
    conn: Mutex<Option<OwnedWriteHalf>>,
}

impl TcpSender {
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            conn: Mutex::new(None),
        }
    }
}

#[async_trait]
impl MessageSender for TcpSender {
    async fn send(&self, data: Bytes) -> Result<(), TransportError> {
        let mut guard = self.conn.lock().await;

        // Lazy connect
        if guard.is_none() {
            let stream = TcpStream::connect(self.addr)
                .await
                .map_err(|e| TransportError::Other(Box::new(e)))?;
            let (_read_half, write_half) = stream.into_split();
            // Drop the read half — sender is write-only; inbound data arrives via TcpReceiver
            *guard = Some(write_half);
        }

        let writer = guard.as_mut().unwrap();

        // Write 4-byte BE length prefix
        let len = data.len() as u32;
        if let Err(_) = writer.write_all(&len.to_be_bytes()).await {
            *guard = None;
            return Err(TransportError::Closed);
        }

        // Write payload
        if let Err(_) = writer.write_all(&data).await {
            *guard = None;
            return Err(TransportError::Closed);
        }

        // Flush
        if let Err(_) = writer.flush().await {
            *guard = None;
            return Err(TransportError::Closed);
        }

        Ok(())
    }
}

// ─── TcpReceiver ─────────────────────────────────────────────────────────────

pub struct TcpReceiver {
    incoming_rx: mpsc::Receiver<Bytes>,
}

impl TcpReceiver {
    pub async fn bind(addr: SocketAddr) -> Result<Self, TransportError> {
        let listener = TcpListener::bind(addr)
            .await
            .map_err(|e| TransportError::Other(Box::new(e)))?;

        let (incoming_tx, incoming_rx) = mpsc::channel(LISTENER_CHANNEL_CAPACITY);

        tokio::spawn(Self::accept_loop(listener, addr, incoming_tx));

        Ok(Self { incoming_rx })
    }

    pub(crate) async fn accept_loop(
        mut listener: TcpListener,
        addr: SocketAddr,
        incoming_tx: mpsc::Sender<Bytes>,
    ) {
        loop {
            match listener.accept().await {
                Ok((stream, peer_addr)) => {
                    tracing::debug!(%peer_addr, "accepted TCP connection");
                    let tx = incoming_tx.clone();
                    tokio::spawn(Self::reader_task(stream, tx));
                }
                Err(e) => {
                    tracing::warn!(error = %e, "TCP listener error, attempting rebind");

                    let mut delay_ms = INITIAL_REBIND_DELAY_MS;
                    loop {
                        if incoming_tx.is_closed() {
                            return; // Receiver dropped, shut down
                        }

                        // Add jitter: ±25%
                        let jitter = (delay_ms as f64
                            * 0.25
                            * (2.0 * rand::random::<f64>() - 1.0))
                            as i64;
                        let actual_delay = (delay_ms as i64 + jitter).max(10) as u64;
                        tokio::time::sleep(std::time::Duration::from_millis(actual_delay)).await;

                        match TcpListener::bind(addr).await {
                            Ok(new_listener) => {
                                tracing::info!("TCP listener rebound successfully");
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

    async fn reader_task(stream: TcpStream, incoming_tx: mpsc::Sender<Bytes>) {
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
impl MessageReceiver for TcpReceiver {
    async fn recv(&mut self) -> Result<Bytes, TransportError> {
        self.incoming_rx.recv().await.ok_or(TransportError::Closed)
    }
}

// ─── TcpTransport factory ────────────────────────────────────────────────────

pub struct TcpTransport;

impl TcpTransport {
    pub async fn new(
        bind_addr: SocketAddr,
        peers: Vec<(NodeId, SocketAddr)>,
    ) -> Result<(Vec<PeerInfo<TcpSender>>, TcpReceiver), TransportError> {
        let receiver = TcpReceiver::bind(bind_addr).await?;
        let peers = peers
            .into_iter()
            .map(|(id, addr)| PeerInfo {
                id,
                sender: TcpSender::new(addr),
            })
            .collect();
        Ok((peers, receiver))
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn tcp_sender_receiver_roundtrip() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let bound_addr = listener.local_addr().unwrap();

        let (tx, rx) = mpsc::channel(16);
        tokio::spawn(TcpReceiver::accept_loop(listener, bound_addr, tx));
        let mut receiver = TcpReceiver { incoming_rx: rx };

        let sender = TcpSender::new(bound_addr);

        let payload = Bytes::from("hello-tcp");
        sender.send(payload.clone()).await.unwrap();

        let received = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            receiver.recv(),
        )
        .await
        .expect("timed out")
        .expect("recv failed");

        assert_eq!(received, payload);
    }

    #[tokio::test]
    async fn tcp_sender_reconnects_after_failure() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let bound_addr = listener.local_addr().unwrap();

        let (tx, rx) = mpsc::channel(16);
        tokio::spawn(TcpReceiver::accept_loop(listener, bound_addr, tx));
        let mut receiver = TcpReceiver { incoming_rx: rx };

        let sender = TcpSender::new(bound_addr);

        // First send — establishes connection
        sender.send(Bytes::from("msg-1")).await.unwrap();
        let r1 = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            receiver.recv(),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(r1, Bytes::from("msg-1"));

        // Force-close the connection by clearing it
        {
            let mut guard = sender.conn.lock().await;
            *guard = None; // Simulate connection drop
        }

        // Second send — should reconnect
        sender.send(Bytes::from("msg-2")).await.unwrap();
        let r2 = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            receiver.recv(),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(r2, Bytes::from("msg-2"));
    }

    #[tokio::test]
    async fn tcp_rejects_oversized_message() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let bound_addr = listener.local_addr().unwrap();

        let (tx, rx) = mpsc::channel(16);
        tokio::spawn(TcpReceiver::accept_loop(listener, bound_addr, tx));
        let mut receiver = TcpReceiver { incoming_rx: rx };

        // Manually connect and send an oversized length prefix
        let mut stream = TcpStream::connect(bound_addr).await.unwrap();
        let fake_len = (MAX_MESSAGE_SIZE as u32) + 1;
        stream.write_all(&fake_len.to_be_bytes()).await.unwrap();
        stream.flush().await.unwrap();
        drop(stream);

        // The reader task should drop this connection.
        // A subsequent valid message from a new connection should still work.
        let sender = TcpSender::new(bound_addr);
        sender.send(Bytes::from("valid")).await.unwrap();

        let received = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            receiver.recv(),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(received, Bytes::from("valid"));
    }

    #[tokio::test]
    async fn tcp_transport_factory() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let bound_addr = listener.local_addr().unwrap();
        drop(listener); // Free the port for TcpTransport::new

        let id_peer = NodeId::new("peer", 1000);
        let result = TcpTransport::new(
            bound_addr,
            vec![(id_peer.clone(), "127.0.0.1:9999".parse().unwrap())],
        )
        .await;

        assert!(result.is_ok());
        let (peers, _receiver) = result.unwrap();
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].id, id_peer);
    }
}
