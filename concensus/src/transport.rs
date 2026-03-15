//! Transport traits and implementations for inter-node communication.
//!
//! The consensus protocol is transport-agnostic. Any type implementing
//! [`MessageSender`] and [`MessageReceiver`] can be used to connect nodes.
//! Three implementations are provided behind feature flags:
//!
//! - [`channel`] — in-memory channels (feature: `channel-transport`)
//! - [`tcp`] — TCP with length-prefixed framing (feature: `tcp-transport`)
//! - [`uds`] — Unix domain sockets (feature: `uds-transport`)

use crate::error::TransportError;
use async_trait::async_trait;
use bytes::Bytes;

#[cfg(feature = "channel-transport")]
pub mod channel;

#[cfg(feature = "tcp-transport")]
pub mod tcp;

#[cfg(feature = "uds-transport")]
pub mod uds;

/// Sends serialized Paxos messages to a single remote peer.
///
/// Each [`PeerInfo`](crate::PeerInfo) holds one `MessageSender` for its peer.
/// The [`Node`](crate::Node) calls `send` to deliver protocol messages;
/// transient failures are tolerated since the Paxos protocol retries.
///
/// Implementations must be `Send + 'static` so they can be held across `.await` points.
#[async_trait]
pub trait MessageSender: Send + 'static {
    /// Send `data` to the peer. Returns an error if the connection is closed
    /// or unrecoverable.
    async fn send(&self, data: Bytes) -> Result<(), TransportError>;
}

/// Receives serialized Paxos messages from any peer in the cluster.
///
/// A single `MessageReceiver` is passed to [`Node::new`](crate::Node::new)
/// and polled in the event loop to process incoming protocol messages.
///
/// Implementations must be `Send + 'static`.
#[async_trait]
pub trait MessageReceiver: Send + 'static {
    /// Wait for the next incoming message. Returns [`TransportError::Closed`]
    /// when no more messages will arrive.
    async fn recv(&mut self) -> Result<Bytes, TransportError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    struct MockSender {
        tx: mpsc::Sender<Bytes>,
    }

    #[async_trait]
    impl MessageSender for MockSender {
        async fn send(&self, data: Bytes) -> Result<(), TransportError> {
            self.tx.send(data).await.map_err(|_| TransportError::Closed)
        }
    }

    struct MockReceiver {
        rx: mpsc::Receiver<Bytes>,
    }

    #[async_trait]
    impl MessageReceiver for MockReceiver {
        async fn recv(&mut self) -> Result<Bytes, TransportError> {
            self.rx.recv().await.ok_or(TransportError::Closed)
        }
    }

    #[tokio::test]
    async fn mock_sender_receiver_roundtrip() {
        let (tx, rx) = mpsc::channel(16);
        let sender = MockSender { tx };
        let mut receiver = MockReceiver { rx };
        let payload = Bytes::from("hello");
        sender.send(payload.clone()).await.unwrap();
        let received = receiver.recv().await.unwrap();
        assert_eq!(received, payload);
    }

    #[tokio::test]
    async fn receiver_returns_closed_on_drop() {
        let (tx, rx) = mpsc::channel::<Bytes>(16);
        let mut receiver = MockReceiver { rx };
        drop(tx);
        let result = receiver.recv().await;
        assert!(matches!(result, Err(TransportError::Closed)));
    }
}
