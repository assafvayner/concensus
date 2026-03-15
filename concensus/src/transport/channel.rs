//! In-memory channel transport for testing and single-process clusters.
//!
//! Uses tokio mpsc channels under the hood. Create pairs with [`channel`]
//! (bounded, with backpressure) or [`unbounded_channel`].

use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::mpsc;

use crate::error::TransportError;
use crate::transport::{MessageReceiver, MessageSender};

enum SenderInner {
    Bounded(mpsc::Sender<Bytes>),
    Unbounded(mpsc::UnboundedSender<Bytes>),
}

enum ReceiverInner {
    Bounded(mpsc::Receiver<Bytes>),
    Unbounded(mpsc::UnboundedReceiver<Bytes>),
}

/// Sender half of an in-memory channel transport. Cloneable.
pub struct ChannelSender {
    inner: SenderInner,
}

/// Receiver half of an in-memory channel transport.
pub struct ChannelReceiver {
    inner: ReceiverInner,
}

/// Creates a bounded channel pair. `send()` waits for capacity (backpressure).
pub fn channel(capacity: usize) -> (ChannelSender, ChannelReceiver) {
    let (tx, rx) = mpsc::channel(capacity);
    (
        ChannelSender {
            inner: SenderInner::Bounded(tx),
        },
        ChannelReceiver {
            inner: ReceiverInner::Bounded(rx),
        },
    )
}

/// Creates an unbounded channel pair. `send()` never blocks.
pub fn unbounded_channel() -> (ChannelSender, ChannelReceiver) {
    let (tx, rx) = mpsc::unbounded_channel();
    (
        ChannelSender {
            inner: SenderInner::Unbounded(tx),
        },
        ChannelReceiver {
            inner: ReceiverInner::Unbounded(rx),
        },
    )
}

impl Clone for ChannelSender {
    fn clone(&self) -> Self {
        Self {
            inner: match &self.inner {
                SenderInner::Bounded(tx) => SenderInner::Bounded(tx.clone()),
                SenderInner::Unbounded(tx) => SenderInner::Unbounded(tx.clone()),
            },
        }
    }
}

#[async_trait]
impl MessageSender for ChannelSender {
    async fn send(&self, data: Bytes) -> Result<(), TransportError> {
        match &self.inner {
            SenderInner::Bounded(tx) => tx.send(data).await.map_err(|_| TransportError::Closed),
            SenderInner::Unbounded(tx) => tx.send(data).map_err(|_| TransportError::Closed),
        }
    }
}

#[async_trait]
impl MessageReceiver for ChannelReceiver {
    async fn recv(&mut self) -> Result<Bytes, TransportError> {
        match &mut self.inner {
            ReceiverInner::Bounded(rx) => rx.recv().await.ok_or(TransportError::Closed),
            ReceiverInner::Unbounded(rx) => rx.recv().await.ok_or(TransportError::Closed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn bounded_channel_roundtrip() {
        let (sender, mut receiver) = channel(16);
        let payload = Bytes::from("hello");
        sender.send(payload.clone()).await.unwrap();
        let received = receiver.recv().await.unwrap();
        assert_eq!(received, payload);
    }

    #[tokio::test]
    async fn unbounded_channel_roundtrip() {
        let (sender, mut receiver) = unbounded_channel();
        let payload = Bytes::from("hello");
        sender.send(payload.clone()).await.unwrap();
        let received = receiver.recv().await.unwrap();
        assert_eq!(received, payload);
    }

    #[tokio::test]
    async fn sender_is_cloneable() {
        let (sender, mut receiver) = channel(16);
        let sender2 = sender.clone();
        sender.send(Bytes::from("from-1")).await.unwrap();
        sender2.send(Bytes::from("from-2")).await.unwrap();
        let msg1 = receiver.recv().await.unwrap();
        let msg2 = receiver.recv().await.unwrap();
        // Both messages received (order is deterministic in bounded channel)
        assert!(msg1 == Bytes::from("from-1") || msg1 == Bytes::from("from-2"));
        assert!(msg2 == Bytes::from("from-1") || msg2 == Bytes::from("from-2"));
        assert_ne!(msg1, msg2);
    }

    #[tokio::test]
    async fn receiver_returns_closed_when_all_senders_dropped() {
        let (sender, mut receiver) = channel(16);
        drop(sender);
        let result = receiver.recv().await;
        assert!(matches!(result, Err(TransportError::Closed)));
    }

    #[tokio::test]
    async fn unbounded_receiver_returns_closed_when_all_senders_dropped() {
        let (sender, mut receiver) = unbounded_channel();
        drop(sender);
        let result = receiver.recv().await;
        assert!(matches!(result, Err(TransportError::Closed)));
    }
}
