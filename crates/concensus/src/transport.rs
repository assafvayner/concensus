use async_trait::async_trait;
use bytes::Bytes;
use crate::error::TransportError;

#[cfg(feature = "channel-transport")]
pub mod channel;

#[async_trait]
pub trait MessageSender: Send + 'static {
    async fn send(&self, data: Bytes) -> Result<(), TransportError>;
}

#[async_trait]
pub trait MessageReceiver: Send + 'static {
    async fn recv(&mut self) -> Result<Bytes, TransportError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    struct MockSender { tx: mpsc::Sender<Bytes> }

    #[async_trait]
    impl MessageSender for MockSender {
        async fn send(&self, data: Bytes) -> Result<(), TransportError> {
            self.tx.send(data).await.map_err(|_| TransportError::Closed)
        }
    }

    struct MockReceiver { rx: mpsc::Receiver<Bytes> }

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
