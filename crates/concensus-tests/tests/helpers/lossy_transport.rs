use async_trait::async_trait;
use bytes::Bytes;
use rand::Rng;
use std::sync::atomic::{AtomicU64, Ordering};

use concensus::{MessageReceiver, MessageSender, TransportError};

/// A sender that randomly drops a configurable fraction of messages.
///
/// Wraps any `MessageSender` and silently discards messages with probability
/// `drop_rate` (0.0 = forward all, 1.0 = drop all). Default is 0.10 (10%).
pub struct LossySender<S: MessageSender> {
    inner: S,
    drop_rate: f64,
    sent_count: AtomicU64,
    dropped_count: AtomicU64,
}

impl<S: MessageSender> LossySender<S> {
    pub fn new(inner: S) -> Self {
        Self::with_drop_rate(inner, 0.10)
    }

    pub fn with_drop_rate(inner: S, drop_rate: f64) -> Self {
        assert!(
            (0.0..=1.0).contains(&drop_rate),
            "drop_rate must be between 0.0 and 1.0"
        );
        Self {
            inner,
            drop_rate,
            sent_count: AtomicU64::new(0),
            dropped_count: AtomicU64::new(0),
        }
    }

    pub fn sent_count(&self) -> u64 {
        self.sent_count.load(Ordering::Relaxed)
    }

    pub fn dropped_count(&self) -> u64 {
        self.dropped_count.load(Ordering::Relaxed)
    }
}

impl<S: MessageSender + Clone> Clone for LossySender<S> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            drop_rate: self.drop_rate,
            sent_count: AtomicU64::new(0),
            dropped_count: AtomicU64::new(0),
        }
    }
}

#[async_trait]
impl<S: MessageSender + Sync> MessageSender for LossySender<S> {
    async fn send(&self, data: Bytes) -> Result<(), TransportError> {
        let should_drop = rand::thread_rng().gen_bool(self.drop_rate);
        if should_drop {
            self.dropped_count.fetch_add(1, Ordering::Relaxed);
            // Silently drop — from the caller's perspective the send succeeded
            Ok(())
        } else {
            self.sent_count.fetch_add(1, Ordering::Relaxed);
            self.inner.send(data).await
        }
    }
}

/// A receiver that randomly drops a configurable fraction of incoming messages.
///
/// Wraps any `MessageReceiver` and silently discards received messages with
/// probability `drop_rate`, then waits for the next one. Default is 0.10 (10%).
pub struct LossyReceiver<R: MessageReceiver> {
    inner: R,
    drop_rate: f64,
}

impl<R: MessageReceiver> LossyReceiver<R> {
    pub fn new(inner: R) -> Self {
        Self::with_drop_rate(inner, 0.10)
    }

    pub fn with_drop_rate(inner: R, drop_rate: f64) -> Self {
        assert!(
            (0.0..=1.0).contains(&drop_rate),
            "drop_rate must be between 0.0 and 1.0"
        );
        Self { inner, drop_rate }
    }
}

#[async_trait]
impl<R: MessageReceiver + Send> MessageReceiver for LossyReceiver<R> {
    async fn recv(&mut self) -> Result<Bytes, TransportError> {
        loop {
            let data = self.inner.recv().await?;
            let mut rng = rand::thread_rng();
            if rng.gen_bool(self.drop_rate) {
                // Drop this message, wait for next
                continue;
            }
            return Ok(data);
        }
    }
}

// ---------------------------------------------------------------------------
// Delayed transport
// ---------------------------------------------------------------------------

/// A sender that adds a random delay before forwarding each message.
///
/// Wraps any `MessageSender` and sleeps for a uniform random duration in
/// `[min_delay, max_delay]` before each send. Default range is 0–100ms.
pub struct DelayedSender<S: MessageSender> {
    inner: S,
    min_delay: std::time::Duration,
    max_delay: std::time::Duration,
}

impl<S: MessageSender> DelayedSender<S> {
    pub fn new(inner: S) -> Self {
        Self::with_range(
            inner,
            std::time::Duration::from_millis(0),
            std::time::Duration::from_millis(100),
        )
    }

    pub fn with_range(
        inner: S,
        min_delay: std::time::Duration,
        max_delay: std::time::Duration,
    ) -> Self {
        assert!(
            min_delay <= max_delay,
            "min_delay must be <= max_delay"
        );
        Self {
            inner,
            min_delay,
            max_delay,
        }
    }
}

impl<S: MessageSender + Clone> Clone for DelayedSender<S> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            min_delay: self.min_delay,
            max_delay: self.max_delay,
        }
    }
}

#[async_trait]
impl<S: MessageSender + Sync> MessageSender for DelayedSender<S> {
    async fn send(&self, data: Bytes) -> Result<(), TransportError> {
        let delay = {
            let min_us = self.min_delay.as_micros() as u64;
            let max_us = self.max_delay.as_micros() as u64;
            if min_us == max_us {
                self.min_delay
            } else {
                let us = rand::thread_rng().gen_range(min_us..=max_us);
                std::time::Duration::from_micros(us)
            }
        };
        tokio::time::sleep(delay).await;
        self.inner.send(data).await
    }
}

/// A receiver that adds a random delay before delivering each message.
///
/// Wraps any `MessageReceiver` and sleeps for a uniform random duration in
/// `[min_delay, max_delay]` after receiving each message before returning it.
/// Default range is 0–100ms.
pub struct DelayedReceiver<R: MessageReceiver> {
    inner: R,
    min_delay: std::time::Duration,
    max_delay: std::time::Duration,
}

impl<R: MessageReceiver> DelayedReceiver<R> {
    pub fn new(inner: R) -> Self {
        Self::with_range(
            inner,
            std::time::Duration::from_millis(0),
            std::time::Duration::from_millis(100),
        )
    }

    pub fn with_range(
        inner: R,
        min_delay: std::time::Duration,
        max_delay: std::time::Duration,
    ) -> Self {
        assert!(
            min_delay <= max_delay,
            "min_delay must be <= max_delay"
        );
        Self {
            inner,
            min_delay,
            max_delay,
        }
    }
}

#[async_trait]
impl<R: MessageReceiver + Send> MessageReceiver for DelayedReceiver<R> {
    async fn recv(&mut self) -> Result<Bytes, TransportError> {
        let data = self.inner.recv().await?;
        let delay = {
            let min_us = self.min_delay.as_micros() as u64;
            let max_us = self.max_delay.as_micros() as u64;
            if min_us == max_us {
                self.min_delay
            } else {
                let us = rand::thread_rng().gen_range(min_us..=max_us);
                std::time::Duration::from_micros(us)
            }
        };
        tokio::time::sleep(delay).await;
        Ok(data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    struct TestSender(mpsc::Sender<Bytes>);

    #[async_trait]
    impl MessageSender for TestSender {
        async fn send(&self, data: Bytes) -> Result<(), TransportError> {
            self.0.send(data).await.map_err(|_| TransportError::Closed)
        }
    }

    impl Clone for TestSender {
        fn clone(&self) -> Self {
            Self(self.0.clone())
        }
    }

    struct TestReceiver(mpsc::Receiver<Bytes>);

    #[async_trait]
    impl MessageReceiver for TestReceiver {
        async fn recv(&mut self) -> Result<Bytes, TransportError> {
            self.0.recv().await.ok_or(TransportError::Closed)
        }
    }

    #[tokio::test]
    async fn lossy_sender_drop_all() {
        let (tx, mut rx) = mpsc::channel(16);
        let sender = LossySender::with_drop_rate(TestSender(tx), 1.0);

        for i in 0..10 {
            sender
                .send(Bytes::from(format!("msg-{}", i)))
                .await
                .unwrap();
        }

        // Nothing should arrive
        assert!(rx.try_recv().is_err());
        assert_eq!(sender.dropped_count(), 10);
        assert_eq!(sender.sent_count(), 0);
    }

    #[tokio::test]
    async fn lossy_sender_forward_all() {
        let (tx, mut rx) = mpsc::channel(16);
        let sender = LossySender::with_drop_rate(TestSender(tx), 0.0);

        for i in 0..10 {
            sender
                .send(Bytes::from(format!("msg-{}", i)))
                .await
                .unwrap();
        }

        for i in 0..10 {
            let msg = rx.recv().await.unwrap();
            assert_eq!(msg, Bytes::from(format!("msg-{}", i)));
        }
        assert_eq!(sender.sent_count(), 10);
        assert_eq!(sender.dropped_count(), 0);
    }

    #[tokio::test]
    async fn lossy_receiver_drop_all_returns_closed() {
        let (tx, rx) = mpsc::channel(16);
        let mut receiver = LossyReceiver::with_drop_rate(TestReceiver(rx), 1.0);

        tx.send(Bytes::from("hello")).await.unwrap();
        drop(tx);

        // Should drop the message, then get Closed when inner is exhausted
        let result = receiver.recv().await;
        assert!(matches!(result, Err(TransportError::Closed)));
    }

    #[tokio::test]
    async fn lossy_receiver_forward_all() {
        let (tx, rx) = mpsc::channel(16);
        let mut receiver = LossyReceiver::with_drop_rate(TestReceiver(rx), 0.0);

        tx.send(Bytes::from("hello")).await.unwrap();
        let msg = receiver.recv().await.unwrap();
        assert_eq!(msg, Bytes::from("hello"));
    }

    #[tokio::test]
    async fn lossy_sender_is_cloneable() {
        let (tx, _rx) = mpsc::channel(16);
        let sender = LossySender::new(TestSender(tx));
        let _cloned = sender.clone();
    }

    // -- Delayed transport tests --

    #[tokio::test]
    async fn delayed_sender_forwards_all_messages() {
        let (tx, mut rx) = mpsc::channel(16);
        let sender = DelayedSender::with_range(
            TestSender(tx),
            std::time::Duration::from_millis(0),
            std::time::Duration::from_millis(5),
        );

        for i in 0..5 {
            sender
                .send(Bytes::from(format!("msg-{}", i)))
                .await
                .unwrap();
        }

        for i in 0..5 {
            let msg = rx.recv().await.unwrap();
            assert_eq!(msg, Bytes::from(format!("msg-{}", i)));
        }
    }

    #[tokio::test]
    async fn delayed_sender_actually_delays() {
        let (tx, mut rx) = mpsc::channel(16);
        let sender = DelayedSender::with_range(
            TestSender(tx),
            std::time::Duration::from_millis(50),
            std::time::Duration::from_millis(50),
        );

        let start = tokio::time::Instant::now();
        sender.send(Bytes::from("hello")).await.unwrap();
        let _ = rx.recv().await.unwrap();
        let elapsed = start.elapsed();
        assert!(
            elapsed >= std::time::Duration::from_millis(40),
            "expected >= 40ms delay, got {:?}",
            elapsed
        );
    }

    #[tokio::test]
    async fn delayed_sender_zero_delay_is_fast() {
        let (tx, mut rx) = mpsc::channel(16);
        let sender = DelayedSender::with_range(
            TestSender(tx),
            std::time::Duration::from_millis(0),
            std::time::Duration::from_millis(0),
        );

        sender.send(Bytes::from("fast")).await.unwrap();
        let msg = rx.recv().await.unwrap();
        assert_eq!(msg, Bytes::from("fast"));
    }

    #[tokio::test]
    async fn delayed_sender_is_cloneable() {
        let (tx, _rx) = mpsc::channel(16);
        let sender = DelayedSender::new(TestSender(tx));
        let _cloned = sender.clone();
    }

    #[tokio::test]
    async fn delayed_receiver_forwards_all_messages() {
        let (tx, rx) = mpsc::channel(16);
        let mut receiver = DelayedReceiver::with_range(
            TestReceiver(rx),
            std::time::Duration::from_millis(0),
            std::time::Duration::from_millis(5),
        );

        tx.send(Bytes::from("hello")).await.unwrap();
        let msg = receiver.recv().await.unwrap();
        assert_eq!(msg, Bytes::from("hello"));
    }

    #[tokio::test]
    async fn delayed_receiver_actually_delays() {
        let (tx, rx) = mpsc::channel(16);
        let mut receiver = DelayedReceiver::with_range(
            TestReceiver(rx),
            std::time::Duration::from_millis(50),
            std::time::Duration::from_millis(50),
        );

        tx.send(Bytes::from("hello")).await.unwrap();
        let start = tokio::time::Instant::now();
        let msg = receiver.recv().await.unwrap();
        let elapsed = start.elapsed();
        assert_eq!(msg, Bytes::from("hello"));
        assert!(
            elapsed >= std::time::Duration::from_millis(40),
            "expected >= 40ms delay, got {:?}",
            elapsed
        );
    }

    #[tokio::test]
    async fn delayed_receiver_propagates_closed() {
        let (tx, rx) = mpsc::channel::<Bytes>(16);
        let mut receiver = DelayedReceiver::new(TestReceiver(rx));
        drop(tx);
        let result = receiver.recv().await;
        assert!(matches!(result, Err(TransportError::Closed)));
    }
}
