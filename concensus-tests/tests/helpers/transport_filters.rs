// Filters are reused across many test binaries; not every binary uses every
// constructor or sender/receiver pair, so suppress dead_code warnings here.
#![allow(dead_code)]

use async_trait::async_trait;
use bytes::Bytes;
use rand::seq::SliceRandom;
use rand::RngExt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

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
        let should_drop = rand::rng().random_bool(self.drop_rate);
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
            let mut rng = rand::rng();
            if rng.random_bool(self.drop_rate) {
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
        assert!(min_delay <= max_delay, "min_delay must be <= max_delay");
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
impl<S: MessageSender + Sync + Clone + 'static> MessageSender for DelayedSender<S> {
    async fn send(&self, data: Bytes) -> Result<(), TransportError> {
        let delay = {
            let min_us = self.min_delay.as_micros() as u64;
            let max_us = self.max_delay.as_micros() as u64;
            if min_us == max_us {
                self.min_delay
            } else {
                let us = rand::rng().random_range(min_us..=max_us);
                std::time::Duration::from_micros(us)
            }
        };
        // Spawn the delayed send as a background task so the caller's event
        // loop is not blocked during the sleep. Without this, the node cannot
        // process incoming messages while sends are delayed, causing livelock
        // under concurrent proposals.
        let inner = self.inner.clone();
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            let _ = inner.send(data).await;
        });
        Ok(())
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
        assert!(min_delay <= max_delay, "min_delay must be <= max_delay");
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
                let us = rand::rng().random_range(min_us..=max_us);
                std::time::Duration::from_micros(us)
            }
        };
        tokio::time::sleep(delay).await;
        Ok(data)
    }
}

// ---------------------------------------------------------------------------
// Reordering transport
// ---------------------------------------------------------------------------

/// A pass-through sender for use with `ReorderingReceiver`.
///
/// Forwards messages unchanged — reordering happens on the receive side.
pub struct ReorderingSender<S: MessageSender> {
    inner: S,
}

impl<S: MessageSender> ReorderingSender<S> {
    pub fn new(inner: S) -> Self {
        Self { inner }
    }
}

impl<S: MessageSender + Clone> Clone for ReorderingSender<S> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

#[async_trait]
impl<S: MessageSender + Sync> MessageSender for ReorderingSender<S> {
    async fn send(&self, data: Bytes) -> Result<(), TransportError> {
        self.inner.send(data).await
    }
}

/// A receiver that randomly reorders incoming messages.
///
/// On each `recv` call, if the internal buffer is empty, the receiver
/// collects messages from the inner receiver for up to `collect_window`
/// (default 10ms) or until `batch_size` messages arrive (default 5),
/// whichever comes first. The collected batch is shuffled and the first
/// message is returned. Subsequent `recv` calls drain the buffer before
/// collecting a new batch.
pub struct ReorderingReceiver<R: MessageReceiver> {
    inner: R,
    buffer: Vec<Bytes>,
    collect_window: Duration,
    batch_size: usize,
}

impl<R: MessageReceiver> ReorderingReceiver<R> {
    pub fn new(inner: R) -> Self {
        Self {
            inner,
            buffer: Vec::new(),
            collect_window: Duration::from_millis(10),
            batch_size: 5,
        }
    }

    pub fn with_params(inner: R, collect_window: Duration, batch_size: usize) -> Self {
        assert!(batch_size > 0, "batch_size must be > 0");
        Self {
            inner,
            buffer: Vec::new(),
            collect_window,
            batch_size,
        }
    }
}

#[async_trait]
impl<R: MessageReceiver + Send> MessageReceiver for ReorderingReceiver<R> {
    async fn recv(&mut self) -> Result<Bytes, TransportError> {
        // Drain buffered messages first
        if let Some(msg) = self.buffer.pop() {
            return Ok(msg);
        }

        // Buffer is empty — collect a new batch.
        // We must get at least one message (block on the first).
        let first = self.inner.recv().await?;
        let mut batch = vec![first];

        // Try to collect more within the time window, up to batch_size.
        let deadline = tokio::time::Instant::now() + self.collect_window;
        while batch.len() < self.batch_size {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                break;
            }
            match tokio::time::timeout(remaining, self.inner.recv()).await {
                Ok(Ok(data)) => batch.push(data),
                Ok(Err(_)) => break, // inner closed — return what we have
                Err(_) => break,     // timeout — return what we have
            }
        }

        // Shuffle and load into buffer (reversed so pop returns in shuffled order)
        batch.shuffle(&mut rand::rng());
        // Return the first, buffer the rest
        let result = batch.remove(0);
        batch.reverse();
        self.buffer = batch;
        Ok(result)
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

        // DelayedSender spawns background tasks with random delays, so
        // messages may arrive out of order. Collect all and check the set.
        let mut received = std::collections::HashSet::new();
        for _ in 0..5 {
            let msg = tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
                .await
                .expect("timed out")
                .unwrap();
            received.insert(msg);
        }
        let expected: std::collections::HashSet<Bytes> =
            (0..5).map(|i| Bytes::from(format!("msg-{}", i))).collect();
        assert_eq!(received, expected);
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

    // -- Reordering transport tests --

    #[tokio::test]
    async fn reordering_receiver_delivers_all_messages() {
        let (tx, rx) = mpsc::channel(16);
        let mut receiver = ReorderingReceiver::new(TestReceiver(rx));

        // Send 10 messages rapidly
        for i in 0..10 {
            tx.send(Bytes::from(format!("msg-{}", i))).await.unwrap();
        }
        drop(tx);

        let mut received = Vec::new();
        loop {
            match receiver.recv().await {
                Ok(data) => received.push(data),
                Err(TransportError::Closed) => break,
                Err(e) => panic!("unexpected error: {}", e),
            }
        }

        // All messages delivered
        assert_eq!(received.len(), 10);
        // All values present (order may differ)
        let mut expected: Vec<Bytes> = (0..10).map(|i| Bytes::from(format!("msg-{}", i))).collect();
        let mut sorted_received = received.clone();
        sorted_received.sort();
        expected.sort();
        assert_eq!(sorted_received, expected);
    }

    #[tokio::test]
    async fn reordering_receiver_single_message() {
        let (tx, rx) = mpsc::channel(16);
        let mut receiver =
            ReorderingReceiver::with_params(TestReceiver(rx), Duration::from_millis(10), 5);

        tx.send(Bytes::from("solo")).await.unwrap();
        // With only 1 message and a 10ms window, it should return after the timeout
        let msg = receiver.recv().await.unwrap();
        assert_eq!(msg, Bytes::from("solo"));
    }

    #[tokio::test]
    async fn reordering_receiver_respects_batch_size() {
        let (tx, rx) = mpsc::channel(16);
        // batch_size=3, long window so batch_size triggers first
        let mut receiver =
            ReorderingReceiver::with_params(TestReceiver(rx), Duration::from_secs(10), 3);

        // Send 3 messages — should fill batch and return immediately
        for i in 0..3 {
            tx.send(Bytes::from(format!("msg-{}", i))).await.unwrap();
        }

        let start = tokio::time::Instant::now();
        let msg1 = receiver.recv().await.unwrap();
        let elapsed = start.elapsed();
        // Should return quickly since batch_size was reached
        assert!(
            elapsed < std::time::Duration::from_millis(500),
            "expected fast return with batch_size=3, got {:?}",
            elapsed
        );

        // Should be able to get the other 2 from buffer without blocking
        let msg2 = receiver.recv().await.unwrap();
        let msg3 = receiver.recv().await.unwrap();

        let mut received = vec![msg1, msg2, msg3];
        received.sort();
        let mut expected: Vec<Bytes> = (0..3).map(|i| Bytes::from(format!("msg-{}", i))).collect();
        expected.sort();
        assert_eq!(received, expected);
    }

    #[tokio::test]
    async fn reordering_receiver_timeout_returns_partial_batch() {
        let (tx, rx) = mpsc::channel(16);
        // batch_size=10 but only 2 messages — window timeout should trigger
        let mut receiver =
            ReorderingReceiver::with_params(TestReceiver(rx), Duration::from_millis(20), 10);

        tx.send(Bytes::from("a")).await.unwrap();
        tx.send(Bytes::from("b")).await.unwrap();
        // Don't send more — let window timeout

        let start = tokio::time::Instant::now();
        let msg1 = receiver.recv().await.unwrap();
        let elapsed = start.elapsed();

        // Should return after ~20ms window, not hang
        assert!(
            elapsed < std::time::Duration::from_millis(200),
            "expected return after window, got {:?}",
            elapsed
        );

        let msg2 = receiver.recv().await.unwrap();
        let mut received = vec![msg1, msg2];
        received.sort();
        assert_eq!(received, vec![Bytes::from("a"), Bytes::from("b")]);
    }

    #[tokio::test]
    async fn reordering_receiver_propagates_closed() {
        let (tx, rx) = mpsc::channel::<Bytes>(16);
        let mut receiver = ReorderingReceiver::new(TestReceiver(rx));
        drop(tx);
        let result = receiver.recv().await;
        assert!(matches!(result, Err(TransportError::Closed)));
    }

    #[tokio::test]
    async fn reordering_sender_is_passthrough() {
        let (tx, mut rx) = mpsc::channel(16);
        let sender = ReorderingSender::new(TestSender(tx));

        sender.send(Bytes::from("hello")).await.unwrap();
        let msg = rx.recv().await.unwrap();
        assert_eq!(msg, Bytes::from("hello"));
    }

    #[tokio::test]
    async fn reordering_sender_is_cloneable() {
        let (tx, _rx) = mpsc::channel(16);
        let sender = ReorderingSender::new(TestSender(tx));
        let _cloned = sender.clone();
    }
}
