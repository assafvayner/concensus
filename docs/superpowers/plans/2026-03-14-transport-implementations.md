# Transport Implementations Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Refactor the core Node API to use a single receiver per node, add `Message`/`MessageVariant` split, and implement channel and TCP transports behind feature flags.

**Architecture:** Three phases: (1) core API refactor (Message rename, PeerConfig→PeerInfo, single receiver event loop), (2) in-memory channel transport, (3) TCP transport. Each phase leaves all tests passing.

**Tech Stack:** Rust, tokio, serde_json, bytes, async-trait, thiserror, tracing

**Spec:** `docs/superpowers/specs/2026-03-14-transport-implementations-design.md`

---

## File Map

| File | Action | Responsibility |
|---|---|---|
| `crates/concensus/Cargo.toml` | Modify | Add feature flags, optional deps |
| `crates/concensus/src/message.rs` | Modify | Rename `Message` → `MessageVariant`, add `Message` wrapper struct |
| `crates/concensus/src/protocol.rs` | Modify | Change `Message` → `MessageVariant` in all signatures and match arms |
| `crates/concensus/src/config.rs` | Modify | Replace `PeerConfig<S,R>` with `PeerInfo<S>`, remove `MessageReceiver` import |
| `crates/concensus/src/node.rs` | Modify | Single receiver API, new event loop, `send_outgoing` wraps with sender |
| `crates/concensus/src/transport.rs` | Modify | Add feature-gated `pub mod channel;` and `pub mod tcp;` |
| `crates/concensus/src/transport/channel.rs` | Create | `ChannelSender`, `ChannelReceiver`, `channel()`, `unbounded_channel()` |
| `crates/concensus/src/transport/tcp.rs` | Create | `TcpSender`, `TcpReceiver`, `TcpTransport` |
| `crates/concensus/src/lib.rs` | Modify | Update re-exports: `PeerInfo`, feature-gated transport types |

---

## Chunk 1: Core API Refactor

### Task 1: Message → MessageVariant Rename

**Files:**
- Modify: `crates/concensus/src/message.rs`
- Modify: `crates/concensus/src/protocol.rs`

This is a mechanical rename. `Message<V>` enum becomes `MessageVariant<V>`. A new `Message<V>` struct wraps it with `sender: NodeId`. All protocol code uses `MessageVariant<V>`. Tests in message.rs update to use `MessageVariant` for the enum, and add tests for the new `Message` wrapper.

- [ ] **Step 1: Rename enum to MessageVariant and add Message struct in message.rs**

In `message.rs`:
- Rename `enum Message<V>` to `enum MessageVariant<V>`
- Add new struct:
```rust
#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) struct Message<V> {
    pub sender: NodeId,
    pub variant: MessageVariant<V>,
}
```
- Move `to_bytes()`/`from_bytes()` to the new `Message<V>` struct (same implementation)
- Update all test references from `Message::Prepare` to `MessageVariant::Prepare` etc.
- Add a test for Message wrapper roundtrip:
```rust
#[test]
fn message_wrapper_serde_roundtrip() {
    let msg = Message {
        sender: test_node_id(),
        variant: MessageVariant::Prepare {
            slot: 0,
            proposal_number: (1, test_node_id()),
        },
    };
    let bytes = msg.to_bytes().unwrap();
    let decoded: Message<String> = Message::from_bytes(&bytes).unwrap();
    assert_eq!(decoded.sender, test_node_id());
    assert!(matches!(decoded.variant, MessageVariant::Prepare { slot: 0, .. }));
}
```

- [ ] **Step 2: Update protocol.rs to use MessageVariant**

In `protocol.rs`:
- Change import: `use crate::message::{MessageVariant, ProposalNumber};` (no longer import `Message`)
- Change `Outgoing<V>.message` field type from `Message<V>` to `MessageVariant<V>`
- Change `handle_message` signature: `msg: MessageVariant<V>` instead of `msg: Message<V>`
- Change all match arms: `MessageVariant::Prepare` instead of `Message::Prepare`, etc.
- Change all `Outgoing { message: Message::... }` to `Outgoing { message: MessageVariant::... }`
- Update all test code: `Message::Prepare` → `MessageVariant::Prepare` etc. in `handle_message` calls and match assertions

- [ ] **Step 3: Run tests to verify all pass**

Run: `cargo test -p concensus`
Expected: All 54 tests pass (existing behavior unchanged, just renamed)

- [ ] **Step 4: Commit**

```bash
git add crates/concensus/src/message.rs crates/concensus/src/protocol.rs
git commit -m "refactor: rename Message to MessageVariant, add Message wrapper with sender"
```

---

### Task 2: PeerConfig → PeerInfo, Single Receiver Node API

**Files:**
- Modify: `crates/concensus/src/config.rs`
- Modify: `crates/concensus/src/node.rs`
- Modify: `crates/concensus/src/lib.rs`

- [ ] **Step 1: Replace PeerConfig with PeerInfo in config.rs**

In `config.rs`:
- Remove `use crate::transport::MessageReceiver;` (keep `MessageSender`)
- Replace `PeerConfig<S: MessageSender, R: MessageReceiver>` with:
```rust
pub struct PeerInfo<S: MessageSender> {
    pub id: NodeId,
    pub sender: S,
}
```
- Update the `peer_config_holds_sender_and_receiver` test to be `peer_info_holds_sender`:
```rust
#[test]
fn peer_info_holds_sender() {
    use crate::transport::MessageSender;
    use crate::error::TransportError;
    use bytes::Bytes;

    struct DummySender;
    #[async_trait::async_trait]
    impl MessageSender for DummySender {
        async fn send(&self, _data: Bytes) -> Result<(), TransportError> { Ok(()) }
    }

    let _info = PeerInfo {
        id: NodeId::new("peer-1", 1000),
        sender: DummySender,
    };
}
```

- [ ] **Step 2: Refactor Node to use single receiver**

In `node.rs`:
- Change import: `use crate::config::{NodeId, PeerInfo};` (not `PeerConfig`)
- Change import: `use crate::message::{Message, MessageVariant};` (need both now)
- Change `Node` struct:
```rust
pub struct Node<V, S: MessageSender, R: MessageReceiver> {
    node_id: NodeId,
    peers: Vec<PeerInfo<S>>,
    receiver: Option<R>,  // Option so we can take() it in run()
    storage: Box<dyn Storage<V> + Send>,
    protocol: ProtocolState<V>,
    proposal_rx: mpsc::Receiver<V>,
    decision_tx: mpsc::Sender<Decided<V>>,
}
```
- Change `new()`, `with_id()`, `with_id_inner()` signatures:
```rust
pub fn new(
    name: impl Into<Arc<str>>,
    peers: Vec<PeerInfo<S>>,
    receiver: R,
    storage: impl Storage<V> + 'static,
) -> (Self, NodeHandle<V>, DecisionReceiver<V>)
```
- Store `receiver: Some(receiver)` in Node struct.

- [ ] **Step 3: Rewrite event loop to use single receiver**

In `Node::run()`:
- Take the receiver: `let mut receiver = self.receiver.take().expect("run() called twice");`
- Remove the per-peer receiver spawning loop
- Extract senders from peers: `let senders: Vec<(NodeId, S)> = self.peers.into_iter().map(|p| (p.id, p.sender)).collect();`
- The `has_peers` check stays the same
- In the `has_peers` select branch, replace `incoming_rx.recv()` with `receiver.recv()`:
```rust
incoming = receiver.recv() => {
    match incoming {
        Ok(data) => {
            self.handle_incoming_message(&data, &senders).await?;
        }
        Err(e) => {
            tracing::warn!(error = %e, "receiver error");
            return Err(NodeError::NoQuorum);
        }
    }
}
```
- Change `handle_incoming_message` — remove `from: NodeId` parameter. Instead, deserialize `Message<V>` and extract sender:
```rust
async fn handle_incoming_message(
    &mut self,
    data: &[u8],
    senders: &[(NodeId, S)],
) -> Result<(), NodeError> {
    match Message::<V>::from_bytes(data) {
        Ok(msg) => {
            let outgoing = self.protocol.handle_message(msg.sender, msg.variant);
            self.send_outgoing(&outgoing, senders).await;
            self.process_decisions().await?;
            let lost = self.protocol.take_lost_proposals();
            for value in lost {
                let (_, outgoing) = self.protocol.propose(value);
                self.send_outgoing(&outgoing, senders).await;
                self.process_decisions().await?;
            }
        }
        Err(e) => {
            tracing::warn!(error = %e, "failed to deserialize message");
        }
    }
    Ok(())
}
```

- [ ] **Step 4: Update send_outgoing to wrap with sender NodeId**

`send_outgoing` now wraps `MessageVariant<V>` in `Message<V>` before serializing:
```rust
async fn send_outgoing(&self, outgoing: &[Outgoing<V>], senders: &[(NodeId, S)]) {
    for out in outgoing {
        let msg = Message {
            sender: self.node_id.clone(),
            variant: out.message.clone(),
        };
        let bytes = match msg.to_bytes() {
            Ok(b) => b,
            Err(e) => {
                tracing::error!(error = %e, "failed to serialize message");
                continue;
            }
        };
        match &out.target {
            SendTarget::Peer(target) => {
                if let Some((_, sender)) = senders.iter().find(|(id, _)| id == target) {
                    let _ = sender.send(bytes).await;
                }
            }
            SendTarget::Broadcast => {
                for (_, sender) in senders {
                    let _ = sender.send(bytes.clone()).await;
                }
            }
        }
    }
}
```
Note: `send_outgoing` is no longer a static method — it needs `&self` to access `self.node_id`. The `handle_proposal` and `handle_retries` methods need to change from `Self::send_outgoing(...)` to `self.send_outgoing(...)`. This requires restructuring to avoid borrow conflicts with `&mut self` (since `process_decisions` borrows `&mut self`). Solution: extract senders into a local variable before the loop, and make `send_outgoing` take `node_id` as a parameter instead:

```rust
async fn send_outgoing(node_id: &NodeId, outgoing: &[Outgoing<V>], senders: &[(NodeId, S)]) {
    for out in outgoing {
        let msg = Message {
            sender: node_id.clone(),
            variant: out.message.clone(),
        };
        // ... same as above
    }
}
```

Call sites: `Self::send_outgoing(&self.node_id, &outgoing, &senders).await;`

- [ ] **Step 5: Update all tests in node.rs**

- `DummyReceiver` stays the same (used for single-node tests — receiver that never returns)
- `node_new_returns_node_handle_and_receiver` — pass `DummyReceiver` as third arg
- `node_handle_is_cloneable` — pass `DummyReceiver` as third arg
- `propose_returns_channel_full_when_full` — pass `DummyReceiver` as third arg
- `single_node_consensus` — pass `DummyReceiver` as third arg
- `three_node_consensus` — major rewrite: use single receiver per node with mpsc channels

For the 3-node test with new API:
```rust
#[tokio::test]
async fn three_node_consensus() {
    use tokio::sync::mpsc as tokio_mpsc;

    struct ChannelSender(tokio_mpsc::Sender<Bytes>);
    #[async_trait::async_trait]
    impl MessageSender for ChannelSender {
        async fn send(&self, data: Bytes) -> Result<(), TransportError> {
            self.0.send(data).await.map_err(|_| TransportError::Closed)
        }
    }
    impl Clone for ChannelSender {
        fn clone(&self) -> Self { Self(self.0.clone()) }
    }

    struct ChannelReceiver(tokio_mpsc::Receiver<Bytes>);
    #[async_trait::async_trait]
    impl MessageReceiver for ChannelReceiver {
        async fn recv(&mut self) -> Result<Bytes, TransportError> {
            self.0.recv().await.ok_or(TransportError::Closed)
        }
    }

    // One channel per node (single receiver)
    let (tx_a, rx_a) = tokio_mpsc::channel(64);
    let (tx_b, rx_b) = tokio_mpsc::channel(64);
    let (tx_c, rx_c) = tokio_mpsc::channel(64);

    let id_a = NodeId::new("a", 1000);
    let id_b = NodeId::new("b", 1000);
    let id_c = NodeId::new("c", 1000);

    // Node A: senders to B and C, receives on rx_a
    let (node_a, handle_a, mut drx_a) = Node::with_id(
        id_a.clone(),
        vec![
            PeerInfo { id: id_b.clone(), sender: ChannelSender(tx_b.clone()) },
            PeerInfo { id: id_c.clone(), sender: ChannelSender(tx_c.clone()) },
        ],
        ChannelReceiver(rx_a),
        MemoryStorage::<String>::new(),
    );
    let (node_b, _handle_b, mut drx_b) = Node::with_id(
        id_b.clone(),
        vec![
            PeerInfo { id: id_a.clone(), sender: ChannelSender(tx_a.clone()) },
            PeerInfo { id: id_c.clone(), sender: ChannelSender(tx_c.clone()) },
        ],
        ChannelReceiver(rx_b),
        MemoryStorage::<String>::new(),
    );
    let (node_c, _handle_c, mut drx_c) = Node::with_id(
        id_c.clone(),
        vec![
            PeerInfo { id: id_a.clone(), sender: ChannelSender(tx_a.clone()) },
            PeerInfo { id: id_b.clone(), sender: ChannelSender(tx_b.clone()) },
        ],
        ChannelReceiver(rx_c),
        MemoryStorage::<String>::new(),
    );

    tokio::spawn(node_a.run());
    tokio::spawn(node_b.run());
    tokio::spawn(node_c.run());

    handle_a.propose("hello".to_string()).await.unwrap();

    let timeout = std::time::Duration::from_secs(5);
    let da = tokio::time::timeout(timeout, drx_a.recv()).await.unwrap().unwrap();
    let db = tokio::time::timeout(timeout, drx_b.recv()).await.unwrap().unwrap();
    let dc = tokio::time::timeout(timeout, drx_c.recv()).await.unwrap().unwrap();

    assert_eq!(da.value, "hello");
    assert_eq!(db.value, "hello");
    assert_eq!(dc.value, "hello");
    assert_eq!(da.slot, db.slot);
    assert_eq!(db.slot, dc.slot);
}
```

- [ ] **Step 6: Update lib.rs re-exports**

```rust
pub use config::{NodeId, PeerInfo};
```
Remove `PeerConfig` from re-exports.

- [ ] **Step 7: Run tests**

Run: `cargo test -p concensus`
Expected: All 54 tests pass

- [ ] **Step 8: Commit**

```bash
git add crates/concensus/src/
git commit -m "refactor: single receiver per node, PeerConfig→PeerInfo, Message wraps MessageVariant"
```

---

## Chunk 2: Channel Transport

### Task 3: Feature Flags in Cargo.toml

**Files:**
- Modify: `crates/concensus/Cargo.toml`

- [ ] **Step 1: Add feature flags**

Add to `crates/concensus/Cargo.toml`:
```toml
[features]
channel-transport = []
tcp-transport = ["tokio/net", "tokio/io-util"]
```

- [ ] **Step 2: Verify compilation**

Run: `cargo check -p concensus`
Expected: success

- [ ] **Step 3: Commit**

```bash
git add crates/concensus/Cargo.toml
git commit -m "feat: add feature flags for channel-transport and tcp-transport"
```

---

### Task 4: Channel Transport Implementation

**Files:**
- Create: `crates/concensus/src/transport/channel.rs`
- Modify: `crates/concensus/src/transport.rs`
- Modify: `crates/concensus/src/lib.rs`

- [ ] **Step 1: Add submodule declaration to transport.rs**

Add to `crates/concensus/src/transport.rs` (after existing trait definitions):
```rust
#[cfg(feature = "channel-transport")]
pub mod channel;
```

- [ ] **Step 2: Create transport directory**

Run: `mkdir -p crates/concensus/src/transport`

- [ ] **Step 3: Write channel.rs with tests**

Create `crates/concensus/src/transport/channel.rs`:

```rust
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

pub struct ChannelSender {
    inner: SenderInner,
}

pub struct ChannelReceiver {
    inner: ReceiverInner,
}

/// Creates a bounded channel pair. `send()` waits for capacity (backpressure).
pub fn channel(capacity: usize) -> (ChannelSender, ChannelReceiver) {
    let (tx, rx) = mpsc::channel(capacity);
    (
        ChannelSender { inner: SenderInner::Bounded(tx) },
        ChannelReceiver { inner: ReceiverInner::Bounded(rx) },
    )
}

/// Creates an unbounded channel pair. `send()` never blocks.
pub fn unbounded_channel() -> (ChannelSender, ChannelReceiver) {
    let (tx, rx) = mpsc::unbounded_channel();
    (
        ChannelSender { inner: SenderInner::Unbounded(tx) },
        ChannelReceiver { inner: ReceiverInner::Unbounded(rx) },
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
            SenderInner::Bounded(tx) => {
                tx.send(data).await.map_err(|_| TransportError::Closed)
            }
            SenderInner::Unbounded(tx) => {
                tx.send(data).map_err(|_| TransportError::Closed)
            }
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
        // Both messages received (order may vary in bounded channel)
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
```

- [ ] **Step 4: Add feature-gated re-export to lib.rs**

Add to `crates/concensus/src/lib.rs`:
```rust
#[cfg(feature = "channel-transport")]
pub use transport::channel::{self, channel, unbounded_channel, ChannelSender, ChannelReceiver};
```

- [ ] **Step 5: Run tests with feature enabled**

Run: `cargo test -p concensus --features channel-transport`
Expected: All tests pass (54 existing + 5 new channel tests = 59)

- [ ] **Step 6: Commit**

```bash
git add crates/concensus/src/transport.rs crates/concensus/src/transport/ crates/concensus/src/lib.rs
git commit -m "feat: add in-memory channel transport (bounded + unbounded)"
```

---

## Chunk 3: TCP Transport

### Task 5: TCP Transport Implementation

**Files:**
- Create: `crates/concensus/src/transport/tcp.rs`
- Modify: `crates/concensus/src/transport.rs`
- Modify: `crates/concensus/src/lib.rs`

- [ ] **Step 1: Add tcp submodule declaration to transport.rs**

Add to `crates/concensus/src/transport.rs`:
```rust
#[cfg(feature = "tcp-transport")]
pub mod tcp;
```

- [ ] **Step 2: Implement TcpSender**

Create `crates/concensus/src/transport/tcp.rs` with TcpSender:

```rust
use std::net::SocketAddr;

use async_trait::async_trait;
use bytes::Bytes;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::{mpsc, Mutex};

use crate::config::NodeId;
use crate::error::TransportError;
use crate::transport::{MessageReceiver, MessageSender};

const MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024; // 16 MiB

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
            let (_, write_half) = stream.into_split();
            *guard = Some(write_half);
        }

        let writer = guard.as_mut().unwrap();

        // Write length prefix (4 bytes BE) + payload
        let len = data.len() as u32;
        if let Err(e) = writer.write_all(&len.to_be_bytes()).await {
            *guard = None;
            return Err(TransportError::Closed);
        }
        if let Err(e) = writer.write_all(&data).await {
            *guard = None;
            return Err(TransportError::Closed);
        }
        if let Err(e) = writer.flush().await {
            *guard = None;
            return Err(TransportError::Closed);
        }

        Ok(())
    }
}
```

- [ ] **Step 3: Implement TcpReceiver**

Add to `tcp.rs`:

```rust
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;

const LISTENER_CHANNEL_CAPACITY: usize = 1024;
const INITIAL_REBIND_DELAY_MS: u64 = 100;
const MAX_REBIND_DELAY_MS: u64 = 5000;

pub struct TcpReceiver {
    incoming_rx: mpsc::Receiver<Bytes>,
}

impl TcpReceiver {
    pub async fn bind(addr: SocketAddr) -> Result<Self, TransportError> {
        let listener = TcpListener::bind(addr)
            .await
            .map_err(|e| TransportError::Other(Box::new(e)))?;

        let (incoming_tx, incoming_rx) = mpsc::channel(LISTENER_CHANNEL_CAPACITY);

        // Spawn accept loop
        tokio::spawn(Self::accept_loop(listener, addr, incoming_tx));

        Ok(Self { incoming_rx })
    }

    async fn accept_loop(
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

                    // Try to rebind with exponential backoff
                    let mut delay_ms = INITIAL_REBIND_DELAY_MS;
                    loop {
                        if incoming_tx.is_closed() {
                            return; // Receiver dropped, shut down
                        }

                        // Add jitter: ±25%
                        let jitter = (delay_ms as f64 * 0.25 * (2.0 * rand::random::<f64>() - 1.0)) as i64;
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
            if let Err(_) = reader.read_exact(&mut len_buf).await {
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
            if let Err(_) = reader.read_exact(&mut payload).await {
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
```

- [ ] **Step 4: Implement TcpTransport factory**

Add to `tcp.rs`:

```rust
use crate::config::PeerInfo;

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
```

- [ ] **Step 5: Add tests**

Add to `tcp.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn tcp_sender_receiver_roundtrip() {
        // Bind receiver
        let mut receiver = TcpReceiver::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

        // Get the actual bound address (port 0 picks a random port)
        // We need to extract the address — TcpReceiver doesn't expose it directly.
        // For testing, we'll bind on a known port.
        // Actually, we need to expose the bound address. Add a field for it.
        // Alternative: bind manually, get the address, then pass the listener.

        // Simpler approach for testing: use a known available port
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let listener = TcpListener::bind(addr).await.unwrap();
        let bound_addr = listener.local_addr().unwrap();

        let (tx, rx) = mpsc::channel(16);
        tokio::spawn(TcpReceiver::accept_loop(listener, bound_addr, tx));
        let mut receiver = TcpReceiver { incoming_rx: rx };

        // Create sender pointing to the receiver's address
        let sender = TcpSender::new(bound_addr);

        // Send a message
        let payload = Bytes::from("hello-tcp");
        sender.send(payload.clone()).await.unwrap();

        // Receive it
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
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let listener = TcpListener::bind(addr).await.unwrap();
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
        ).await.unwrap().unwrap();
        assert_eq!(r1, Bytes::from("msg-1"));

        // Force-close the connection by replacing it
        {
            let mut guard = sender.conn.lock().await;
            *guard = None; // Simulate connection drop
        }

        // Second send — should reconnect
        sender.send(Bytes::from("msg-2")).await.unwrap();
        let r2 = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            receiver.recv(),
        ).await.unwrap().unwrap();
        assert_eq!(r2, Bytes::from("msg-2"));
    }

    #[tokio::test]
    async fn tcp_rejects_oversized_message() {
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let listener = TcpListener::bind(addr).await.unwrap();
        let bound_addr = listener.local_addr().unwrap();

        let (tx, rx) = mpsc::channel(16);
        tokio::spawn(TcpReceiver::accept_loop(listener, bound_addr, tx));
        let mut receiver = TcpReceiver { incoming_rx: rx };

        // Manually connect and send an oversized length prefix
        let mut stream = TcpStream::connect(bound_addr).await.unwrap();
        let fake_len = (MAX_MESSAGE_SIZE as u32) + 1;
        stream.write_all(&fake_len.to_be_bytes()).await.unwrap();
        stream.flush().await.unwrap();

        // The reader task should drop this connection.
        // A subsequent valid message from a new connection should still work.
        let sender = TcpSender::new(bound_addr);
        sender.send(Bytes::from("valid")).await.unwrap();

        let received = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            receiver.recv(),
        ).await.unwrap().unwrap();
        assert_eq!(received, Bytes::from("valid"));
    }

    #[tokio::test]
    async fn tcp_transport_factory() {
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let listener = TcpListener::bind(addr).await.unwrap();
        let bound_addr = listener.local_addr().unwrap();
        drop(listener); // Free the port for TcpTransport::new

        let id_peer = NodeId::new("peer", 1000);
        let result = TcpTransport::new(
            bound_addr,
            vec![(id_peer.clone(), "127.0.0.1:9999".parse().unwrap())],
        ).await;

        assert!(result.is_ok());
        let (peers, _receiver) = result.unwrap();
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].id, id_peer);
    }
}
```

- [ ] **Step 6: Add feature-gated re-export and tcp submodule declaration**

Add to `crates/concensus/src/transport.rs`:
```rust
#[cfg(feature = "tcp-transport")]
pub mod tcp;
```

Add to `crates/concensus/src/lib.rs`:
```rust
#[cfg(feature = "tcp-transport")]
pub use transport::tcp::{TcpSender, TcpReceiver, TcpTransport};
```

- [ ] **Step 7: Run tests with tcp feature**

Run: `cargo test -p concensus --features tcp-transport`
Expected: All tests pass (54 existing + TCP tests)

- [ ] **Step 8: Run all tests with all features**

Run: `cargo test -p concensus --all-features`
Expected: All tests pass

- [ ] **Step 9: Commit**

```bash
git add crates/concensus/src/transport/ crates/concensus/src/transport.rs crates/concensus/src/lib.rs
git commit -m "feat: add TCP transport with length-prefixed framing and reconnection"
```

---

### Task 6: Final Cleanup

**Files:**
- All source files

- [ ] **Step 1: Run clippy with all features**

Run: `cargo clippy -p concensus --all-features -- -D warnings`
Expected: no warnings

- [ ] **Step 2: Fix any clippy issues**

- [ ] **Step 3: Run full test suite with all features**

Run: `cargo test -p concensus --all-features`
Expected: All tests pass

- [ ] **Step 4: Run cargo doc**

Run: `cargo doc -p concensus --all-features --no-deps`
Expected: success

- [ ] **Step 5: Commit cleanup**

```bash
git add -A
git commit -m "chore: fix clippy warnings and clean up"
```
