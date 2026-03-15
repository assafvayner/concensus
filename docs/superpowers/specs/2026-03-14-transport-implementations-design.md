# Transport Implementations Design

## Overview

Add two `MessageSender`/`MessageReceiver` implementations to the `concensus` crate behind feature flags: an in-memory channel transport for testing and a TCP transport for real networked deployments. This also requires refactoring the core `Node` API to use a single receiver per node (rather than one per peer) and restructuring `Message<V>` to include sender identity.

## Prerequisites: Core API Changes

### Message Refactor

Rename the current `Message<V>` enum to `MessageVariant<V>`. Introduce a new `Message<V>` struct that wraps the variant with sender identity:

```rust
// message.rs
pub(crate) type ProposalNumber = (u64, NodeId);

#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) struct Message<V> {
    pub sender: NodeId,
    pub variant: MessageVariant<V>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) enum MessageVariant<V> {
    Prepare { slot: u64, proposal_number: ProposalNumber },
    Promise { slot: u64, proposal_number: ProposalNumber, accepted: Option<(ProposalNumber, V)> },
    Accept { slot: u64, proposal_number: ProposalNumber, value: V },
    Accepted { slot: u64, proposal_number: ProposalNumber, value: V },
    Decide { slot: u64, value: V },
    NackPrepare { slot: u64, proposal_number: ProposalNumber, highest_promised: ProposalNumber },
    NackAccept { slot: u64, proposal_number: ProposalNumber, highest_promised: ProposalNumber },
}
```

`to_bytes()` and `from_bytes()` operate on `Message<V>` (the wrapper struct). Both `Message<V>` and `MessageVariant<V>` are `pub(crate)` — not part of the public API. The node serializes `Message { sender: self.node_id, variant }` before calling `send()`.

### Node API Change

Replace `PeerConfig<S, R>` with `PeerInfo<S>` and accept a single receiver:

```rust
// config.rs
pub struct PeerInfo<S: MessageSender> {
    pub id: NodeId,
    pub sender: S,
}
```

```rust
// node.rs
impl<V, S, R> Node<V, S, R>
where
    V: Serialize + DeserializeOwned + Clone + Send + 'static,
    S: MessageSender,
    R: MessageReceiver,
{
    pub fn new(
        name: impl Into<Arc<str>>,
        peers: Vec<PeerInfo<S>>,
        receiver: R,
        storage: impl Storage<V> + 'static,
    ) -> (Self, NodeHandle<V>, DecisionReceiver<V>);
}
```

### Event Loop Change

The event loop no longer spawns a task per peer receiver. Instead, it calls `receiver.recv()` directly in the `tokio::select!` loop.

**Incoming path:** `receiver.recv()` returns `Bytes`. The node deserializes this into `Message<V>`, extracts `msg.sender` as the `from: NodeId`, and passes `msg.variant` to the protocol state machine. The previous `from` parameter (which came from per-peer receiver identity) is replaced entirely by `msg.sender`.

**Outgoing path:** `ProtocolState` continues to produce `Outgoing<V>` where the message field is `MessageVariant<V>` (renamed from the old `Message<V>`). The `send_outgoing()` method in the node wraps each variant as `Message { sender: self.node_id.clone(), variant }`, serializes to bytes, and calls `sender.send(bytes)`.

For the single-node (no peers) case, the receiver is never polled — the separate `select!` branch without incoming messages is retained.

### Protocol State Machine

`ProtocolState::handle_message()` and related methods change to accept `MessageVariant<V>` instead of `Message<V>` (since the sender is extracted at the node level). The `from: NodeId` parameter is already passed separately. `Outgoing<V>.message` changes from `Message<V>` to `MessageVariant<V>` — the node layer wraps it with sender identity before serialization.

## Module Structure

The existing `transport.rs` stays in place as the module root. Submodules are added in a `transport/` directory alongside it (Rust 2018+ path-based module system — `transport.rs` acts as the module root for `transport/` submodules). `transport.rs` gains `pub mod channel;` and `pub mod tcp;` declarations behind feature gates.

```
crates/concensus/src/
├── transport.rs           # MessageSender, MessageReceiver traits + submodule declarations
├── transport/
│   ├── channel.rs         # #[cfg(feature = "channel-transport")]
│   └── tcp.rs             # #[cfg(feature = "tcp-transport")]
├── config.rs              # NodeId, PeerInfo<S>
├── error.rs
├── message.rs             # Message<V>, MessageVariant<V>
├── node.rs
├── protocol.rs
├── storage.rs
└── lib.rs
```

## Feature Flags

```toml
[features]
channel-transport = []
tcp-transport = ["tokio/net", "tokio/io-util"]

[dependencies]
tokio = { version = "1", features = ["sync", "macros", "rt", "time"] }
```

- `channel-transport` — no extra dependencies, uses `tokio::sync::mpsc` (already a dependency)
- `tcp-transport` — adds `tokio/net` (TCP sockets) and `tokio/io-util` (`AsyncReadExt`/`AsyncWriteExt` for length-prefixed framing)

## In-Memory Channel Transport

**Feature flag:** `channel-transport`

```rust
// transport/channel.rs

pub struct ChannelSender {
    tx: SenderInner,  // enum wrapping bounded or unbounded sender
}

pub struct ChannelReceiver {
    rx: ReceiverInner,  // enum wrapping bounded or unbounded receiver
}

/// Creates a bounded (sender, receiver) pair. The sender is cloneable —
/// clone it once per peer that needs to send to this node.
/// `send()` waits for capacity (backpressure).
pub fn channel(capacity: usize) -> (ChannelSender, ChannelReceiver);

/// Creates an unbounded (sender, receiver) pair. The sender is cloneable.
/// `send()` never blocks — messages buffer without limit.
pub fn unbounded_channel() -> (ChannelSender, ChannelReceiver);

impl Clone for ChannelSender { ... }
```

Internally, `ChannelSender` and `ChannelReceiver` use an enum to wrap either the bounded or unbounded variant:

```rust
enum SenderInner {
    Bounded(mpsc::Sender<Bytes>),
    Unbounded(mpsc::UnboundedSender<Bytes>),
}

enum ReceiverInner {
    Bounded(mpsc::Receiver<Bytes>),
    Unbounded(mpsc::UnboundedReceiver<Bytes>),
}
```

**Bounded channel:** `ChannelSender` implements `MessageSender` by forwarding to `mpsc::Sender::send().await` (async, waits for capacity). This provides backpressure — if the receiving node is slow, the sending node's event loop blocks on `send()` until there's capacity. Useful for deterministic testing.

**Unbounded channel:** `ChannelSender` implements `MessageSender` by forwarding to `mpsc::UnboundedSender::send()` (infallible unless receiver dropped). Messages buffer without limit. Useful when backpressure is undesirable (e.g., high-throughput testing, or when the consumer is always fast enough).

`ChannelReceiver` implements `MessageReceiver` by forwarding to the appropriate `recv()` — both bounded and unbounded receivers have the same `recv()` signature.

**Usage for a 3-node cluster:**

```rust
use concensus::transport::channel::{channel, unbounded_channel};

// Bounded — with backpressure
let (sender_to_a, receiver_a) = channel(64);
let (sender_to_b, receiver_b) = channel(64);
let (sender_to_c, receiver_c) = channel(64);

// Or unbounded — no backpressure
// let (sender_to_a, receiver_a) = unbounded_channel();
// let (sender_to_b, receiver_b) = unbounded_channel();
// let (sender_to_c, receiver_c) = unbounded_channel();

// Node A: senders to B and C, its own receiver
let (node_a, handle_a, rx_a) = Node::new(
    "a",
    vec![
        PeerInfo { id: id_b, sender: sender_to_b.clone() },
        PeerInfo { id: id_c, sender: sender_to_c.clone() },
    ],
    receiver_a,
    MemoryStorage::new(),
);
// Nodes B and C similarly
```

## TCP Transport

**Feature flag:** `tcp-transport`

### Message Framing

4-byte big-endian length prefix followed by the payload:

```
[4 bytes: payload length as u32 BE] [payload bytes]
```

This is used for both sending and receiving. The transport layer handles framing; the caller sees complete `Bytes` messages.

**Maximum message size:** 16 MiB (`16 * 1024 * 1024` bytes). If a length prefix exceeds this, the receiver drops the connection (the peer is considered buggy or malicious). The sender should never produce messages this large in normal operation — Paxos messages are small.

### TcpSender

One per peer. Wraps a TCP connection to the peer's listen address.

```rust
pub struct TcpSender {
    addr: SocketAddr,
    conn: tokio::sync::Mutex<Option<OwnedWriteHalf>>,
}

impl TcpSender {
    pub fn new(addr: SocketAddr) -> Self;
}
```

**`send()` behavior:**
1. Lock the mutex.
2. If no connection, connect to `addr`. Split the `TcpStream` — store the `OwnedWriteHalf`, drop the `OwnedReadHalf` (the sender's connection is write-only; inbound data from peers arrives via the `TcpReceiver`'s listener, not via the sender's connection).
3. Write 4-byte BE length prefix + payload.
4. If write fails, drop the connection (set to `None`), return `TransportError::Closed`.
5. Next `send()` call will reconnect lazily.

The sender does NOT retry the current message on failure. Paxos tolerates message loss — the protocol will retry the proposal via its own retry mechanism.

`tokio::sync::Mutex` is used because the lock is held across `.await` (socket write). Contention is low since the node event loop sends sequentially.

### TcpReceiver

One per node. Wraps a TCP listener that accepts connections from any peer.

```rust
pub struct TcpReceiver {
    incoming_rx: mpsc::Receiver<Bytes>,
}

impl TcpReceiver {
    pub async fn bind(addr: SocketAddr) -> Result<Self, TransportError>;
}
```

**Internal architecture:**

`bind()` creates an `mpsc::channel(1024)` (bounded, capacity 1024 — provides backpressure if the node falls behind processing messages) and spawns a background accept loop task:

1. **Accept loop task:** Binds `TcpListener`, accepts connections in a loop. For each accepted connection, spawns a reader task. If the listener encounters an error, logs a warning and retries binding with exponential backoff: initial delay 100ms, doubling each attempt, capped at 5 seconds, with random jitter (±25%). Already-connected peers continue uninterrupted.

2. **Reader task (one per accepted connection):** Reads length-prefixed messages from the `OwnedReadHalf`. Validates length prefix against the 16 MiB maximum. Forwards each complete message as `Bytes` into `incoming_rx`. If the read errors or the length exceeds the maximum, the task ends — the peer will reconnect via its `TcpSender`, and the accept loop will spawn a new reader task.

**Multiple connections from the same peer:** If a peer reconnects while a previous connection's reader task is still running, both reader tasks will feed into `incoming_rx`. This is safe — messages carry sender identity in the payload, and the stale connection will eventually error out and its reader task will end. No deduplication is needed.

**`recv()` behavior:** Reads from `incoming_rx`. Returns `TransportError::Closed` only if the internal channel closes (all background tasks dead AND listener can't rebind).

**Graceful shutdown:** When `TcpReceiver` is dropped, the `incoming_rx` receiver side closes. Background tasks detect this when their `incoming_tx.send()` calls return `Err` — they exit cleanly. The accept loop also needs to `select!` between `listener.accept()` and a check on the channel (e.g., try a zero-capacity send or use a `CancellationToken`). Alternatively, the accept loop can check `incoming_tx.is_closed()` between accepts.

### TcpTransport Factory

Convenience for creating the full transport layer for a node:

```rust
pub struct TcpTransport;

impl TcpTransport {
    pub async fn new(
        bind_addr: SocketAddr,
        peers: Vec<(NodeId, SocketAddr)>,
    ) -> Result<(Vec<PeerInfo<TcpSender>>, TcpReceiver), TransportError> {
        let receiver = TcpReceiver::bind(bind_addr).await?;
        let peers = peers.into_iter().map(|(id, addr)| {
            PeerInfo { id, sender: TcpSender::new(addr) }
        }).collect();
        Ok((peers, receiver))
    }
}
```

## Error Handling

The existing `TransportError` enum is sufficient:

```rust
pub enum TransportError {
    Closed,
    Other(Box<dyn std::error::Error + Send + Sync>),
}
```

| Scenario | Error |
|---|---|
| `ChannelSender::send()` — receiver dropped | `Closed` |
| `ChannelReceiver::recv()` — all senders dropped | `Closed` |
| `TcpSender::send()` — connection broken | `Closed` (reconnects on next call) |
| `TcpReceiver::recv()` — all background tasks dead | `Closed` |
| `TcpReceiver::bind()` — initial bind fails | `Other(io::Error)` |
| Individual TCP connection read error | Handled internally, task ends, peer reconnects |
| TCP listener error after initial bind | Handled internally with rebind + backoff, existing connections unaffected |

## Public API Re-exports

```rust
// lib.rs
pub mod transport; // always: traits + feature-gated sub-modules

pub use config::{NodeId, PeerInfo};
// ... existing re-exports ...

#[cfg(feature = "channel-transport")]
pub use transport::channel::{self, channel, unbounded_channel, ChannelSender, ChannelReceiver};
#[cfg(feature = "tcp-transport")]
pub use transport::tcp::{self, TcpSender, TcpReceiver, TcpTransport};
```

## Design Decisions & Rationale

| Decision | Rationale |
|---|---|
| Single receiver per node | Matches natural network model (one listen socket); simpler than per-peer receivers |
| `Message<V>` wraps `MessageVariant<V>` with sender NodeId | Transport stays pure bytes; identity is in the message format; extensible for future metadata |
| `PeerInfo<S>` replaces `PeerConfig<S, R>` | Receiver is no longer per-peer; sender-only peer config is cleaner |
| Feature flags in core crate | Simple for two transports; avoids multi-crate overhead |
| `channel-transport` has no extra deps | Just uses tokio mpsc which is already a dependency |
| `tcp-transport` adds only `tokio/net` + `tokio/io-util` | Minimal additions; only when explicitly opted in |
| 4-byte BE length prefix framing | Industry standard, handles arbitrary payloads, no escaping needed |
| Lazy connect + reconnect on TcpSender | Simple; Paxos tolerates message loss so no need to retry current message |
| TcpReceiver keeps live connections on listener failure | Maximizes availability; only new/reconnecting peers affected |
| Listener rebind with backoff | Recovers from transient OS errors without tight loops |
| `tokio::sync::Mutex` for TcpSender connection | Held across `.await`; low contention (sequential sends from event loop) |
| `ChannelSender` is `Clone` | One channel per node, senders cloned for each peer — natural mpsc pattern |
| Bounded and unbounded channel variants | Bounded provides backpressure for deterministic testing; unbounded for high-throughput scenarios |
| Single `ChannelSender`/`ChannelReceiver` type for both | Enum internally; same public type avoids generic parameter pollution in `Node` |
| 16 MiB max message size | Prevents OOM from malicious/buggy length prefixes; Paxos messages are small |
| TcpSender drops OwnedReadHalf | Sender connection is write-only; inbound data arrives via TcpReceiver's listener |
| Listener rebind: 100ms initial, doubling, 5s cap, ±25% jitter | Standard exponential backoff; prevents tight retry loops |
| TcpReceiver internal channel capacity 1024 | Bounded for backpressure; large enough for burst traffic |
| Multiple connections from same peer are allowed | Stale connections error out naturally; no deduplication needed |
| Background tasks exit on TcpReceiver drop | Channel close propagates to tasks via send() failure |
| `Outgoing<V>.message` is `MessageVariant<V>` | Node wraps with sender identity; protocol doesn't know about transport framing |
