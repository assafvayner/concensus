# Paxos Consensus Library Design

## Overview

A Rust library implementing the Paxos consensus algorithm. The library provides a `Node<V, S, R>` struct that participates in the Paxos protocol to reach consensus on a sequence of values. Designed for modularity: users supply their own transport and can swap storage backends.

**Scope for v1:** Basic (single-decree) Paxos with in-memory storage. Architecture supports extension to Multi-Paxos and persistent storage (e.g., DuckDB) in future versions.

## Project Structure

Cargo workspace with a single published crate. A non-published testing crate will be added later.

```
concensus/
├── Cargo.toml              # workspace root
├── crates/
│   └── concensus/
│       ├── Cargo.toml
│       └── src/
│           ├── lib.rs       # re-exports public API
│           ├── node.rs      # Node<V, S, R> struct + run loop
│           ├── protocol.rs  # Paxos state machine (internal)
│           ├── message.rs   # Protocol message types
│           ├── transport.rs # Sender + Receiver traits
│           ├── storage.rs   # Storage trait + MemoryStorage
│           ├── config.rs    # NodeId, PeerConfig
│           └── error.rs     # thiserror error types
```

## Public API

### Node Identity

```rust
// config.rs
#[derive(Clone, Debug, Serialize, Deserialize, Hash, Eq, PartialEq)]
pub struct NodeId(Arc<str>);
// Implements: From<String>, From<&str>, Display
```

`Arc<str>` for cheap clones without lifetime parameters. Serializes as a plain string via serde.

### Transport Traits

Transport operates on raw bytes. The library serializes its internal `Message<V>` types to bytes before passing to the transport layer. One sender + receiver pair per peer.

```rust
// transport.rs
#[async_trait]
pub trait Sender: Send + 'static {
    async fn send(&self, data: Bytes) -> Result<(), TransportError>;
}

#[async_trait]
pub trait Receiver: Send + 'static {
    async fn recv(&mut self) -> Result<Bytes, TransportError>;
}
```

Reconnection is the responsibility of the `Receiver` implementor. When a peer connection fails, the node continues operating with remaining peers as long as a quorum is reachable.

### Peer Configuration

```rust
// config.rs
pub struct PeerConfig<S: Sender, R: Receiver> {
    pub id: NodeId,
    pub sender: S,
    pub receiver: R,
}
```

All peers use the same `S` and `R` types (one transport implementation per node).

### Node

```rust
// node.rs
pub struct Node<V, S: Sender, R: Receiver> { ... }

pub type DecisionReceiver<V> = mpsc::Receiver<Decided<V>>;

#[derive(Clone, Debug)]
pub struct Decided<V> {
    pub slot: u64,
    pub value: V,
}

impl<V, S, R> Node<V, S, R>
where
    V: Serialize + DeserializeOwned + Clone + Send + 'static,
    S: Sender,
    R: Receiver,
{
    pub fn new(
        id: NodeId,
        peers: Vec<PeerConfig<S, R>>,
        storage: impl Storage<V> + Send + 'static,
    ) -> (Self, DecisionReceiver<V>);

    /// Submit a value for consensus. Returns when the proposal is enqueued.
    pub async fn propose(&self, value: V) -> Result<(), ProposeError>;

    /// Run the protocol event loop. The caller drives this — either via
    /// tokio::spawn, on the main thread, or on a dedicated OS thread.
    pub async fn run(self) -> Result<(), NodeError>;
}
```

**Future extension (not v1):** `propose()` could return a `DecisionFuture<V>` that resolves when the specific value is decided, allowing callers to await the outcome of their proposal.

### Storage

Only decided values go through the storage trait. Acceptor state (promises, accepted values) is held in memory.

```rust
// storage.rs
#[async_trait]
pub trait Storage<V>: Send + 'static
where
    V: Serialize + DeserializeOwned + Clone + Send,
{
    async fn save_decision(&mut self, slot: u64, value: V) -> Result<(), StorageError>;
    async fn load_decisions(&self) -> Result<Vec<(u64, V)>, StorageError>;
}

pub struct MemoryStorage<V> {
    decisions: HashMap<u64, V>,
}

impl<V> MemoryStorage<V> {
    pub fn new() -> Self { ... }
}
```

Future backends (e.g., DuckDB) implement the same `Storage<V>` trait behind feature flags.

## Internal Design

### Protocol Messages

```rust
// message.rs (internal, serialized to JSON before transport)
pub type ProposalNumber = (u64, NodeId); // (round, proposer) — total ordering

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum Message<V> {
    // Phase 1
    Prepare { proposal_number: ProposalNumber },
    Promise {
        proposal_number: ProposalNumber,
        accepted: Option<(ProposalNumber, V)>,
    },

    // Phase 2
    Accept { proposal_number: ProposalNumber, value: V },
    Accepted { proposal_number: ProposalNumber },

    // Decision announcement
    Decide { slot: u64, value: V },
}
```

`ProposalNumber` is `(u64, NodeId)` — compare by round first, break ties by node ID. Standard Paxos approach for total ordering.

### Paxos State Machine

```rust
// protocol.rs (internal)
struct PaxosInstance<V> {
    slot: u64,
    // Proposer state
    proposal_number: ProposalNumber,
    promises_received: HashSet<NodeId>,
    highest_accepted: Option<(ProposalNumber, V)>,
    // Acceptor state
    highest_promised: Option<ProposalNumber>,
    accepted: Option<(ProposalNumber, V)>,
}
```

### Event Loop (`Node::run`)

The `run()` method uses `tokio::select!` to multiplex:

1. **Peer receivers** — incoming protocol messages from all peers
2. **Proposal channel** — values submitted via `propose()`
3. **Timers** — retries and timeouts for in-progress proposals

Each event drives the state machine for the relevant slot. When a quorum of `Accepted` messages is received, the value is decided: stored via `Storage`, sent on the `DecisionReceiver`, and a `Decide` message is broadcast to all peers.

### Quorum

Quorum size = `(total_nodes / 2) + 1` where `total_nodes` includes the local node. The node can make progress as long as a quorum of nodes is reachable (including itself).

## Error Types

```rust
// error.rs
#[derive(Error, Debug)]
pub enum ProposeError {
    #[error("node is not running")]
    NotRunning,
    #[error("proposal channel full")]
    ChannelFull,
}

#[derive(Error, Debug)]
pub enum NodeError {
    #[error("all peers disconnected, cannot form quorum")]
    NoQuorum,
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),
}

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("failed to persist decision: {0}")]
    Persist(String),
    #[error("failed to load decisions: {0}")]
    Load(String),
}

#[derive(Error, Debug)]
pub enum TransportError {
    #[error("connection closed")]
    Closed,
    #[error("transport error: {0}")]
    Other(Box<dyn std::error::Error + Send + Sync>),
}
```

## Dependencies

```toml
[dependencies]
tokio = { version = "1", features = ["sync", "macros", "rt"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
bytes = "1"
async-trait = "0.1"
thiserror = "2"
tracing = "0.1"
```

- `serde_json` for internal message serialization (readable format for v1; will be made configurable later)
- `bytes` for the `Bytes` type in transport traits
- `async-trait` until Rust stabilizes async traits in all needed positions
- `tokio` with `sync` (channels), `macros`, `rt` — user drives the runtime

## Design Decisions & Rationale

| Decision | Rationale |
|---|---|
| Basic Paxos first | Correct, testable foundation; Multi-Paxos layered later |
| Generic `V` with serde bounds | Type-safe; serde already needed for transport |
| `NodeId(Arc<str>)` | Cheap clones, no lifetime params, clean serde |
| Transport traits on `Bytes` | Decouples transport from protocol; transport doesn't know about `V` |
| Static dispatch for transport | Performance; all peers use same transport type |
| Caller-driven `run()` | User decides threading model (tokio::spawn, main thread, OS thread) |
| `propose()` returns on enqueue | Simple for v1; decision-tracking future documented as extension |
| `DecisionReceiver<V>` type alias | Clean API; wraps as stream trivially via `tokio_stream` |
| Storage trait for decisions only | Acceptor state is ephemeral; keeps storage interface minimal |
| JSON serialization for v1 | Readable/debuggable; configurable serialization planned for later |
| `tracing` for observability | De facto standard; opt-in for consumers |
| Peer failure = continue | Paxos tolerates minority failures; reconnection is transport's job |
