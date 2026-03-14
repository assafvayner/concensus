# Paxos Consensus Library Design

## Overview

A Rust library implementing the Paxos consensus algorithm. The library provides a `Node<V, S, R>` struct that participates in the Paxos protocol to reach consensus on a sequence of values. Each value is decided in an independent Paxos instance identified by a slot number. Designed for modularity: users supply their own transport and can swap storage backends.

**Scope for v1:** Multiple independent single-decree Paxos instances (one per slot), with in-memory acceptor state. Architecture supports extension to Multi-Paxos optimizations (stable leader, skipping prepare phase) and persistent storage (e.g., DuckDB) in future versions.

**v1 limitations:**
- Acceptor state is not persisted. A node that crashes and restarts has no memory of its promises or accepted values. This is safe because restarted nodes automatically get a new `NodeId` (same name, different incarnation timestamp), so the cluster treats them as a fresh participant. Crash recovery with the same identity (preserving promises) requires persistent acceptor state (future work).
- No catch-up protocol. `Decide` messages are broadcast once. If a peer is disconnected when a `Decide` is sent, it will have a gap in its decision sequence. A gap-detection and catch-up mechanism (e.g., `GetDecision { slot }` request/response) is future work.

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
#[derive(Clone, Debug, Serialize, Deserialize, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub struct NodeId {
    name: Arc<str>,       // user-provided, stable across restarts
    incarnation: u64,     // unix timestamp (seconds), set automatically in Node::new()
}
// Implements: Display (formats as "name/incarnation"), e.g. "node-1/1710412800"
```

`NodeId` combines a stable human-readable name with a per-run incarnation timestamp. This ensures a restarted node automatically gets a distinct identity (avoiding Paxos safety violations from stale promises) while remaining identifiable. The `name` component uses `Arc<str>` for cheap clones. `Node::new()` takes the name as a string and sets the incarnation to the current unix timestamp.

`Ord` ordering: compares by `name` first (lexicographic), then by `incarnation`. Since `incarnation` is a `u64`, numeric ordering is correct. Users should use consistent naming if name-based ordering matters to them.

### Transport Traits

Transport operates on raw bytes. The library serializes its internal `Message<V>` types to bytes before passing to the transport layer. One sender + receiver pair per peer.

Each `send()` call transmits exactly one complete message. Each `recv()` call returns exactly one complete message. Message framing (length-prefixing, delimiters, etc.) is the transport implementor's responsibility.

```rust
// transport.rs
#[async_trait]
pub trait MessageSender: Send + 'static {
    async fn send(&self, data: Bytes) -> Result<(), TransportError>;
}

#[async_trait]
pub trait MessageReceiver: Send + 'static {
    async fn recv(&mut self) -> Result<Bytes, TransportError>;
}
```

Reconnection is the responsibility of the `MessageReceiver` implementor. When a peer connection fails, the node continues operating with remaining peers as long as a quorum is reachable.

### Peer Configuration

```rust
// config.rs
pub struct PeerConfig<S: MessageSender, R: MessageReceiver> {
    pub id: NodeId,
    pub sender: S,
    pub receiver: R,
}
```

All peers use the same `S` and `R` types (one transport implementation per node).

### Node

`Node::new()` returns the node and a `DecisionReceiver`. Internally, `propose()` communicates with the event loop via a channel, so a `NodeHandle` is split off to allow calling `propose()` after `run()` consumes the node.

```rust
// node.rs
pub struct Node<V, S: MessageSender, R: MessageReceiver> { ... }
pub struct NodeHandle<V> { ... }

pub type DecisionReceiver<V> = mpsc::Receiver<Decided<V>>;

#[derive(Clone, Debug)]
pub struct Decided<V> {
    pub slot: u64,
    pub value: V,
}

impl<V, S, R> Node<V, S, R>
where
    V: Serialize + DeserializeOwned + Clone + Send + 'static,
    S: MessageSender,
    R: MessageReceiver,
{
    pub fn new(
        name: impl Into<Arc<str>>,   // stable node name; incarnation set automatically
        peers: Vec<PeerConfig<S, R>>,
        storage: impl Storage<V> + Send + 'static,
    ) -> (Self, NodeHandle<V>, DecisionReceiver<V>);

    /// Run the protocol event loop. The caller drives this — either via
    /// tokio::spawn, on the main thread, or on a dedicated OS thread.
    pub async fn run(self) -> Result<(), NodeError>;
}

impl<V> NodeHandle<V>
where
    V: Serialize + DeserializeOwned + Clone + Send + 'static,
{
    /// Submit a value for consensus. Returns when the proposal is enqueued.
    /// The value will be assigned a slot and driven through the Paxos protocol.
    pub async fn propose(&self, value: V) -> Result<(), ProposeError>;
}

impl<V> Clone for NodeHandle<V> { ... } // NodeHandle is cheaply cloneable (wraps an mpsc::Sender)
```

### Channel Configuration

Both internal channels (proposal and decision) are bounded:

- **Proposal channel** (NodeHandle -> event loop): bounded, default capacity 1024. Configurable via `NodeConfig` (future work). When full, `propose()` returns `ProposeError::ChannelFull`.
- **Decision channel** (event loop -> DecisionReceiver): bounded, default capacity 1024. If the consumer falls behind and the channel fills, the event loop will block on sending decisions until the consumer catches up. This provides backpressure — the protocol pauses rather than dropping decisions.

```rust
// Future: NodeConfig for channel sizing
// pub struct NodeConfig {
//     pub proposal_channel_capacity: usize,  // default: 1024
//     pub decision_channel_capacity: usize,  // default: 1024
// }
```

All nodes receive all decisions through `DecisionReceiver`, not just decisions for values they proposed. To correlate a proposal with its decision, the caller can scan the decision stream for their value (this requires the caller to track proposed values). **Future extension (not v1):** `propose()` could return a `DecisionFuture<V>` that resolves when the specific value is decided.

### Storage

Only decided values go through the storage trait. Acceptor state (promises, accepted values) is held in memory only (see v1 limitations above).

```rust
// storage.rs
#[async_trait]
pub trait Storage<V>: Send + 'static
where
    V: Serialize + DeserializeOwned + Clone + Send,
{
    async fn save_decision(&mut self, slot: u64, value: V) -> Result<(), StorageError>;
    async fn load_decisions(&self) -> Result<Vec<(u64, V)>, StorageError>; // unordered; node computes max(slot)+1
}

pub struct MemoryStorage<V> {
    decisions: HashMap<u64, V>,
}

impl<V> MemoryStorage<V> {
    pub fn new() -> Self { ... }
}
```

`Storage` is boxed internally by `Node` (as `Box<dyn Storage<V>>`) so it does not add a generic parameter to `Node`. The `&mut self` on `save_decision` is fine because the node event loop has exclusive ownership.

Future backends (e.g., DuckDB) implement the same `Storage<V>` trait behind feature flags.

## Internal Design

### Protocol Messages

All protocol messages carry a `slot` field to identify which Paxos instance they belong to.

```rust
// message.rs (pub(crate), serialized to JSON before transport)
pub type ProposalNumber = (u64, NodeId); // (round, proposer) — total ordering

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum Message<V> {
    // Phase 1
    Prepare {
        slot: u64,
        proposal_number: ProposalNumber,
    },
    Promise {
        slot: u64,
        proposal_number: ProposalNumber,
        accepted: Option<(ProposalNumber, V)>, // highest previously accepted
    },

    // Phase 2
    Accept {
        slot: u64,
        proposal_number: ProposalNumber,
        value: V,
    },
    Accepted {
        slot: u64,
        proposal_number: ProposalNumber,
        value: V, // echo back accepted value for confirmation
    },

    // Decision announcement
    Decide { slot: u64, value: V },

    // Rejections (optimization — without these, stale proposers rely on timeouts)
    NackPrepare {
        slot: u64,
        proposal_number: ProposalNumber,       // the rejected proposal
        highest_promised: ProposalNumber,       // so proposer can pick a higher number
    },
    NackAccept {
        slot: u64,
        proposal_number: ProposalNumber,       // the rejected proposal
        highest_promised: ProposalNumber,       // so proposer can pick a higher number
    },
}
```

`ProposalNumber` is `(u64, NodeId)` — compare by round first, break ties by node ID. Standard Paxos approach for total ordering.

`Message<V>` is `pub(crate)` — it is not part of the public API. Transport implementors only see `Bytes`.

`NackPrepare` and `NackAccept` are separate variants so the proposer knows which phase was rejected. On `NackPrepare`, the proposer must restart from Phase 1 with a higher proposal number. On `NackAccept`, the proposer must also restart from Phase 1 (since another proposer has made a higher promise, Phase 2 cannot succeed). Both carry `highest_promised` so the proposer can pick a number higher than any known promise.

### Slot Allocation

Each node maintains a `next_slot` counter, initialized to 0 (or to `max(slot) + 1` over all slots returned by `load_decisions()` on startup). The counter is also advanced when the node learns of decided slots from other nodes: `next_slot = max(next_slot, decided_slot + 1)`. Gaps in the slot sequence are allowed in v1 (see v1 limitations — no catch-up protocol).

When `propose()` is called:

1. The node claims `next_slot` and increments the counter.
2. It starts a new `PaxosInstance` for that slot.
3. If a `Decide` for a **different value** arrives for that slot, the proposal has lost — the node retries its value in a new slot (using the current `next_slot`).
4. If the proposer receives a `Nack` or times out waiting for a quorum, it retries in the **same slot** with a higher proposal number (after backoff). It only moves to a new slot when a different value is decided for the current slot.

Multiple nodes may contend for the same slot. This is safe — Paxos guarantees at most one value is decided per slot.

**v1 known inefficiency:** On a fresh cluster, all nodes start at slot 0, so concurrent proposals always collide. This is correct but wasteful — every slot contention triggers a retry. Slot partitioning (e.g., interleaving by node index) or leader-based slot assignment would reduce contention but is deferred to Multi-Paxos optimizations.

### Paxos State Machine

```rust
// protocol.rs (internal)

/// Manages all active Paxos instances
struct ProtocolState<V> {
    node_id: NodeId,
    instances: HashMap<u64, PaxosInstance<V>>,
    next_slot: u64,
    quorum_size: usize,
}

/// Per-slot Paxos instance
struct PaxosInstance<V> {
    slot: u64,
    // Proposer state
    proposal_number: ProposalNumber,
    promises_received: HashSet<NodeId>,
    accepts_received: HashSet<NodeId>,   // tracks Phase 2 Accepted responses for quorum
    highest_accepted: Option<(ProposalNumber, V)>,
    proposed_value: Option<V>,
    // Acceptor state
    highest_promised: Option<ProposalNumber>,
    accepted: Option<(ProposalNumber, V)>,
    // Resolution
    decided: bool,
}
```

### Protocol Rules

**Self-vote:** When a node initiates Phase 1 (Prepare) or Phase 2 (Accept), it immediately processes the message as its own acceptor and records itself in the corresponding response set (`promises_received` or `accepts_received`). This is required for single-node clusters (quorum = 1) to decide trivially, and is standard Paxos behavior.

**Phase 2 value selection:** When a proposer collects a quorum of `Promise` responses, it MUST use the value associated with the highest `ProposalNumber` among all `Promise` responses that carry an `accepted` value. Only if no `Promise` carries an accepted value may the proposer use its own originally proposed value. This is the core Paxos safety rule — it ensures that once a value is accepted by any majority, all future proposals will converge on that value.

### Event Loop (`Node::run`)

The `run()` method uses `tokio::select!` to multiplex:

1. **Peer receivers** — incoming protocol messages from all peers
2. **Proposal channel** — values submitted via `NodeHandle::propose()`
3. **Retry timers** — for in-progress proposals that haven't received a quorum response

Each event drives the state machine for the relevant slot. When a quorum of `Accepted` messages is received (tracked via `accepts_received`), the value is decided: stored via `Storage`, sent on the `DecisionReceiver`, and a `Decide` message is broadcast to all peers.

### Instance Garbage Collection

Once a slot is decided and the `Decide` message has been broadcast, the `PaxosInstance` for that slot is removed from the `instances` map. Late-arriving messages for a decided slot are ignored (the node checks `Storage` or a decided-slots set to recognize already-decided slots without keeping the full instance in memory).

### Livelock Mitigation

When multiple nodes propose simultaneously for the same slot, they can preempt each other with increasing proposal numbers. To mitigate:

- On receiving a `Nack`, the proposer waits a random backoff (with exponential increase) before retrying with a higher proposal number.
- The backoff introduces asymmetry that allows one proposer to complete. This is standard for Basic Paxos; full leader election is a Multi-Paxos optimization (future work).

### Quorum

Quorum size = `(total_nodes / 2) + 1` where `total_nodes` includes the local node. The node can make progress as long as a quorum of nodes is reachable (including itself).

**Cluster size notes:** Minimum cluster size is 1 (single node, trivially decides). A 2-node cluster requires unanimity (quorum = 2) and has zero fault tolerance. Recommended minimum for fault tolerance is 3 nodes (tolerates 1 failure). Odd-numbered clusters are preferred since even-numbered clusters waste a node (e.g., 4 nodes tolerates 1 failure, same as 3).

### `NoQuorum` Error Semantics

`Node::run()` returns `Err(NodeError::NoQuorum)` when the number of reachable peers drops below `quorum_size - 1` (i.e., even counting the local node, a quorum cannot be formed) AND there are active proposals that cannot make progress. The node does not immediately terminate on transient disconnections — it waits for peers to potentially reconnect (since reconnection is handled by the `MessageReceiver` implementor). If all peer receivers return permanent errors, the node concludes no quorum is possible and returns.

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
| Multiple single-decree instances | Each slot is an independent Paxos instance; simple and correct. Multi-Paxos optimizations are future work. |
| Generic `V` with serde bounds | Type-safe; serde already needed for transport |
| `NodeId { name, incarnation }` | Name is stable/human-readable; incarnation (unix timestamp) ensures uniqueness per run, avoiding Paxos safety violations on restart |
| Transport traits on `Bytes` | Decouples transport from protocol; transport doesn't know about `V` |
| `MessageSender`/`MessageReceiver` names | Avoids collision with `std::marker::Send` and other common trait names |
| Static dispatch for transport | Performance; all peers use same transport type |
| `NodeHandle` split from `Node` | `run()` consumes `Node`; handle allows proposing after the event loop starts |
| Caller-driven `run()` | User decides threading model (tokio::spawn, main thread, OS thread) |
| `propose()` returns on enqueue | Simple for v1; decision-tracking future documented as extension |
| `DecisionReceiver<V>` type alias | Clean API; wraps as stream trivially via `tokio_stream` |
| All nodes receive all decisions | `DecisionReceiver` emits every decided value, not just local proposals |
| Storage trait for decisions only | Acceptor state is ephemeral in v1; keeps storage interface minimal |
| Storage boxed internally | Avoids adding a storage generic parameter to `Node` |
| JSON serialization for v1 | Readable/debuggable; configurable serialization planned for later |
| `tracing` for observability | De facto standard; opt-in for consumers |
| Peer failure = continue | Paxos tolerates minority failures; reconnection is transport's job |
| Separate NackPrepare/NackAccept | Proposer knows which phase was rejected; both restart from Phase 1 |
| Random backoff on Nack | Mitigates livelock without full leader election (future work) |
| Auto-new incarnation on restart | Incarnation timestamp in NodeId ensures restarted nodes are distinct without user action |
| One send/recv = one message | Message framing is transport implementor's responsibility |
| Bounded channels with defaults | 1024 capacity for both proposal and decision channels; backpressure over dropping |
| GC decided instances immediately | Prevents unbounded memory growth; late messages checked against decided-slots set |
| No catch-up protocol in v1 | Simplicity; nodes may have gaps if disconnected during Decide broadcast |
| NodeId ordering: name then incarnation | Name-first lexicographic, then incarnation (u64); stable across restarts for same-named nodes |
| Self-vote on propose | Proposer acts as own acceptor; required for single-node clusters |
| Phase 2 highest-value rule | Core Paxos safety: adopt highest-numbered accepted value from promises |
| `Message<V>` is `pub(crate)` | Internal protocol detail; transport only sees `Bytes` |
| Retry same slot on Nack/timeout | Move to new slot only when different value decided for current slot |
| `load_decisions` unordered | Node computes `max(slot)+1` from the vec; no ordering requirement on storage |
