# daccord

A consensus library in Rust with pluggable transports and storage. Supports both Multi-Paxos (default) and Raft.

## Overview

`daccord` implements two consensus algorithms behind a single `Node` API:

- **Multi-Paxos** (default) — leader-based optimization of Classic Paxos with Phase 1 skip on the steady-state leader, exponential-backoff retries, and nack-based conflict resolution.
- **Raft** — strong-leader consensus with randomized election timeouts, log-replication via `AppendEntries`, persistent term/votedFor/log state, and conflict-index / conflict-term hints for fast follower catch-up.

Both algorithms expose the same external interface: `NodeHandle::propose` to submit values and `DecisionReceiver` to observe ordered decisions. The choice is per-cluster — every node in a cluster must run the same algorithm.

Quorum is `(N/2) + 1` where N is the total number of nodes.

## Architecture

```
┌─────────────────────────────────────────┐
│  Application                            │
│  ┌──────────┐  ┌────────────────────┐   │
│  │NodeHandle │  │ DecisionReceiver   │   │
│  │.propose() │  │ .recv() -> Decided │   │
│  └─────┬─────┘  └────────▲──────────┘   │
│        │                 │               │
│  ┌─────▼─────────────────┴──────────┐   │
│  │            Node                   │   │
│  │  ┌──────────────────────────┐    │   │
│  │  │     ProtocolState        │    │   │
│  │  │  (per-slot Paxos FSM)    │    │   │
│  │  └──────────────────────────┘    │   │
│  │  ┌──────────┐  ┌────────────┐   │   │
│  │  │ Storage  │  │ Transport  │   │   │
│  │  └──────────┘  └────────────┘   │   │
│  └──────────────────────────────────┘   │
└─────────────────────────────────────────┘
```

## Usage

Add to your `Cargo.toml`:

```toml
[dependencies]
daccord = { path = "daccord", features = ["tcp-transport"] }
```

Basic 3-node cluster with TCP:

```rust
use daccord::{Node, NodeId, MemoryStorage, TcpTransport};

let bind_addr = "0.0.0.0:9000".parse().unwrap();
let peers = vec![
    (NodeId::new("node-2", 0), "192.168.1.2:9000".parse().unwrap()),
    (NodeId::new("node-3", 0), "192.168.1.3:9000".parse().unwrap()),
];

let storage = MemoryStorage::<String>::new();
let (peer_infos, receiver) = TcpTransport::create(bind_addr, peers).await?;

let node_id = NodeId::new("node-1", 0);
let (node, handle, mut decision_rx) = Node::with_id(node_id, peer_infos, receiver, storage);

// Run the node in the background
tokio::spawn(node.run());

// Propose a value
handle.propose("hello".to_string()).await?;

// Receive decided values
while let Some(decided) = decision_rx.recv().await {
    println!("slot {}: {}", decided.slot, decided.value);
}
```

## Choosing an algorithm

`Node::new` and `Node::with_id` run Multi-Paxos and require storage that implements `Storage<V>`. To run Raft instead, use `Node::with_raft_config` and provide storage that implements `RaftStorage<V>` (which extends `Storage<V>` with persistent log + current term + votedFor).

```rust
use daccord::{Node, NodeId, MemoryStorage, RaftConfig};

// MemoryStorage implements both Storage and RaftStorage.
let storage = MemoryStorage::<String>::new();
let (node, handle, mut decisions) = Node::with_raft_config(
    NodeId::new("node-1", 0),
    RaftConfig::default(),
    peer_infos,
    receiver,
    storage,
);
```

| Constructor | Algorithm | Storage requirement |
|-------------|-----------|---------------------|
| `Node::new` / `Node::with_id` | Multi-Paxos | `Storage<V>` |
| `Node::with_paxos_config` | Multi-Paxos | `Storage<V>` |
| `Node::with_raft_config` | Raft | `RaftStorage<V>` |

`MemoryStorage<V>` implements both traits and is suitable for tests and ephemeral deployments. For production Raft deployments, provide a `RaftStorage` implementation that durably persists the current term, vote, and log on every state change before any wire message is sent.

Cross-algorithm wire messages are silently dropped, so a Paxos node and a Raft node cannot accidentally interfere with each other.

## Feature Flags

| Feature | Description |
|---------|-------------|
| `channel-transport` | In-memory bounded/unbounded channel transport (useful for testing) |
| `tcp-transport` | TCP transport with length-prefixed framing, lazy connect, and automatic reconnection |
| `uds-transport` | Unix domain socket transport (same framing as TCP) |
| `test-support` | Enables `Node::with_id()` for deterministic node identity in tests |

## Core Types

| Type | Description |
|------|-------------|
| `Node<V, S, R>` | Consensus node — runs the Paxos or Raft event loop |
| `NodeHandle<V>` | Cloneable handle for submitting proposals |
| `Decided<V>` | A decided value with its slot number |
| `DecisionReceiver<V>` | Channel receiver for consensus decisions |
| `NodeId` | Node identifier (name + incarnation) |
| `MemoryStorage<V>` | In-memory storage (implements both `Storage` and `RaftStorage`) |
| `NodeConfig` | Algorithm + per-algorithm tuning, used by `Node::with_*_config` |
| `ConsensusAlgorithm` | `Paxos` \| `Raft` discriminator |
| `PaxosConfig` | Multi-Paxos tuning (retries, backoff) |
| `RaftConfig` | Raft tuning (election timeout, heartbeat interval) |

### Traits

| Trait | Description |
|-------|-------------|
| `MessageSender` | Send bytes to a peer |
| `MessageReceiver` | Receive bytes from peers |
| `Storage<V>` | Persist and load decided slot values (used by Multi-Paxos) |
| `RaftStorage<V>` | Extends `Storage<V>` with persistent log entries, current term, and votedFor (required by Raft) |

## Workspace Crates

```
daccord/
├── daccord/          # Core library
├── daccord-tests/    # Integration tests
└── daccord-demo/     # Docker demo application
```

### daccord-tests

Integration test suite exercising multi-node consensus under various conditions:

- **Cluster tests** — single-value consensus, sequential proposals, concurrent proposals from different nodes, single-node clusters, quorum with a dead node, rapid concurrent proposals (50 simultaneous)
- **Lossy transport tests** — consensus under 1%, 10%, 20%, and 30% message drop rates using a `LossySender`/`LossyReceiver` wrapper that randomly drops packets

Test helpers provide cluster creation utilities (`create_cluster`, `create_cluster_with_dead_node`, `create_lossy_cluster`) and assertion helpers (`assert_consistent_decisions`) for verifying all nodes agree on the same slot-to-value mapping.

Run the tests:

```bash
cargo test -p daccord-tests
```

### daccord-demo

A Docker-based demo that runs a 3-node Paxos cluster with a gRPC API and CLI client.

**Components:**
- `daccord-node` — consensus node binary with a gRPC server (propose values, query decisions, health checks)
- `daccord-cli` — CLI client for interacting with nodes

**Transport options:**
- TCP (`docker-compose.tcp.yml`) — nodes communicate over a Docker bridge network
- UDS (`docker-compose.uds.yml`) — nodes communicate via Unix domain sockets on a shared volume

**Algorithm options:**
- Multi-Paxos (default)
- Raft (`docker-compose.raft.yml`, or set `ALGORITHM=raft` on the node binary)

#### Quick Start

```bash
cd daccord-demo

# Start a 3-node TCP cluster
docker compose -f docker-compose.tcp.yml up --build -d

# Wait for nodes to become healthy
docker compose -f docker-compose.tcp.yml ps

# Propose values
cargo run -p daccord-demo --bin daccord-cli -- --addr localhost:50051 propose --value "alice"
cargo run -p daccord-demo --bin daccord-cli -- --addr localhost:50051 propose --value "bob"

# View decided values
cargo run -p daccord-demo --bin daccord-cli -- --addr localhost:50051 decisions

# Shut down
docker compose -f docker-compose.tcp.yml down
```

See [`daccord-demo/DEMO.md`](daccord-demo/DEMO.md) for the full guide including prerequisites, environment variables, scaling, and cleanup.

## Development

```bash
# Format
cargo +nightly fmt

# Lint
cargo clippy -- -D warnings

# Test everything
cargo test --workspace

# Test with all features
cargo test -p daccord --all-features
```

## License

MIT
