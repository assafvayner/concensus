# concensus

A Paxos consensus library in Rust with pluggable transports and storage.

## Overview

`concensus` implements the Classic Paxos (Multi-Paxos) consensus protocol as an async Rust library. Nodes coordinate over configurable transport layers to agree on an ordered sequence of values, each assigned to a numbered slot. The library handles leader election, proposal retries with exponential backoff, nack-based conflict resolution, and decision re-broadcasting for late joiners.

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
concensus = { path = "concensus", features = ["tcp-transport"] }
```

Basic 3-node cluster with TCP:

```rust
use concensus::{Node, NodeId, MemoryStorage, TcpTransport};

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
| `Node<V, S, R>` | Consensus node — runs the Paxos event loop |
| `NodeHandle<V>` | Cloneable handle for submitting proposals |
| `Decided<V>` | A decided value with its slot number |
| `DecisionReceiver<V>` | Channel receiver for consensus decisions |
| `NodeId` | Node identifier (name + incarnation) |
| `MemoryStorage<V>` | In-memory storage implementation |

### Traits

| Trait | Description |
|-------|-------------|
| `MessageSender` | Send bytes to a peer |
| `MessageReceiver` | Receive bytes from peers |
| `Storage<V>` | Persist and load decisions |

## Workspace Crates

```
concensus/
├── concensus/          # Core library
├── concensus-tests/    # Integration tests
└── concensus-demo/     # Docker demo application
```

### concensus-tests

Integration test suite exercising multi-node consensus under various conditions:

- **Cluster tests** — single-value consensus, sequential proposals, concurrent proposals from different nodes, single-node clusters, quorum with a dead node, rapid concurrent proposals (50 simultaneous)
- **Lossy transport tests** — consensus under 1%, 10%, 20%, and 30% message drop rates using a `LossySender`/`LossyReceiver` wrapper that randomly drops packets

Test helpers provide cluster creation utilities (`create_cluster`, `create_cluster_with_dead_node`, `create_lossy_cluster`) and assertion helpers (`assert_consistent_decisions`) for verifying all nodes agree on the same slot-to-value mapping.

Run the tests:

```bash
cargo test -p concensus-tests
```

### concensus-demo

A Docker-based demo that runs a 3-node Paxos cluster with a gRPC API and CLI client.

**Components:**
- `concensus-node` — consensus node binary with a gRPC server (propose values, query decisions, health checks)
- `concensus-cli` — CLI client for interacting with nodes

**Transport options:**
- TCP (`docker-compose.tcp.yml`) — nodes communicate over a Docker bridge network
- UDS (`docker-compose.uds.yml`) — nodes communicate via Unix domain sockets on a shared volume

#### Quick Start

```bash
cd concensus-demo

# Start a 3-node TCP cluster
docker compose -f docker-compose.tcp.yml up --build -d

# Wait for nodes to become healthy
docker compose -f docker-compose.tcp.yml ps

# Propose values
cargo run -p concensus-demo --bin concensus-cli -- --addr localhost:50051 propose --value "alice"
cargo run -p concensus-demo --bin concensus-cli -- --addr localhost:50051 propose --value "bob"

# View decided values
cargo run -p concensus-demo --bin concensus-cli -- --addr localhost:50051 decisions

# Shut down
docker compose -f docker-compose.tcp.yml down
```

See [`concensus-demo/DEMO.md`](concensus-demo/DEMO.md) for the full guide including prerequisites, environment variables, scaling, and cleanup.

## Development

```bash
# Format
cargo +nightly fmt

# Lint
cargo clippy -- -D warnings

# Test everything
cargo test --workspace

# Test with all features
cargo test -p concensus --all-features
```

## License

MIT
