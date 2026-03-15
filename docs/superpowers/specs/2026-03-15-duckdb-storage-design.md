# DuckDB Storage Backend — Design Spec

## Goal

Add a DuckDB-backed `Storage<V>` implementation that persists both decisions and acceptor state, enabling full crash recovery. After a process crash, a node can read back its state from the DuckDB file and resume participating in the Paxos protocol without violating safety invariants.

## Storage Trait Changes

### New Methods on `Storage<V>`

The existing trait gains three methods:

```rust
async fn save_acceptor_state(
    &mut self,
    slot: u64,
    highest_promised: Option<ProposalNumber>,
    accepted: Option<(ProposalNumber, V)>,
) -> Result<(), StorageError>;

async fn load_acceptor_states(&self) -> Result<Vec<AcceptorState<V>>, StorageError>;

async fn delete_acceptor_state(&mut self, slot: u64) -> Result<(), StorageError>;
```

### New Public Types

```rust
pub struct AcceptorState<V> {
    pub slot: u64,
    pub highest_promised: Option<ProposalNumber>,
    pub accepted: Option<(ProposalNumber, V)>,
}
```

`ProposalNumber` (currently `pub(crate)` in `message.rs`) becomes `pub` since it is now part of the storage API.

### Breaking Change

This is a breaking change to the `Storage<V>` trait. All implementors must add the three new methods. `MemoryStorage` is updated accordingly. This is acceptable for a pre-1.0 library.

## Recovery Flow

### Startup Sequence (in `Node::run`)

1. `load_decisions()` — populate `decided_slots` and `next_slot` (unchanged)
2. `load_acceptor_states()` — validate each state, then recreate `PaxosInstance` entries with acceptor fields restored

### Recovery Validation

Each loaded `AcceptorState` is validated before being applied. Invariants checked:

1. If `accepted` is `Some((pn, _))` and `highest_promised` is `Some(hp)`, then `pn <= hp`
2. `ProposalNumber` round values must be > 0
3. If `accepted` is `Some` then `highest_promised` must also be `Some`
4. Acceptor state for already-decided slots is stale — skip and clean up

On validation failure: log a warning with the slot number and which invariant failed, skip that slot entirely. Dropping acceptor state is safe because it can only cause a liveness issue (re-doing work), never a safety violation — a single node forgetting a promise doesn't break safety as long as quorum members remember theirs.

For decisions: if a loaded decision fails to deserialize, log a warning and skip it. The node will re-learn that decision from peers via the Decide re-broadcast mechanism.

### New Protocol Method

```rust
impl<V> AcceptorState<V> {
    pub fn is_valid(&self) -> bool { /* checks above */ }
}

impl<V> ProtocolState<V> {
    pub(crate) fn initialize_from_acceptor_states(&mut self, states: Vec<AcceptorState<V>>) {
        for state in states {
            if self.decided_slots.contains_key(&state.slot) {
                continue;
            }
            let instance = self.get_or_create_instance(state.slot);
            instance.highest_promised = state.highest_promised;
            instance.accepted = state.accepted;
        }
    }
}
```

Restored instances have `is_proposer = false` — we never resume as proposer. Only acceptor promises are safety-critical.

## Persistence Call Sites

### Where Acceptor State Changes

1. `handle_prepare` — acceptor updates `highest_promised`
2. `handle_accept` — acceptor updates both `highest_promised` and `accepted`

### How Node Persists

The protocol state machine remains pure (no storage access). Instead, `ProtocolState` tracks which slots had acceptor state changes:

```rust
pub(crate) fn take_dirty_acceptor_slots(&mut self)
    -> Vec<(u64, Option<ProposalNumber>, Option<(ProposalNumber, V)>)>
```

The `Node` event loop calls this after `handle_message`, persists each dirty slot via `save_acceptor_state`, then sends outgoing messages. When a slot is decided, the node calls `delete_acceptor_state` to clean up.

## DuckDB Implementation

### Location

`crates/concensus/src/storage/duckdb.rs`, gated behind a `duckdb-storage` feature flag in `Cargo.toml`.

### Schema

```sql
CREATE TABLE IF NOT EXISTS decisions (
    slot UBIGINT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS acceptor_state (
    slot UBIGINT PRIMARY KEY,
    highest_promised TEXT,
    accepted TEXT
);
```

Values and protocol types are stored as JSON text via `serde_json`, consistent with existing transport serialization.

### API

```rust
pub struct DuckDbStorage<V> {
    conn: duckdb::Connection,
    _phantom: PhantomData<V>,
}

impl<V> DuckDbStorage<V> {
    pub fn new(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        // Opens or creates the DB file, creates tables if they don't exist
    }
}
```

### Implementation Details

- DuckDB's Rust crate provides a synchronous API. Calls are wrapped in `tokio::task::spawn_blocking` to avoid blocking the event loop.
- `save_acceptor_state` uses `INSERT OR REPLACE` (upsert) semantics.
- `delete_acceptor_state` uses `DELETE WHERE slot = ?`.
- `save_decision` inserts the decision and also deletes the corresponding acceptor state (two idempotent statements — partial failure is safe).

### Dependency

`duckdb` crate added as an optional dependency in `crates/concensus/Cargo.toml`:

```toml
[features]
duckdb-storage = ["dep:duckdb"]

[dependencies]
duckdb = { version = "1", optional = true }
```

## Module Reorganization

`storage.rs` becomes a module directory:

```
src/storage/
    mod.rs          -- trait, AcceptorState, re-exports
    memory.rs       -- MemoryStorage
    duckdb.rs       -- DuckDbStorage (behind feature flag)
```

### Public API Additions

- `AcceptorState<V>` — new public struct
- `DuckDbStorage<V>` — new, gated behind `duckdb-storage` feature
- `ProposalNumber` — promoted from `pub(crate)` to `pub`

All new types re-exported from `lib.rs`.

## Demo Integration

### Config Changes

The demo node (`crates/concensus-demo/src/node.rs`) gains a new environment variable:

- `STORAGE` — `"memory"` (default) or `"duckdb"`
- `DUCKDB_PATH` — file path for DuckDB database (required when `STORAGE=duckdb`, e.g. `/data/consensus.db`)

The `Config` struct gains a `storage` field:

```rust
enum StorageBackend {
    Memory,
    DuckDb { path: PathBuf },
}
```

`parse_config()` reads these env vars. The `start_node_tcp` and `start_node_uds` functions accept the storage backend config and construct either `MemoryStorage` or `DuckDbStorage` accordingly.

### Demo Dependency Changes

`crates/concensus-demo/Cargo.toml` adds the `duckdb-storage` feature:

```toml
concensus = { path = "../concensus", features = ["tcp-transport", "uds-transport", "test-support", "duckdb-storage"] }
```

### Docker Compose Changes

New compose file `docker-compose.duckdb.yml` (TCP transport + DuckDB storage) with mounted volumes for each node's database:

```yaml
x-node: &node-base
  build:
    context: ../..
    dockerfile: crates/concensus-demo/Dockerfile
  environment: &node-env
    TRANSPORT: tcp
    BIND_ADDR: "0.0.0.0:9000"
    PEERS: "node-1=node-1:9000,node-2=node-2:9000,node-3=node-3:9000"
    GRPC_PORT: "50051"
    STORAGE: duckdb
    DUCKDB_PATH: "/data/consensus.db"
    RUST_LOG: info
  healthcheck:
    test: ["CMD", "concensus-cli", "--addr", "localhost:50051", "health"]
    interval: 5s
    timeout: 3s
    retries: 10
    start_period: 10s
  networks:
    - consensus-net

services:
  node-1:
    <<: *node-base
    hostname: node-1
    ports:
      - "50051:50051"
    volumes:
      - node1-data:/data
  node-2:
    <<: *node-base
    hostname: node-2
    volumes:
      - node2-data:/data
  node-3:
    <<: *node-base
    hostname: node-3
    volumes:
      - node3-data:/data

volumes:
  node1-data:
  node2-data:
  node3-data:

networks:
  consensus-net:
    driver: bridge
```

Each node gets its own named volume mounted at `/data`. The DuckDB file lives at `/data/consensus.db` inside each container. Named volumes survive `docker compose stop` and `docker compose restart`, enabling crash recovery testing.

### Demo Testing

Manual testing workflow using the Docker compose setup:

1. Start cluster: `docker compose -f docker-compose.duckdb.yml up -d`
2. Propose values: `concensus-cli --addr localhost:50051 propose "value1"` (repeat several times)
3. Verify decisions: `concensus-cli --addr localhost:50051 decisions`
4. Stop one node: `docker compose -f docker-compose.duckdb.yml stop node-2`
5. Restart the stopped node: `docker compose -f docker-compose.duckdb.yml start node-2`
6. Verify recovered node has all prior decisions: query decisions from node-2 via its gRPC port
7. Propose new values and verify all nodes (including recovered node) participate in consensus

This validates that the DuckDB file on the mounted volume survives a container restart and the node correctly recovers its state.

## Testing Strategy

### Unit Tests (`storage/duckdb.rs`)

- Create DB in temp file, save and load decisions
- Save and load acceptor state roundtrip
- Delete acceptor state after decision
- Reopen DB from same file path, verify data persists across "restarts"

### Unit Tests (`storage/memory.rs`)

- Existing tests plus new acceptor state method tests

### Validation Tests

- Load corrupted acceptor state (e.g. `accepted` set but `highest_promised` is `None`) — verify it's skipped with log warning
- Load acceptor state for already-decided slot — verify it's skipped and cleaned up

### Integration Test (`concensus-tests`)

- Spin up a 3-node cluster with DuckDB storage (temp files)
- Propose and decide several values
- Stop a node, create a new node pointing at the same DB file
- Verify the recovered node has correct decisions and can participate in new rounds
