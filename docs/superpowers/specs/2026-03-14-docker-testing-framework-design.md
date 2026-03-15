# Docker Integration Testing & Demo Framework

## Overview

A Docker-based framework for running multi-node consensus clusters using TCP and UDS transports. Primarily a demo/exploration tool with REST API for manual interaction, with automated random-proposal scenarios for smoke testing.

## New Crate: `crates/concensus-demo`

Single binary, two modes (`random` / `api`), two transports (`tcp` / `uds`), configured entirely via environment variables.

### Dependencies

- `concensus` (with `tcp-transport`, `uds-transport`, and `test-support` features)
- `axum` — HTTP server for API mode
- `tokio` (full runtime, including `signal` for graceful shutdown)
- `tracing` + `tracing-subscriber` — structured JSON logging
- `rand = "0.8"` — random proposal generation (pinned to match library)
- `serde` / `serde_json` — request/response serialization

### Environment Variables

| Var | Example | Required | Description |
|-----|---------|----------|-------------|
| `MODE` | `random` or `api` | Yes | Operating mode |
| `TRANSPORT` | `tcp` or `uds` | Yes | Which transport to use |
| `NODE_NAME` | `node-1` | Yes | Node identity |
| `BIND_ADDR` | `0.0.0.0:9000` | TCP mode | TCP listen address |
| `BIND_PATH` | `/sockets/node-1.sock` | UDS mode | Unix socket file path |
| `PEERS` | `node-2=10.0.0.3:9000,node-3=10.0.0.4:9000` | Yes | Comma-separated `name=addr_or_path` |
| `API_PORT` | `3000` | API mode | HTTP listen port |
| `PROPOSAL_INTERVAL_MS` | `2000` | No (default 2000) | Mean interval between random proposals |

### NodeId Strategy

All nodes use `Node::with_id()` (from `test-support` feature) with incarnation fixed to `0`. This ensures that `NodeId`s are deterministic and match between peers — each node constructs peer `NodeId`s as `NodeId::new(peer_name, 0)`, which will match the actual `NodeId` the remote node created for itself. The `test-support` feature is required in the demo's `concensus` dependency.

### Startup Flow

1. Parse environment variables into a config struct
2. Create `MemoryStorage::<String>::new()` for node storage
3. Parse `PEERS` env var into peer tuples: for each `name=addr_or_path`, construct `(NodeId::new(name, 0), parsed_addr_or_path)` to get `Vec<(NodeId, SocketAddr)>` (TCP) or `Vec<(NodeId, PathBuf)>` (UDS)
4. Create transport via factory methods (both return `Result`, propagate errors):
   - TCP: `TcpTransport::create(bind_addr, peers).await?` → `(Vec<PeerInfo<TcpSender>>, TcpReceiver)`
   - UDS: `UdsTransport::create(bind_path, peers).await?` → `(Vec<PeerInfo<UdsSender>>, UdsReceiver)`
5. Create node via `Node::with_id(NodeId::new(node_name, 0), peers, receiver, storage)` → `(Node, NodeHandle, DecisionReceiver)`
6. Spawn the node's `run()` in a background task with error logging: if `run()` returns an error, log it and exit the process
7. Spawn a decision logger task that reads from `DecisionReceiver`, logs each decision, and appends to `Arc<RwLock<Vec<DecisionResponse>>>`
8. Based on `MODE`:
   - `random`: spawn a loop proposing values like `"rand-48291"` at random intervals (Poisson-ish around `PROPOSAL_INTERVAL_MS`)
   - `api`: start axum server
9. Await `tokio::signal::ctrl_c()` for graceful shutdown — on signal, drop `NodeHandle` to trigger node task exit

### Value Type

`String` for all modes. Random mode produces `"rand-{number}"`. API mode accepts arbitrary user-provided strings.

## REST API (api mode)

Three endpoints on `API_PORT`:

### `POST /propose`

- Body: `{"value": "some-string"}`
- Calls `NodeHandle::propose(value)`
- Returns:
  - `200 {"status": "proposed"}` on success
  - `429 {"error": "channel full"}` when `ProposeError::ChannelFull`
  - `503 {"error": "node not running"}` when `ProposeError::NotRunning`

### `GET /decisions`

- Returns all decisions observed so far
- Response: `[{"slot": 0, "value": "some-string"}, ...]`
- Uses a `DecisionResponse` DTO struct (since `Decided<V>` does not implement `Serialize`):
  ```rust
  #[derive(Serialize)]
  struct DecisionResponse { slot: u64, value: String }
  ```
- Reads from `Arc<RwLock<Vec<DecisionResponse>>>`

### `GET /health`

- Returns `200 {"status": "ok"}`

## Docker Infrastructure

### Dockerfile (multi-stage with cargo-chef)

```dockerfile
# Stage 1: Chef - install cargo-chef
FROM rust:1.94 AS chef
RUN cargo install cargo-chef
WORKDIR /app

# Stage 2: Planner - generate recipe.json
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# Stage 3: Builder - cache deps, then build
FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json -p concensus-demo
COPY . .
RUN cargo build --release -p concensus-demo

# Stage 4: Runtime
FROM debian:bookworm-slim
COPY --from=builder /app/target/release/concensus-demo /usr/local/bin/
ENTRYPOINT ["concensus-demo"]
```

Containers run as root (default for debian:bookworm-slim). Acceptable for a demo tool.

### `docker-compose.tcp.yml` — 3-node TCP cluster

- Shared Docker network for inter-node communication
- `node-1`: `MODE=api`, `TRANSPORT=tcp`, `API_PORT=3000` exposed to host
- `node-2`, `node-3`: `MODE=random`, `TRANSPORT=tcp`
- Each node binds on `0.0.0.0:9000`, peers reference each other by container hostname
- Default 3 nodes; configurable by adding/removing service definitions

### `docker-compose.uds.yml` — 3-node UDS cluster

- Same structure but `TRANSPORT=uds`
- Shared named volume mounted at `/sockets` in all containers
- Each node binds at `/sockets/node-X.sock`, peers reference socket paths
- `node-1` API port still exposed over TCP to host (UDS is inter-node only)

### Workspace Configuration

The root `Cargo.toml` must be updated to add `"crates/concensus-demo"` to the workspace members list.

## Logging & Observability

**tracing-subscriber** with JSON format, controlled by `RUST_LOG` env var (default: `info`).

### Key Log Events

| Event | Level | Fields |
|-------|-------|--------|
| Node started | `info` | `node_name`, `transport`, `mode`, `bind` |
| Proposal submitted | `info` | `node_name`, `value` |
| Decision reached | `info` | `node_name`, `slot`, `value` |
| API request received | `debug` | `endpoint`, `method` |
| Transport error | `warn` | `node_name`, `error` |
| Node task exited with error | `error` | `node_name`, `error` |

### Usage

```bash
# TCP cluster
docker compose -f docker-compose.tcp.yml up --build

# UDS cluster
docker compose -f docker-compose.uds.yml up --build

# Stream logs
docker compose -f docker-compose.tcp.yml logs -f

# Manual proposal via REST
curl -X POST http://localhost:3000/propose -H 'Content-Type: application/json' -d '{"value": "hello"}'

# View decisions
curl http://localhost:3000/decisions
```

## Project Layout

```
concensus/
├── Cargo.toml                          # workspace: add concensus-demo member
├── Dockerfile                          # multi-stage cargo-chef build
├── docker-compose.tcp.yml              # 3-node TCP cluster
├── docker-compose.uds.yml              # 3-node UDS cluster (shared volume)
├── crates/
│   ├── concensus/                      # existing library, unchanged
│   ├── concensus-tests/                # existing tests, unchanged
│   └── concensus-demo/
│       ├── Cargo.toml
│       └── src/
│           └── main.rs                 # all demo app code in one file
```

### `main.rs` Structure

1. Env parsing into a config struct
2. `run_node()` — creates storage, transport, node (via `with_id`), spawns decision logger
3. `run_random_mode()` — proposal loop with random intervals
4. `run_api_mode()` — axum router with 3 endpoints, shared decision state via `DecisionResponse` DTO
5. `main()` — init tracing, parse config, call `run_node()`, dispatch to mode, await shutdown signal

Stays in one file unless it grows beyond ~300-400 lines.
