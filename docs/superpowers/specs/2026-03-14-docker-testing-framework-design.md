# Docker Integration Testing & Demo Framework

## Overview

A Docker-based framework for running multi-node consensus clusters using TCP and UDS transports. Primarily a demo/exploration tool with a gRPC API for interaction and a CLI client for issuing commands.

## New Crate: `crates/concensus-demo`

Two binaries in one crate:
- `concensus-node` — runs a consensus node with a gRPC server
- `concensus-cli` — CLI client that connects to a node's gRPC server

Both transports (`tcp` / `uds`) configured via environment variables.

### Dependencies

- `concensus` (with `tcp-transport`, `uds-transport`, and `test-support` features)
- `tonic` — gRPC server and client
- `prost` — protobuf message types
- `tonic-build` — build.rs protobuf codegen
- `tokio` (full runtime, including `signal` for graceful shutdown)
- `tracing` + `tracing-subscriber` — structured JSON logging
- `clap` — CLI argument parsing for `concensus-cli`

### Protobuf Definition

`crates/concensus-demo/proto/consensus.proto`:

```protobuf
syntax = "proto3";
package consensus;

service ConsensusService {
  rpc Propose (ProposeRequest) returns (ProposeResponse);
  rpc GetDecisions (GetDecisionsRequest) returns (GetDecisionsResponse);
  rpc Health (HealthRequest) returns (HealthResponse);
}

message ProposeRequest {
  string value = 1;
}

message ProposeResponse {
  string status = 1;
}

message GetDecisionsRequest {}

message GetDecisionsResponse {
  repeated Decision decisions = 1;
}

message Decision {
  uint64 slot = 1;
  string value = 2;
}

message HealthRequest {}

message HealthResponse {
  string status = 1;
}
```

### Environment Variables (node binary)

| Var | Example | Required | Description |
|-----|---------|----------|-------------|
| `TRANSPORT` | `tcp` or `uds` | Yes | Which transport to use |
| `NODE_NAME` | `node-1` | Yes | Node identity |
| `BIND_ADDR` | `0.0.0.0:9000` | TCP mode | TCP listen address for consensus transport |
| `BIND_PATH` | `/sockets/node-1.sock` | UDS mode | Unix socket file path for consensus transport |
| `PEERS` | `node-2=10.0.0.3:9000,node-3=10.0.0.4:9000` | Yes | Comma-separated `name=addr_or_path` |
| `GRPC_PORT` | `50051` | Yes | gRPC server listen port |

### NodeId Strategy

All nodes use `Node::with_id()` (from `test-support` feature) with incarnation fixed to `0`. This ensures that `NodeId`s are deterministic and match between peers — each node constructs peer `NodeId`s as `NodeId::new(peer_name, 0)`, which will match the actual `NodeId` the remote node created for itself. The `test-support` feature is required in the demo's `concensus` dependency.

### Startup Flow (node binary)

1. Parse environment variables into a config struct
2. Create `MemoryStorage::<String>::new()` for node storage
3. Parse `PEERS` env var into peer tuples: for each `name=addr_or_path`, construct `(NodeId::new(name, 0), parsed_addr_or_path)` to get `Vec<(NodeId, SocketAddr)>` (TCP) or `Vec<(NodeId, PathBuf)>` (UDS)
4. Create transport via factory methods (both return `Result`, propagate errors):
   - TCP: `TcpTransport::create(bind_addr, peers).await?` → `(Vec<PeerInfo<TcpSender>>, TcpReceiver)`
   - UDS: `UdsTransport::create(bind_path, peers).await?` → `(Vec<PeerInfo<UdsSender>>, UdsReceiver)`
5. Create node via `Node::with_id(NodeId::new(node_name, 0), peers, receiver, storage)` → `(Node, NodeHandle, DecisionReceiver)`
6. Spawn the node's `run()` in a background task with error logging: if `run()` returns an error, log it and exit the process
7. Spawn a decision collector task that reads from `DecisionReceiver`, logs each decision, and appends to `Arc<RwLock<Vec<Decision>>>` (using the protobuf `Decision` type or an equivalent internal struct)
8. Start gRPC server on `0.0.0.0:GRPC_PORT` with shared access to `NodeHandle` and decision list
9. Await `tokio::signal::ctrl_c()` for graceful shutdown — on signal, trigger gRPC server graceful shutdown and drop `NodeHandle` to trigger node task exit

### Value Type

`String` for all operations. The CLI and gRPC API accept arbitrary user-provided strings.

## gRPC API

### `Propose`

- Takes `ProposeRequest { value }`, calls `NodeHandle::propose(value)`
- Returns `ProposeResponse { status: "proposed" }` on success
- Returns gRPC status `RESOURCE_EXHAUSTED` when `ProposeError::ChannelFull`
- Returns gRPC status `UNAVAILABLE` when `ProposeError::NotRunning`

### `GetDecisions`

- Takes empty `GetDecisionsRequest`
- Returns `GetDecisionsResponse` with all decisions observed so far
- Reads from shared `Arc<RwLock<Vec<Decision>>>`

### `Health`

- Returns `HealthResponse { status: "ok" }`

## CLI Client (`concensus-cli`)

Subcommand-based CLI using `clap`:

```
concensus-cli --addr <host:port> propose --value <string>
concensus-cli --addr <host:port> decisions
concensus-cli --addr <host:port> health
```

### Subcommands

**`propose`**
- `--value <string>` (required) — the value to propose
- Connects to gRPC server, calls `Propose`, prints result or error

**`decisions`**
- No extra args
- Calls `GetDecisions`, prints a table of slot/value pairs

**`health`**
- No extra args
- Calls `Health`, prints status

### Output

Plain text to stdout. Errors to stderr with non-zero exit code.

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
RUN apt-get update && apt-get install -y protobuf-compiler && rm -rf /var/lib/apt/lists/*
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json -p concensus-demo
COPY . .
RUN cargo build --release -p concensus-demo

# Stage 4: Runtime
FROM debian:bookworm-slim
COPY --from=builder /app/target/release/concensus-node /usr/local/bin/
COPY --from=builder /app/target/release/concensus-cli /usr/local/bin/
ENTRYPOINT ["concensus-node"]
```

Containers run as root (default for debian:bookworm-slim). Acceptable for a demo tool.

### `docker-compose.tcp.yml` — 3-node TCP cluster

- Shared Docker network for inter-node communication
- `node-1`, `node-2`, `node-3`: `TRANSPORT=tcp`, `GRPC_PORT=50051`
- Each node binds consensus transport on `0.0.0.0:9000`, peers reference each other by container hostname
- `node-1` exposes gRPC port `50051` to host for CLI access
- Default 3 nodes; configurable by adding/removing service definitions
- Health checks using `concensus-cli health` against each node's gRPC port

### `docker-compose.uds.yml` — 3-node UDS cluster

- Same structure but `TRANSPORT=uds`
- Shared named volume mounted at `/sockets` in all containers
- Each node binds at `/sockets/node-X.sock`, peers reference socket paths
- `node-1` exposes gRPC port `50051` to host (gRPC always over TCP, UDS is inter-node consensus transport only)
- Health checks using `concensus-cli health` against each node's gRPC port

### Startup Race Conditions

When containers start simultaneously, a node may try to connect to a peer that hasn't bound its socket/port yet. This is handled gracefully: both `TcpSender` and `UdsSender` use lazy connect (first connection attempt happens on first `send()`), and the Paxos protocol retries proposals with exponential backoff. No special startup ordering is needed.

### Workspace Configuration

The root `Cargo.toml` must be updated to add `"crates/concensus-demo"` to the workspace members list.

## Logging & Observability

**tracing-subscriber** with JSON format, controlled by `RUST_LOG` env var (default: `info`).

### Key Log Events

| Event | Level | Fields |
|-------|-------|--------|
| Node started | `info` | `node_name`, `transport`, `bind`, `grpc_port` |
| Proposal submitted | `info` | `node_name`, `value` |
| Decision reached | `info` | `node_name`, `slot`, `value` |
| gRPC request received | `debug` | `method` |
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

# Propose a value via CLI (run locally or via docker exec)
concensus-cli --addr localhost:50051 propose --value "hello"

# View all decisions
concensus-cli --addr localhost:50051 decisions

# Health check
concensus-cli --addr localhost:50051 health
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
│       ├── Cargo.toml                  # two [[bin]] targets
│       ├── build.rs                    # tonic-build protobuf codegen
│       ├── proto/
│       │   └── consensus.proto         # gRPC service definition
│       └── src/
│           ├── node.rs                 # concensus-node binary: main, env parsing, gRPC server
│           └── cli.rs                  # concensus-cli binary: clap arg parsing, gRPC client calls
```

### `node.rs` Structure

1. Env parsing into a config struct
2. `run_node()` — creates storage, transport, node (via `with_id`), spawns decision collector
3. gRPC service impl — `ConsensusServiceServer` with shared `NodeHandle` and decision list
4. `main()` — init tracing, parse config, call `run_node()`, start gRPC server, await shutdown signal

### `cli.rs` Structure

1. Clap arg definitions: `--addr`, subcommands `propose`/`decisions`/`health`
2. `main()` — parse args, connect gRPC client, dispatch to subcommand handler
3. Each handler: make RPC call, format and print response
