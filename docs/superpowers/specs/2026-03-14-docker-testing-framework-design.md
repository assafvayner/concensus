# Docker Integration Testing & Demo Framework

## Overview

A Docker-based framework for running multi-node consensus clusters using TCP and UDS transports. Primarily a demo/exploration tool with REST API for manual interaction, with automated random-proposal scenarios for smoke testing.

## New Crate: `crates/concensus-demo`

Single binary, two modes (`random` / `api`), two transports (`tcp` / `uds`), configured entirely via environment variables.

### Dependencies

- `concensus` (with `tcp-transport` and `uds-transport` features)
- `axum` — HTTP server for API mode
- `tokio` (full runtime)
- `tracing` + `tracing-subscriber` — structured JSON logging
- `rand` — random proposal generation
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

### Startup Flow

1. Parse environment variables into a config struct
2. Create transport (TCP or UDS) via existing factory methods
3. Create `Node<String, _, _>` + `NodeHandle` + `DecisionReceiver`
4. Spawn the node's `run()` in a background task
5. Spawn a decision logger task that reads from `DecisionReceiver`, logs each decision, and appends to shared state
6. Based on `MODE`:
   - `random`: spawn a loop proposing values like `"rand-48291"` at random intervals (Poisson-ish around `PROPOSAL_INTERVAL_MS`)
   - `api`: start axum server

### Value Type

`String` for all modes. Random mode produces `"rand-{number}"`. API mode accepts arbitrary user-provided strings.

## REST API (api mode)

Three endpoints on `API_PORT`:

### `POST /propose`

- Body: `{"value": "some-string"}`
- Calls `NodeHandle::propose(value)`
- Returns `200 {"status": "proposed"}` or `500` on failure

### `GET /decisions`

- Returns all decisions observed so far
- Response: `[{"slot": 0, "value": "some-string"}, ...]`
- Reads from `Arc<RwLock<Vec<Decided<String>>>>`

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
2. `run_node()` — creates transport, node, spawns decision logger
3. `run_random_mode()` — proposal loop with random intervals
4. `run_api_mode()` — axum router with 3 endpoints, shared decision state
5. `main()` — init tracing, parse config, call `run_node()`, dispatch to mode

Stays in one file unless it grows beyond ~300-400 lines.
