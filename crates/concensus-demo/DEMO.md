# Concensus Demo

Run a multi-node Paxos consensus cluster in Docker and interact with it via a gRPC CLI client.

## Prerequisites

The following tools must be installed on your system:

| Tool | Version | Purpose |
|------|---------|---------|
| [Docker](https://docs.docker.com/get-docker/) | 20.10+ | Container runtime |
| [Docker Compose](https://docs.docker.com/compose/install/) | v2+ (included with Docker Desktop) | Multi-container orchestration |
| [Rust](https://rustup.rs/) | 1.94+ | Building the CLI client locally (optional if using `docker exec`) |
| [protoc](https://grpc.io/docs/protoc-installation/) | 3.x+ | Protobuf compiler (required for local builds) |

Install protoc:
- macOS: `brew install protobuf`
- Ubuntu/Debian: `apt install protobuf-compiler`

## Architecture

The demo runs a 3-node Paxos consensus cluster. Each node runs:
- A **consensus transport** (TCP or UDS) for inter-node Paxos messages
- A **gRPC server** for external interaction (propose values, query decisions, health checks)

Two transport configurations are available:
- **TCP** (`docker-compose.tcp.yml`) — nodes communicate over a Docker bridge network
- **UDS** (`docker-compose.uds.yml`) — nodes communicate via Unix domain sockets on a shared volume

Node 1 exposes its gRPC port (50051) to the host for CLI access.

## Running the Cluster

All `docker compose` commands should be run from this directory (`crates/concensus-demo/`):

### TCP transport

```bash
docker compose -f docker-compose.tcp.yml up --build -d
```

### UDS transport

```bash
docker compose -f docker-compose.uds.yml up --build -d
```

### Verify nodes are healthy

```bash
# TCP
docker compose -f docker-compose.tcp.yml ps

# UDS
docker compose -f docker-compose.uds.yml ps
```

All 3 nodes should show `(healthy)` status. Health checks begin after a 10-second start period.

### View logs

```bash
# All nodes
docker compose -f docker-compose.tcp.yml logs -f

# Single node
docker compose -f docker-compose.tcp.yml logs -f node-1

# Filter for consensus decisions
docker compose -f docker-compose.tcp.yml logs | grep "decision reached"
```

Logs are structured JSON with fields: `node_name`, `slot`, `value`, `timestamp`.

## Using the CLI Client

The `concensus-cli` binary connects to a node's gRPC server. You can run it locally (requires Rust) or via `docker exec`.

### Running locally

```bash
# Build once
cargo build -p concensus-demo --bin concensus-cli

# Or run directly with cargo
cargo run -p concensus-demo --bin concensus-cli -- --addr localhost:50051 <command>
```

### Running via Docker

```bash
docker exec concensus-node-1-1 concensus-cli --addr localhost:50051 <command>
```

### Commands

#### Health check

```bash
concensus-cli --addr localhost:50051 health
```

Output: `ok`

#### Propose a value

```bash
concensus-cli --addr localhost:50051 propose --value "my-value"
```

Output: `proposed`

The value is submitted to the node's consensus protocol. Once a quorum agrees, it becomes a decided value assigned to a slot.

#### List decisions

```bash
concensus-cli --addr localhost:50051 decisions
```

Output:
```
SLOT     VALUE
0        my-value
1        another-value
```

Shows all values that have reached consensus, ordered by slot number. All nodes in the cluster will agree on the same slot-to-value mapping.

### Example session

```bash
# Start cluster
docker compose -f docker-compose.tcp.yml up --build -d

# Wait for healthy
sleep 15

# Propose some values
concensus-cli --addr localhost:50051 propose --value "alice"
concensus-cli --addr localhost:50051 propose --value "bob"
concensus-cli --addr localhost:50051 propose --value "charlie"

# View decisions
concensus-cli --addr localhost:50051 decisions

# Verify all nodes agree
docker compose -f docker-compose.tcp.yml logs | grep "decision reached"
```

### Error responses

| gRPC Status | Meaning |
|-------------|---------|
| `RESOURCE_EXHAUSTED` | Proposal channel is full (back off and retry) |
| `UNAVAILABLE` | Node is not running or shutting down |
| Connection refused | Node hasn't started yet or gRPC port not exposed |

## Shutdown and Cleanup

### Stop the cluster

```bash
# TCP
docker compose -f docker-compose.tcp.yml down

# UDS (add -v to remove the shared socket volume)
docker compose -f docker-compose.uds.yml down -v
```

### Remove built images

```bash
docker compose -f docker-compose.tcp.yml down --rmi all
# or
docker compose -f docker-compose.uds.yml down --rmi all -v
```

### Full cleanup

Remove all containers, images, volumes, and networks created by the demo:

```bash
docker compose -f docker-compose.tcp.yml down --rmi all --volumes --remove-orphans
docker compose -f docker-compose.uds.yml down --rmi all --volumes --remove-orphans
```

## Environment Variables

These are configured in the compose files but can be overridden:

| Variable | Default | Description |
|----------|---------|-------------|
| `TRANSPORT` | — | `tcp` or `uds` |
| `NODE_NAME` | — | Node identifier (e.g. `node-1`) |
| `BIND_ADDR` | — | TCP listen address (e.g. `0.0.0.0:9000`) |
| `BIND_PATH` | — | UDS socket path (e.g. `/sockets/node-1.sock`) |
| `PEERS` | — | Comma-separated `name=address` pairs |
| `GRPC_PORT` | — | gRPC server port |
| `RUST_LOG` | `info` | Log level filter (`debug`, `info`, `warn`, `error`) |

## Adding More Nodes

To scale beyond 3 nodes, duplicate a service definition in the compose file, give it a unique `NODE_NAME`, and update the `PEERS` list on all nodes to include the new member. Quorum is `(N/2) + 1`.
