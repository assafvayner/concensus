# Docker Demo Framework Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Docker-based demo framework with a gRPC API and CLI client for running multi-node Paxos consensus clusters over TCP and UDS transports.

**Architecture:** A new workspace crate `daccord-demo` produces two binaries: `daccord-node` (runs a consensus node with a gRPC server) and `daccord-cli` (CLI client for interacting with nodes). Docker Compose files orchestrate 3-node clusters using either TCP or UDS transport.

**Tech Stack:** Rust, tonic (gRPC), prost (protobuf), clap (CLI), tokio, tracing, Docker, Docker Compose, cargo-chef

**Spec:** `docs/superpowers/specs/2026-03-14-docker-testing-framework-design.md`

---

## File Structure

```
daccord/
├── Cargo.toml                                    # MODIFY: add daccord-demo to workspace members
├── .dockerignore                                 # CREATE: exclude target/, .git/, etc from build context
├── Dockerfile                                    # CREATE: multi-stage cargo-chef build
├── docker-compose.tcp.yml                        # CREATE: 3-node TCP cluster
├── docker-compose.uds.yml                        # CREATE: 3-node UDS cluster
├── crates/
│   └── daccord-demo/
│       ├── Cargo.toml                            # CREATE: two [[bin]] targets, dependencies
│       ├── build.rs                              # CREATE: tonic-build protobuf codegen
│       ├── proto/
│       │   └── consensus.proto                   # CREATE: gRPC service definition
│       └── src/
│           ├── node.rs                           # CREATE: daccord-node binary
│           └── cli.rs                            # CREATE: daccord-cli binary
```

---

## Chunk 1: Crate Scaffold and Protobuf Codegen

### Task 1: Create crate directory and Cargo.toml

**Files:**
- Create: `crates/daccord-demo/Cargo.toml`
- Modify: `Cargo.toml` (workspace root)

- [ ] **Step 1: Create directory structure**

Run:
```bash
mkdir -p crates/daccord-demo/proto crates/daccord-demo/src
```

- [ ] **Step 2: Create `crates/daccord-demo/Cargo.toml`**

```toml
[package]
name = "daccord-demo"
version = "0.1.0"
edition = "2021"

[[bin]]
name = "daccord-node"
path = "src/node.rs"

[[bin]]
name = "daccord-cli"
path = "src/cli.rs"

[dependencies]
daccord = { path = "../daccord", features = ["tcp-transport", "uds-transport", "test-support"] }
tonic = "0.12"
prost = "0.13"
tokio = { version = "1", features = ["full"] }
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["json", "env-filter"] }
clap = { version = "4", features = ["derive"] }

[build-dependencies]
tonic-build = "0.12"
```

- [ ] **Step 3: Add crate to workspace**

In root `Cargo.toml`, change the `members` line to:
```toml
members = ["crates/daccord", "crates/daccord-tests", "crates/daccord-demo"]
```

- [ ] **Step 4: Create placeholder binaries so the crate compiles**

Create `crates/daccord-demo/src/node.rs`:
```rust
fn main() {
    println!("daccord-node placeholder");
}
```

Create `crates/daccord-demo/src/cli.rs`:
```rust
fn main() {
    println!("daccord-cli placeholder");
}
```

- [ ] **Step 5: Verify the workspace compiles**

Run: `cargo check -p daccord-demo`
Expected: success (no errors)

- [ ] **Step 6: Commit**

```bash
git add crates/daccord-demo/ Cargo.toml
git commit -m "feat(demo): scaffold daccord-demo crate with two binary targets"
```

---

### Task 2: Add protobuf definition and build.rs codegen

**Files:**
- Create: `crates/daccord-demo/proto/consensus.proto`
- Create: `crates/daccord-demo/build.rs`

- [ ] **Step 1: Create `crates/daccord-demo/proto/consensus.proto`**

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

- [ ] **Step 2: Create `crates/daccord-demo/build.rs`**

```rust
fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("proto/consensus.proto")?;
    Ok(())
}
```

- [ ] **Step 3: Verify protobuf codegen works**

Run: `cargo check -p daccord-demo`
Expected: success. tonic-build generates Rust code from the proto file. The `protoc` compiler must be installed on the system — if this fails with "protoc not found", install it via `brew install protobuf` (macOS) or `apt install protobuf-compiler` (Linux).

- [ ] **Step 4: Commit**

```bash
git add crates/daccord-demo/proto/ crates/daccord-demo/build.rs
git commit -m "feat(demo): add consensus.proto and tonic-build codegen"
```

---

## Chunk 2: Node Binary — gRPC Server

### Task 3: Implement env parsing and config

**Files:**
- Modify: `crates/daccord-demo/src/node.rs`

- [ ] **Step 1: Write the node binary with env parsing**

Replace `crates/daccord-demo/src/node.rs` with:

```rust
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use tokio::sync::RwLock;
use tonic::{transport::Server, Request, Response, Status};

use daccord::{
    Decided, DecisionReceiver, MemoryStorage, Node, NodeHandle, NodeId, PeerInfo,
    TcpSender, TcpTransport, TcpReceiver,
    UdsSender, UdsTransport, UdsReceiver,
    ProposeError,
};

pub mod consensus_proto {
    tonic::include_proto!("consensus");
}

use consensus_proto::consensus_service_server::{ConsensusService, ConsensusServiceServer};
use consensus_proto::{
    Decision, GetDecisionsRequest, GetDecisionsResponse, HealthRequest, HealthResponse,
    ProposeRequest, ProposeResponse,
};

// ─── Config ──────────────────────────────────────────────────────────────────

#[derive(Debug)]
enum Transport {
    Tcp {
        bind_addr: SocketAddr,
        peers: Vec<(NodeId, SocketAddr)>,
    },
    Uds {
        bind_path: PathBuf,
        peers: Vec<(NodeId, PathBuf)>,
    },
}

#[derive(Debug)]
struct Config {
    node_name: String,
    transport: Transport,
    grpc_port: u16,
}

fn parse_config() -> Config {
    let node_name = std::env::var("NODE_NAME").expect("NODE_NAME env var required");
    let transport_str = std::env::var("TRANSPORT").expect("TRANSPORT env var required");
    let peers_str = std::env::var("PEERS").unwrap_or_default();
    let grpc_port: u16 = std::env::var("GRPC_PORT")
        .expect("GRPC_PORT env var required")
        .parse()
        .expect("GRPC_PORT must be a valid port number");

    let transport = match transport_str.as_str() {
        "tcp" => {
            let bind_addr: SocketAddr = std::env::var("BIND_ADDR")
                .expect("BIND_ADDR required for tcp transport")
                .parse()
                .expect("BIND_ADDR must be a valid socket address");
            let peers = parse_tcp_peers(&peers_str);
            Transport::Tcp { bind_addr, peers }
        }
        "uds" => {
            let bind_path: PathBuf = std::env::var("BIND_PATH")
                .expect("BIND_PATH required for uds transport")
                .into();
            let peers = parse_uds_peers(&peers_str);
            Transport::Uds { bind_path, peers }
        }
        other => panic!("TRANSPORT must be 'tcp' or 'uds', got '{}'", other),
    };

    Config {
        node_name,
        transport,
        grpc_port,
    }
}

fn parse_tcp_peers(peers_str: &str) -> Vec<(NodeId, SocketAddr)> {
    if peers_str.is_empty() {
        return Vec::new();
    }
    peers_str
        .split(',')
        .map(|entry| {
            let (name, addr_str) = entry
                .split_once('=')
                .unwrap_or_else(|| panic!("invalid peer format '{}', expected 'name=addr'", entry));
            let addr: SocketAddr = addr_str
                .parse()
                .unwrap_or_else(|_| panic!("invalid socket address '{}' for peer '{}'", addr_str, name));
            (NodeId::new(name, 0), addr)
        })
        .collect()
}

fn parse_uds_peers(peers_str: &str) -> Vec<(NodeId, PathBuf)> {
    if peers_str.is_empty() {
        return Vec::new();
    }
    peers_str
        .split(',')
        .map(|entry| {
            let (name, path_str) = entry
                .split_once('=')
                .unwrap_or_else(|| panic!("invalid peer format '{}', expected 'name=path'", entry));
            (NodeId::new(name, 0), PathBuf::from(path_str))
        })
        .collect()
}

// ─── gRPC Service ────────────────────────────────────────────────────────────

type DecisionList = Arc<RwLock<Vec<Decision>>>;

struct ConsensusServiceImpl {
    handle: NodeHandle<String>,
    decisions: DecisionList,
}

#[tonic::async_trait]
impl ConsensusService for ConsensusServiceImpl {
    async fn propose(
        &self,
        request: Request<ProposeRequest>,
    ) -> Result<Response<ProposeResponse>, Status> {
        let value = request.into_inner().value;
        tracing::info!(value = %value, "gRPC propose request");

        match self.handle.propose(value).await {
            Ok(()) => Ok(Response::new(ProposeResponse {
                status: "proposed".to_string(),
            })),
            Err(ProposeError::ChannelFull) => {
                Err(Status::resource_exhausted("proposal channel full"))
            }
            Err(ProposeError::NotRunning) => {
                Err(Status::unavailable("node is not running"))
            }
        }
    }

    async fn get_decisions(
        &self,
        _request: Request<GetDecisionsRequest>,
    ) -> Result<Response<GetDecisionsResponse>, Status> {
        let decisions = self.decisions.read().await.clone();
        Ok(Response::new(GetDecisionsResponse { decisions }))
    }

    async fn health(
        &self,
        _request: Request<HealthRequest>,
    ) -> Result<Response<HealthResponse>, Status> {
        Ok(Response::new(HealthResponse {
            status: "ok".to_string(),
        }))
    }
}

// ─── Decision collector ──────────────────────────────────────────────────────

async fn collect_decisions(
    mut decision_rx: DecisionReceiver<String>,
    decisions: DecisionList,
    node_name: String,
) {
    while let Some(decided) = decision_rx.recv().await {
        tracing::info!(
            node_name = %node_name,
            slot = decided.slot,
            value = %decided.value,
            "decision reached"
        );
        decisions.write().await.push(Decision {
            slot: decided.slot,
            value: decided.value,
        });
    }
    tracing::info!("decision channel closed");
}

// ─── Node startup (generic over transport) ───────────────────────────────────

async fn start_node_tcp(
    node_name: &str,
    bind_addr: SocketAddr,
    peers: Vec<(NodeId, SocketAddr)>,
) -> (NodeHandle<String>, DecisionReceiver<String>) {
    let storage = MemoryStorage::<String>::new();
    let (peer_infos, receiver) = TcpTransport::create(bind_addr, peers)
        .await
        .expect("failed to bind TCP transport");

    let node_id = NodeId::new(node_name, 0);
    let (node, handle, decision_rx) = Node::with_id(node_id, peer_infos, receiver, storage);

    let name = node_name.to_string();
    tokio::spawn(async move {
        if let Err(e) = node.run().await {
            tracing::error!(node_name = %name, error = %e, "node task exited with error");
            std::process::exit(1);
        }
    });

    (handle, decision_rx)
}

async fn start_node_uds(
    node_name: &str,
    bind_path: PathBuf,
    peers: Vec<(NodeId, PathBuf)>,
) -> (NodeHandle<String>, DecisionReceiver<String>) {
    let storage = MemoryStorage::<String>::new();
    let (peer_infos, receiver) = UdsTransport::create(bind_path, peers)
        .await
        .expect("failed to bind UDS transport");

    let node_id = NodeId::new(node_name, 0);
    let (node, handle, decision_rx) = Node::with_id(node_id, peer_infos, receiver, storage);

    let name = node_name.to_string();
    tokio::spawn(async move {
        if let Err(e) = node.run().await {
            tracing::error!(node_name = %name, error = %e, "node task exited with error");
            std::process::exit(1);
        }
    });

    (handle, decision_rx)
}

// ─── Main ────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let config = parse_config();

    let (handle, decision_rx) = match config.transport {
        Transport::Tcp { bind_addr, peers } => {
            tracing::info!(
                node_name = %config.node_name,
                transport = "tcp",
                bind = %bind_addr,
                grpc_port = config.grpc_port,
                "node started"
            );
            start_node_tcp(&config.node_name, bind_addr, peers).await
        }
        Transport::Uds { bind_path, peers } => {
            tracing::info!(
                node_name = %config.node_name,
                transport = "uds",
                bind = ?bind_path,
                grpc_port = config.grpc_port,
                "node started"
            );
            start_node_uds(&config.node_name, bind_path, peers).await
        }
    };

    let decisions: DecisionList = Arc::new(RwLock::new(Vec::new()));

    tokio::spawn(collect_decisions(
        decision_rx,
        decisions.clone(),
        config.node_name.clone(),
    ));

    let grpc_addr: SocketAddr = format!("0.0.0.0:{}", config.grpc_port)
        .parse()
        .expect("invalid gRPC address");

    let service = ConsensusServiceImpl {
        handle,
        decisions,
    };

    tracing::info!(addr = %grpc_addr, "gRPC server starting");

    Server::builder()
        .add_service(ConsensusServiceServer::new(service))
        .serve_with_shutdown(grpc_addr, async {
            tokio::signal::ctrl_c().await.ok();
            tracing::info!("shutdown signal received");
        })
        .await
        .expect("gRPC server error");

    tracing::info!("node shut down");
}
```

- [ ] **Step 2: Verify it compiles**

Run: `cargo check -p daccord-demo --bin daccord-node`
Expected: success

Note: if `protoc` is not installed, install it first:
- macOS: `brew install protobuf`
- Linux: `apt install protobuf-compiler`

- [ ] **Step 3: Commit**

```bash
git add crates/daccord-demo/src/node.rs
git commit -m "feat(demo): implement daccord-node binary with gRPC server"
```

---

## Chunk 3: CLI Client Binary

### Task 4: Implement the CLI client

**Files:**
- Modify: `crates/daccord-demo/src/cli.rs`

- [ ] **Step 1: Write the CLI client**

Replace `crates/daccord-demo/src/cli.rs` with:

```rust
use clap::{Parser, Subcommand};

pub mod consensus_proto {
    tonic::include_proto!("consensus");
}

use consensus_proto::consensus_service_client::ConsensusServiceClient;
use consensus_proto::{GetDecisionsRequest, HealthRequest, ProposeRequest};

#[derive(Parser)]
#[command(name = "daccord-cli", about = "CLI client for daccord-node gRPC API")]
struct Cli {
    /// gRPC server address (e.g. http://localhost:50051)
    #[arg(long)]
    addr: String,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Propose a value for consensus
    Propose {
        /// The value to propose
        #[arg(long)]
        value: String,
    },
    /// List all decided values
    Decisions,
    /// Check node health
    Health,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    let addr = if cli.addr.starts_with("http://") || cli.addr.starts_with("https://") {
        cli.addr.clone()
    } else {
        format!("http://{}", cli.addr)
    };

    let mut client = ConsensusServiceClient::connect(addr)
        .await
        .unwrap_or_else(|e| {
            eprintln!("failed to connect: {}", e);
            std::process::exit(1);
        });

    match cli.command {
        Command::Propose { value } => {
            match client.propose(ProposeRequest { value }).await {
                Ok(response) => {
                    println!("{}", response.into_inner().status);
                }
                Err(status) => {
                    eprintln!("error: {} ({})", status.message(), status.code());
                    std::process::exit(1);
                }
            }
        }
        Command::Decisions => {
            match client.get_decisions(GetDecisionsRequest {}).await {
                Ok(response) => {
                    let decisions = response.into_inner().decisions;
                    if decisions.is_empty() {
                        println!("no decisions yet");
                    } else {
                        println!("{:<8} {}", "SLOT", "VALUE");
                        for d in decisions {
                            println!("{:<8} {}", d.slot, d.value);
                        }
                    }
                }
                Err(status) => {
                    eprintln!("error: {} ({})", status.message(), status.code());
                    std::process::exit(1);
                }
            }
        }
        Command::Health => {
            match client.health(HealthRequest {}).await {
                Ok(response) => {
                    println!("{}", response.into_inner().status);
                }
                Err(status) => {
                    eprintln!("error: {} ({})", status.message(), status.code());
                    std::process::exit(1);
                }
            }
        }
    }
}
```

- [ ] **Step 2: Verify it compiles**

Run: `cargo check -p daccord-demo --bin daccord-cli`
Expected: success

- [ ] **Step 3: Verify CLI help output**

Run: `cargo run -p daccord-demo --bin daccord-cli -- --help`
Expected: prints usage showing `--addr`, and subcommands `propose`, `decisions`, `health`

- [ ] **Step 4: Commit**

```bash
git add crates/daccord-demo/src/cli.rs
git commit -m "feat(demo): implement daccord-cli with propose, decisions, and health commands"
```

---

## Chunk 4: Clippy, Format, Build Verification

### Task 5: Run linting and formatting

**Files:** All files in `crates/daccord-demo/`

- [ ] **Step 1: Run cargo fmt**

Run: `cargo fmt -p daccord-demo` (or `cargo +nightly fmt -p daccord-demo` if nightly is installed)

- [ ] **Step 2: Run clippy**

Run: `cargo clippy -p daccord-demo -- -D warnings`
Expected: no warnings. Fix any that arise.

- [ ] **Step 3: Build release binaries**

Run: `cargo build --release -p daccord-demo`
Expected: produces `target/release/daccord-node` and `target/release/daccord-cli`

- [ ] **Step 4: Commit any formatting changes**

```bash
git add -A crates/daccord-demo/
git commit -m "style(demo): apply formatting and fix clippy warnings"
```

(Skip this commit if there were no changes.)

---

## Chunk 5: Docker Infrastructure

### Task 6: Create Dockerfile and .dockerignore

**Files:**
- Create: `Dockerfile`
- Create: `.dockerignore`

- [ ] **Step 1: Create `.dockerignore` at repository root**

```
target/
.git/
.gitignore
*.md
docs/
.claude/
```

- [ ] **Step 2: Create `Dockerfile` at repository root**

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
RUN cargo chef cook --release --recipe-path recipe.json -p daccord-demo
COPY . .
RUN cargo build --release -p daccord-demo

# Stage 4: Runtime
FROM debian:bookworm-slim
COPY --from=builder /app/target/release/daccord-node /usr/local/bin/
COPY --from=builder /app/target/release/daccord-cli /usr/local/bin/
ENTRYPOINT ["daccord-node"]
```

Note: `protobuf-compiler` is installed in the builder stage because `tonic-build` requires `protoc` at build time.

- [ ] **Step 3: Commit**

```bash
git add Dockerfile .dockerignore
git commit -m "feat(demo): add multi-stage Dockerfile with cargo-chef caching and .dockerignore"
```

---

### Task 7: Create docker-compose.tcp.yml

**Files:**
- Create: `docker-compose.tcp.yml`

- [ ] **Step 1: Create `docker-compose.tcp.yml` at repository root**

```yaml
services:
  node-1:
    build: .
    environment:
      TRANSPORT: tcp
      NODE_NAME: node-1
      BIND_ADDR: "0.0.0.0:9000"
      PEERS: "node-2=node-2:9000,node-3=node-3:9000"
      GRPC_PORT: "50051"
      RUST_LOG: info
    ports:
      - "50051:50051"
    healthcheck:
      test: ["CMD", "daccord-cli", "--addr", "localhost:50051", "health"]
      interval: 5s
      timeout: 3s
      retries: 10
      start_period: 10s
    networks:
      - consensus-net

  node-2:
    build: .
    environment:
      TRANSPORT: tcp
      NODE_NAME: node-2
      BIND_ADDR: "0.0.0.0:9000"
      PEERS: "node-1=node-1:9000,node-3=node-3:9000"
      GRPC_PORT: "50051"
      RUST_LOG: info
    healthcheck:
      test: ["CMD", "daccord-cli", "--addr", "localhost:50051", "health"]
      interval: 5s
      timeout: 3s
      retries: 10
      start_period: 10s
    networks:
      - consensus-net

  node-3:
    build: .
    environment:
      TRANSPORT: tcp
      NODE_NAME: node-3
      BIND_ADDR: "0.0.0.0:9000"
      PEERS: "node-1=node-1:9000,node-2=node-2:9000"
      GRPC_PORT: "50051"
      RUST_LOG: info
    healthcheck:
      test: ["CMD", "daccord-cli", "--addr", "localhost:50051", "health"]
      interval: 5s
      timeout: 3s
      retries: 10
      start_period: 10s
    networks:
      - consensus-net

networks:
  consensus-net:
    driver: bridge
```

- [ ] **Step 2: Commit**

```bash
git add docker-compose.tcp.yml
git commit -m "feat(demo): add docker-compose.tcp.yml for 3-node TCP cluster"
```

---

### Task 8: Create docker-compose.uds.yml

**Files:**
- Create: `docker-compose.uds.yml`

- [ ] **Step 1: Create `docker-compose.uds.yml` at repository root**

```yaml
services:
  node-1:
    build: .
    environment:
      TRANSPORT: uds
      NODE_NAME: node-1
      BIND_PATH: "/sockets/node-1.sock"
      PEERS: "node-2=/sockets/node-2.sock,node-3=/sockets/node-3.sock"
      GRPC_PORT: "50051"
      RUST_LOG: info
    ports:
      - "50051:50051"
    volumes:
      - sockets:/sockets
    healthcheck:
      test: ["CMD", "daccord-cli", "--addr", "localhost:50051", "health"]
      interval: 5s
      timeout: 3s
      retries: 10
      start_period: 10s
    networks:
      - consensus-net

  node-2:
    build: .
    environment:
      TRANSPORT: uds
      NODE_NAME: node-2
      BIND_PATH: "/sockets/node-2.sock"
      PEERS: "node-1=/sockets/node-1.sock,node-3=/sockets/node-3.sock"
      GRPC_PORT: "50051"
      RUST_LOG: info
    volumes:
      - sockets:/sockets
    healthcheck:
      test: ["CMD", "daccord-cli", "--addr", "localhost:50051", "health"]
      interval: 5s
      timeout: 3s
      retries: 10
      start_period: 10s
    networks:
      - consensus-net

  node-3:
    build: .
    environment:
      TRANSPORT: uds
      NODE_NAME: node-3
      BIND_PATH: "/sockets/node-3.sock"
      PEERS: "node-1=/sockets/node-1.sock,node-2=/sockets/node-2.sock"
      GRPC_PORT: "50051"
      RUST_LOG: info
    volumes:
      - sockets:/sockets
    healthcheck:
      test: ["CMD", "daccord-cli", "--addr", "localhost:50051", "health"]
      interval: 5s
      timeout: 3s
      retries: 10
      start_period: 10s
    networks:
      - consensus-net

volumes:
  sockets:

networks:
  consensus-net:
    driver: bridge
```

- [ ] **Step 2: Commit**

```bash
git add docker-compose.uds.yml
git commit -m "feat(demo): add docker-compose.uds.yml for 3-node UDS cluster"
```

---

## Chunk 6: Build and Smoke Test

### Task 9: Docker build and TCP cluster smoke test

**Files:** None (verification only)

- [ ] **Step 1: Build the Docker image**

Run: `docker compose -f docker-compose.tcp.yml build`
Expected: successful multi-stage build producing an image with both `daccord-node` and `daccord-cli`

- [ ] **Step 2: Start the TCP cluster**

Run: `docker compose -f docker-compose.tcp.yml up -d`
Expected: 3 containers start. Wait for health checks to pass:
Run: `docker compose -f docker-compose.tcp.yml ps`
Expected: all 3 services show `healthy` status (may take up to 30s for start_period + health checks)

- [ ] **Step 3: Test health endpoint via CLI**

Run: `cargo run -p daccord-demo --bin daccord-cli -- --addr localhost:50051 health`
Expected: prints `ok`

- [ ] **Step 4: Test proposal via CLI**

Run: `cargo run -p daccord-demo --bin daccord-cli -- --addr localhost:50051 propose --value "hello-world"`
Expected: prints `proposed`

- [ ] **Step 5: Test decisions via CLI**

Run: `cargo run -p daccord-demo --bin daccord-cli -- --addr localhost:50051 decisions`
Expected: prints a table with at least one decision showing `hello-world`

- [ ] **Step 6: Check logs for consensus across all nodes**

Run: `docker compose -f docker-compose.tcp.yml logs | grep "decision reached"`
Expected: all 3 nodes should show a `decision reached` log entry for the same slot and value

- [ ] **Step 7: Tear down**

Run: `docker compose -f docker-compose.tcp.yml down`

---

### Task 10: UDS cluster smoke test

**Files:** None (verification only)

- [ ] **Step 1: Start the UDS cluster**

Run: `docker compose -f docker-compose.uds.yml up -d`
Expected: 3 containers start with shared `/sockets` volume

- [ ] **Step 2: Wait for health checks**

Run: `docker compose -f docker-compose.uds.yml ps`
Expected: all 3 services show `healthy` status

- [ ] **Step 3: Test proposal via CLI**

Run: `cargo run -p daccord-demo --bin daccord-cli -- --addr localhost:50051 propose --value "hello-uds"`
Expected: prints `proposed`

- [ ] **Step 4: Test decisions via CLI**

Run: `cargo run -p daccord-demo --bin daccord-cli -- --addr localhost:50051 decisions`
Expected: prints a table with the `hello-uds` decision

- [ ] **Step 5: Check logs for consensus across all nodes**

Run: `docker compose -f docker-compose.uds.yml logs | grep "decision reached"`
Expected: all 3 nodes show a `decision reached` log entry for the same slot and value

- [ ] **Step 6: Tear down**

Run: `docker compose -f docker-compose.uds.yml down -v`
(The `-v` flag removes the sockets volume)
