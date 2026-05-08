use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tonic::transport::Server;

use daccord::{
    DecisionReceiver, Node, NodeHandle, NodeId, PaxosConfig, PaxosMemoryStorage, RaftConfig,
    RaftMemoryStorage, TcpTransport, UdsTransport,
};
#[cfg(feature = "duckdb-bundled")]
use daccord::{DuckdbPaxosStorage, DuckdbRaftStorage};

use daccord_demo::consensus_proto::consensus_service_server::ConsensusServiceServer;
use daccord_demo::service::{
    collect_decisions, Algorithm, ConsensusServiceImpl, DecisionLog, DECISION_BROADCAST_CAPACITY,
};

// ─── Config ──────────────────────────────────────────────────────────────────

#[derive(Debug)]
enum Transport {
    Tcp {
        bind_addr: SocketAddr,
        peers: Vec<(NodeId, String)>,
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
    algorithm: Algorithm,
    data_dir: Option<PathBuf>,
}

fn resolve_algorithm() -> Algorithm {
    let raw = std::env::var("ALGORITHM").unwrap_or_else(|_| "paxos".to_string());
    match raw.as_str() {
        "raft" => Algorithm::Raft,
        "paxos" | "" => Algorithm::Paxos,
        other => panic!("ALGORITHM must be 'paxos' or 'raft', got '{}'", other),
    }
}

fn resolve_node_name() -> String {
    // Prefer explicit NODE_NAME, fall back to container hostname
    if let Ok(name) = std::env::var("NODE_NAME") {
        return name;
    }
    hostname::get()
        .expect("failed to get hostname")
        .into_string()
        .expect("hostname is not valid UTF-8")
}

fn parse_config() -> Config {
    let node_name = resolve_node_name();
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
            let mut peers = parse_tcp_peers(&peers_str);
            peers.retain(|(id, _)| id.name() != node_name);
            Transport::Tcp { bind_addr, peers }
        }
        "uds" => {
            let bind_path: PathBuf = std::env::var("BIND_PATH")
                .unwrap_or_else(|_| format!("/sockets/{}.sock", node_name))
                .into();
            let mut peers = parse_uds_peers(&peers_str);
            peers.retain(|(id, _)| id.name() != node_name);
            Transport::Uds { bind_path, peers }
        }
        other => panic!("TRANSPORT must be 'tcp' or 'uds', got '{}'", other),
    };

    let algorithm = resolve_algorithm();
    let data_dir = std::env::var("DATA_DIR").ok().map(PathBuf::from);

    Config {
        node_name,
        transport,
        grpc_port,
        algorithm,
        data_dir,
    }
}

fn parse_tcp_peers(peers_str: &str) -> Vec<(NodeId, String)> {
    if peers_str.is_empty() {
        return Vec::new();
    }
    peers_str
        .split(',')
        .map(|entry| {
            let (name, addr_str) = entry
                .split_once('=')
                .unwrap_or_else(|| panic!("invalid peer format '{}', expected 'name=addr'", entry));
            (NodeId::new(name, 0), addr_str.to_string())
        })
        .collect()
}

async fn resolve_tcp_peers(peers: Vec<(NodeId, String)>) -> Vec<(NodeId, SocketAddr)> {
    let mut resolved = Vec::with_capacity(peers.len());
    for (id, addr_str) in peers {
        let addr = tokio::net::lookup_host(&addr_str)
            .await
            .unwrap_or_else(|e| panic!("failed to resolve '{}': {}", addr_str, e))
            .next()
            .unwrap_or_else(|| panic!("no addresses found for '{}'", addr_str));
        resolved.push((id, addr));
    }
    resolved
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

// ─── Node startup (generic over transport) ───────────────────────────────────

async fn start_node_tcp(
    node_name: &str,
    algorithm: Algorithm,
    bind_addr: SocketAddr,
    peers: Vec<(NodeId, String)>,
    data_dir: Option<&Path>,
) -> (NodeHandle<Vec<u8>>, DecisionReceiver<Vec<u8>>) {
    let peers = resolve_tcp_peers(peers).await;
    let (peer_infos, receiver) = TcpTransport::create(bind_addr, peers)
        .await
        .expect("failed to bind TCP transport");

    let (node, handle, decision_rx) = match (algorithm, data_dir) {
        #[cfg(feature = "duckdb-bundled")]
        (Algorithm::Paxos, Some(dir)) => Node::paxos_with_id(
            NodeId::new(node_name, 0),
            PaxosConfig::default(),
            peer_infos,
            receiver,
            DuckdbPaxosStorage::<Vec<u8>>::open(dir.join("paxos.db"))
                .expect("failed to open DuckDB Paxos storage"),
        ),
        #[cfg(feature = "duckdb-bundled")]
        (Algorithm::Raft, Some(dir)) => Node::raft_with_id(
            NodeId::new(node_name, 0),
            RaftConfig::default(),
            peer_infos,
            receiver,
            DuckdbRaftStorage::<Vec<u8>>::open(dir.join("raft.db"))
                .expect("failed to open DuckDB Raft storage"),
        ),
        (Algorithm::Paxos, _) => Node::paxos_with_id(
            NodeId::new(node_name, 0),
            PaxosConfig::default(),
            peer_infos,
            receiver,
            PaxosMemoryStorage::<Vec<u8>>::new(),
        ),
        (Algorithm::Raft, _) => Node::raft_with_id(
            NodeId::new(node_name, 0),
            RaftConfig::default(),
            peer_infos,
            receiver,
            RaftMemoryStorage::<Vec<u8>>::new(),
        ),
    };

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
    algorithm: Algorithm,
    bind_path: PathBuf,
    peers: Vec<(NodeId, PathBuf)>,
    data_dir: Option<&Path>,
) -> (NodeHandle<Vec<u8>>, DecisionReceiver<Vec<u8>>) {
    let (peer_infos, receiver) = UdsTransport::create(bind_path, peers)
        .await
        .expect("failed to bind UDS transport");

    let (node, handle, decision_rx) = match (algorithm, data_dir) {
        #[cfg(feature = "duckdb-bundled")]
        (Algorithm::Paxos, Some(dir)) => Node::paxos_with_id(
            NodeId::new(node_name, 0),
            PaxosConfig::default(),
            peer_infos,
            receiver,
            DuckdbPaxosStorage::<Vec<u8>>::open(dir.join("paxos.db"))
                .expect("failed to open DuckDB Paxos storage"),
        ),
        #[cfg(feature = "duckdb-bundled")]
        (Algorithm::Raft, Some(dir)) => Node::raft_with_id(
            NodeId::new(node_name, 0),
            RaftConfig::default(),
            peer_infos,
            receiver,
            DuckdbRaftStorage::<Vec<u8>>::open(dir.join("raft.db"))
                .expect("failed to open DuckDB Raft storage"),
        ),
        (Algorithm::Paxos, _) => Node::paxos_with_id(
            NodeId::new(node_name, 0),
            PaxosConfig::default(),
            peer_infos,
            receiver,
            PaxosMemoryStorage::<Vec<u8>>::new(),
        ),
        (Algorithm::Raft, _) => Node::raft_with_id(
            NodeId::new(node_name, 0),
            RaftConfig::default(),
            peer_infos,
            receiver,
            RaftMemoryStorage::<Vec<u8>>::new(),
        ),
    };

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

    tracing::info!(
        algorithm = %config.algorithm.as_str(),
        "using consensus algorithm"
    );

    let data_dir = config.data_dir.as_deref();
    let (handle, decision_rx) = match config.transport {
        Transport::Tcp { bind_addr, peers } => {
            tracing::info!(
                node_name = %config.node_name,
                transport = "tcp",
                bind = %bind_addr,
                grpc_port = config.grpc_port,
                algorithm = %config.algorithm.as_str(),
                data_dir = ?data_dir,
                "node started"
            );
            start_node_tcp(
                &config.node_name,
                config.algorithm,
                bind_addr,
                peers,
                data_dir,
            )
            .await
        }
        Transport::Uds { bind_path, peers } => {
            tracing::info!(
                node_name = %config.node_name,
                transport = "uds",
                bind = ?bind_path,
                grpc_port = config.grpc_port,
                algorithm = %config.algorithm.as_str(),
                data_dir = ?data_dir,
                "node started"
            );
            start_node_uds(
                &config.node_name,
                config.algorithm,
                bind_path,
                peers,
                data_dir,
            )
            .await
        }
    };

    let decisions = Arc::new(DecisionLog::new(DECISION_BROADCAST_CAPACITY));

    tokio::spawn(collect_decisions(
        decision_rx,
        decisions.clone(),
        config.node_name.clone(),
    ));

    let grpc_addr: SocketAddr = format!("0.0.0.0:{}", config.grpc_port)
        .parse()
        .expect("invalid gRPC address");

    let service = ConsensusServiceImpl::new(handle, decisions, config.algorithm);

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
