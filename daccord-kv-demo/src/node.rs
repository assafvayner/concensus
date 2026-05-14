//! `daccord-kv-node` — the replicated KV server binary.
//!
//! Boots a daccord node with redb-backed storage, wraps it in a [`KvNode`],
//! and exposes the KV API over gRPC.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tonic::transport::Server;

use daccord::{
    DecisionReceiver, Node, NodeHandle, NodeId, PaxosConfig, RaftConfig, RedbPaxosStorage,
    RedbRaftStorage, TcpTransport, UdsTransport,
};
use daccord_kv_demo::kv::{KvNode, KvOp};
use daccord_kv_demo::kv_proto::kv_service_server::KvServiceServer;
use daccord_kv_demo::service::{Algorithm, KvServiceImpl};

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
    data_dir: PathBuf,
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
    let data_dir: PathBuf = std::env::var("DATA_DIR")
        .expect("DATA_DIR env var required")
        .into();

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

    Config {
        node_name,
        transport,
        grpc_port,
        algorithm: resolve_algorithm(),
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

async fn start_node_tcp(
    node_name: &str,
    algorithm: Algorithm,
    bind_addr: SocketAddr,
    peers: Vec<(NodeId, String)>,
    data_dir: &Path,
) -> (NodeHandle<KvOp>, DecisionReceiver<KvOp>) {
    let peers = resolve_tcp_peers(peers).await;
    let (peer_infos, receiver) = TcpTransport::create(bind_addr, peers)
        .await
        .expect("failed to bind TCP transport");

    let consensus_path = data_dir.join("consensus.redb");
    let (node, handle, decision_rx) = match algorithm {
        Algorithm::Paxos => Node::paxos_with_id(
            NodeId::new(node_name, 0),
            PaxosConfig::default(),
            peer_infos,
            receiver,
            RedbPaxosStorage::<KvOp>::open(&consensus_path)
                .expect("failed to open redb Paxos storage"),
        ),
        Algorithm::Raft => Node::raft_with_id(
            NodeId::new(node_name, 0),
            RaftConfig::default(),
            peer_infos,
            receiver,
            RedbRaftStorage::<KvOp>::open(&consensus_path)
                .expect("failed to open redb Raft storage"),
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
    data_dir: &Path,
) -> (NodeHandle<KvOp>, DecisionReceiver<KvOp>) {
    let (peer_infos, receiver) = UdsTransport::create(bind_path, peers)
        .await
        .expect("failed to bind UDS transport");

    let consensus_path = data_dir.join("consensus.redb");
    let (node, handle, decision_rx) = match algorithm {
        Algorithm::Paxos => Node::paxos_with_id(
            NodeId::new(node_name, 0),
            PaxosConfig::default(),
            peer_infos,
            receiver,
            RedbPaxosStorage::<KvOp>::open(&consensus_path)
                .expect("failed to open redb Paxos storage"),
        ),
        Algorithm::Raft => Node::raft_with_id(
            NodeId::new(node_name, 0),
            RaftConfig::default(),
            peer_infos,
            receiver,
            RedbRaftStorage::<KvOp>::open(&consensus_path)
                .expect("failed to open redb Raft storage"),
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

    std::fs::create_dir_all(&config.data_dir)
        .unwrap_or_else(|e| panic!("failed to create DATA_DIR {:?}: {}", config.data_dir, e));

    tracing::info!(
        node_name = %config.node_name,
        algorithm = config.algorithm.as_str(),
        data_dir = %config.data_dir.display(),
        grpc_port = config.grpc_port,
        "daccord-kv-node starting"
    );

    let (handle, decisions) = match config.transport {
        Transport::Tcp { bind_addr, peers } => {
            tracing::info!(transport = "tcp", bind = %bind_addr, "transport configured");
            start_node_tcp(
                &config.node_name,
                config.algorithm,
                bind_addr,
                peers,
                &config.data_dir,
            )
            .await
        }
        Transport::Uds { bind_path, peers } => {
            tracing::info!(transport = "uds", bind = ?bind_path, "transport configured");
            start_node_uds(
                &config.node_name,
                config.algorithm,
                bind_path,
                peers,
                &config.data_dir,
            )
            .await
        }
    };

    let state_path = config.data_dir.join("kv-state.redb");
    let kv_node = KvNode::spawn(handle, decisions, &state_path)
        .unwrap_or_else(|e| panic!("failed to open KV state at {:?}: {}", state_path, e));

    let grpc_addr: SocketAddr = format!("0.0.0.0:{}", config.grpc_port)
        .parse()
        .expect("invalid gRPC address");
    let service = KvServiceImpl::new(
        Arc::new(kv_node),
        config.node_name.clone(),
        config.algorithm,
    );

    tracing::info!(addr = %grpc_addr, "gRPC server starting");
    Server::builder()
        .add_service(KvServiceServer::new(service))
        .serve_with_shutdown(grpc_addr, async {
            tokio::signal::ctrl_c().await.ok();
            tracing::info!("shutdown signal received");
        })
        .await
        .expect("gRPC server error");

    tracing::info!("daccord-kv-node shut down");
}
