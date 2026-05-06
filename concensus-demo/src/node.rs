use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use tokio::sync::RwLock;
use tonic::{transport::Server, Request, Response, Status};

use concensus::{
    DecisionReceiver, Node, NodeHandle, NodeId, PaxosMemoryStorage, ProposeError, RaftConfig,
    RaftMemoryStorage, TcpTransport, UdsTransport,
};

pub mod consensus_proto {
    tonic::include_proto!("consensus");
}

use consensus_proto::consensus_service_server::{ConsensusService, ConsensusServiceServer};
use consensus_proto::{
    Decision, GetDecisionsRequest, GetDecisionsResponse, HealthRequest, HealthResponse,
    ProposeRequest, ProposeResponse, StatusRequest, StatusResponse,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Algorithm {
    Paxos,
    Raft,
}

impl Algorithm {
    fn as_str(self) -> &'static str {
        match self {
            Algorithm::Paxos => "paxos",
            Algorithm::Raft => "raft",
        }
    }
}

#[derive(Debug)]
struct Config {
    node_name: String,
    transport: Transport,
    grpc_port: u16,
    algorithm: Algorithm,
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

    Config {
        node_name,
        transport,
        grpc_port,
        algorithm,
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

// ─── gRPC Service ────────────────────────────────────────────────────────────

type DecisionList = Arc<RwLock<Vec<Decision>>>;

struct ConsensusServiceImpl {
    handle: NodeHandle<String>,
    decisions: DecisionList,
    node_id: String,
    algorithm: Algorithm,
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
            Err(ProposeError::NotRunning) => Err(Status::unavailable("node is not running")),
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

    async fn status(
        &self,
        _request: Request<StatusRequest>,
    ) -> Result<Response<StatusResponse>, Status> {
        Ok(Response::new(StatusResponse {
            node_id: self.node_id.clone(),
            algorithm: self.algorithm.as_str().to_string(),
            role: "N/A".into(),
            term: 0,
            leader: String::new(),
            log_len: 0,
            has_commit_index: false,
            commit_index: 0,
            has_last_applied: false,
            last_applied: 0,
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
    algorithm: Algorithm,
    bind_addr: SocketAddr,
    peers: Vec<(NodeId, String)>,
) -> (NodeHandle<String>, DecisionReceiver<String>) {
    let peers = resolve_tcp_peers(peers).await;
    let (peer_infos, receiver) = TcpTransport::create(bind_addr, peers)
        .await
        .expect("failed to bind TCP transport");

    let node_id = NodeId::new(node_name, 0);
    let (node, handle, decision_rx) = match algorithm {
        Algorithm::Paxos => Node::with_id(
            node_id,
            peer_infos,
            receiver,
            PaxosMemoryStorage::<String>::new(),
        ),
        Algorithm::Raft => Node::with_raft_config_and_id(
            node_id,
            RaftConfig::default(),
            peer_infos,
            receiver,
            RaftMemoryStorage::<String>::new(),
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
) -> (NodeHandle<String>, DecisionReceiver<String>) {
    let (peer_infos, receiver) = UdsTransport::create(bind_path, peers)
        .await
        .expect("failed to bind UDS transport");

    let node_id = NodeId::new(node_name, 0);
    let (node, handle, decision_rx) = match algorithm {
        Algorithm::Paxos => Node::with_id(
            node_id,
            peer_infos,
            receiver,
            PaxosMemoryStorage::<String>::new(),
        ),
        Algorithm::Raft => Node::with_raft_config_and_id(
            node_id,
            RaftConfig::default(),
            peer_infos,
            receiver,
            RaftMemoryStorage::<String>::new(),
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

    let (handle, decision_rx) = match config.transport {
        Transport::Tcp { bind_addr, peers } => {
            tracing::info!(
                node_name = %config.node_name,
                transport = "tcp",
                bind = %bind_addr,
                grpc_port = config.grpc_port,
                algorithm = %config.algorithm.as_str(),
                "node started"
            );
            start_node_tcp(&config.node_name, config.algorithm, bind_addr, peers).await
        }
        Transport::Uds { bind_path, peers } => {
            tracing::info!(
                node_name = %config.node_name,
                transport = "uds",
                bind = ?bind_path,
                grpc_port = config.grpc_port,
                algorithm = %config.algorithm.as_str(),
                "node started"
            );
            start_node_uds(&config.node_name, config.algorithm, bind_path, peers).await
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
        node_id: config.node_name.clone(),
        algorithm: config.algorithm,
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
