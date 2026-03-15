use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use tokio::sync::RwLock;
use tonic::{transport::Server, Request, Response, Status};

use concensus::{
    DecisionReceiver, MemoryStorage, Node, NodeHandle, NodeId,
    TcpTransport,
    UdsTransport,
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
