use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;

use tokio::sync::{broadcast, RwLock};
use tokio_stream::Stream;
use tonic::{transport::Server, Request, Response, Status};

use daccord::{
    DecisionReceiver, Node, NodeAlgorithm as CoreAlgorithm, NodeHandle, NodeId, NodeRole,
    NodeState, PaxosConfig, PaxosMemoryStorage, ProposeError, RaftConfig, RaftMemoryStorage,
    TcpTransport, UdsTransport,
};
#[cfg(feature = "duckdb-bundled")]
use daccord::{DuckdbPaxosStorage, DuckdbRaftStorage};

pub mod consensus_proto {
    tonic::include_proto!("daccord.v1");
}

use consensus_proto::consensus_service_server::{ConsensusService, ConsensusServiceServer};
use consensus_proto::{
    Decision, GetDecisionsRequest, GetDecisionsResponse, HealthRequest, HealthResponse,
    ProposeRequest, ProposeResponse, StatusRequest, StatusResponse, WatchRequest,
};

const DEFAULT_GET_LIMIT: usize = 1000;
/// Capacity of the decision broadcast channel used to fan out live updates
/// to `Watch` subscribers. Larger values cost a bit of memory but tolerate
/// slow / mid-snapshot consumers without forcing them to reconnect with
/// `ResourceExhausted` ("watch lagged"). 4096 leaves comfortable headroom
/// for the typical demo workload.
const DECISION_BROADCAST_CAPACITY: usize = 4096;

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

// ─── Decision log (snapshot + broadcast) ─────────────────────────────────────

/// Append-only in-memory log of decisions plus a broadcast channel for
/// streaming live updates to `Watch` subscribers.
struct DecisionLog {
    entries: RwLock<Vec<Decision>>,
    tx: broadcast::Sender<Decision>,
}

impl DecisionLog {
    fn new(capacity: usize) -> Self {
        let (tx, _) = broadcast::channel(capacity);
        Self {
            entries: RwLock::new(Vec::new()),
            tx,
        }
    }

    async fn append(&self, decision: Decision) {
        let mut entries = self.entries.write().await;
        // Core's `DecisionReceiver` is at-least-once: after a crash + recovery,
        // `restore_state` may re-deliver decisions for slots already appended,
        // and multi-Paxos may decide multiple slots concurrently and emit them
        // out of slot order on the broadcast channel. Sorted-insert with
        // dedup-by-slot handles both cases. The cluster invariant guarantees
        // any duplicate slot carries an identical value.
        match entries.binary_search_by_key(&decision.slot, |d| d.slot) {
            Ok(_) => {
                // Already have this slot; skip the broadcast too so live
                // subscribers don't see redelivered duplicates.
                return;
            }
            Err(pos) => {
                entries.insert(pos, decision.clone());
            }
        }
        // Drop the write lock before sending so a slow subscriber can't pin
        // the log under the write lock.
        drop(entries);
        // Errors only when no receivers are subscribed; that's fine. Watch
        // consumers may receive decisions out of slot order — they dedupe and
        // sort on their side.
        let _ = self.tx.send(decision);
    }

    async fn snapshot(&self) -> Vec<Decision> {
        self.entries.read().await.clone()
    }

    async fn range(&self, start: usize, limit: usize) -> (Vec<Decision>, u64) {
        let entries = self.entries.read().await;
        if start >= entries.len() {
            return (Vec::new(), start as u64);
        }
        let end = (start + limit).min(entries.len());
        (entries[start..end].to_vec(), end as u64)
    }

    fn subscribe(&self) -> broadcast::Receiver<Decision> {
        self.tx.subscribe()
    }
}

// ─── gRPC Service ────────────────────────────────────────────────────────────

struct ConsensusServiceImpl {
    handle: NodeHandle<Vec<u8>>,
    decisions: Arc<DecisionLog>,
    algorithm: Algorithm,
}

type DecisionStream = Pin<Box<dyn Stream<Item = Result<Decision, Status>> + Send + 'static>>;

#[tonic::async_trait]
impl ConsensusService for ConsensusServiceImpl {
    async fn propose(
        &self,
        request: Request<ProposeRequest>,
    ) -> Result<Response<ProposeResponse>, Status> {
        let payload = request.into_inner().payload;
        tracing::info!(payload_len = payload.len(), "gRPC propose request");

        match self.handle.propose(payload).await {
            Ok(decided) => Ok(Response::new(ProposeResponse {
                slot: decided.slot,
                payload: decided.value,
            })),
            Err(ProposeError::ChannelFull) => {
                Err(Status::resource_exhausted("proposal channel full"))
            }
            Err(ProposeError::NotRunning) => Err(Status::unavailable("node is not running")),
            Err(ProposeError::Cancelled) => Err(Status::unavailable(
                "node was shut down before proposal was decided",
            )),
            Err(ProposeError::Superseded) => {
                Err(Status::aborted("proposal was superseded by another leader"))
            }
        }
    }

    type WatchStream = DecisionStream;

    async fn watch(
        &self,
        request: Request<WatchRequest>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        let start_index = request.into_inner().start_index;
        let log = self.decisions.clone();

        let stream = async_stream::stream! {
            // Subscribe BEFORE snapshotting, so we don't miss decisions appended
            // between the snapshot read and the live tail subscription.
            let mut rx = log.subscribe();
            let snapshot = log.snapshot().await;

            // Track the last slot already yielded from the snapshot to filter
            // out duplicates that the broadcast may also deliver.
            let mut last_yielded: Option<u64> = None;

            for d in snapshot.into_iter() {
                if d.slot >= start_index {
                    last_yielded = Some(d.slot);
                    yield Ok(d);
                }
            }

            loop {
                match rx.recv().await {
                    Ok(d) => {
                        if d.slot < start_index {
                            continue;
                        }
                        if let Some(last) = last_yielded {
                            if d.slot <= last {
                                continue;
                            }
                        }
                        last_yielded = Some(d.slot);
                        yield Ok(d);
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        yield Err(Status::resource_exhausted(
                            "watch lagged behind, reconnect with start_index = last_seen + 1",
                        ));
                        break;
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        break;
                    }
                }
            }
        };

        Ok(Response::new(Box::pin(stream) as Self::WatchStream))
    }

    async fn get_decisions(
        &self,
        request: Request<GetDecisionsRequest>,
    ) -> Result<Response<GetDecisionsResponse>, Status> {
        let req = request.into_inner();
        let start_index: usize = req.start_index.try_into().unwrap_or(usize::MAX);
        let limit = req
            .limit
            .filter(|n| *n != 0)
            .map(|n| n as usize)
            .unwrap_or(DEFAULT_GET_LIMIT);

        let (decisions, next_index) = self.decisions.range(start_index, limit).await;

        Ok(Response::new(GetDecisionsResponse {
            decisions,
            next_index,
        }))
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
        let state: NodeState = self.handle.status();
        Ok(Response::new(project_status(state, self.algorithm)))
    }
}

fn project_status(state: NodeState, configured: Algorithm) -> StatusResponse {
    let algorithm = match state.algorithm {
        CoreAlgorithm::Paxos => "paxos",
        CoreAlgorithm::Raft => "raft",
    };
    // Sanity: configured and reported algorithms must agree; if not, we trust
    // the core's report and just log.
    if (configured == Algorithm::Paxos && state.algorithm != CoreAlgorithm::Paxos)
        || (configured == Algorithm::Raft && state.algorithm != CoreAlgorithm::Raft)
    {
        tracing::warn!(
            configured = configured.as_str(),
            reported = algorithm,
            "configured algorithm differs from core node state"
        );
    }

    let role = match state.role {
        Some(NodeRole::Follower) => "follower",
        Some(NodeRole::Candidate) => "candidate",
        Some(NodeRole::Leader) => "leader",
        None => "n/a",
    };

    StatusResponse {
        node_id: state.node_id.to_string(),
        algorithm: algorithm.to_string(),
        role: role.to_string(),
        term: state.term,
        leader_id: state
            .leader
            .as_ref()
            .map(|n| n.to_string())
            .unwrap_or_default(),
        log_len: state.log_len,
        commit_index: state.commit_index,
        last_applied: state.last_applied,
    }
}

// ─── Decision collector ──────────────────────────────────────────────────────

async fn collect_decisions(
    mut decision_rx: DecisionReceiver<Vec<u8>>,
    decisions: Arc<DecisionLog>,
    node_name: String,
) {
    while let Some(decided) = decision_rx.recv().await {
        tracing::info!(
            node_name = %node_name,
            slot = decided.slot,
            payload_len = decided.value.len(),
            "decision reached"
        );
        decisions
            .append(Decision {
                slot: decided.slot,
                payload: decided.value,
            })
            .await;
    }
    tracing::info!("decision channel closed");
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

    let node_id = NodeId::new(node_name, 0);
    let (node, handle, decision_rx) = match (algorithm, data_dir) {
        #[cfg(feature = "duckdb-bundled")]
        (Algorithm::Paxos, Some(dir)) => Node::paxos_with_id(
            node_id,
            PaxosConfig::default(),
            peer_infos,
            receiver,
            DuckdbPaxosStorage::<Vec<u8>>::open(dir.join("paxos.db"))
                .expect("failed to open DuckDB Paxos storage"),
        ),
        #[cfg(feature = "duckdb-bundled")]
        (Algorithm::Raft, Some(dir)) => Node::raft_with_id(
            node_id,
            RaftConfig::default(),
            peer_infos,
            receiver,
            DuckdbRaftStorage::<Vec<u8>>::open(dir.join("raft.db"))
                .expect("failed to open DuckDB Raft storage"),
        ),
        (Algorithm::Paxos, _) => Node::paxos_with_id(
            node_id,
            PaxosConfig::default(),
            peer_infos,
            receiver,
            PaxosMemoryStorage::<Vec<u8>>::new(),
        ),
        (Algorithm::Raft, _) => Node::raft_with_id(
            node_id,
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

    let node_id = NodeId::new(node_name, 0);
    let (node, handle, decision_rx) = match (algorithm, data_dir) {
        #[cfg(feature = "duckdb-bundled")]
        (Algorithm::Paxos, Some(dir)) => Node::paxos_with_id(
            node_id,
            PaxosConfig::default(),
            peer_infos,
            receiver,
            DuckdbPaxosStorage::<Vec<u8>>::open(dir.join("paxos.db"))
                .expect("failed to open DuckDB Paxos storage"),
        ),
        #[cfg(feature = "duckdb-bundled")]
        (Algorithm::Raft, Some(dir)) => Node::raft_with_id(
            node_id,
            RaftConfig::default(),
            peer_infos,
            receiver,
            DuckdbRaftStorage::<Vec<u8>>::open(dir.join("raft.db"))
                .expect("failed to open DuckDB Raft storage"),
        ),
        (Algorithm::Paxos, _) => Node::paxos_with_id(
            node_id,
            PaxosConfig::default(),
            peer_infos,
            receiver,
            PaxosMemoryStorage::<Vec<u8>>::new(),
        ),
        (Algorithm::Raft, _) => Node::raft_with_id(
            node_id,
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

    let service = ConsensusServiceImpl {
        handle,
        decisions,
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
