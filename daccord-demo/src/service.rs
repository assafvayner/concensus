//! gRPC `ConsensusService` implementation backed by a `daccord::NodeHandle`
//! and an in-memory decision log.
//!
//! This module is shared between the production `daccord-node` binary and the
//! `test-support` helpers so test code can spin up the same gRPC surface that
//! the binary exposes — just over channel transport, without touching the
//! network for the consensus messages themselves.

use std::pin::Pin;
use std::sync::Arc;

use tokio::sync::{broadcast, RwLock};
use tokio_stream::Stream;
use tonic::{Request, Response, Status};

use daccord::{NodeAlgorithm as CoreAlgorithm, NodeHandle, NodeRole, NodeState, ProposeError};

use crate::consensus_proto::consensus_service_server::ConsensusService;
use crate::consensus_proto::{
    Decision, GetDecisionsRequest, GetDecisionsResponse, HealthRequest, HealthResponse,
    ProposeRequest, ProposeResponse, StatusRequest, StatusResponse, WatchRequest,
};

/// Default page size for `GetDecisions` when the client does not specify one.
pub const DEFAULT_GET_LIMIT: usize = 1000;

/// Capacity of the decision broadcast channel used to fan out live updates
/// to `Watch` subscribers. Larger values cost a bit of memory but tolerate
/// slow / mid-snapshot consumers without forcing them to reconnect with
/// `ResourceExhausted` ("watch lagged"). 4096 leaves comfortable headroom
/// for the typical demo workload.
pub const DECISION_BROADCAST_CAPACITY: usize = 4096;

/// Which consensus algorithm is being run by the underlying node. Mirrors
/// [`daccord::NodeAlgorithm`] but lives here so the demo can keep its own
/// configuration vocabulary independent of the core crate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Algorithm {
    Paxos,
    Raft,
}

impl Algorithm {
    pub fn as_str(self) -> &'static str {
        match self {
            Algorithm::Paxos => "paxos",
            Algorithm::Raft => "raft",
        }
    }
}

/// Append-only in-memory log of decisions plus a broadcast channel for
/// streaming live updates to `Watch` subscribers.
///
/// Wrapped in `Arc` by callers so the same log can be shared between a
/// `collect_decisions` task and one or more `ConsensusServiceImpl`s (e.g. when
/// the gRPC server is restarted across the same node).
pub struct DecisionLog {
    entries: RwLock<Vec<Decision>>,
    tx: broadcast::Sender<Decision>,
}

impl DecisionLog {
    pub fn new(capacity: usize) -> Self {
        let (tx, _) = broadcast::channel(capacity);
        Self {
            entries: RwLock::new(Vec::new()),
            tx,
        }
    }

    pub async fn append(&self, decision: Decision) {
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

    pub async fn snapshot(&self) -> Vec<Decision> {
        self.entries.read().await.clone()
    }

    pub async fn range(&self, start: usize, limit: usize) -> (Vec<Decision>, u64) {
        let entries = self.entries.read().await;
        if start >= entries.len() {
            return (Vec::new(), start as u64);
        }
        let end = (start + limit).min(entries.len());
        (entries[start..end].to_vec(), end as u64)
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Decision> {
        self.tx.subscribe()
    }
}

/// gRPC service implementation that wires `ConsensusService` to a running
/// `daccord` node and a shared `DecisionLog`.
pub struct ConsensusServiceImpl {
    handle: NodeHandle<Vec<u8>>,
    decisions: Arc<DecisionLog>,
    algorithm: Algorithm,
}

impl ConsensusServiceImpl {
    pub fn new(
        handle: NodeHandle<Vec<u8>>,
        decisions: Arc<DecisionLog>,
        algorithm: Algorithm,
    ) -> Self {
        Self {
            handle,
            decisions,
            algorithm,
        }
    }
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

/// Project a core [`NodeState`] into the gRPC [`StatusResponse`] shape.
pub fn project_status(state: NodeState, configured: Algorithm) -> StatusResponse {
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

/// Forward decisions from a core `DecisionReceiver` into the shared
/// `DecisionLog`. Returns when the decision channel is closed.
pub async fn collect_decisions(
    mut decision_rx: daccord::DecisionReceiver<Vec<u8>>,
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
