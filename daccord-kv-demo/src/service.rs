//! gRPC `KvService` implementation that wraps a [`KvNode`].

use std::sync::Arc;

use bytes::Bytes;
use tonic::{Request, Response, Status};

use daccord::{NodeAlgorithm, NodeRole};

use crate::error::KvError;
use crate::kv::KvNode;
use crate::kv_proto::kv_service_server::KvService;
use crate::kv_proto::{
    GetRequest, GetResponse, HealthRequest, HealthResponse, PutRequest, PutResponse, StatusRequest,
    StatusResponse,
};

/// Algorithm tag held by the service so `Health` and `Status` can echo it
/// back without re-querying.
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

/// gRPC service wrapping a [`KvNode`]. Cloning is cheap (just bumps `Arc`s)
/// so `tonic` can move it into per-request tasks.
#[derive(Clone)]
pub struct KvServiceImpl {
    node: Arc<KvNode>,
    node_name: String,
    algorithm: Algorithm,
}

impl KvServiceImpl {
    pub fn new(node: Arc<KvNode>, node_name: String, algorithm: Algorithm) -> Self {
        Self {
            node,
            node_name,
            algorithm,
        }
    }
}

#[tonic::async_trait]
impl KvService for KvServiceImpl {
    async fn put(&self, request: Request<PutRequest>) -> Result<Response<PutResponse>, Status> {
        let req = request.into_inner();
        if req.key.is_empty() {
            return Err(Status::invalid_argument("key must be non-empty"));
        }
        let value = Bytes::from(req.value);
        let was_new = self
            .node
            .put(req.key, value)
            .await
            .map_err(kv_error_to_status)?;
        Ok(Response::new(PutResponse { was_new }))
    }

    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let req = request.into_inner();
        if req.key.is_empty() {
            return Err(Status::invalid_argument("key must be non-empty"));
        }
        let value = self.node.get(req.key).await.map_err(kv_error_to_status)?;
        let resp = match value {
            Some(v) => GetResponse {
                found: true,
                value: v.to_vec(),
            },
            None => GetResponse {
                found: false,
                value: Vec::new(),
            },
        };
        Ok(Response::new(resp))
    }

    async fn health(
        &self,
        _request: Request<HealthRequest>,
    ) -> Result<Response<HealthResponse>, Status> {
        Ok(Response::new(HealthResponse {
            node_name: self.node_name.clone(),
            algorithm: self.algorithm.as_str().to_string(),
        }))
    }

    async fn status(
        &self,
        _request: Request<StatusRequest>,
    ) -> Result<Response<StatusResponse>, Status> {
        let s = self.node.status();
        let algorithm = match s.algorithm {
            NodeAlgorithm::Paxos => "paxos",
            NodeAlgorithm::Raft => "raft",
        }
        .to_string();
        let role = match s.role {
            Some(NodeRole::Follower) => "follower",
            Some(NodeRole::Candidate) => "candidate",
            Some(NodeRole::Leader) => "leader",
            None => "n/a",
        }
        .to_string();
        let leader_id = s
            .leader
            .as_ref()
            .map(|id| id.name().to_string())
            .unwrap_or_default();
        Ok(Response::new(StatusResponse {
            node_id: s.node_id.name().to_string(),
            algorithm,
            role,
            term: s.term,
            leader_id,
            log_len: s.log_len,
            commit_index: s.commit_index,
            last_applied: s.last_applied,
        }))
    }
}

fn kv_error_to_status(e: KvError) -> Status {
    match e {
        KvError::Propose(daccord::ProposeError::NotRunning) => {
            Status::unavailable("consensus node not running")
        }
        KvError::Propose(daccord::ProposeError::ChannelFull) => {
            Status::resource_exhausted("propose queue full, retry with backoff")
        }
        KvError::Propose(daccord::ProposeError::Cancelled) => {
            Status::aborted("proposal cancelled by node shutdown")
        }
        KvError::Propose(daccord::ProposeError::Superseded) => {
            Status::aborted("proposal superseded by another leader; retry")
        }
        KvError::Cancelled => Status::aborted("proposal cancelled before result was available"),
        KvError::ApplierGone => Status::internal("kv applier task is no longer running"),
        KvError::Storage(msg) => Status::internal(format!("state-store error: {msg}")),
    }
}
