//! Helpers shared between the demo binaries and integration tests.

pub mod consensus_proto {
    tonic::include_proto!("daccord.v1");
}

pub mod service;

#[cfg(feature = "test-support")]
pub mod test_support;

pub use consensus_proto::consensus_service_client::ConsensusServiceClient;
pub use consensus_proto::consensus_service_server::{ConsensusService, ConsensusServiceServer};
pub use consensus_proto::{
    Decision, GetDecisionsRequest, GetDecisionsResponse, HealthRequest, HealthResponse,
    ProposeRequest, ProposeResponse, StatusRequest, StatusResponse, WatchRequest,
};
pub use service::{Algorithm, ConsensusServiceImpl, DecisionLog, DECISION_BROADCAST_CAPACITY};
