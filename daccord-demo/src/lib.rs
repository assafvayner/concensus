//! Helpers shared between the demo binaries and the chaos integration test.

pub mod consensus_proto {
    tonic::include_proto!("daccord.v1");
}

pub use consensus_proto::consensus_service_client::ConsensusServiceClient;
pub use consensus_proto::{
    Decision, GetDecisionsRequest, GetDecisionsResponse, HealthRequest, HealthResponse,
    ProposeRequest, ProposeResponse, StatusRequest, StatusResponse, WatchRequest,
};
