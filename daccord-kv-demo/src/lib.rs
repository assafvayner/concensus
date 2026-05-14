//! Replicated, linearizable key-value store demo built on top of the
//! [`daccord`] consensus library.
//!
//! Both `put` and `get` are submitted as [`KvOp`] values into the consensus
//! log so every node applies the same totally-ordered sequence of operations
//! to a local redb-backed state machine. See `docs/superpowers/specs/` for
//! the full design.

pub mod error;
pub mod kv;
pub mod service;
pub mod state;

pub mod kv_proto {
    tonic::include_proto!("daccord.kv.v1");
}

pub use error::{KvError, KvResult};
pub use kv::{KvNode, KvOp, KvOpKind, KvOpResult};
pub use service::{Algorithm, KvServiceImpl};
pub use state::{ApplyOutcome, KvState};

pub use kv_proto::kv_service_client::KvServiceClient;
pub use kv_proto::kv_service_server::KvServiceServer;
