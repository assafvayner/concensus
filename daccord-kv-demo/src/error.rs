//! Error type for the KV demo crate.

use thiserror::Error;

/// Errors returned by [`KvNode::put`](crate::KvNode::put) and
/// [`KvNode::get`](crate::KvNode::get).
#[derive(Debug, Error)]
pub enum KvError {
    /// The underlying consensus proposal failed.
    #[error("consensus proposal failed: {0}")]
    Propose(#[from] daccord::ProposeError),

    /// The node was shut down before the operation's result was available.
    #[error("node shut down before response was available")]
    Cancelled,

    /// The applier task is no longer running. Subsequent operations will
    /// also fail with this error until the node is restarted.
    #[error("internal: applier task is no longer running")]
    ApplierGone,

    /// A state-store error (redb).
    #[error("state-store error: {0}")]
    Storage(String),
}

impl From<redb::Error> for KvError {
    fn from(e: redb::Error) -> Self {
        KvError::Storage(e.to_string())
    }
}

impl From<redb::DatabaseError> for KvError {
    fn from(e: redb::DatabaseError) -> Self {
        KvError::Storage(e.to_string())
    }
}

impl From<redb::TransactionError> for KvError {
    fn from(e: redb::TransactionError) -> Self {
        KvError::Storage(e.to_string())
    }
}

impl From<redb::TableError> for KvError {
    fn from(e: redb::TableError) -> Self {
        KvError::Storage(e.to_string())
    }
}

impl From<redb::StorageError> for KvError {
    fn from(e: redb::StorageError) -> Self {
        KvError::Storage(e.to_string())
    }
}

impl From<redb::CommitError> for KvError {
    fn from(e: redb::CommitError) -> Self {
        KvError::Storage(e.to_string())
    }
}

pub type KvResult<T> = Result<T, KvError>;
