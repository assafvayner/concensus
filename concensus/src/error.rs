use thiserror::Error;

/// Errors returned by [`NodeHandle::propose`](crate::NodeHandle::propose).
#[derive(Error, Debug)]
pub enum ProposeError {
    /// The node's event loop has stopped (all handles dropped or fatal error).
    #[error("node is not running")]
    NotRunning,
    /// The internal proposal queue is full. Back off and retry.
    #[error("proposal channel full")]
    ChannelFull,
}

/// Errors returned by [`Node::run`](crate::Node::run).
#[derive(Error, Debug)]
pub enum NodeError {
    /// The transport receiver closed and the cluster cannot form a quorum,
    /// making further progress impossible.
    #[error("all peers disconnected, cannot form quorum")]
    NoQuorum,
    /// A storage operation failed during decision persistence or recovery.
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),
}

/// Errors from the [`Storage`](crate::Storage) trait.
#[derive(Error, Debug)]
pub enum StorageError {
    /// Failed to persist a decided value.
    #[error("failed to persist decision: {0}")]
    Persist(String),
    /// Failed to load previously decided values during recovery.
    #[error("failed to load decisions: {0}")]
    Load(String),
    /// Failed to delete acceptor state.
    #[error("failed to delete acceptor state: {0}")]
    Delete(String),
}

/// Errors from the [`MessageSender`](crate::MessageSender) and
/// [`MessageReceiver`](crate::MessageReceiver) traits.
#[derive(Error, Debug)]
pub enum TransportError {
    /// The underlying connection or channel has been closed.
    #[error("connection closed")]
    Closed,
    /// A transport-specific error (I/O failure, bind error, etc.).
    #[error("transport error: {0}")]
    Other(Box<dyn std::error::Error + Send + Sync>),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn propose_error_display() {
        assert_eq!(ProposeError::NotRunning.to_string(), "node is not running");
        assert_eq!(
            ProposeError::ChannelFull.to_string(),
            "proposal channel full"
        );
    }

    #[test]
    fn node_error_display() {
        assert_eq!(
            NodeError::NoQuorum.to_string(),
            "all peers disconnected, cannot form quorum"
        );
    }

    #[test]
    fn storage_error_display() {
        let persist = StorageError::Persist("disk full".into());
        assert_eq!(persist.to_string(), "failed to persist decision: disk full");
        let load = StorageError::Load("corrupt".into());
        assert_eq!(load.to_string(), "failed to load decisions: corrupt");
    }

    #[test]
    fn transport_error_display() {
        assert_eq!(TransportError::Closed.to_string(), "connection closed");
        let other = TransportError::Other(Box::new(std::io::Error::new(
            std::io::ErrorKind::Other,
            "boom",
        )));
        assert_eq!(other.to_string(), "transport error: boom");
    }

    #[test]
    fn node_error_from_storage_error() {
        let storage_err = StorageError::Load("corrupt".into());
        let node_err: NodeError = storage_err.into();
        assert!(matches!(node_err, NodeError::Storage(_)));
    }
}
