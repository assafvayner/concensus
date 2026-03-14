use thiserror::Error;

#[derive(Error, Debug)]
pub enum ProposeError {
    #[error("node is not running")]
    NotRunning,
    #[error("proposal channel full")]
    ChannelFull,
}

#[derive(Error, Debug)]
pub enum NodeError {
    #[error("all peers disconnected, cannot form quorum")]
    NoQuorum,
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),
}

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("failed to persist decision: {0}")]
    Persist(String),
    #[error("failed to load decisions: {0}")]
    Load(String),
}

#[derive(Error, Debug)]
pub enum TransportError {
    #[error("connection closed")]
    Closed,
    #[error("transport error: {0}")]
    Other(Box<dyn std::error::Error + Send + Sync>),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn propose_error_display() {
        assert_eq!(ProposeError::NotRunning.to_string(), "node is not running");
        assert_eq!(ProposeError::ChannelFull.to_string(), "proposal channel full");
    }

    #[test]
    fn node_error_display() {
        assert_eq!(NodeError::NoQuorum.to_string(), "all peers disconnected, cannot form quorum");
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
        let other = TransportError::Other(Box::new(std::io::Error::new(std::io::ErrorKind::Other, "boom")));
        assert_eq!(other.to_string(), "transport error: boom");
    }

    #[test]
    fn node_error_from_storage_error() {
        let storage_err = StorageError::Load("corrupt".into());
        let node_err: NodeError = storage_err.into();
        assert!(matches!(node_err, NodeError::Storage(_)));
    }
}
