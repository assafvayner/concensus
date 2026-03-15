use std::fmt;
use std::sync::Arc;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use crate::transport::MessageSender;

/// A remote peer's identity paired with a sender for delivering messages to it.
///
/// Each peer in the cluster is represented by a `PeerInfo` that the [`Node`](crate::Node)
/// uses to route outgoing Paxos messages.
pub struct PeerInfo<S: MessageSender> {
    /// The peer's unique identity.
    pub id: NodeId,
    /// A transport sender connected to this peer.
    pub sender: S,
}

/// Unique identity of a node in the cluster.
///
/// A `NodeId` consists of a human-readable name (e.g. `"node-1"`) and a numeric
/// incarnation. The incarnation distinguishes restarts of the same named node so
/// that stale messages from a previous process are not confused with the current one.
///
/// `NodeId` is cheaply cloneable (the name is reference-counted) and implements
/// `Ord` — ordering is lexicographic by name first, then by incarnation. This
/// ordering is used internally for proposal number tie-breaking.
///
/// # Display
///
/// Formats as `name/incarnation`, e.g. `"node-1/1710412800"`.
#[derive(Clone, Debug, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub struct NodeId {
    name: Arc<str>,
    incarnation: u64,
}

impl NodeId {
    /// Creates a new `NodeId` with the given name and incarnation number.
    pub fn new(name: impl Into<Arc<str>>, incarnation: u64) -> Self {
        Self { name: name.into(), incarnation }
    }

    /// Returns the human-readable name portion of this node identity.
    pub fn name(&self) -> &str { &self.name }

    /// Returns the incarnation number, typically a UNIX timestamp set at node startup.
    pub fn incarnation(&self) -> u64 { self.incarnation }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.name, self.incarnation)
    }
}

// Manual serde using a helper struct with String instead of Arc<str>
#[derive(Serialize, Deserialize)]
struct NodeIdHelper {
    name: String,
    incarnation: u64,
}

impl Serialize for NodeId {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        NodeIdHelper {
            name: self.name.to_string(),
            incarnation: self.incarnation,
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for NodeId {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let helper = NodeIdHelper::deserialize(deserializer)?;
        Ok(NodeId::new(helper.name.as_str(), helper.incarnation))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_id_from_string() {
        let id = NodeId::new("node-1", 1000);
        assert_eq!(id.name(), "node-1");
        assert_eq!(id.incarnation(), 1000);
    }

    #[test]
    fn node_id_display() {
        let id = NodeId::new("node-1", 1710412800);
        assert_eq!(id.to_string(), "node-1/1710412800");
    }

    #[test]
    fn node_id_clone_is_cheap() {
        let id = NodeId::new("node-1", 1000);
        let cloned = id.clone();
        assert_eq!(id, cloned);
    }

    #[test]
    fn node_id_ordering() {
        let a = NodeId::new("a", 100);
        let b = NodeId::new("b", 50);
        let a2 = NodeId::new("a", 200);
        assert!(a < b);
        assert!(a < a2);
    }

    #[test]
    fn node_id_serde_roundtrip() {
        let id = NodeId::new("node-1", 1710412800);
        let json = serde_json::to_string(&id).unwrap();
        let deserialized: NodeId = serde_json::from_str(&json).unwrap();
        assert_eq!(id, deserialized);
    }

    #[test]
    fn node_id_hash_consistent() {
        use std::collections::HashSet;
        let id1 = NodeId::new("node-1", 1000);
        let id2 = NodeId::new("node-1", 1000);
        let mut set = HashSet::new();
        set.insert(id1);
        assert!(set.contains(&id2));
    }

    #[test]
    fn peer_info_holds_sender() {
        use crate::transport::MessageSender;
        use crate::error::TransportError;
        use bytes::Bytes;

        struct DummySender;
        #[async_trait::async_trait]
        impl MessageSender for DummySender {
            async fn send(&self, _data: Bytes) -> Result<(), TransportError> { Ok(()) }
        }

        let _info = PeerInfo {
            id: NodeId::new("peer-1", 1000),
            sender: DummySender,
        };
    }
}
