use std::fmt;
use std::sync::Arc;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[derive(Clone, Debug, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub struct NodeId {
    name: Arc<str>,
    incarnation: u64,
}

impl NodeId {
    pub fn new(name: impl Into<Arc<str>>, incarnation: u64) -> Self {
        Self { name: name.into(), incarnation }
    }
    pub fn name(&self) -> &str { &self.name }
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
}
