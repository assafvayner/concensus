use bytes::Bytes;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::config::NodeId;

pub(crate) type ProposalNumber = (u64, NodeId);

#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) enum MessageVariant<V> {
    Prepare {
        slot: u64,
        proposal_number: ProposalNumber,
    },
    Promise {
        slot: u64,
        proposal_number: ProposalNumber,
        accepted: Option<(ProposalNumber, V)>,
    },
    Accept {
        slot: u64,
        proposal_number: ProposalNumber,
        value: V,
    },
    Accepted {
        slot: u64,
        proposal_number: ProposalNumber,
        value: V,
    },
    Decide {
        slot: u64,
        value: V,
    },
    NackPrepare {
        slot: u64,
        proposal_number: ProposalNumber,
        highest_promised: ProposalNumber,
    },
    NackAccept {
        slot: u64,
        proposal_number: ProposalNumber,
        highest_promised: ProposalNumber,
    },
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) struct Message<V> {
    pub sender: NodeId,
    pub variant: MessageVariant<V>,
}

impl<V> Message<V>
where
    V: Serialize + DeserializeOwned,
{
    pub(crate) fn to_bytes(&self) -> Result<Bytes, serde_json::Error> {
        let json = serde_json::to_vec(self)?;
        Ok(Bytes::from(json))
    }

    pub(crate) fn from_bytes(data: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::NodeId;

    fn test_node_id() -> NodeId {
        NodeId::new("node-1", 1000)
    }

    #[test]
    fn proposal_number_ordering() {
        let n1 = test_node_id();
        let n2 = NodeId::new("node-2", 1000);

        let pn1: ProposalNumber = (1, n1.clone());
        let pn2: ProposalNumber = (2, n1.clone());
        let pn3: ProposalNumber = (1, n2.clone());

        // Round takes priority
        assert!(pn1 < pn2);
        // Same round, compare by node id
        assert!(pn1 < pn3);
    }

    #[test]
    fn prepare_message_serde_roundtrip() {
        let msg: MessageVariant<String> = MessageVariant::Prepare {
            slot: 0,
            proposal_number: (1, test_node_id()),
        };
        let json = serde_json::to_string(&msg).unwrap();
        let deserialized: MessageVariant<String> = serde_json::from_str(&json).unwrap();
        assert!(matches!(deserialized, MessageVariant::Prepare { slot: 0, .. }));
    }

    #[test]
    fn promise_with_accepted_value_serde_roundtrip() {
        let msg: MessageVariant<String> = MessageVariant::Promise {
            slot: 0,
            proposal_number: (1, test_node_id()),
            accepted: Some(((0, test_node_id()), "hello".to_string())),
        };
        let json = serde_json::to_string(&msg).unwrap();
        let deserialized: MessageVariant<String> = serde_json::from_str(&json).unwrap();
        match deserialized {
            MessageVariant::Promise { accepted: Some((_, val)), .. } => assert_eq!(val, "hello"),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn decide_message_serde_roundtrip() {
        let msg = MessageVariant::Decide { slot: 5, value: 42u64 };
        let json = serde_json::to_string(&msg).unwrap();
        let deserialized: MessageVariant<u64> = serde_json::from_str(&json).unwrap();
        match deserialized {
            MessageVariant::Decide { slot, value } => {
                assert_eq!(slot, 5);
                assert_eq!(value, 42);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn nack_prepare_serde_roundtrip() {
        let msg: MessageVariant<String> = MessageVariant::NackPrepare {
            slot: 0,
            proposal_number: (1, test_node_id()),
            highest_promised: (2, test_node_id()),
        };
        let json = serde_json::to_string(&msg).unwrap();
        let deserialized: MessageVariant<String> = serde_json::from_str(&json).unwrap();
        assert!(matches!(deserialized, MessageVariant::NackPrepare { .. }));
    }

    #[test]
    fn nack_accept_serde_roundtrip() {
        let msg: MessageVariant<String> = MessageVariant::NackAccept {
            slot: 0,
            proposal_number: (1, test_node_id()),
            highest_promised: (2, test_node_id()),
        };
        let json = serde_json::to_string(&msg).unwrap();
        let deserialized: MessageVariant<String> = serde_json::from_str(&json).unwrap();
        assert!(matches!(deserialized, MessageVariant::NackAccept { .. }));
    }

    #[test]
    fn message_to_bytes_and_back() {
        let variant = MessageVariant::Accept {
            slot: 3,
            proposal_number: (1, test_node_id()),
            value: "test".to_string(),
        };
        let msg = Message { sender: test_node_id(), variant };
        let bytes = msg.to_bytes().unwrap();
        let decoded: Message<String> = Message::from_bytes(&bytes).unwrap();
        match decoded.variant {
            MessageVariant::Accept { slot, value, .. } => {
                assert_eq!(slot, 3);
                assert_eq!(value, "test");
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn message_wrapper_roundtrip() {
        let sender = test_node_id();
        let variant = MessageVariant::Prepare {
            slot: 7,
            proposal_number: (2, test_node_id()),
        };
        let msg: Message<String> = Message { sender: sender.clone(), variant };
        let bytes = msg.to_bytes().unwrap();
        let decoded: Message<String> = Message::from_bytes(&bytes).unwrap();
        assert_eq!(decoded.sender, sender);
        assert!(matches!(decoded.variant, MessageVariant::Prepare { slot: 7, .. }));
    }
}
