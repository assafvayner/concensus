pub(crate) mod paxos;

use bytes::Bytes;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::config::NodeId;

pub(crate) use paxos::{PaxosMessage, ProposalNumber};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) enum WireVariant<V> {
    Paxos(PaxosMessage<V>),
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) struct Message<V> {
    pub sender: NodeId,
    pub variant: WireVariant<V>,
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
    fn message_to_bytes_and_back() {
        let variant = WireVariant::Paxos(PaxosMessage::Accept {
            slot: 3,
            proposal_number: (1, test_node_id()),
            value: "test".to_string(),
        });
        let msg = Message {
            sender: test_node_id(),
            variant,
        };
        let bytes = msg.to_bytes().unwrap();
        let decoded: Message<String> = Message::from_bytes(&bytes).unwrap();
        match decoded.variant {
            WireVariant::Paxos(PaxosMessage::Accept { slot, value, .. }) => {
                assert_eq!(slot, 3);
                assert_eq!(value, "test");
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn message_wrapper_roundtrip() {
        let sender = test_node_id();
        let variant = WireVariant::Paxos(PaxosMessage::Prepare {
            slot: 7,
            proposal_number: (2, test_node_id()),
        });
        let msg: Message<String> = Message {
            sender: sender.clone(),
            variant,
        };
        let bytes = msg.to_bytes().unwrap();
        let decoded: Message<String> = Message::from_bytes(&bytes).unwrap();
        assert_eq!(decoded.sender, sender);
        assert!(matches!(
            decoded.variant,
            WireVariant::Paxos(PaxosMessage::Prepare { slot: 7, .. })
        ));
    }
}
