pub(crate) mod paxos;
pub(crate) mod raft;

use bytes::Bytes;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::config::NodeId;

pub(crate) use paxos::{PaxosMessage, ProposalNumber};
#[allow(unused_imports)]
pub(crate) use raft::{LogEntry, RaftMessage};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) enum WireVariant<V> {
    Paxos(PaxosMessage<V>),
    Raft(RaftMessage<V>),
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

    #[test]
    fn raft_request_vote_serde_roundtrip() {
        let msg: WireVariant<String> =
            WireVariant::Raft(crate::message::raft::RaftMessage::RequestVote {
                term: 5,
                candidate: test_node_id(),
                last_log_index: None,
                last_log_term: 0,
            });
        let json = serde_json::to_string(&msg).unwrap();
        let decoded: WireVariant<String> = serde_json::from_str(&json).unwrap();
        assert!(matches!(
            decoded,
            WireVariant::Raft(crate::message::raft::RaftMessage::RequestVote { term: 5, .. })
        ));
    }

    #[test]
    fn raft_append_entries_serde_roundtrip() {
        use crate::message::raft::{LogEntry, RaftMessage};
        let msg: WireVariant<String> = WireVariant::Raft(RaftMessage::AppendEntries {
            term: 7,
            leader: test_node_id(),
            prev_log_index: Some(2),
            prev_log_term: 6,
            entries: vec![LogEntry {
                term: 7,
                value: "x".to_string(),
            }],
            leader_commit: Some(2),
        });
        let json = serde_json::to_string(&msg).unwrap();
        let decoded: WireVariant<String> = serde_json::from_str(&json).unwrap();
        match decoded {
            WireVariant::Raft(RaftMessage::AppendEntries { term, entries, .. }) => {
                assert_eq!(term, 7);
                assert_eq!(entries.len(), 1);
                assert_eq!(entries[0].value, "x");
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn raft_log_entry_eq() {
        use crate::message::raft::LogEntry;
        let a = LogEntry {
            term: 1,
            value: "x".to_string(),
        };
        let b = LogEntry {
            term: 1,
            value: "x".to_string(),
        };
        let c = LogEntry {
            term: 2,
            value: "x".to_string(),
        };
        assert_eq!(a, b);
        assert_ne!(a, c);
    }
}
