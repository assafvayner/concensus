use serde::{Deserialize, Serialize};

use crate::config::NodeId;

pub(crate) type ProposalNumber = (u64, NodeId);

#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) enum PaxosMessage<V> {
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
    #[cfg(feature = "multi-paxos")]
    Forward {
        value: V,
    },
    #[cfg(feature = "multi-paxos")]
    Heartbeat {
        term: u64,
    },
    /// Multi-Paxos election Prepare: a single Phase 1 covering ALL future slots
    /// at the given proposal number. Acceptors promise globally and respond
    /// with every accepted value they hold for slots `>= since_slot`.
    #[cfg(feature = "multi-paxos")]
    PrepareLeader {
        proposal_number: ProposalNumber,
        /// Recovery floor: acceptor reports accepted values for slots `>= since_slot`.
        /// Set to `min(decided slots + 1, lowest in-flight slot)` by the candidate.
        since_slot: u64,
    },
    /// Response to PrepareLeader. `accepted` lists every accepted value the
    /// acceptor holds at slots `>= since_slot` from the Prepare.
    #[cfg(feature = "multi-paxos")]
    PromiseLeader {
        proposal_number: ProposalNumber,
        accepted: Vec<(u64, ProposalNumber, V)>,
    },
    /// Nack for PrepareLeader: the acceptor has already promised a higher leader.
    #[cfg(feature = "multi-paxos")]
    NackLeader {
        proposal_number: ProposalNumber,
        highest_promised: ProposalNumber,
    },
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
        let msg: PaxosMessage<String> = PaxosMessage::Prepare {
            slot: 0,
            proposal_number: (1, test_node_id()),
        };
        let json = serde_json::to_string(&msg).unwrap();
        let deserialized: PaxosMessage<String> = serde_json::from_str(&json).unwrap();
        assert!(matches!(
            deserialized,
            PaxosMessage::Prepare { slot: 0, .. }
        ));
    }

    #[test]
    fn promise_with_accepted_value_serde_roundtrip() {
        let msg: PaxosMessage<String> = PaxosMessage::Promise {
            slot: 0,
            proposal_number: (1, test_node_id()),
            accepted: Some(((0, test_node_id()), "hello".to_string())),
        };
        let json = serde_json::to_string(&msg).unwrap();
        let deserialized: PaxosMessage<String> = serde_json::from_str(&json).unwrap();
        match deserialized {
            PaxosMessage::Promise {
                accepted: Some((_, val)),
                ..
            } => assert_eq!(val, "hello"),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn decide_message_serde_roundtrip() {
        let msg = PaxosMessage::Decide {
            slot: 5,
            value: 42u64,
        };
        let json = serde_json::to_string(&msg).unwrap();
        let deserialized: PaxosMessage<u64> = serde_json::from_str(&json).unwrap();
        match deserialized {
            PaxosMessage::Decide { slot, value } => {
                assert_eq!(slot, 5);
                assert_eq!(value, 42);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn nack_prepare_serde_roundtrip() {
        let msg: PaxosMessage<String> = PaxosMessage::NackPrepare {
            slot: 0,
            proposal_number: (1, test_node_id()),
            highest_promised: (2, test_node_id()),
        };
        let json = serde_json::to_string(&msg).unwrap();
        let deserialized: PaxosMessage<String> = serde_json::from_str(&json).unwrap();
        assert!(matches!(deserialized, PaxosMessage::NackPrepare { .. }));
    }

    #[test]
    fn nack_accept_serde_roundtrip() {
        let msg: PaxosMessage<String> = PaxosMessage::NackAccept {
            slot: 0,
            proposal_number: (1, test_node_id()),
            highest_promised: (2, test_node_id()),
        };
        let json = serde_json::to_string(&msg).unwrap();
        let deserialized: PaxosMessage<String> = serde_json::from_str(&json).unwrap();
        assert!(matches!(deserialized, PaxosMessage::NackAccept { .. }));
    }
}
