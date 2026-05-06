// concensus/src/protocol/mod.rs
pub(crate) mod paxos;

use std::time::Instant;

use serde::{de::DeserializeOwned, Serialize};

use crate::config::NodeId;
use crate::message::{PaxosMessage, WireVariant};

pub(crate) use paxos::PaxosProtocol;

pub(crate) enum SendTarget {
    Peer(NodeId),
    Broadcast,
}

pub(crate) struct Outgoing<M> {
    pub target: SendTarget,
    pub message: M,
}

pub(crate) struct Decision<V> {
    pub slot: u64,
    pub value: V,
}

pub(crate) trait ConsensusProtocol<V> {
    type Message: Serialize + DeserializeOwned + Clone + Send + 'static;

    fn propose(&mut self, value: V) -> Vec<Outgoing<Self::Message>>;
    fn handle_message(&mut self, from: NodeId, msg: Self::Message) -> Vec<Outgoing<Self::Message>>;
    fn on_tick(&mut self, now: Instant) -> Vec<Outgoing<Self::Message>>;
    fn take_decisions(&mut self) -> Vec<Decision<V>>;
    fn take_lost_proposals(&mut self) -> Vec<V>;
    fn is_idle(&self) -> bool;
}

pub(crate) enum ProtocolImpl<V> {
    Paxos(PaxosProtocol<V>),
    // Raft variant added in Task 8.
}

fn wrap_paxos<V>(o: Outgoing<PaxosMessage<V>>) -> Outgoing<WireVariant<V>> {
    Outgoing {
        target: o.target,
        message: WireVariant::Paxos(o.message),
    }
}

impl<V> ProtocolImpl<V>
where
    V: Serialize + DeserializeOwned + Clone + Send + PartialEq + 'static,
{
    pub(crate) fn propose(&mut self, value: V) -> Vec<Outgoing<WireVariant<V>>> {
        match self {
            Self::Paxos(p) => <PaxosProtocol<V> as ConsensusProtocol<V>>::propose(p, value)
                .into_iter()
                .map(wrap_paxos)
                .collect(),
        }
    }

    pub(crate) fn handle_wire_message(
        &mut self,
        from: NodeId,
        msg: WireVariant<V>,
    ) -> Vec<Outgoing<WireVariant<V>>> {
        match (self, msg) {
            (Self::Paxos(p), WireVariant::Paxos(m)) => {
                <PaxosProtocol<V> as ConsensusProtocol<V>>::handle_message(p, from, m)
                    .into_iter()
                    .map(wrap_paxos)
                    .collect()
            }
            (Self::Paxos(_), WireVariant::Raft(_)) => {
                tracing::warn!("Paxos node received Raft message, dropping");
                Vec::new()
            } // Once a Raft arm exists in ProtocolImpl, also handle (Raft, Raft) and (Raft, Paxos) warn-and-drop.
        }
    }

    pub(crate) fn on_tick(&mut self, now: Instant) -> Vec<Outgoing<WireVariant<V>>> {
        match self {
            Self::Paxos(p) => <PaxosProtocol<V> as ConsensusProtocol<V>>::on_tick(p, now)
                .into_iter()
                .map(wrap_paxos)
                .collect(),
        }
    }

    pub(crate) fn take_decisions(&mut self) -> Vec<Decision<V>> {
        match self {
            Self::Paxos(p) => <PaxosProtocol<V> as ConsensusProtocol<V>>::take_decisions(p),
        }
    }

    pub(crate) fn take_lost_proposals(&mut self) -> Vec<V> {
        match self {
            Self::Paxos(p) => <PaxosProtocol<V> as ConsensusProtocol<V>>::take_lost_proposals(p),
        }
    }

    pub(crate) fn is_idle(&self) -> bool {
        match self {
            Self::Paxos(p) => <PaxosProtocol<V> as ConsensusProtocol<V>>::is_idle(p),
        }
    }

    /// Initialize from previously persisted decisions. Protocol-specific.
    pub(crate) fn initialize_from_decisions(&mut self, decisions: Vec<(u64, V)>) {
        match self {
            Self::Paxos(p) => p.initialize_from_decisions(decisions),
        }
    }
}
