// concensus/src/protocol/mod.rs
pub(crate) mod paxos;

use crate::config::NodeId;

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
