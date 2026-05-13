use std::str::FromStr;

use bytes::Bytes;

/// A value that has been decided by the cluster, paired with the slot it
/// occupies in the commit log.
///
/// Slots are zero-indexed and assigned sequentially. Every node in a healthy
/// cluster agrees on the same `(slot, payload)` pair for each slot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Decision {
    /// The slot number this value was decided in.
    pub slot: u64,
    /// The opaque payload that was decided.
    pub payload: Bytes,
}

/// Which consensus algorithm a node is running.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Algorithm {
    /// Multi-Paxos (default).
    Paxos,
    /// Raft.
    Raft,
}

/// A node's current Raft role.
///
/// Paxos nodes report [`Role::NotApplicable`] because Multi-Paxos has no
/// equivalent role concept.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
    /// The node does not track a role for the active algorithm (e.g. Paxos).
    NotApplicable,
}

/// Snapshot of a single node's view of the cluster.
///
/// Returned by [`Client::status`](crate::Client::status). Fields that don't
/// apply to the active algorithm carry sentinel values: a Paxos node always
/// reports `role = NotApplicable`, `term = 0`, `log_len = 0`,
/// `commit_index = None`, `last_applied = None`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClusterStatus {
    /// Stable identifier of the node that served the status request, in
    /// `name@incarnation` form.
    pub node_id: String,
    pub algorithm: Algorithm,
    pub role: Role,
    /// Current Raft term, or 0 for Paxos.
    pub term: u64,
    /// Identifier of the current leader as known to this node, or `None` if
    /// no leader is currently known.
    pub leader_id: Option<String>,
    /// Length of the local log. Always 0 for Paxos.
    pub log_len: u64,
    /// Index of the highest log entry known to be committed. `None` for Paxos
    /// or before any entries are committed.
    pub commit_index: Option<u64>,
    /// Index of the highest log entry applied to the state machine. `None` for
    /// Paxos or before any entries are applied.
    pub last_applied: Option<u64>,
}

impl FromStr for Algorithm {
    type Err = crate::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "paxos" => Ok(Algorithm::Paxos),
            "raft" => Ok(Algorithm::Raft),
            other => Err(crate::Error::Invalid(format!(
                "unknown algorithm: {other:?}"
            ))),
        }
    }
}

impl FromStr for Role {
    type Err = crate::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "follower" => Ok(Role::Follower),
            "candidate" => Ok(Role::Candidate),
            "leader" => Ok(Role::Leader),
            "n/a" => Ok(Role::NotApplicable),
            other => Err(crate::Error::Invalid(format!("unknown role: {other:?}"))),
        }
    }
}
