use bytes::Bytes;

#[derive(Clone, Debug)]
pub struct Decision {
    pub slot: u64,
    pub payload: Bytes,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Algorithm {
    Paxos,
    Raft,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
    NotApplicable,
}

#[derive(Clone, Debug)]
pub struct ClusterStatus {
    pub node_id: String,
    pub algorithm: Algorithm,
    pub role: Role,
    pub term: u64,
    pub leader_id: Option<String>,
    pub log_len: u64,
    pub commit_index: Option<u64>,
    pub last_applied: Option<u64>,
}

impl Algorithm {
    pub(crate) fn from_str(s: &str) -> Result<Self, crate::Error> {
        match s {
            "paxos" => Ok(Algorithm::Paxos),
            "raft" => Ok(Algorithm::Raft),
            other => Err(crate::Error::Invalid(format!(
                "unknown algorithm: {other:?}"
            ))),
        }
    }
}

impl Role {
    pub(crate) fn from_str(s: &str) -> Result<Self, crate::Error> {
        match s {
            "follower" => Ok(Role::Follower),
            "candidate" => Ok(Role::Candidate),
            "leader" => Ok(Role::Leader),
            "n/a" => Ok(Role::NotApplicable),
            other => Err(crate::Error::Invalid(format!("unknown role: {other:?}"))),
        }
    }
}
