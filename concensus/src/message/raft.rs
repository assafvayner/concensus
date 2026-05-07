use serde::{Deserialize, Serialize};

use crate::config::NodeId;

/// A single replicated log entry.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct LogEntry<V> {
    pub term: u64,
    pub value: V,
}

/// Raft RPC message variants used between nodes.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) enum RaftMessage<V> {
    /// Candidate -> peers: request a vote in `term`.
    RequestVote {
        term: u64,
        candidate: NodeId,
        last_log_index: Option<u64>,
        last_log_term: u64,
    },
    /// Voter -> candidate: response with grant/deny.
    RequestVoteResponse { term: u64, vote_granted: bool },
    /// Leader -> followers: replicate log entries (or empty heartbeat).
    AppendEntries {
        term: u64,
        leader: NodeId,
        prev_log_index: Option<u64>,
        prev_log_term: u64,
        entries: Vec<LogEntry<V>>,
        leader_commit: Option<u64>,
    },
    /// Follower -> leader: response to AppendEntries.
    AppendEntriesResponse {
        term: u64,
        success: bool,
        match_index: Option<u64>,
        conflict_term: Option<u64>,
        conflict_index: Option<u64>,
    },
    /// Follower -> leader: forward a client proposal originally received
    /// locally. `hops` is a TTL that increments at each chain-forward
    /// (follower→follower→leader); recipients drop forwards exceeding
    /// [`MAX_FORWARD_HOPS`] to bound the chain length.
    ///
    /// Older payloads without `hops` deserialize with a default of 0 thanks
    /// to `#[serde(default)]`.
    Forward {
        value: V,
        #[serde(default)]
        hops: u8,
    },
}

/// Maximum number of hops a `Forward` message is allowed to traverse before
/// being dropped. Three covers the worst-case chain on a typical 3–5 node
/// cluster (originator → intermediary → leader). Cluster diameters above
/// that should bump this constant.
pub(crate) const MAX_FORWARD_HOPS: u8 = 3;
