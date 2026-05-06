use serde::{Deserialize, Serialize};

use crate::config::NodeId;

/// A single replicated log entry.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub(crate) struct LogEntry<V> {
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
    /// Follower -> leader: forward a client proposal originally received locally.
    Forward { value: V },
}
