use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use rand::RngExt;
use serde::{de::DeserializeOwned, Serialize};

use crate::config::{NodeId, RaftConfig};
use crate::message::{LogEntry, RaftMessage};
use crate::protocol::{ConsensusProtocol, Decision, Outgoing};

/// Role in the Raft state machine.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) enum Role {
    Follower,
    Candidate,
    Leader,
}

/// Raft state machine. Implements `ConsensusProtocol<V>` with stubbed message
/// handling and `on_tick` for now; election and log-replication land in
/// subsequent tasks.
#[allow(dead_code)]
pub(crate) struct RaftProtocol<V> {
    pub(crate) node_id: NodeId,
    pub(crate) total_nodes: usize,
    pub(crate) config: RaftConfig,

    // Persistent state (mirrored to RaftStorage by the Node)
    pub(crate) current_term: u64,
    pub(crate) voted_for: Option<NodeId>,
    pub(crate) log: Vec<LogEntry<V>>,

    // Volatile state on all servers
    pub(crate) commit_index: Option<u64>,
    pub(crate) last_applied: Option<u64>,
    pub(crate) role: Role,
    pub(crate) leader: Option<NodeId>,

    // Volatile state on leaders
    pub(crate) next_index: HashMap<NodeId, u64>,
    pub(crate) match_index: HashMap<NodeId, Option<u64>>,

    // Election bookkeeping
    pub(crate) election_deadline: Instant,
    pub(crate) votes_received: HashSet<NodeId>,

    // Heartbeat bookkeeping (leader)
    pub(crate) last_heartbeat_sent: Option<Instant>,

    // Decisions ready to be drained by Node
    pub(crate) pending_decisions: Vec<Decision<V>>,

    // Persistence intent — Node calls drain_persist_intent() and writes through RaftStorage.
    pub(crate) pending_persist_term: bool,
    pub(crate) pending_persist_voted_for: bool,
    pub(crate) pending_persist_log_from: Option<u64>,
    pub(crate) pending_truncate_from: Option<u64>,

    // Buffered proposals from before a leader is known (drained on AppendEntries).
    pub(crate) pending_proposals: Vec<V>,
}

#[allow(dead_code)]
impl<V> RaftProtocol<V>
where
    V: Serialize + DeserializeOwned + Clone + Send + PartialEq + 'static,
{
    pub(crate) fn new(node_id: NodeId, total_nodes: usize, config: RaftConfig) -> Self {
        let election_deadline = Instant::now() + sample_election_timeout(&config);
        Self {
            node_id,
            total_nodes,
            config,
            current_term: 0,
            voted_for: None,
            log: Vec::new(),
            commit_index: None,
            last_applied: None,
            role: Role::Follower,
            leader: None,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            election_deadline,
            votes_received: HashSet::new(),
            last_heartbeat_sent: None,
            pending_decisions: Vec::new(),
            pending_persist_term: false,
            pending_persist_voted_for: false,
            pending_persist_log_from: None,
            pending_truncate_from: None,
            pending_proposals: Vec::new(),
        }
    }

    pub(crate) fn quorum(&self) -> usize {
        (self.total_nodes / 2) + 1
    }
}

#[allow(dead_code)]
pub(crate) fn sample_election_timeout(cfg: &RaftConfig) -> Duration {
    let lo = cfg.election_timeout_min.as_millis() as u64;
    let hi = cfg.election_timeout_max.as_millis() as u64;
    let chosen = if hi <= lo {
        lo
    } else {
        rand::rng().random_range(lo..=hi)
    };
    Duration::from_millis(chosen)
}

impl<V> ConsensusProtocol<V> for RaftProtocol<V>
where
    V: Serialize + DeserializeOwned + Clone + Send + PartialEq + 'static,
{
    type Message = RaftMessage<V>;

    fn propose(&mut self, value: V) -> Vec<Outgoing<Self::Message>> {
        // Filled in by later tasks. For now, buffer.
        self.pending_proposals.push(value);
        Vec::new()
    }

    fn handle_message(
        &mut self,
        _from: NodeId,
        _msg: Self::Message,
    ) -> Vec<Outgoing<Self::Message>> {
        Vec::new()
    }

    fn on_tick(&mut self, _now: Instant) -> Vec<Outgoing<Self::Message>> {
        Vec::new()
    }

    fn take_decisions(&mut self) -> Vec<Decision<V>> {
        std::mem::take(&mut self.pending_decisions)
    }

    fn take_lost_proposals(&mut self) -> Vec<V> {
        Vec::new()
    }

    fn is_idle(&self) -> bool {
        self.log.is_empty() && self.pending_proposals.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{NodeId, RaftConfig};

    #[test]
    fn new_raft_protocol_starts_as_follower() {
        let proto = RaftProtocol::<String>::new(NodeId::new("a", 1), 3, RaftConfig::default());
        assert!(matches!(proto.role, Role::Follower));
        assert_eq!(proto.current_term, 0);
        assert!(proto.voted_for.is_none());
    }

    #[test]
    fn is_idle_when_log_empty() {
        let proto = RaftProtocol::<String>::new(NodeId::new("a", 1), 3, RaftConfig::default());
        assert!(proto.is_idle());
    }

    #[test]
    fn quorum_for_three_nodes() {
        let proto = RaftProtocol::<String>::new(NodeId::new("a", 1), 3, RaftConfig::default());
        assert_eq!(proto.quorum(), 2);
    }

    #[test]
    fn quorum_for_five_nodes() {
        let proto = RaftProtocol::<String>::new(NodeId::new("a", 1), 5, RaftConfig::default());
        assert_eq!(proto.quorum(), 3);
    }
}
