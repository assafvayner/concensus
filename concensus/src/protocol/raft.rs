use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use rand::RngExt;
use serde::{de::DeserializeOwned, Serialize};

use crate::config::{NodeId, RaftConfig};
use crate::message::{LogEntry, RaftMessage};
use crate::protocol::{ConsensusProtocol, Decision, Outgoing, SendTarget};

/// Role in the Raft state machine.
#[derive(Debug, Clone)]
pub(crate) enum Role {
    Follower,
    Candidate,
    Leader,
}

/// Raft state machine. Implements `ConsensusProtocol<V>` with stubbed message
/// handling and `on_tick` for now; election and log-replication land in
/// subsequent tasks.
#[allow(dead_code)] // pending_persist_*/pending_truncate_from drained in Task 15
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

impl<V> RaftProtocol<V>
where
    V: Serialize + DeserializeOwned + Clone + Send + PartialEq + 'static,
{
    #[allow(dead_code)] // wired in Task 10
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

    fn last_log_index(&self) -> Option<u64> {
        if self.log.is_empty() {
            None
        } else {
            Some(self.log.len() as u64 - 1)
        }
    }

    fn last_log_term(&self) -> u64 {
        self.log.last().map(|e| e.term).unwrap_or(0)
    }

    /// Whether a candidate's log (cand_last_term, cand_last_index) is at least as
    /// up-to-date as ours per Raft's election restriction.
    fn candidate_log_at_least_as_up_to_date(
        &self,
        cand_last_term: u64,
        cand_last_index: Option<u64>,
    ) -> bool {
        let my_term = self.last_log_term();
        if cand_last_term != my_term {
            return cand_last_term > my_term;
        }
        match (cand_last_index, self.last_log_index()) {
            (Some(c), Some(m)) => c >= m,
            (Some(_), None) => true,
            (None, None) => true,
            (None, Some(_)) => false,
        }
    }

    fn become_follower(&mut self, term: u64, leader: Option<NodeId>) {
        if term > self.current_term {
            self.current_term = term;
            self.voted_for = None;
            self.pending_persist_term = true;
            self.pending_persist_voted_for = true;
        }
        self.role = Role::Follower;
        self.leader = leader;
        self.votes_received.clear();
        self.election_deadline = Instant::now() + sample_election_timeout(&self.config);
    }

    fn start_election(&mut self) -> Vec<Outgoing<RaftMessage<V>>> {
        self.current_term += 1;
        self.role = Role::Candidate;
        self.voted_for = Some(self.node_id.clone());
        self.votes_received.clear();
        self.votes_received.insert(self.node_id.clone());
        self.leader = None;
        self.election_deadline = Instant::now() + sample_election_timeout(&self.config);
        self.pending_persist_term = true;
        self.pending_persist_voted_for = true;
        tracing::debug!(term = self.current_term, "starting Raft election");
        vec![Outgoing {
            target: SendTarget::Broadcast,
            message: RaftMessage::RequestVote {
                term: self.current_term,
                candidate: self.node_id.clone(),
                last_log_index: self.last_log_index(),
                last_log_term: self.last_log_term(),
            },
        }]
    }

    fn become_leader(&mut self) -> Vec<Outgoing<RaftMessage<V>>> {
        tracing::info!(term = self.current_term, "becoming Raft leader");
        self.role = Role::Leader;
        self.leader = Some(self.node_id.clone());
        self.next_index.clear();
        self.match_index.clear();
        self.broadcast_heartbeat()
    }

    fn broadcast_heartbeat(&mut self) -> Vec<Outgoing<RaftMessage<V>>> {
        self.last_heartbeat_sent = Some(Instant::now());
        vec![Outgoing {
            target: SendTarget::Broadcast,
            message: RaftMessage::AppendEntries {
                term: self.current_term,
                leader: self.node_id.clone(),
                prev_log_index: self.last_log_index(),
                prev_log_term: self.last_log_term(),
                entries: Vec::new(),
                leader_commit: self.commit_index,
            },
        }]
    }

    fn handle_request_vote(
        &mut self,
        term: u64,
        candidate: NodeId,
        last_log_index: Option<u64>,
        last_log_term: u64,
    ) -> Vec<Outgoing<RaftMessage<V>>> {
        let grant = if term < self.current_term {
            false
        } else {
            let already_voted_for_other = self.voted_for.as_ref().is_some_and(|v| *v != candidate);
            let log_ok = self.candidate_log_at_least_as_up_to_date(last_log_term, last_log_index);
            !already_voted_for_other && log_ok
        };
        if grant {
            self.voted_for = Some(candidate.clone());
            self.pending_persist_voted_for = true;
            self.election_deadline = Instant::now() + sample_election_timeout(&self.config);
        }
        vec![Outgoing {
            target: SendTarget::Peer(candidate),
            message: RaftMessage::RequestVoteResponse {
                term: self.current_term,
                vote_granted: grant,
            },
        }]
    }

    fn handle_request_vote_response(
        &mut self,
        from: NodeId,
        term: u64,
        vote_granted: bool,
    ) -> Vec<Outgoing<RaftMessage<V>>> {
        if !matches!(self.role, Role::Candidate) || term != self.current_term {
            return Vec::new();
        }
        if vote_granted {
            self.votes_received.insert(from);
            if self.votes_received.len() >= self.quorum() {
                return self.become_leader();
            }
        }
        Vec::new()
    }
}

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

    fn handle_message(&mut self, from: NodeId, msg: Self::Message) -> Vec<Outgoing<Self::Message>> {
        // Term-bump: any incoming message with higher term forces step-down.
        let incoming_term = match &msg {
            RaftMessage::RequestVote { term, .. }
            | RaftMessage::RequestVoteResponse { term, .. }
            | RaftMessage::AppendEntries { term, .. }
            | RaftMessage::AppendEntriesResponse { term, .. } => Some(*term),
            RaftMessage::Forward { .. } => None,
        };
        if let Some(t) = incoming_term {
            if t > self.current_term {
                self.become_follower(t, None);
            }
        }
        match msg {
            RaftMessage::RequestVote {
                term,
                candidate,
                last_log_index,
                last_log_term,
            } => self.handle_request_vote(term, candidate, last_log_index, last_log_term),
            RaftMessage::RequestVoteResponse { term, vote_granted } => {
                self.handle_request_vote_response(from, term, vote_granted)
            }
            RaftMessage::AppendEntries { .. } => Vec::new(), // Task 12
            RaftMessage::AppendEntriesResponse { .. } => Vec::new(), // Task 13
            RaftMessage::Forward { .. } => Vec::new(),       // Task 17
        }
    }

    fn on_tick(&mut self, now: Instant) -> Vec<Outgoing<Self::Message>> {
        match self.role {
            Role::Leader => {
                let heartbeat_due = self
                    .last_heartbeat_sent
                    .map(|t| now.duration_since(t) >= self.config.heartbeat_interval)
                    .unwrap_or(true);
                if heartbeat_due {
                    self.broadcast_heartbeat()
                } else {
                    Vec::new()
                }
            }
            Role::Follower | Role::Candidate => {
                if now >= self.election_deadline {
                    self.start_election()
                } else {
                    Vec::new()
                }
            }
        }
    }

    fn take_decisions(&mut self) -> Vec<Decision<V>> {
        std::mem::take(&mut self.pending_decisions)
    }

    fn take_lost_proposals(&mut self) -> Vec<V> {
        Vec::new()
    }

    fn is_idle(&self) -> bool {
        self.log.is_empty()
            && self.pending_proposals.is_empty()
            && matches!(self.role, Role::Follower | Role::Candidate)
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

    #[test]
    fn election_timeout_starts_election() {
        use crate::message::RaftMessage;
        let mut p = RaftProtocol::<String>::new(NodeId::new("a", 1), 3, RaftConfig::default());
        p.election_deadline = Instant::now() - Duration::from_millis(10);
        let out = p.on_tick(Instant::now());
        assert!(matches!(p.role, Role::Candidate));
        assert_eq!(p.current_term, 1);
        assert_eq!(p.voted_for, Some(p.node_id.clone()));
        assert_eq!(out.len(), 1);
        assert!(matches!(out[0].target, SendTarget::Broadcast));
        assert!(matches!(
            out[0].message,
            RaftMessage::RequestVote { term: 1, .. }
        ));
        // Self-vote should be counted
        assert!(p.votes_received.contains(&p.node_id));
    }

    #[test]
    fn request_vote_grants_when_term_higher() {
        use crate::message::RaftMessage;
        let mut p = RaftProtocol::<String>::new(NodeId::new("a", 1), 3, RaftConfig::default());
        let candidate = NodeId::new("b", 1);
        let out = p.handle_message(
            candidate.clone(),
            RaftMessage::RequestVote {
                term: 5,
                candidate: candidate.clone(),
                last_log_index: None,
                last_log_term: 0,
            },
        );
        assert_eq!(p.current_term, 5);
        assert_eq!(p.voted_for, Some(candidate.clone()));
        assert_eq!(out.len(), 1);
        match &out[0].message {
            RaftMessage::RequestVoteResponse { term, vote_granted } => {
                assert_eq!(*term, 5);
                assert!(*vote_granted);
            }
            _ => panic!("expected RequestVoteResponse"),
        }
        match &out[0].target {
            SendTarget::Peer(t) => assert_eq!(t, &candidate),
            _ => panic!("expected Peer target"),
        }
    }

    #[test]
    fn request_vote_rejects_lower_term() {
        use crate::message::RaftMessage;
        let mut p = RaftProtocol::<String>::new(NodeId::new("a", 1), 3, RaftConfig::default());
        p.current_term = 5;
        let out = p.handle_message(
            NodeId::new("b", 1),
            RaftMessage::RequestVote {
                term: 3,
                candidate: NodeId::new("b", 1),
                last_log_index: None,
                last_log_term: 0,
            },
        );
        assert_eq!(p.current_term, 5);
        match &out[0].message {
            RaftMessage::RequestVoteResponse {
                term: 5,
                vote_granted: false,
            } => {}
            other => panic!("expected denied RVR with our term, got {:?}", other),
        }
    }

    #[test]
    fn request_vote_rejects_stale_log() {
        use crate::message::{LogEntry, RaftMessage};
        let mut p = RaftProtocol::<String>::new(NodeId::new("a", 1), 3, RaftConfig::default());
        p.log.push(LogEntry {
            term: 3,
            value: "x".into(),
        });
        p.current_term = 3;
        let out = p.handle_message(
            NodeId::new("b", 1),
            RaftMessage::RequestVote {
                term: 4,
                candidate: NodeId::new("b", 1),
                last_log_index: None,
                last_log_term: 0,
            },
        );
        match &out[0].message {
            RaftMessage::RequestVoteResponse { vote_granted, .. } => assert!(!*vote_granted),
            _ => panic!(),
        }
    }

    #[test]
    fn request_vote_already_voted_in_same_term_rejects_other() {
        use crate::message::RaftMessage;
        let mut p = RaftProtocol::<String>::new(NodeId::new("a", 1), 3, RaftConfig::default());
        let b = NodeId::new("b", 1);
        let c = NodeId::new("c", 1);
        let _ = p.handle_message(
            b.clone(),
            RaftMessage::RequestVote {
                term: 5,
                candidate: b.clone(),
                last_log_index: None,
                last_log_term: 0,
            },
        );
        assert_eq!(p.voted_for, Some(b));
        let out = p.handle_message(
            c.clone(),
            RaftMessage::RequestVote {
                term: 5,
                candidate: c,
                last_log_index: None,
                last_log_term: 0,
            },
        );
        match &out[0].message {
            RaftMessage::RequestVoteResponse {
                vote_granted: false,
                ..
            } => {}
            _ => panic!("should reject second vote in same term"),
        }
    }

    #[test]
    fn vote_quorum_promotes_to_leader_and_heartbeats() {
        use crate::message::RaftMessage;
        let me = NodeId::new("a", 1);
        let mut p = RaftProtocol::<String>::new(me.clone(), 3, RaftConfig::default());
        p.election_deadline = Instant::now() - Duration::from_millis(1);
        let _ = p.on_tick(Instant::now());
        assert!(matches!(p.role, Role::Candidate));

        let out = p.handle_message(
            NodeId::new("b", 1),
            RaftMessage::RequestVoteResponse {
                term: 1,
                vote_granted: true,
            },
        );
        assert!(matches!(p.role, Role::Leader));
        assert_eq!(p.leader, Some(me));
        assert_eq!(out.len(), 1);
        assert!(matches!(out[0].target, SendTarget::Broadcast));
        match &out[0].message {
            RaftMessage::AppendEntries {
                term: 1, entries, ..
            } => assert!(entries.is_empty()),
            _ => panic!("expected empty AppendEntries heartbeat"),
        }
    }

    #[test]
    fn higher_term_steps_down_to_follower() {
        use crate::message::RaftMessage;
        let mut p = RaftProtocol::<String>::new(NodeId::new("a", 1), 3, RaftConfig::default());
        p.role = Role::Leader;
        p.current_term = 3;
        p.voted_for = Some(p.node_id.clone());
        let _ = p.handle_message(
            NodeId::new("b", 1),
            RaftMessage::RequestVote {
                term: 5,
                candidate: NodeId::new("b", 1),
                last_log_index: None,
                last_log_term: 0,
            },
        );
        assert_eq!(p.current_term, 5);
        assert!(matches!(p.role, Role::Follower));
    }

    #[test]
    fn vote_response_for_old_term_ignored() {
        use crate::message::RaftMessage;
        let mut p = RaftProtocol::<String>::new(NodeId::new("a", 1), 3, RaftConfig::default());
        p.election_deadline = Instant::now() - Duration::from_millis(1);
        let _ = p.on_tick(Instant::now()); // p.current_term = 1
        let out = p.handle_message(
            NodeId::new("b", 1),
            RaftMessage::RequestVoteResponse {
                term: 0, // stale
                vote_granted: true,
            },
        );
        assert!(out.is_empty());
        assert!(matches!(p.role, Role::Candidate)); // unchanged
    }
}
