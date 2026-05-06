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

/// Snapshot of pending persistence work drained from a `RaftProtocol`.
///
/// The Node calls [`RaftProtocol::drain_persist_intent`] before sending any
/// outgoing message and writes the captured state through `RaftStorage`. This
/// satisfies Raft's "durability before send" rule.
pub(crate) struct PersistIntent<V> {
    pub term: Option<u64>,
    pub voted_for: Option<Option<NodeId>>,
    pub truncate_from: Option<u64>,
    pub append_from: Option<u64>,
    pub log_snapshot: Vec<LogEntry<V>>,
}

/// Raft state machine. Implements `ConsensusProtocol<V>` with stubbed message
/// handling and `on_tick` for now; election and log-replication land in
/// subsequent tasks.
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

/// Inner snapshot used by `ConsensusProtocol::peek_state`. Mirrors `NodeState`
/// minus `node_id` and `algorithm`, which the wrapper fills in.
#[cfg(any(test, feature = "test-support"))]
pub(crate) struct ProtocolSnapshot {
    pub role: Option<crate::node::NodeRole>,
    pub term: u64,
    pub leader: Option<NodeId>,
    pub voted_for: Option<NodeId>,
    pub log_len: u64,
    pub commit_index: Option<u64>,
    pub last_applied: Option<u64>,
}

impl<V> RaftProtocol<V>
where
    V: Serialize + DeserializeOwned + Clone + Send + PartialEq + 'static,
{
    pub(crate) fn new(node_id: NodeId, total_nodes: usize, config: RaftConfig) -> Self {
        let election_deadline = Instant::now() + sample_election_timeout(&config);
        let (
            role,
            current_term,
            voted_for,
            leader,
            pending_persist_term,
            pending_persist_voted_for,
        ) = if total_nodes == 1 {
            (
                Role::Leader,
                1,
                Some(node_id.clone()),
                Some(node_id.clone()),
                true,
                true,
            )
        } else {
            (Role::Follower, 0, None, None, false, false)
        };
        Self {
            node_id,
            total_nodes,
            config,
            current_term,
            voted_for,
            log: Vec::new(),
            commit_index: None,
            last_applied: None,
            role,
            leader,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            election_deadline,
            votes_received: HashSet::new(),
            last_heartbeat_sent: None,
            pending_decisions: Vec::new(),
            pending_persist_term,
            pending_persist_voted_for,
            pending_persist_log_from: None,
            pending_truncate_from: None,
            pending_proposals: Vec::new(),
        }
    }

    pub(crate) fn quorum(&self) -> usize {
        (self.total_nodes / 2) + 1
    }

    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn peek_state(&self) -> ProtocolSnapshot {
        let role = Some(match self.role {
            Role::Follower => crate::node::NodeRole::Follower,
            Role::Candidate => crate::node::NodeRole::Candidate,
            Role::Leader => crate::node::NodeRole::Leader,
        });
        ProtocolSnapshot {
            role,
            term: self.current_term,
            leader: self.leader.clone(),
            voted_for: self.voted_for.clone(),
            log_len: self.log.len() as u64,
            commit_index: self.commit_index,
            last_applied: self.last_applied,
        }
    }

    /// Restore persistent state at startup. Called by the Node before entering
    /// the event loop with `term`, `voted_for`, and `log` from `RaftStorage`
    /// and `decisions` from `Storage::load_decisions`. The committed prefix of
    /// the log is exactly the set of decided slots, so `commit_index` and
    /// `last_applied` are set to the highest decided slot (or `None` if there
    /// are no decisions).
    ///
    /// Role normally resets to Follower: a freshly-started node cannot assume
    /// it is still leader, and the election timeout will trigger a fresh
    /// election if needed. Single-node clusters are the exception — there are
    /// no peers to elect us, so we mirror the constructor's bootstrap and
    /// resume directly as Leader at term ≥ 1 with a vote for self.
    pub(crate) fn recover(
        &mut self,
        term: u64,
        voted_for: Option<NodeId>,
        log: Vec<LogEntry<V>>,
        decisions: Vec<(u64, V)>,
    ) {
        self.current_term = term;
        self.voted_for = voted_for;
        self.log = log;
        let max_decided = decisions.iter().map(|(s, _)| *s).max();
        self.commit_index = max_decided;
        self.last_applied = max_decided;
        self.election_deadline = Instant::now() + sample_election_timeout(&self.config);
        self.pending_persist_term = false;
        self.pending_persist_voted_for = false;
        self.pending_persist_log_from = None;
        self.pending_truncate_from = None;
        self.pending_decisions.clear();
        self.pending_proposals.clear();
        self.votes_received.clear();
        self.next_index.clear();
        self.match_index.clear();

        if self.total_nodes == 1 {
            if self.current_term == 0 {
                self.current_term = 1;
                self.pending_persist_term = true;
            }
            if self.voted_for.as_ref() != Some(&self.node_id) {
                self.voted_for = Some(self.node_id.clone());
                self.pending_persist_voted_for = true;
            }
            self.role = Role::Leader;
            self.leader = Some(self.node_id.clone());
        } else {
            self.role = Role::Follower;
            self.leader = None;
        }
    }

    /// Drain the pending persistence intent — the Node will write it through
    /// `RaftStorage` before sending any outgoing wire message. Clears the
    /// underlying flags so subsequent calls return empty intents until new
    /// state changes occur.
    pub(crate) fn drain_persist_intent(&mut self) -> PersistIntent<V> {
        let term = if self.pending_persist_term {
            Some(self.current_term)
        } else {
            None
        };
        let voted_for = if self.pending_persist_voted_for {
            Some(self.voted_for.clone())
        } else {
            None
        };
        let truncate_from = self.pending_truncate_from.take();
        let append_from = self.pending_persist_log_from.take();
        self.pending_persist_term = false;
        self.pending_persist_voted_for = false;
        let log_snapshot = if append_from.is_some() {
            self.log.clone()
        } else {
            Vec::new()
        };
        PersistIntent {
            term,
            voted_for,
            truncate_from,
            append_from,
            log_snapshot,
        }
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

        // Drain any proposals buffered while we had no leader; treat each as a fresh
        // proposal now that we are the leader.
        let buffered: Vec<V> = std::mem::take(&mut self.pending_proposals);
        if buffered.is_empty() {
            return self.broadcast_heartbeat();
        }
        let mut new_entries: Vec<LogEntry<V>> = Vec::with_capacity(buffered.len());
        let start_idx = self.log.len() as u64;
        for value in buffered {
            let entry = LogEntry {
                term: self.current_term,
                value,
            };
            self.log.push(entry.clone());
            new_entries.push(entry);
        }
        if self.pending_persist_log_from.is_none() {
            self.pending_persist_log_from = Some(start_idx);
        }
        let last_idx = self.log.len() as u64 - 1;
        self.match_index
            .insert(self.node_id.clone(), Some(last_idx));
        // Single-node clusters can commit immediately.
        self.try_advance_commit();
        self.broadcast_with_entries(new_entries)
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

    #[allow(clippy::too_many_arguments)]
    fn handle_append_entries(
        &mut self,
        _from: NodeId,
        term: u64,
        leader: NodeId,
        prev_log_index: Option<u64>,
        prev_log_term: u64,
        entries: Vec<LogEntry<V>>,
        leader_commit: Option<u64>,
    ) -> Vec<Outgoing<RaftMessage<V>>> {
        // Reject if leader's term is stale.
        if term < self.current_term {
            return vec![Outgoing {
                target: SendTarget::Peer(leader),
                message: RaftMessage::AppendEntriesResponse {
                    term: self.current_term,
                    success: false,
                    match_index: None,
                    conflict_term: None,
                    conflict_index: None,
                },
            }];
        }
        // Recognize the leader for the current term and reset election timer.
        self.role = Role::Follower;
        self.leader = Some(leader.clone());
        self.election_deadline = Instant::now() + sample_election_timeout(&self.config);
        // Note: term-bump already happened in handle_message via become_follower
        // when term > current_term.

        // Log consistency check: our log must contain prev_log_index with prev_log_term.
        let prev_ok = match prev_log_index {
            None => true,
            Some(idx) => {
                let i = idx as usize;
                i < self.log.len() && self.log[i].term == prev_log_term
            }
        };
        if !prev_ok {
            let (conflict_term, conflict_index) = match prev_log_index {
                Some(idx) if (idx as usize) >= self.log.len() => {
                    // Log is shorter than the leader expects. Hint: resume from our log's end.
                    (None, Some(self.log.len() as u64))
                }
                Some(idx) => {
                    // Term mismatch at prev_log_index. Hint with our term + first index of that term.
                    let conflict_term_val = self.log[idx as usize].term;
                    let first = (0..=idx as usize)
                        .find(|&i| self.log[i].term == conflict_term_val)
                        .map(|i| i as u64);
                    (Some(conflict_term_val), first)
                }
                None => (None, Some(0)),
            };
            return vec![Outgoing {
                target: SendTarget::Peer(leader),
                message: RaftMessage::AppendEntriesResponse {
                    term: self.current_term,
                    success: false,
                    match_index: None,
                    conflict_term,
                    conflict_index,
                },
            }];
        }

        // Append entries, truncating any conflict at insertion points.
        let start = match prev_log_index {
            Some(i) => (i + 1) as usize,
            None => 0,
        };
        let new_entries_len = entries.len();
        for (insert_at, entry) in (start..).zip(entries) {
            if insert_at < self.log.len() {
                if self.log[insert_at].term != entry.term {
                    self.log.truncate(insert_at);
                    self.pending_truncate_from = Some(insert_at as u64);
                    self.log.push(entry);
                    if self.pending_persist_log_from.is_none()
                        || self.pending_persist_log_from.unwrap() > insert_at as u64
                    {
                        self.pending_persist_log_from = Some(insert_at as u64);
                    }
                }
                // else: entry already matches; idempotent no-op.
            } else {
                let from_idx = self.log.len() as u64;
                self.log.push(entry);
                if self.pending_persist_log_from.is_none() {
                    self.pending_persist_log_from = Some(from_idx);
                }
            }
        }

        // Update commit_index from leader_commit, capped at our last index.
        if let Some(lc) = leader_commit {
            let last_idx = self.log.len().saturating_sub(1) as u64;
            let new_ci = lc.min(last_idx);
            if !self.log.is_empty() && self.commit_index.is_none_or(|c| new_ci > c) {
                self.commit_index = Some(new_ci);
                self.apply_committed_entries();
            }
        }

        // Report only the index this RPC actually verified — `prev_log_index`
        // plus the entries we just placed. The follower's local log may extend
        // beyond that with an unverified stale suffix from a prior leader; if
        // we reported `self.log.len() - 1` here, an empty heartbeat that
        // matched only at `prev_log_index` would let the leader treat that
        // stale suffix as replicated, inflating `match_index`/`next_index` and
        // letting `try_advance_commit` rely on a quorum that hasn't actually
        // matched. The standard suffix is left in place — a future non-empty
        // AppendEntries that conflicts will truncate it via the loop above.
        let match_idx = match (prev_log_index, new_entries_len) {
            (Some(p), n) => Some(p + n as u64),
            (None, 0) => None,
            (None, n) => Some(n as u64 - 1),
        };
        let response = Outgoing {
            target: SendTarget::Peer(leader.clone()),
            message: RaftMessage::AppendEntriesResponse {
                term: self.current_term,
                success: true,
                match_index: match_idx,
                conflict_term: None,
                conflict_index: None,
            },
        };
        let mut all = vec![response];
        for value in std::mem::take(&mut self.pending_proposals) {
            all.push(Outgoing {
                target: SendTarget::Peer(leader.clone()),
                message: RaftMessage::Forward { value },
            });
        }
        all
    }

    fn broadcast_with_entries(
        &mut self,
        entries: Vec<LogEntry<V>>,
    ) -> Vec<Outgoing<RaftMessage<V>>> {
        self.last_heartbeat_sent = Some(Instant::now());
        let prev_log_index = if self.log.len() > entries.len() {
            Some((self.log.len() - entries.len()) as u64 - 1)
        } else {
            None
        };
        let prev_log_term = match prev_log_index {
            Some(i) => self.log[i as usize].term,
            None => 0,
        };
        vec![Outgoing {
            target: SendTarget::Broadcast,
            message: RaftMessage::AppendEntries {
                term: self.current_term,
                leader: self.node_id.clone(),
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit: self.commit_index,
            },
        }]
    }

    fn handle_append_entries_response(
        &mut self,
        from: NodeId,
        term: u64,
        success: bool,
        match_index: Option<u64>,
        conflict_term: Option<u64>,
        conflict_index: Option<u64>,
    ) -> Vec<Outgoing<RaftMessage<V>>> {
        let _ = conflict_term;
        if !matches!(self.role, Role::Leader) || term != self.current_term {
            return Vec::new();
        }
        if success {
            if let Some(mi) = match_index {
                self.match_index.insert(from.clone(), Some(mi));
                self.next_index.insert(from, mi + 1);
                self.try_advance_commit();
            }
            Vec::new()
        } else {
            let new_next = if let Some(ci) = conflict_index {
                ci
            } else {
                let cur = *self
                    .next_index
                    .get(&from)
                    .unwrap_or(&(self.log.len() as u64));
                cur.saturating_sub(1)
            };
            self.next_index.insert(from.clone(), new_next);
            let prev = if new_next == 0 {
                None
            } else {
                Some(new_next - 1)
            };
            let prev_term = match prev {
                Some(i) => self.log[i as usize].term,
                None => 0,
            };
            let entries: Vec<LogEntry<V>> = self.log[new_next as usize..].to_vec();
            vec![Outgoing {
                target: SendTarget::Peer(from),
                message: RaftMessage::AppendEntries {
                    term: self.current_term,
                    leader: self.node_id.clone(),
                    prev_log_index: prev,
                    prev_log_term: prev_term,
                    entries,
                    leader_commit: self.commit_index,
                },
            }]
        }
    }

    fn try_advance_commit(&mut self) {
        let mut indexes: Vec<Option<u64>> = self.match_index.values().copied().collect();
        indexes.sort_by(|a, b| b.cmp(a));
        let q = self.quorum();
        if indexes.len() < q {
            return;
        }
        let candidate = indexes[q - 1];
        if let Some(c) = candidate {
            let entry_term = self.log[c as usize].term;
            if entry_term == self.current_term && self.commit_index.is_none_or(|cur| c > cur) {
                self.commit_index = Some(c);
                self.apply_committed_entries();
            }
        }
    }

    fn apply_committed_entries(&mut self) {
        let target = match self.commit_index {
            Some(c) => c,
            None => return,
        };
        let start = match self.last_applied {
            Some(a) => a + 1,
            None => 0,
        };
        for idx in start..=target {
            let entry = &self.log[idx as usize];
            self.pending_decisions.push(Decision {
                slot: idx,
                value: entry.value.clone(),
            });
        }
        self.last_applied = Some(target);
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
        if matches!(self.role, Role::Leader) {
            let entry = LogEntry {
                term: self.current_term,
                value,
            };
            let idx = self.log.len() as u64;
            self.log.push(entry.clone());
            if self.pending_persist_log_from.is_none() {
                self.pending_persist_log_from = Some(idx);
            }
            self.match_index.insert(self.node_id.clone(), Some(idx));
            self.try_advance_commit();
            return self.broadcast_with_entries(vec![entry]);
        }
        if let Some(leader) = self.leader.clone() {
            return vec![Outgoing {
                target: SendTarget::Peer(leader),
                message: RaftMessage::Forward { value },
            }];
        }
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
            RaftMessage::AppendEntries {
                term,
                leader,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            } => self.handle_append_entries(
                from,
                term,
                leader,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            ),
            RaftMessage::AppendEntriesResponse {
                term,
                success,
                match_index,
                conflict_term,
                conflict_index,
            } => self.handle_append_entries_response(
                from,
                term,
                success,
                match_index,
                conflict_term,
                conflict_index,
            ),
            RaftMessage::Forward { value } => {
                if matches!(self.role, Role::Leader) {
                    // Treat as a fresh proposal.
                    <RaftProtocol<V> as ConsensusProtocol<V>>::propose(self, value)
                } else if self.leader.as_ref().is_some_and(|l| *l != from) {
                    // Chain-forward to whoever we currently believe is the leader.
                    // The sender's view was stale; ours may still be wrong, but as
                    // long as some node in the chain has accurate state the value
                    // reaches a real leader. Refusing to forward back to `from`
                    // prevents two-node ping-pong loops when both peers disagree
                    // about who is leader.
                    <RaftProtocol<V> as ConsensusProtocol<V>>::propose(self, value)
                } else {
                    // No usable leader to forward to (unknown, or it's the sender).
                    // Buffer so the value drains to whichever leader we next learn
                    // about (via `handle_append_entries` or `become_leader`).
                    self.pending_proposals.push(value);
                    Vec::new()
                }
            }
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

    // A Leader is never idle — it sends periodic heartbeats. Only Followers and
    // Candidates with no log entries and no buffered proposals count as idle.
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

    #[test]
    fn append_entries_resets_election_timer_and_accepts_leader() {
        use crate::message::RaftMessage;
        let mut p = RaftProtocol::<String>::new(NodeId::new("a", 1), 3, RaftConfig::default());
        let leader = NodeId::new("b", 1);
        // Force a known-stale deadline so the reset is observable regardless of
        // how the next randomized sample lands.
        let earlier_deadline = Instant::now() - Duration::from_secs(1);
        p.election_deadline = earlier_deadline;
        let out = p.handle_message(
            leader.clone(),
            RaftMessage::AppendEntries {
                term: 1,
                leader: leader.clone(),
                prev_log_index: None,
                prev_log_term: 0,
                entries: vec![],
                leader_commit: None,
            },
        );
        assert_eq!(p.current_term, 1);
        assert!(matches!(p.role, Role::Follower));
        assert_eq!(p.leader, Some(leader.clone()));
        assert!(p.election_deadline > earlier_deadline);
        match &out[0].message {
            RaftMessage::AppendEntriesResponse {
                success: true,
                term: 1,
                ..
            } => {}
            other => panic!("expected success AER, got {:?}", other),
        }
        match &out[0].target {
            SendTarget::Peer(t) => assert_eq!(t, &leader),
            _ => panic!("expected Peer target"),
        }
    }

    #[test]
    fn append_entries_rejects_lower_term() {
        use crate::message::RaftMessage;
        let mut p = RaftProtocol::<String>::new(NodeId::new("a", 1), 3, RaftConfig::default());
        p.current_term = 5;
        let out = p.handle_message(
            NodeId::new("b", 1),
            RaftMessage::AppendEntries {
                term: 3,
                leader: NodeId::new("b", 1),
                prev_log_index: None,
                prev_log_term: 0,
                entries: vec![],
                leader_commit: None,
            },
        );
        match &out[0].message {
            RaftMessage::AppendEntriesResponse {
                success: false,
                term: 5,
                ..
            } => {}
            _ => panic!("expected rejection with our term"),
        }
    }

    #[test]
    fn append_entries_rejects_log_gap() {
        use crate::message::{LogEntry, RaftMessage};
        let mut p = RaftProtocol::<String>::new(NodeId::new("a", 1), 3, RaftConfig::default());
        let out = p.handle_message(
            NodeId::new("b", 1),
            RaftMessage::AppendEntries {
                term: 1,
                leader: NodeId::new("b", 1),
                prev_log_index: Some(2), // claims our log has 3+ entries; we have 0
                prev_log_term: 1,
                entries: vec![LogEntry {
                    term: 1,
                    value: "x".into(),
                }],
                leader_commit: None,
            },
        );
        match &out[0].message {
            RaftMessage::AppendEntriesResponse { success: false, .. } => {}
            _ => panic!(),
        }
        assert!(p.log.is_empty());
    }

    #[test]
    fn append_entries_rejects_term_mismatch_at_prev() {
        use crate::message::{LogEntry, RaftMessage};
        let mut p = RaftProtocol::<String>::new(NodeId::new("a", 1), 3, RaftConfig::default());
        p.log.push(LogEntry {
            term: 1,
            value: "old".into(),
        });
        p.current_term = 1;
        let out = p.handle_message(
            NodeId::new("b", 1),
            RaftMessage::AppendEntries {
                term: 1,
                leader: NodeId::new("b", 1),
                prev_log_index: Some(0),
                prev_log_term: 5, // mismatch — our log[0].term is 1
                entries: vec![],
                leader_commit: None,
            },
        );
        match &out[0].message {
            RaftMessage::AppendEntriesResponse { success: false, .. } => {}
            _ => panic!(),
        }
    }

    #[test]
    fn append_entries_appends_when_prev_matches() {
        use crate::message::{LogEntry, RaftMessage};
        let mut p = RaftProtocol::<String>::new(NodeId::new("a", 1), 3, RaftConfig::default());
        let _ = p.handle_message(
            NodeId::new("b", 1),
            RaftMessage::AppendEntries {
                term: 1,
                leader: NodeId::new("b", 1),
                prev_log_index: None,
                prev_log_term: 0,
                entries: vec![LogEntry {
                    term: 1,
                    value: "x".into(),
                }],
                leader_commit: None,
            },
        );
        assert_eq!(p.log.len(), 1);
        assert_eq!(p.log[0].value, "x");
        let _ = p.handle_message(
            NodeId::new("b", 1),
            RaftMessage::AppendEntries {
                term: 1,
                leader: NodeId::new("b", 1),
                prev_log_index: Some(0),
                prev_log_term: 1,
                entries: vec![LogEntry {
                    term: 1,
                    value: "y".into(),
                }],
                leader_commit: None,
            },
        );
        assert_eq!(p.log.len(), 2);
        assert_eq!(p.log[1].value, "y");
    }

    #[test]
    fn append_entries_truncates_conflicting_suffix() {
        use crate::message::{LogEntry, RaftMessage};
        let mut p = RaftProtocol::<String>::new(NodeId::new("a", 1), 3, RaftConfig::default());
        p.current_term = 2;
        p.log = vec![
            LogEntry {
                term: 1,
                value: "a".into(),
            },
            LogEntry {
                term: 1,
                value: "stale".into(),
            },
        ];
        let _ = p.handle_message(
            NodeId::new("b", 1),
            RaftMessage::AppendEntries {
                term: 2,
                leader: NodeId::new("b", 1),
                prev_log_index: Some(0),
                prev_log_term: 1,
                entries: vec![LogEntry {
                    term: 2,
                    value: "fresh".into(),
                }],
                leader_commit: None,
            },
        );
        assert_eq!(p.log.len(), 2);
        assert_eq!(p.log[0].value, "a");
        assert_eq!(p.log[1].value, "fresh");
        assert_eq!(p.log[1].term, 2);
    }

    #[test]
    fn empty_heartbeat_match_index_reflects_only_prev_log_index_with_stale_suffix() {
        use crate::message::{LogEntry, RaftMessage};
        // Follower has a matching prefix [0..=2] from term 1 plus an unverified
        // stale suffix [3..=5] from a prior term that the current leader did
        // not produce. A heartbeat with `prev_log_index = Some(2)` must report
        // match_index = Some(2), not Some(5) — otherwise the leader treats the
        // stale suffix as replicated.
        let mut p = RaftProtocol::<String>::new(NodeId::new("a", 1), 3, RaftConfig::default());
        p.current_term = 5;
        p.log = vec![
            LogEntry {
                term: 1,
                value: "0".into(),
            },
            LogEntry {
                term: 1,
                value: "1".into(),
            },
            LogEntry {
                term: 1,
                value: "2".into(),
            },
            // Stale suffix from a prior term — never confirmed by current leader.
            LogEntry {
                term: 2,
                value: "stale-3".into(),
            },
            LogEntry {
                term: 2,
                value: "stale-4".into(),
            },
            LogEntry {
                term: 2,
                value: "stale-5".into(),
            },
        ];
        let leader = NodeId::new("b", 1);
        let out = p.handle_message(
            leader.clone(),
            RaftMessage::AppendEntries {
                term: 5,
                leader: leader.clone(),
                prev_log_index: Some(2),
                prev_log_term: 1,
                entries: vec![],
                leader_commit: None,
            },
        );
        let response = out
            .iter()
            .find_map(|o| match &o.message {
                RaftMessage::AppendEntriesResponse {
                    success,
                    match_index,
                    ..
                } => Some((*success, *match_index)),
                _ => None,
            })
            .expect("expected an AppendEntriesResponse");
        assert!(response.0, "heartbeat with matching prev should succeed");
        assert_eq!(
            response.1,
            Some(2),
            "match_index should reflect only the verified prefix, not the stale suffix"
        );
        // Suffix is intentionally retained — a future non-empty AE will truncate it
        // via the conflict path. Don't assert truncation here.
        assert_eq!(p.log.len(), 6);
    }

    #[test]
    fn append_entries_match_index_excludes_stale_suffix_beyond_request() {
        use crate::message::{LogEntry, RaftMessage};
        // Leader sends prev=2 with two new entries (covering indices 3 and 4).
        // Follower has a longer stale suffix at indices 3..=5. After the loop,
        // indices 3 and 4 match the leader (truncate-and-append on conflict),
        // but index 5 is still unverified. match_index should be 4, not 5.
        let mut p = RaftProtocol::<String>::new(NodeId::new("a", 1), 3, RaftConfig::default());
        p.current_term = 5;
        p.log = vec![
            LogEntry {
                term: 1,
                value: "0".into(),
            },
            LogEntry {
                term: 1,
                value: "1".into(),
            },
            LogEntry {
                term: 1,
                value: "2".into(),
            },
            LogEntry {
                term: 2,
                value: "stale-3".into(),
            },
            LogEntry {
                term: 2,
                value: "stale-4".into(),
            },
            LogEntry {
                term: 2,
                value: "stale-5".into(),
            },
        ];
        let leader = NodeId::new("b", 1);
        let out = p.handle_message(
            leader.clone(),
            RaftMessage::AppendEntries {
                term: 5,
                leader: leader.clone(),
                prev_log_index: Some(2),
                prev_log_term: 1,
                entries: vec![
                    LogEntry {
                        term: 5,
                        value: "new-3".into(),
                    },
                    LogEntry {
                        term: 5,
                        value: "new-4".into(),
                    },
                ],
                leader_commit: None,
            },
        );
        let match_index = out
            .iter()
            .find_map(|o| match &o.message {
                RaftMessage::AppendEntriesResponse { match_index, .. } => Some(*match_index),
                _ => None,
            })
            .expect("expected an AppendEntriesResponse");
        assert_eq!(
            match_index,
            Some(4),
            "match_index should be prev_log_index + entries.len(), not log.len() - 1"
        );
    }

    #[test]
    fn append_entries_match_index_none_for_empty_initial_heartbeat() {
        use crate::message::RaftMessage;
        // First heartbeat from a new leader to a fresh follower: prev_log_index
        // is None and entries is empty. Nothing has been verified by this RPC.
        let mut p = RaftProtocol::<String>::new(NodeId::new("a", 1), 3, RaftConfig::default());
        let leader = NodeId::new("b", 1);
        let out = p.handle_message(
            leader.clone(),
            RaftMessage::AppendEntries {
                term: 1,
                leader: leader.clone(),
                prev_log_index: None,
                prev_log_term: 0,
                entries: vec![],
                leader_commit: None,
            },
        );
        let match_index = out
            .iter()
            .find_map(|o| match &o.message {
                RaftMessage::AppendEntriesResponse { match_index, .. } => Some(*match_index),
                _ => None,
            })
            .expect("expected an AppendEntriesResponse");
        assert_eq!(match_index, None);
    }

    #[test]
    fn append_entries_match_index_when_prev_none_with_entries() {
        use crate::message::{LogEntry, RaftMessage};
        // prev_log_index = None, entries fill indices [0..=1]. Verified up to 1.
        let mut p = RaftProtocol::<String>::new(NodeId::new("a", 1), 3, RaftConfig::default());
        let leader = NodeId::new("b", 1);
        let out = p.handle_message(
            leader.clone(),
            RaftMessage::AppendEntries {
                term: 1,
                leader: leader.clone(),
                prev_log_index: None,
                prev_log_term: 0,
                entries: vec![
                    LogEntry {
                        term: 1,
                        value: "x".into(),
                    },
                    LogEntry {
                        term: 1,
                        value: "y".into(),
                    },
                ],
                leader_commit: None,
            },
        );
        let match_index = out
            .iter()
            .find_map(|o| match &o.message {
                RaftMessage::AppendEntriesResponse { match_index, .. } => Some(*match_index),
                _ => None,
            })
            .expect("expected an AppendEntriesResponse");
        assert_eq!(match_index, Some(1));
    }

    #[test]
    fn append_entries_idempotent_for_already_present_entries() {
        use crate::message::{LogEntry, RaftMessage};
        let mut p = RaftProtocol::<String>::new(NodeId::new("a", 1), 3, RaftConfig::default());
        p.current_term = 1;
        p.log = vec![
            LogEntry {
                term: 1,
                value: "a".into(),
            },
            LogEntry {
                term: 1,
                value: "b".into(),
            },
        ];
        let _ = p.handle_message(
            NodeId::new("b", 1),
            RaftMessage::AppendEntries {
                term: 1,
                leader: NodeId::new("b", 1),
                prev_log_index: Some(0),
                prev_log_term: 1,
                // Re-sends entry at index 1 — already matches our log.
                entries: vec![LogEntry {
                    term: 1,
                    value: "b".into(),
                }],
                leader_commit: None,
            },
        );
        assert_eq!(p.log.len(), 2);
        assert_eq!(p.log[1].value, "b");
    }

    #[test]
    fn leader_commit_advances_commit_index_and_yields_decisions() {
        use crate::message::{LogEntry, RaftMessage};
        let mut p = RaftProtocol::<String>::new(NodeId::new("a", 1), 3, RaftConfig::default());
        let _ = p.handle_message(
            NodeId::new("b", 1),
            RaftMessage::AppendEntries {
                term: 1,
                leader: NodeId::new("b", 1),
                prev_log_index: None,
                prev_log_term: 0,
                entries: vec![
                    LogEntry {
                        term: 1,
                        value: "x".into(),
                    },
                    LogEntry {
                        term: 1,
                        value: "y".into(),
                    },
                ],
                leader_commit: Some(1),
            },
        );
        assert_eq!(p.commit_index, Some(1));
        let decisions = p.take_decisions();
        assert_eq!(decisions.len(), 2);
        assert_eq!(decisions[0].slot, 0);
        assert_eq!(decisions[0].value, "x");
        assert_eq!(decisions[1].slot, 1);
        assert_eq!(decisions[1].value, "y");
    }

    #[test]
    fn leader_propose_appends_to_log_and_emits_append_entries() {
        use crate::message::RaftMessage;
        let me = NodeId::new("a", 1);
        let mut p = RaftProtocol::<String>::new(me.clone(), 3, RaftConfig::default());
        p.role = Role::Leader;
        p.current_term = 1;
        p.leader = Some(me.clone());
        let out =
            <RaftProtocol<String> as ConsensusProtocol<String>>::propose(&mut p, "hello".into());
        assert_eq!(p.log.len(), 1);
        assert_eq!(p.log[0].value, "hello");
        assert_eq!(p.log[0].term, 1);
        assert_eq!(p.match_index.get(&me), Some(&Some(0)));
        assert!(out.iter().any(|o| matches!(&o.message,
            RaftMessage::AppendEntries { entries, .. } if entries.len() == 1 && entries[0].value == "hello"
        )));
    }

    #[test]
    fn leader_advances_commit_on_majority_match() {
        use crate::message::{LogEntry, RaftMessage};
        let me = NodeId::new("a", 1);
        let b = NodeId::new("b", 1);
        let mut p = RaftProtocol::<String>::new(me.clone(), 3, RaftConfig::default());
        p.role = Role::Leader;
        p.current_term = 1;
        p.leader = Some(me.clone());
        p.log.push(LogEntry {
            term: 1,
            value: "x".into(),
        });
        p.match_index.insert(me.clone(), Some(0));
        let _ = p.handle_message(
            b.clone(),
            RaftMessage::AppendEntriesResponse {
                term: 1,
                success: true,
                match_index: Some(0),
                conflict_term: None,
                conflict_index: None,
            },
        );
        assert_eq!(p.commit_index, Some(0));
        let decisions = p.take_decisions();
        assert_eq!(decisions.len(), 1);
        assert_eq!(decisions[0].slot, 0);
        assert_eq!(decisions[0].value, "x");
    }

    #[test]
    fn leader_does_not_commit_entry_from_prior_term() {
        use crate::message::{LogEntry, RaftMessage};
        let me = NodeId::new("a", 1);
        let b = NodeId::new("b", 1);
        let mut p = RaftProtocol::<String>::new(me.clone(), 3, RaftConfig::default());
        p.role = Role::Leader;
        p.current_term = 5;
        p.leader = Some(me.clone());
        p.log.push(LogEntry {
            term: 3,
            value: "old".into(),
        });
        p.match_index.insert(me.clone(), Some(0));
        let _ = p.handle_message(
            b.clone(),
            RaftMessage::AppendEntriesResponse {
                term: 5,
                success: true,
                match_index: Some(0),
                conflict_term: None,
                conflict_index: None,
            },
        );
        assert_eq!(p.commit_index, None);
    }

    #[test]
    fn leader_response_for_old_term_ignored() {
        use crate::message::RaftMessage;
        let me = NodeId::new("a", 1);
        let b = NodeId::new("b", 1);
        let mut p = RaftProtocol::<String>::new(me.clone(), 3, RaftConfig::default());
        p.role = Role::Leader;
        p.current_term = 5;
        p.leader = Some(me.clone());
        let _ = p.handle_message(
            b,
            RaftMessage::AppendEntriesResponse {
                term: 3,
                success: true,
                match_index: Some(0),
                conflict_term: None,
                conflict_index: None,
            },
        );
        assert!(p.match_index.is_empty());
        assert_eq!(p.commit_index, None);
    }

    #[test]
    fn leader_rewinds_next_index_on_rejection() {
        use crate::message::{LogEntry, RaftMessage};
        let me = NodeId::new("a", 1);
        let b = NodeId::new("b", 1);
        let mut p = RaftProtocol::<String>::new(me.clone(), 3, RaftConfig::default());
        p.role = Role::Leader;
        p.current_term = 1;
        p.leader = Some(me.clone());
        p.log = (0..5)
            .map(|i| LogEntry {
                term: 1,
                value: format!("v{}", i),
            })
            .collect();
        p.next_index.insert(b.clone(), 5);
        let out = p.handle_message(
            b.clone(),
            RaftMessage::AppendEntriesResponse {
                term: 1,
                success: false,
                match_index: None,
                conflict_term: None,
                conflict_index: None,
            },
        );
        assert_eq!(p.next_index.get(&b), Some(&4));
        assert!(matches!(&out[0].message, RaftMessage::AppendEntries { .. }));
    }

    #[test]
    fn leader_commit_is_capped_at_last_log_index() {
        use crate::message::{LogEntry, RaftMessage};
        let mut p = RaftProtocol::<String>::new(NodeId::new("a", 1), 3, RaftConfig::default());
        let _ = p.handle_message(
            NodeId::new("b", 1),
            RaftMessage::AppendEntries {
                term: 1,
                leader: NodeId::new("b", 1),
                prev_log_index: None,
                prev_log_term: 0,
                entries: vec![LogEntry {
                    term: 1,
                    value: "x".into(),
                }],
                leader_commit: Some(99), // way past end
            },
        );
        // Our log has only index 0; commit_index should be Some(0), not Some(99).
        assert_eq!(p.commit_index, Some(0));
    }

    #[test]
    fn follower_reports_conflict_index_on_log_gap() {
        use crate::message::RaftMessage;
        let mut p = RaftProtocol::<String>::new(NodeId::new("a", 1), 3, RaftConfig::default());
        p.current_term = 5;
        let out = p.handle_message(
            NodeId::new("b", 1),
            RaftMessage::AppendEntries {
                term: 5,
                leader: NodeId::new("b", 1),
                prev_log_index: Some(3), // gap: our log is empty, claimed prior at index 3
                prev_log_term: 2,
                entries: vec![],
                leader_commit: None,
            },
        );
        match &out[0].message {
            RaftMessage::AppendEntriesResponse {
                success: false,
                conflict_term: None,     // we have no entry at that position
                conflict_index: Some(0), // resume from start
                ..
            } => {}
            other => panic!("expected reject with conflict_index hint, got {:?}", other),
        }
    }

    #[test]
    fn follower_reports_conflict_term_on_term_mismatch_at_prev() {
        use crate::message::{LogEntry, RaftMessage};
        let mut p = RaftProtocol::<String>::new(NodeId::new("a", 1), 3, RaftConfig::default());
        p.current_term = 5;
        p.log = vec![
            LogEntry {
                term: 1,
                value: "a".into(),
            },
            LogEntry {
                term: 2,
                value: "b".into(),
            },
            LogEntry {
                term: 2,
                value: "c".into(),
            },
        ];
        let out = p.handle_message(
            NodeId::new("b", 1),
            RaftMessage::AppendEntries {
                term: 5,
                leader: NodeId::new("b", 1),
                prev_log_index: Some(2),
                prev_log_term: 4, // mismatch: ours at idx 2 has term 2
                entries: vec![],
                leader_commit: None,
            },
        );
        match &out[0].message {
            RaftMessage::AppendEntriesResponse {
                success: false,
                conflict_term: Some(2),
                conflict_index: Some(1), // first index of term 2 in our log
                ..
            } => {}
            other => panic!("expected reject with conflict hints, got {:?}", other),
        }
    }

    #[test]
    fn leader_uses_conflict_index_to_rewind_next_index_in_one_step() {
        use crate::message::{LogEntry, RaftMessage};
        let me = NodeId::new("a", 1);
        let b = NodeId::new("b", 1);
        let mut p = RaftProtocol::<String>::new(me.clone(), 3, RaftConfig::default());
        p.role = Role::Leader;
        p.current_term = 5;
        p.leader = Some(me.clone());
        p.log = (0..10)
            .map(|i| LogEntry {
                term: 5,
                value: format!("v{}", i),
            })
            .collect();
        p.next_index.insert(b.clone(), 9);
        let out = p.handle_message(
            b.clone(),
            RaftMessage::AppendEntriesResponse {
                term: 5,
                success: false,
                match_index: None,
                conflict_term: None,
                conflict_index: Some(2),
            },
        );
        assert_eq!(p.next_index.get(&b), Some(&2));
        match &out[0].message {
            RaftMessage::AppendEntries {
                prev_log_index: Some(1),
                entries,
                ..
            } => {
                assert_eq!(entries.len(), 8); // indices 2..=9
            }
            other => panic!("expected re-send from index 2, got {:?}", other),
        }
    }

    #[test]
    fn drain_persist_intent_returns_pending_state_and_clears_flags() {
        use crate::message::LogEntry;
        let mut p = RaftProtocol::<String>::new(NodeId::new("a", 1), 3, RaftConfig::default());
        p.current_term = 5;
        p.voted_for = Some(NodeId::new("b", 1));
        p.log = vec![
            LogEntry {
                term: 5,
                value: "x".into(),
            },
            LogEntry {
                term: 5,
                value: "y".into(),
            },
        ];
        p.pending_persist_term = true;
        p.pending_persist_voted_for = true;
        p.pending_persist_log_from = Some(1);
        p.pending_truncate_from = Some(1);

        let intent = p.drain_persist_intent();
        assert_eq!(intent.term, Some(5));
        assert_eq!(intent.voted_for, Some(Some(NodeId::new("b", 1))));
        assert_eq!(intent.truncate_from, Some(1));
        assert_eq!(intent.append_from, Some(1));
        assert_eq!(intent.log_snapshot.len(), 2);
        assert!(!p.pending_persist_term);
        assert!(!p.pending_persist_voted_for);
        assert!(p.pending_persist_log_from.is_none());
        assert!(p.pending_truncate_from.is_none());
    }

    #[test]
    fn drain_persist_intent_returns_empty_when_no_pending() {
        let mut p = RaftProtocol::<String>::new(NodeId::new("a", 1), 3, RaftConfig::default());
        let intent = p.drain_persist_intent();
        assert!(intent.term.is_none());
        assert!(intent.voted_for.is_none());
        assert!(intent.truncate_from.is_none());
        assert!(intent.append_from.is_none());
        assert!(intent.log_snapshot.is_empty());
    }

    #[test]
    fn recover_loads_state_correctly() {
        use crate::message::LogEntry;
        let mut p = RaftProtocol::<String>::new(NodeId::new("a", 1), 3, RaftConfig::default());
        p.recover(
            7,
            Some(NodeId::new("b", 1)),
            vec![
                LogEntry {
                    term: 5,
                    value: "x".into(),
                },
                LogEntry {
                    term: 7,
                    value: "y".into(),
                },
            ],
            vec![(0, "x".into())],
        );
        assert_eq!(p.current_term, 7);
        assert_eq!(p.voted_for, Some(NodeId::new("b", 1)));
        assert_eq!(p.log.len(), 2);
        assert_eq!(p.log[0].term, 5);
        assert_eq!(p.log[1].term, 7);
        assert_eq!(p.commit_index, Some(0));
        assert_eq!(p.last_applied, Some(0));
        assert!(matches!(p.role, Role::Follower));
        assert!(!p.pending_persist_term);
        assert!(!p.pending_persist_voted_for);
        assert!(p.pending_persist_log_from.is_none());
        assert!(p.pending_truncate_from.is_none());
    }

    #[test]
    fn recover_with_no_decisions_leaves_commit_index_none() {
        use crate::message::LogEntry;
        let mut p = RaftProtocol::<String>::new(NodeId::new("a", 1), 3, RaftConfig::default());
        p.recover(
            2,
            None,
            vec![LogEntry {
                term: 2,
                value: "x".into(),
            }],
            vec![],
        );
        assert_eq!(p.current_term, 2);
        assert!(p.voted_for.is_none());
        assert_eq!(p.log.len(), 1);
        assert_eq!(p.commit_index, None);
        assert_eq!(p.last_applied, None);
    }

    #[test]
    fn follower_forwards_proposal_to_known_leader() {
        use crate::message::RaftMessage;
        let mut p = RaftProtocol::<String>::new(NodeId::new("a", 1), 3, RaftConfig::default());
        let leader = NodeId::new("b", 1);
        // Receive an AppendEntries from a leader to set p.leader.
        let _ = p.handle_message(
            leader.clone(),
            RaftMessage::AppendEntries {
                term: 1,
                leader: leader.clone(),
                prev_log_index: None,
                prev_log_term: 0,
                entries: vec![],
                leader_commit: None,
            },
        );
        assert_eq!(p.leader, Some(leader.clone()));

        let out =
            <RaftProtocol<String> as ConsensusProtocol<String>>::propose(&mut p, "hello".into());
        assert_eq!(out.len(), 1);
        match (&out[0].target, &out[0].message) {
            (SendTarget::Peer(t), RaftMessage::Forward { value }) => {
                assert_eq!(t, &leader);
                assert_eq!(value, "hello");
            }
            (_, msg) => panic!("expected Forward to leader, got message {:?}", msg),
        }
        assert!(p.pending_proposals.is_empty());
    }

    #[test]
    fn follower_buffers_proposal_when_no_leader_known() {
        let mut p = RaftProtocol::<String>::new(NodeId::new("a", 1), 3, RaftConfig::default());
        assert!(p.leader.is_none());
        let out =
            <RaftProtocol<String> as ConsensusProtocol<String>>::propose(&mut p, "hello".into());
        assert!(out.is_empty());
        assert_eq!(p.pending_proposals.len(), 1);
        assert_eq!(p.pending_proposals[0], "hello");
    }

    #[test]
    fn leader_handles_forwarded_proposal_as_normal_propose() {
        use crate::message::RaftMessage;
        let me = NodeId::new("a", 1);
        let mut p = RaftProtocol::<String>::new(me.clone(), 3, RaftConfig::default());
        p.role = Role::Leader;
        p.current_term = 1;
        p.leader = Some(me.clone());
        let _ = p.handle_message(
            NodeId::new("b", 1),
            RaftMessage::Forward {
                value: "from-b".into(),
            },
        );
        assert_eq!(p.log.len(), 1);
        assert_eq!(p.log[0].value, "from-b");
    }

    #[test]
    fn pending_proposals_drained_as_forwards_when_leader_learned() {
        use crate::message::RaftMessage;
        let mut p = RaftProtocol::<String>::new(NodeId::new("a", 1), 3, RaftConfig::default());
        let _ =
            <RaftProtocol<String> as ConsensusProtocol<String>>::propose(&mut p, "buffered".into());
        assert_eq!(p.pending_proposals.len(), 1);
        let leader = NodeId::new("b", 1);
        let out = p.handle_message(
            leader.clone(),
            RaftMessage::AppendEntries {
                term: 1,
                leader: leader.clone(),
                prev_log_index: None,
                prev_log_term: 0,
                entries: vec![],
                leader_commit: None,
            },
        );
        assert!(p.pending_proposals.is_empty());
        // Out should contain the AER + a Forward
        let has_forward = out
            .iter()
            .any(|o| matches!(&o.message, RaftMessage::Forward { value } if value == "buffered"));
        assert!(has_forward, "expected buffered proposal to be forwarded");
    }

    #[test]
    fn non_leader_buffers_received_forward_when_no_leader_known() {
        use crate::message::RaftMessage;
        let mut p = RaftProtocol::<String>::new(NodeId::new("a", 1), 3, RaftConfig::default());
        // p.role is Follower, p.leader is None
        let out = p.handle_message(
            NodeId::new("b", 1),
            RaftMessage::Forward { value: "x".into() },
        );
        assert!(out.is_empty());
        assert!(p.log.is_empty());
        assert_eq!(p.pending_proposals, vec!["x".to_string()]);
    }

    #[test]
    fn non_leader_chain_forwards_received_forward_to_known_leader() {
        use crate::message::RaftMessage;
        let mut p = RaftProtocol::<String>::new(NodeId::new("a", 1), 3, RaftConfig::default());
        let leader = NodeId::new("c", 1);
        // Establish that we know c is the leader.
        let _ = p.handle_message(
            leader.clone(),
            RaftMessage::AppendEntries {
                term: 1,
                leader: leader.clone(),
                prev_log_index: None,
                prev_log_term: 0,
                entries: vec![],
                leader_commit: None,
            },
        );
        // A different peer (b) forwards to us; we should chain-forward to c.
        let out = p.handle_message(
            NodeId::new("b", 1),
            RaftMessage::Forward {
                value: "relay".into(),
            },
        );
        assert!(p.pending_proposals.is_empty());
        let forward_to_leader = out.iter().any(|o| {
            matches!(&o.target, SendTarget::Peer(t) if t == &leader)
                && matches!(&o.message, RaftMessage::Forward { value } if value == "relay")
        });
        assert!(forward_to_leader, "expected chain-forward to known leader");
    }

    #[test]
    fn non_leader_buffers_forward_when_sender_is_believed_leader() {
        use crate::message::RaftMessage;
        let mut p = RaftProtocol::<String>::new(NodeId::new("a", 1), 3, RaftConfig::default());
        let b = NodeId::new("b", 1);
        // Let p believe b is leader.
        let _ = p.handle_message(
            b.clone(),
            RaftMessage::AppendEntries {
                term: 1,
                leader: b.clone(),
                prev_log_index: None,
                prev_log_term: 0,
                entries: vec![],
                leader_commit: None,
            },
        );
        // b now forwards to us — which would imply b stepped down. Avoid the
        // ping-pong loop by buffering instead of forwarding back to b.
        let out = p.handle_message(
            b.clone(),
            RaftMessage::Forward {
                value: "loop-guard".into(),
            },
        );
        assert!(out.is_empty());
        assert_eq!(p.pending_proposals, vec!["loop-guard".to_string()]);
    }

    #[test]
    fn single_node_raft_starts_as_leader() {
        let p = RaftProtocol::<String>::new(NodeId::new("solo", 1), 1, RaftConfig::default());
        assert!(matches!(p.role, Role::Leader));
        assert_eq!(p.current_term, 1);
        assert_eq!(p.voted_for, Some(p.node_id.clone()));
        assert_eq!(p.leader, Some(p.node_id.clone()));
        assert!(p.pending_persist_term);
        assert!(p.pending_persist_voted_for);
    }

    #[test]
    fn single_node_raft_commits_immediately_on_propose() {
        let mut p = RaftProtocol::<String>::new(NodeId::new("solo", 1), 1, RaftConfig::default());
        let _ =
            <RaftProtocol<String> as ConsensusProtocol<String>>::propose(&mut p, "hello".into());
        assert_eq!(p.commit_index, Some(0));
        let decisions = p.take_decisions();
        assert_eq!(decisions.len(), 1);
        assert_eq!(decisions[0].slot, 0);
        assert_eq!(decisions[0].value, "hello");
    }

    #[test]
    fn three_node_raft_does_not_commit_immediately_on_propose() {
        let mut p = RaftProtocol::<String>::new(NodeId::new("a", 1), 3, RaftConfig::default());
        p.role = Role::Leader;
        p.current_term = 1;
        p.leader = Some(p.node_id.clone());
        let _ =
            <RaftProtocol<String> as ConsensusProtocol<String>>::propose(&mut p, "hello".into());
        // Quorum (2 of 3) not yet reached; only self matches.
        assert_eq!(p.commit_index, None);
        assert!(p.take_decisions().is_empty());
    }

    #[test]
    fn buffered_proposals_drain_when_self_becomes_leader() {
        use crate::message::RaftMessage;
        let me = NodeId::new("a", 1);
        let mut p = RaftProtocol::<String>::new(me.clone(), 3, RaftConfig::default());
        // Buffer two proposals while no leader is known.
        let _ = <RaftProtocol<String> as ConsensusProtocol<String>>::propose(&mut p, "x".into());
        let _ = <RaftProtocol<String> as ConsensusProtocol<String>>::propose(&mut p, "y".into());
        assert_eq!(p.pending_proposals.len(), 2);
        // Trigger an election.
        p.election_deadline = Instant::now() - Duration::from_millis(1);
        let _ = p.on_tick(Instant::now());
        assert!(matches!(p.role, Role::Candidate));
        // Receive a yes vote from one peer (with self = quorum on 3-node).
        let out = p.handle_message(
            NodeId::new("b", 1),
            RaftMessage::RequestVoteResponse {
                term: 1,
                vote_granted: true,
            },
        );
        assert!(matches!(p.role, Role::Leader));
        // Buffered proposals should have been drained into the log.
        assert!(p.pending_proposals.is_empty());
        assert_eq!(p.log.len(), 2);
        assert_eq!(p.log[0].value, "x");
        assert_eq!(p.log[1].value, "y");
        // The outgoing message should be an AppendEntries carrying the two new entries.
        assert_eq!(out.len(), 1);
        match &out[0].message {
            RaftMessage::AppendEntries { entries, .. } => {
                assert_eq!(entries.len(), 2);
                assert_eq!(entries[0].value, "x");
                assert_eq!(entries[1].value, "y");
            }
            _ => panic!("expected AppendEntries with buffered entries"),
        }
    }

    #[test]
    fn leader_falls_back_to_decrement_when_no_conflict_index_hint() {
        use crate::message::{LogEntry, RaftMessage};
        let me = NodeId::new("a", 1);
        let b = NodeId::new("b", 1);
        let mut p = RaftProtocol::<String>::new(me.clone(), 3, RaftConfig::default());
        p.role = Role::Leader;
        p.current_term = 5;
        p.leader = Some(me.clone());
        p.log = (0..5)
            .map(|i| LogEntry {
                term: 5,
                value: format!("v{}", i),
            })
            .collect();
        p.next_index.insert(b.clone(), 5);
        let _ = p.handle_message(
            b.clone(),
            RaftMessage::AppendEntriesResponse {
                term: 5,
                success: false,
                match_index: None,
                conflict_term: None,
                conflict_index: None,
            },
        );
        assert_eq!(p.next_index.get(&b), Some(&4));
    }

    #[test]
    fn leader_does_not_heartbeat_within_interval() {
        let me = NodeId::new("a", 1);
        let mut p = RaftProtocol::<String>::new(me.clone(), 3, RaftConfig::default());
        p.role = Role::Leader;
        p.current_term = 1;
        p.leader = Some(me.clone());
        p.last_heartbeat_sent = Some(Instant::now());
        let out = p.on_tick(Instant::now());
        assert!(
            out.is_empty(),
            "leader should suppress heartbeat within interval"
        );
    }

    #[test]
    fn granting_vote_resets_election_deadline() {
        use crate::message::RaftMessage;
        let mut p = RaftProtocol::<String>::new(NodeId::new("a", 1), 3, RaftConfig::default());
        let earlier = Instant::now() - Duration::from_secs(1);
        p.election_deadline = earlier;
        let candidate = NodeId::new("b", 1);
        let _ = p.handle_message(
            candidate.clone(),
            RaftMessage::RequestVote {
                term: 5,
                candidate,
                last_log_index: None,
                last_log_term: 0,
            },
        );
        assert!(
            p.election_deadline > earlier,
            "deadline should be reset after granting vote"
        );
    }

    #[test]
    fn append_entries_with_conflict_yields_truncate_and_append_intent() {
        use crate::message::{LogEntry, RaftMessage};
        let mut p = RaftProtocol::<String>::new(NodeId::new("a", 1), 3, RaftConfig::default());
        p.current_term = 2;
        p.log = vec![
            LogEntry {
                term: 1,
                value: "a".into(),
            },
            LogEntry {
                term: 1,
                value: "stale".into(),
            },
        ];
        let _ = p.handle_message(
            NodeId::new("b", 1),
            RaftMessage::AppendEntries {
                term: 2,
                leader: NodeId::new("b", 1),
                prev_log_index: Some(0),
                prev_log_term: 1,
                entries: vec![LogEntry {
                    term: 2,
                    value: "fresh".into(),
                }],
                leader_commit: None,
            },
        );
        let intent = p.drain_persist_intent();
        assert_eq!(intent.truncate_from, Some(1));
        assert_eq!(intent.append_from, Some(1));
        assert_eq!(intent.log_snapshot.len(), 2);
        assert_eq!(intent.log_snapshot[1].value, "fresh");
    }

    #[test]
    fn recover_with_partial_decisions_sets_commit_index_correctly() {
        use crate::message::LogEntry;
        let mut p = RaftProtocol::<String>::new(NodeId::new("a", 1), 3, RaftConfig::default());
        let log = (0..5)
            .map(|i| LogEntry {
                term: 1,
                value: format!("v{i}"),
            })
            .collect();
        p.recover(
            1,
            None,
            log,
            vec![(0, "v0".into()), (1, "v1".into()), (2, "v2".into())],
        );
        assert_eq!(p.commit_index, Some(2));
        assert_eq!(p.last_applied, Some(2));
        assert!(p.take_decisions().is_empty());
        let _ = p.on_tick(Instant::now());
        assert!(p.take_decisions().is_empty());
    }

    #[test]
    fn is_idle_false_when_leader() {
        let me = NodeId::new("a", 1);
        let mut p = RaftProtocol::<String>::new(me.clone(), 3, RaftConfig::default());
        p.role = Role::Leader;
        p.leader = Some(me);
        assert!(!p.is_idle(), "leader is never idle");
    }

    #[test]
    fn is_idle_false_when_pending_proposals() {
        let mut p = RaftProtocol::<String>::new(NodeId::new("a", 1), 3, RaftConfig::default());
        p.pending_proposals.push("x".into());
        assert!(!p.is_idle());
    }

    #[test]
    fn is_idle_false_when_log_nonempty() {
        use crate::message::LogEntry;
        let mut p = RaftProtocol::<String>::new(NodeId::new("a", 1), 3, RaftConfig::default());
        p.log.push(LogEntry {
            term: 1,
            value: "x".into(),
        });
        assert!(!p.is_idle());
    }

    #[test]
    fn conflict_hint_uses_first_index_of_term() {
        use crate::message::{LogEntry, RaftMessage};
        let mut p = RaftProtocol::<String>::new(NodeId::new("a", 1), 3, RaftConfig::default());
        p.current_term = 5;
        p.log = vec![
            LogEntry {
                term: 1,
                value: "a".into(),
            },
            LogEntry {
                term: 1,
                value: "b".into(),
            },
            LogEntry {
                term: 2,
                value: "c".into(),
            },
            LogEntry {
                term: 2,
                value: "d".into(),
            },
            LogEntry {
                term: 3,
                value: "e".into(),
            },
        ];
        let out = p.handle_message(
            NodeId::new("b", 1),
            RaftMessage::AppendEntries {
                term: 5,
                leader: NodeId::new("b", 1),
                prev_log_index: Some(4),
                prev_log_term: 99,
                entries: vec![],
                leader_commit: None,
            },
        );
        match &out[0].message {
            RaftMessage::AppendEntriesResponse {
                success: false,
                conflict_term: Some(3),
                conflict_index: Some(4),
                ..
            } => {}
            other => panic!(
                "expected conflict_term=Some(3), conflict_index=Some(4); got {:?}",
                other
            ),
        }
    }

    #[test]
    fn leader_does_not_advance_commit_past_log_end() {
        use crate::message::{LogEntry, RaftMessage};
        let me = NodeId::new("a", 1);
        let b = NodeId::new("b", 1);
        let mut p = RaftProtocol::<String>::new(me.clone(), 3, RaftConfig::default());
        p.role = Role::Leader;
        p.current_term = 1;
        p.leader = Some(me.clone());
        p.log.push(LogEntry {
            term: 1,
            value: "x".into(),
        });
        p.match_index.insert(me.clone(), Some(0));
        let _ = p.handle_message(
            b,
            RaftMessage::AppendEntriesResponse {
                term: 1,
                success: true,
                match_index: Some(99),
                conflict_term: None,
                conflict_index: None,
            },
        );
        assert!(p.commit_index.is_none() || p.commit_index.unwrap() < p.log.len() as u64);
    }

    #[test]
    fn single_node_raft_drains_initial_persist_intent() {
        let mut p = RaftProtocol::<String>::new(NodeId::new("solo", 1), 1, RaftConfig::default());
        let intent = p.drain_persist_intent();
        assert_eq!(intent.term, Some(1));
        assert!(intent.voted_for.is_some());
    }

    #[test]
    fn single_node_recover_from_empty_state_resumes_as_leader() {
        let me = NodeId::new("solo", 1);
        let mut p = RaftProtocol::<String>::new(me.clone(), 1, RaftConfig::default());
        p.recover(0, None, Vec::new(), Vec::new());
        assert!(matches!(p.role, Role::Leader));
        assert_eq!(p.current_term, 1);
        assert_eq!(p.voted_for, Some(me.clone()));
        assert_eq!(p.leader, Some(me));
        let intent = p.drain_persist_intent();
        assert_eq!(intent.term, Some(1));
        assert!(intent.voted_for.is_some());
    }

    #[test]
    fn single_node_recover_preserves_persisted_term_and_log() {
        use crate::message::LogEntry;
        let me = NodeId::new("solo", 1);
        let mut p = RaftProtocol::<String>::new(me.clone(), 1, RaftConfig::default());
        let log = vec![LogEntry {
            term: 1,
            value: "x".into(),
        }];
        p.recover(1, Some(me.clone()), log, vec![(0, "x".into())]);
        assert!(matches!(p.role, Role::Leader));
        assert_eq!(p.current_term, 1);
        assert_eq!(p.commit_index, Some(0));
        assert_eq!(p.last_applied, Some(0));
        // No persist needed — recovered state already matches leader bootstrap.
        let intent = p.drain_persist_intent();
        assert!(intent.term.is_none());
        assert!(intent.voted_for.is_none());
    }

    #[test]
    fn multi_node_recover_resets_to_follower() {
        let me = NodeId::new("a", 1);
        let mut p = RaftProtocol::<String>::new(me.clone(), 3, RaftConfig::default());
        p.role = Role::Leader;
        p.leader = Some(me.clone());
        p.recover(5, Some(me.clone()), Vec::new(), Vec::new());
        assert!(matches!(p.role, Role::Follower));
        assert_eq!(p.leader, None);
        assert_eq!(p.current_term, 5);
    }

    #[test]
    fn old_leader_after_term_bump_chain_forwards() {
        use crate::message::RaftMessage;
        let me = NodeId::new("a", 1);
        let mut p = RaftProtocol::<String>::new(me.clone(), 3, RaftConfig::default());
        p.role = Role::Leader;
        p.current_term = 5;
        p.leader = Some(me.clone());
        // Step down on a higher-term AppendEntries from b. AE sets leader=b.
        let new_leader = NodeId::new("b", 1);
        let _ = p.handle_message(
            new_leader.clone(),
            RaftMessage::AppendEntries {
                term: 7,
                leader: new_leader.clone(),
                prev_log_index: None,
                prev_log_term: 0,
                entries: vec![],
                leader_commit: None,
            },
        );
        assert!(matches!(p.role, Role::Follower));
        assert_eq!(p.leader, Some(new_leader.clone()));
        // c was holding stale state and forwards to us; we should chain-forward
        // to the leader we now know about (b) rather than silently drop.
        let out = p.handle_message(
            NodeId::new("c", 1),
            RaftMessage::Forward {
                value: "delayed".into(),
            },
        );
        assert!(p.log.is_empty());
        let forward_to_b = out.iter().any(|o| {
            matches!(&o.target, SendTarget::Peer(t) if t == &new_leader)
                && matches!(&o.message, RaftMessage::Forward { value } if value == "delayed")
        });
        assert!(forward_to_b, "expected chain-forward to new leader");
    }

    #[test]
    fn stale_leader_steps_down_on_higher_term_heartbeat() {
        use crate::message::RaftMessage;
        let me = NodeId::new("a", 1);
        let mut old = RaftProtocol::<String>::new(me.clone(), 3, RaftConfig::default());
        old.role = Role::Leader;
        old.current_term = 3;
        old.voted_for = Some(me.clone());
        old.leader = Some(me.clone());
        let new_leader = NodeId::new("b", 1);
        let _ = old.handle_message(
            new_leader.clone(),
            RaftMessage::AppendEntries {
                term: 5,
                leader: new_leader.clone(),
                prev_log_index: None,
                prev_log_term: 0,
                entries: vec![],
                leader_commit: None,
            },
        );
        assert!(matches!(old.role, Role::Follower));
        assert_eq!(old.current_term, 5);
        assert_eq!(old.leader, Some(new_leader));
    }

    #[test]
    fn peek_state_reports_raft_state() {
        use crate::message::LogEntry;
        let mut p = RaftProtocol::<String>::new(NodeId::new("a", 1), 3, RaftConfig::default());
        p.current_term = 4;
        p.log.push(LogEntry {
            term: 4,
            value: "x".into(),
        });
        p.commit_index = Some(0);
        p.last_applied = Some(0);
        let snap = p.peek_state();
        assert_eq!(snap.term, 4);
        assert_eq!(snap.log_len, 1);
        assert_eq!(snap.commit_index, Some(0));
        assert_eq!(snap.last_applied, Some(0));
        assert!(matches!(snap.role, Some(crate::node::NodeRole::Follower)));
    }
}
