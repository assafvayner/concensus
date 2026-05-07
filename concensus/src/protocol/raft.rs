use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use rand::RngExt;
use serde::{de::DeserializeOwned, Serialize};

use crate::config::{NodeId, RaftConfig};
use crate::error::StorageError;
use crate::message::{LogEntry, RaftMessage, MAX_FORWARD_HOPS};
use crate::protocol::{ConsensusProtocol, Decision, Outgoing, SendTarget};
use crate::storage::RaftStorage;

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
    /// `Some(value)` when commit_index has changed and needs to be persisted.
    /// The inner `Option<u64>` is the value to write (may itself be `None`).
    pub commit_index: Option<Option<u64>>,
}

/// Raft peers begin in term 0 as followers; single-node clusters complete a
/// one-vote election on the first tick. `recover()` resumes leadership for a
/// lone peer only when persisted state shows it already won an election in the
/// loaded term (`term > 0` and `voted_for` is self).
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
    pub(crate) pending_persist_commit_index: bool,

    // Buffered proposals from before a leader is known (drained on AppendEntries).
    pub(crate) pending_proposals: Vec<V>,

    // Proposals surfaced via `take_lost_proposals`. We populate this when a
    // follower's election timer fires while pending_proposals is non-empty —
    // the buffered values' presumed leader is gone, and the Node should
    // re-attempt routing through `propose` (which will forward to whichever
    // leader we next learn about, or buffer again if we still don't know).
    pub(crate) lost_proposals: Vec<V>,

    // Owned storage backend. Persists term, voted_for, log entries, and decisions.
    storage: Box<dyn RaftStorage<V> + Send + Sync>,
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
    pub(crate) fn new(
        node_id: NodeId,
        total_nodes: usize,
        config: RaftConfig,
        storage: Box<dyn RaftStorage<V> + Send + Sync>,
    ) -> Self {
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
            pending_persist_commit_index: false,
            pending_proposals: Vec::new(),
            lost_proposals: Vec::new(),
            storage,
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

    /// Restore persistent state at startup from explicit values. Used by the
    /// async [`recover`](Self::recover) wrapper after it loads through owned
    /// storage, and directly by property tests that simulate restarts from
    /// arbitrary external state.
    ///
    /// `commit_index` is set to `max(persisted_commit_index, max_decided_slot)`.
    /// Persisted commit_index closes the recovery window where a crash between
    /// log persistence and decision persistence would have rolled commit_index
    /// back; `max_decided_slot` is the safety floor for older storage backends
    /// that don't yet persist commit_index. `last_applied` is set to
    /// `max_decided_slot`, so the slots between `max_decided_slot+1` and
    /// `commit_index` are re-applied on the next event-loop iteration —
    /// consumers see at-least-once delivery (see `Decided` doc).
    ///
    /// Role normally resets to Follower: a freshly-started node cannot assume
    /// it is still leader, and the election timeout will trigger a fresh
    /// election if needed. For a single-node cluster, resume as Leader only
    /// when durable state shows this node already won an election in the
    /// loaded term (`term > 0` and `voted_for == self`); otherwise stay
    /// Follower at `term` until the event loop runs an election.
    pub(crate) fn restore_state(
        &mut self,
        term: u64,
        voted_for: Option<NodeId>,
        log: Vec<LogEntry<V>>,
        decisions: Vec<(u64, V)>,
        persisted_commit_index: Option<u64>,
    ) {
        self.current_term = term;
        self.voted_for = voted_for;
        self.log = log;
        let max_decided = decisions.iter().map(|(s, _)| *s).max();
        self.commit_index = match (persisted_commit_index, max_decided) {
            (Some(p), Some(d)) => Some(p.max(d)),
            (Some(p), None) => Some(p),
            (None, Some(d)) => Some(d),
            (None, None) => None,
        };
        // Cap commit_index at the last log entry — a persisted value past the
        // end of the log would crash `apply_committed_entries`. This shouldn't
        // happen under normal operation (commit_index is only persisted after
        // the corresponding log append is flushed) but a corrupted backing
        // store should not panic the protocol.
        if let Some(c) = self.commit_index {
            let last = self.log.len().checked_sub(1).map(|i| i as u64);
            self.commit_index = last.map(|l| c.min(l));
        }
        self.last_applied = max_decided;
        self.election_deadline = Instant::now() + sample_election_timeout(&self.config);
        self.pending_persist_term = false;
        self.pending_persist_voted_for = false;
        self.pending_persist_log_from = None;
        self.pending_truncate_from = None;
        self.pending_persist_commit_index = false;
        self.pending_decisions.clear();
        self.pending_proposals.clear();
        self.lost_proposals.clear();
        self.votes_received.clear();
        self.next_index.clear();
        self.match_index.clear();

        if self.total_nodes == 1 {
            let resume_leader =
                self.current_term > 0 && self.voted_for.as_ref() == Some(&self.node_id);
            if resume_leader {
                self.role = Role::Leader;
                self.leader = Some(self.node_id.clone());
                // Seed match_index[self] so a future call to try_advance_commit
                // (e.g. after a refactor) correctly observes the local replica.
                if let Some(last) = self.log.len().checked_sub(1) {
                    self.match_index
                        .insert(self.node_id.clone(), Some(last as u64));
                }
            } else {
                self.role = Role::Follower;
                self.leader = None;
            }
        } else {
            self.role = Role::Follower;
            self.leader = None;
        }
    }

    /// Load persisted state from owned storage and seed in-memory state via
    /// [`restore_state`](Self::restore_state). Called once at startup before
    /// the event loop begins.
    pub(crate) async fn recover(&mut self) -> Result<(), StorageError> {
        let term = self.storage.load_term().await?;
        let voted_for = self.storage.load_voted_for().await?;
        let log = self.storage.load_log().await?;
        let decisions = self.storage.load_decisions().await?;
        let commit_index = self.storage.load_commit_index().await?;
        self.restore_state(term, voted_for, log, decisions, commit_index);
        Ok(())
    }

    /// Drain the pending persistence intent and write it through owned
    /// storage. Order matters: term -> voted_for -> truncate -> append ->
    /// commit_index. Truncating before appending guarantees we never briefly
    /// persist entries that conflict with what's about to be truncated.
    /// Persisting commit_index last guarantees the entries it points at are
    /// already durable.
    pub(crate) async fn flush_persist(&mut self) -> Result<(), StorageError> {
        let intent = self.drain_persist_intent();
        if let Some(term) = intent.term {
            self.storage.save_term(term).await?;
        }
        if let Some(vf) = intent.voted_for {
            self.storage.save_voted_for(vf).await?;
        }
        if let Some(idx) = intent.truncate_from {
            self.storage.truncate_log_from(idx).await?;
        }
        if let Some(idx) = intent.append_from {
            let to_append = &intent.log_snapshot[idx as usize..];
            self.storage.append_log(to_append).await?;
        }
        if let Some(ci) = intent.commit_index {
            self.storage.save_commit_index(ci).await?;
        }
        Ok(())
    }

    /// Drain the in-memory pending decisions, persisting each through owned
    /// storage before returning the list to the caller.
    pub(crate) async fn drain_decisions(&mut self) -> Result<Vec<Decision<V>>, StorageError> {
        let decisions = self.take_decisions();
        for d in &decisions {
            self.storage.save_decision(d.slot, d.value.clone()).await?;
        }
        Ok(decisions)
    }

    /// Drain the pending persistence intent — used internally by
    /// [`flush_persist`](Self::flush_persist) and exposed for property tests
    /// that manage persistence externally. Clears the underlying flags so
    /// subsequent calls return empty intents until new state changes occur.
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
        let commit_index = if self.pending_persist_commit_index {
            Some(self.commit_index)
        } else {
            None
        };
        self.pending_persist_term = false;
        self.pending_persist_voted_for = false;
        self.pending_persist_commit_index = false;
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
            commit_index,
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
        // Clear leader-side persistence intent. Any uncommitted entries this
        // node appended as a candidate/leader stay in `self.log`; if a new
        // leader's AppendEntries conflicts they'll be truncated, otherwise
        // they'll be re-flagged for persistence by the conflict-loop in
        // `handle_append_entries`. Carrying the intent across a role change
        // would persist entries we may immediately truncate.
        self.pending_persist_log_from = None;
    }

    fn start_election(&mut self) -> Vec<Outgoing<RaftMessage<V>>> {
        // Was this a re-election (we were already a Candidate)? If so, the
        // previous election didn't reach quorum — drain buffered proposals
        // to lost_proposals so the Node can re-route them. The first
        // Follower→Candidate transition keeps proposals buffered so they can
        // still land in our own log if we win immediately.
        let was_candidate = matches!(self.role, Role::Candidate);
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
        if self.votes_received.len() >= self.quorum() {
            // Immediate-quorum fast-path (e.g., single-node cluster):
            // become_leader will consume pending_proposals into the log, so
            // don't surface them as lost.
            return self.become_leader();
        }
        if was_candidate {
            self.lost_proposals
                .extend(std::mem::take(&mut self.pending_proposals));
        }
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
        // Reject same-term AppendEntries when we ourselves are Leader of this
        // term. Per Election Safety (§5.4.1), at most one node can be leader
        // in any term, so this branch is only reachable on a protocol
        // violation (or a buggy/malicious peer). Without this guard we would
        // fall through, set `role = Follower`, and potentially truncate our
        // own log — which a leader is paper-bound never to do.
        if term == self.current_term && matches!(self.role, Role::Leader) {
            tracing::warn!(
                term,
                peer = %leader,
                self_id = %self.node_id,
                "AppendEntries from another leader in same term — Election Safety violation; rejecting"
            );
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
                // Unreachable in practice: `prev_ok` is unconditionally true
                // when `prev_log_index = None` (see the match a few lines up),
                // so this arm of the `if !prev_ok` block can't fire. Kept for
                // exhaustiveness; the hint mirrors what we'd want if the
                // upstream check ever changed.
                None => (None, Some(self.log.len() as u64)),
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

        // The last index this RPC actually verified — `prev_log_index` plus
        // the entries we just placed. The follower's local log may extend
        // beyond that with a stale suffix from a prior leader (e.g., when the
        // current RPC is an empty heartbeat that only verifies up to
        // `prev_log_index`). We proactively truncate that suffix below;
        // capping commit_index at this verified index defends against
        // committing entries the cluster never agreed on.
        let match_idx = match (prev_log_index, new_entries_len) {
            (Some(p), n) => Some(p + n as u64),
            (None, 0) => None,
            (None, n) => Some(n as u64 - 1),
        };

        // Truncate any unverified suffix past the last verified index. This
        // simplifies recovery (the in-memory log no longer carries entries a
        // future leader hasn't endorsed) and means `match_idx` and
        // `self.log.len() - 1` agree after this point. When `match_idx ==
        // None` (no `prev_log_index` and no entries) we leave the log as-is —
        // nothing was verified, so we have no basis to truncate.
        if let Some(last) = match_idx {
            let cutoff = (last + 1) as usize;
            if self.log.len() > cutoff {
                self.log.truncate(cutoff);
                let cutoff_u64 = cutoff as u64;
                self.pending_truncate_from = Some(match self.pending_truncate_from {
                    Some(prev) => prev.min(cutoff_u64),
                    None => cutoff_u64,
                });
            }
        }

        // Update commit_index from leader_commit, capped at the last verified
        // index. Skip entirely when nothing was verified (None case).
        if let (Some(lc), Some(last)) = (leader_commit, match_idx) {
            let new_ci = lc.min(last);
            if self.commit_index.is_none_or(|c| new_ci > c) {
                self.commit_index = Some(new_ci);
                self.pending_persist_commit_index = true;
                self.apply_committed_entries();
            }
        }

        // Report only the verified prefix as match_index.
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
            // hops resets to 0 — these proposals were buffered locally and
            // are now being forwarded fresh, not relayed.
            all.push(Outgoing {
                target: SendTarget::Peer(leader.clone()),
                message: RaftMessage::Forward { value, hops: 0 },
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
        if !matches!(self.role, Role::Leader) || term != self.current_term {
            return Vec::new();
        }
        if success {
            if let Some(mi) = match_index {
                // Clamp at our own log length. A leader never overwrites or
                // truncates its own log (Figure 2), so any honest peer's
                // match_index for our current term must be < self.log.len().
                // A buggy or malicious peer reporting a larger value would
                // crash `try_advance_commit` on indexing without this guard.
                let log_len = self.log.len() as u64;
                if log_len == 0 || mi >= log_len {
                    tracing::warn!(
                        peer = %from,
                        reported = mi,
                        log_len,
                        "AppendEntriesResponse reports match_index past leader's log; ignoring"
                    );
                    return Vec::new();
                }
                self.match_index.insert(from.clone(), Some(mi));
                self.next_index.insert(from, mi + 1);
                self.try_advance_commit();
            }
            Vec::new()
        } else {
            let log_len = self.log.len() as u64;
            let new_next = if let Some(ci) = conflict_index {
                if let Some(ct) = conflict_term {
                    if let Some(last_same_term_idx) = self
                        .log
                        .iter()
                        .enumerate()
                        .rev()
                        .find(|(_, e)| e.term == ct)
                        .map(|(i, _)| i as u64)
                    {
                        last_same_term_idx.saturating_add(1)
                    } else {
                        ci
                    }
                } else {
                    ci
                }
            } else {
                let cur = *self.next_index.get(&from).unwrap_or(&log_len);
                cur.saturating_sub(1)
            };
            let new_next = new_next.min(log_len);
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
            // Defensive: a buggy peer could report a match_index past the
            // leader's own log. The response handler clamps, but guard here
            // too — `try_advance_commit` is also reachable from `propose`
            // and `become_leader` for the leader's own match_index.
            let Some(entry) = self.log.get(c as usize) else {
                tracing::warn!(
                    candidate = c,
                    log_len = self.log.len(),
                    "try_advance_commit: candidate index past end of log; ignoring"
                );
                return;
            };
            let entry_term = entry.term;
            if entry_term == self.current_term && self.commit_index.is_none_or(|cur| c > cur) {
                self.commit_index = Some(c);
                self.pending_persist_commit_index = true;
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
                message: RaftMessage::Forward { value, hops: 0 },
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
            RaftMessage::Forward { value, hops } => {
                if matches!(self.role, Role::Leader) {
                    // Chain ends here — treat as a fresh proposal.
                    <RaftProtocol<V> as ConsensusProtocol<V>>::propose(self, value)
                } else if self.leader.as_ref().is_some_and(|l| *l != from) {
                    // Chain-forward to whoever we currently believe is the
                    // leader. The `from != leader` check breaks 2-node
                    // ping-pong loops; the hops TTL bounds 3+ node loops.
                    let next_hops = hops.saturating_add(1);
                    if next_hops >= MAX_FORWARD_HOPS {
                        tracing::warn!(
                            hops,
                            self_id = %self.node_id,
                            sender = %from,
                            "dropping Forward — exceeded MAX_FORWARD_HOPS"
                        );
                        return Vec::new();
                    }
                    let leader = self.leader.clone().unwrap();
                    vec![Outgoing {
                        target: SendTarget::Peer(leader),
                        message: RaftMessage::Forward {
                            value,
                            hops: next_hops,
                        },
                    }]
                } else {
                    // No usable leader to forward to (unknown, or it's the
                    // sender). Buffer so the value drains to whichever leader
                    // we next learn about (via `handle_append_entries` or
                    // `become_leader`). The hops counter is dropped — the
                    // buffered value is logically a fresh proposal from this
                    // node's perspective and will be re-forwarded with hops=0.
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
        std::mem::take(&mut self.lost_proposals)
    }

    // A Leader is never idle — it sends periodic heartbeats. Only Followers and
    // Candidates with no log entries and no buffered proposals count as idle.
    fn is_idle(&self) -> bool {
        self.log.is_empty()
            && self.pending_proposals.is_empty()
            && self.lost_proposals.is_empty()
            && matches!(self.role, Role::Follower | Role::Candidate)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{NodeId, RaftConfig};
    use crate::storage::RaftMemoryStorage;

    fn raft<V>(node_id: NodeId, total_nodes: usize, config: RaftConfig) -> RaftProtocol<V>
    where
        V: serde::Serialize
            + serde::de::DeserializeOwned
            + Clone
            + Send
            + Sync
            + PartialEq
            + 'static,
    {
        RaftProtocol::new(
            node_id,
            total_nodes,
            config,
            Box::new(RaftMemoryStorage::<V>::new()),
        )
    }

    #[test]
    fn new_raft_protocol_starts_as_follower() {
        let proto = raft::<String>(NodeId::new("a", 1), 3, RaftConfig::default());
        assert!(matches!(proto.role, Role::Follower));
        assert_eq!(proto.current_term, 0);
        assert!(proto.voted_for.is_none());
    }

    #[test]
    fn is_idle_when_log_empty() {
        let proto = raft::<String>(NodeId::new("a", 1), 3, RaftConfig::default());
        assert!(proto.is_idle());
    }

    #[test]
    fn quorum_for_three_nodes() {
        let proto = raft::<String>(NodeId::new("a", 1), 3, RaftConfig::default());
        assert_eq!(proto.quorum(), 2);
    }

    #[test]
    fn quorum_for_five_nodes() {
        let proto = raft::<String>(NodeId::new("a", 1), 5, RaftConfig::default());
        assert_eq!(proto.quorum(), 3);
    }

    #[test]
    fn election_timeout_starts_election() {
        use crate::message::RaftMessage;
        let mut p = raft::<String>(NodeId::new("a", 1), 3, RaftConfig::default());
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
        let mut p = raft::<String>(NodeId::new("a", 1), 3, RaftConfig::default());
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
        let mut p = raft::<String>(NodeId::new("a", 1), 3, RaftConfig::default());
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
        let mut p = raft::<String>(NodeId::new("a", 1), 3, RaftConfig::default());
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
        let mut p = raft::<String>(NodeId::new("a", 1), 3, RaftConfig::default());
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
        let mut p = raft::<String>(me.clone(), 3, RaftConfig::default());
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
        let mut p = raft::<String>(NodeId::new("a", 1), 3, RaftConfig::default());
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
        let mut p = raft::<String>(NodeId::new("a", 1), 3, RaftConfig::default());
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
        let mut p = raft::<String>(NodeId::new("a", 1), 3, RaftConfig::default());
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
        let mut p = raft::<String>(NodeId::new("a", 1), 3, RaftConfig::default());
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
        let mut p = raft::<String>(NodeId::new("a", 1), 3, RaftConfig::default());
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
        let mut p = raft::<String>(NodeId::new("a", 1), 3, RaftConfig::default());
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
        let mut p = raft::<String>(NodeId::new("a", 1), 3, RaftConfig::default());
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
        let mut p = raft::<String>(NodeId::new("a", 1), 3, RaftConfig::default());
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
        let mut p = raft::<String>(NodeId::new("a", 1), 3, RaftConfig::default());
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
        // M1: stale suffix is proactively truncated on successful AppendEntries
        // to keep the log == verified prefix. Prior behavior was to retain the
        // suffix and rely on a future conflict to truncate it.
        assert_eq!(p.log.len(), 3);
    }

    #[test]
    fn append_entries_match_index_excludes_stale_suffix_beyond_request() {
        use crate::message::{LogEntry, RaftMessage};
        // Leader sends prev=2 with two new entries (covering indices 3 and 4).
        // Follower has a longer stale suffix at indices 3..=5. After the loop,
        // indices 3 and 4 match the leader (truncate-and-append on conflict),
        // but index 5 is still unverified. match_index should be 4, not 5.
        let mut p = raft::<String>(NodeId::new("a", 1), 3, RaftConfig::default());
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
        let mut p = raft::<String>(NodeId::new("a", 1), 3, RaftConfig::default());
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
        let mut p = raft::<String>(NodeId::new("a", 1), 3, RaftConfig::default());
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
        let mut p = raft::<String>(NodeId::new("a", 1), 3, RaftConfig::default());
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
        let mut p = raft::<String>(NodeId::new("a", 1), 3, RaftConfig::default());
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
        let mut p = raft::<String>(me.clone(), 3, RaftConfig::default());
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
        let mut p = raft::<String>(me.clone(), 3, RaftConfig::default());
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
        let mut p = raft::<String>(me.clone(), 3, RaftConfig::default());
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
        let mut p = raft::<String>(me.clone(), 3, RaftConfig::default());
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
        let mut p = raft::<String>(me.clone(), 3, RaftConfig::default());
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
        let mut p = raft::<String>(NodeId::new("a", 1), 3, RaftConfig::default());
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
    fn empty_heartbeat_does_not_commit_unverified_stale_suffix() {
        // Regression: a heartbeat with `leader_commit > prev_log_index` must
        // not advance commit_index over a stale suffix the new leader hasn't
        // verified. Prior code capped at `self.log.len() - 1`, allowing the
        // follower to commit entries from a deposed leader's term.
        use crate::message::{LogEntry, RaftMessage};
        let mut p = raft::<String>(NodeId::new("f", 1), 3, RaftConfig::default());
        p.current_term = 5;
        // Stale suffix from a deposed leader: indices 0..=4 with terms 1,1,1,2,2.
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
                term: 1,
                value: "c".into(),
            },
            LogEntry {
                term: 2,
                value: "d".into(),
            },
            LogEntry {
                term: 2,
                value: "e".into(),
            },
        ];
        // New leader L2 in term 5 sends a heartbeat verifying only up to index 2,
        // but reports leader_commit=3 (a value committed via a different quorum).
        let _ = p.handle_message(
            NodeId::new("L2", 1),
            RaftMessage::AppendEntries {
                term: 5,
                leader: NodeId::new("L2", 1),
                prev_log_index: Some(2),
                prev_log_term: 1,
                entries: vec![],
                leader_commit: Some(3),
            },
        );
        // commit_index must be capped at the verified index (2), NOT at
        // leader_commit (3) — index 3 in our log is the stale `T2:d`, which
        // L2 did not commit.
        assert_eq!(p.commit_index, Some(2));
    }

    #[test]
    fn empty_heartbeat_with_no_prev_does_not_commit() {
        // When prev_log_index is None and entries is empty, nothing is
        // verified — commit_index must not advance even if the local log is
        // non-empty (stale suffix case at the very start of the log).
        use crate::message::{LogEntry, RaftMessage};
        let mut p = raft::<String>(NodeId::new("f", 1), 3, RaftConfig::default());
        p.current_term = 5;
        p.log = vec![LogEntry {
            term: 2,
            value: "stale".into(),
        }];
        let _ = p.handle_message(
            NodeId::new("L2", 1),
            RaftMessage::AppendEntries {
                term: 5,
                leader: NodeId::new("L2", 1),
                prev_log_index: None,
                prev_log_term: 0,
                entries: vec![],
                leader_commit: Some(0),
            },
        );
        assert_eq!(p.commit_index, None);
    }

    #[test]
    fn successful_heartbeat_truncates_unverified_stale_suffix() {
        // M1: a successful AppendEntries (including an empty heartbeat) should
        // truncate any stale suffix beyond the verified prefix, so the log
        // matches the leader's view exactly.
        use crate::message::{LogEntry, RaftMessage};
        let mut p = raft::<String>(NodeId::new("f", 1), 3, RaftConfig::default());
        p.current_term = 5;
        // 5 entries; only the first 3 are confirmable by the new leader.
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
                term: 1,
                value: "c".into(),
            },
            LogEntry {
                term: 2,
                value: "stale-d".into(),
            },
            LogEntry {
                term: 2,
                value: "stale-e".into(),
            },
        ];
        let _ = p.handle_message(
            NodeId::new("L2", 1),
            RaftMessage::AppendEntries {
                term: 5,
                leader: NodeId::new("L2", 1),
                prev_log_index: Some(2),
                prev_log_term: 1,
                entries: vec![],
                leader_commit: None,
            },
        );
        assert_eq!(p.log.len(), 3, "stale suffix should be truncated");
        assert_eq!(p.pending_truncate_from, Some(3));
    }

    #[test]
    fn successful_appendentries_truncates_suffix_past_new_entries() {
        // M1: when AE includes new entries that overlap an existing suffix,
        // any extra suffix beyond the new entries is also truncated.
        use crate::message::{LogEntry, RaftMessage};
        let mut p = raft::<String>(NodeId::new("f", 1), 3, RaftConfig::default());
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
                term: 1,
                value: "c".into(),
            },
            LogEntry {
                term: 2,
                value: "stale-d".into(),
            },
            LogEntry {
                term: 2,
                value: "stale-e".into(),
            },
            LogEntry {
                term: 2,
                value: "stale-f".into(),
            },
        ];
        let _ = p.handle_message(
            NodeId::new("L2", 1),
            RaftMessage::AppendEntries {
                term: 5,
                leader: NodeId::new("L2", 1),
                prev_log_index: Some(2),
                prev_log_term: 1,
                entries: vec![LogEntry {
                    term: 5,
                    value: "new-d".into(),
                }],
                leader_commit: None,
            },
        );
        // Verified through index 3 (prev=2 + 1 new entry); indices 4, 5 truncated.
        assert_eq!(p.log.len(), 4);
        assert_eq!(p.log[3].term, 5);
        assert_eq!(p.log[3].value, "new-d");
    }

    #[test]
    fn leader_ignores_match_index_past_own_log() {
        // M2: a peer reporting match_index >= self.log.len() must not crash
        // try_advance_commit. The leader simply ignores the response.
        use crate::message::RaftMessage;
        let me = NodeId::new("a", 1);
        let b = NodeId::new("b", 1);
        let mut p = raft::<String>(me.clone(), 3, RaftConfig::default());
        p.role = Role::Leader;
        p.current_term = 1;
        p.leader = Some(me.clone());
        // No log entries, but peer claims match_index = u64::MAX.
        let out = p.handle_message(
            b,
            RaftMessage::AppendEntriesResponse {
                term: 1,
                success: true,
                match_index: Some(u64::MAX),
                conflict_term: None,
                conflict_index: None,
            },
        );
        assert!(out.is_empty());
        assert_eq!(p.commit_index, None);
        assert!(p.match_index.values().all(|v| *v != Some(u64::MAX)));
    }

    #[test]
    fn leader_rejects_same_term_appendentries_from_other_leader() {
        // M4: an AppendEntries arriving in the same term while we are also
        // Leader is an Election Safety violation. Reject without truncating.
        use crate::message::{LogEntry, RaftMessage};
        let me = NodeId::new("a", 1);
        let other = NodeId::new("rogue", 1);
        let mut p = raft::<String>(me.clone(), 3, RaftConfig::default());
        p.role = Role::Leader;
        p.current_term = 5;
        p.leader = Some(me.clone());
        p.log = vec![LogEntry {
            term: 5,
            value: "ours".into(),
        }];
        let out = p.handle_message(
            other.clone(),
            RaftMessage::AppendEntries {
                term: 5,
                leader: other.clone(),
                prev_log_index: None,
                prev_log_term: 0,
                entries: vec![LogEntry {
                    term: 5,
                    value: "theirs".into(),
                }],
                leader_commit: None,
            },
        );
        // Still leader, log untouched.
        assert!(matches!(p.role, Role::Leader));
        assert_eq!(p.log.len(), 1);
        assert_eq!(p.log[0].value, "ours");
        // Response is a rejection.
        match &out[0].message {
            RaftMessage::AppendEntriesResponse {
                success: false,
                term: 5,
                ..
            } => {}
            other => panic!("expected rejection, got {:?}", other),
        }
    }

    #[test]
    fn re_election_drains_pending_proposals_to_lost() {
        // L3: when a candidate's election times out without quorum, buffered
        // proposals are surfaced to lost_proposals so the Node can re-route.
        let mut p = raft::<String>(NodeId::new("a", 1), 5, RaftConfig::default());
        let _ = <RaftProtocol<String> as ConsensusProtocol<String>>::propose(&mut p, "x".into());
        let _ = <RaftProtocol<String> as ConsensusProtocol<String>>::propose(&mut p, "y".into());
        assert_eq!(p.pending_proposals.len(), 2);
        // First election: Follower → Candidate. Pending proposals retained.
        p.election_deadline = Instant::now() - Duration::from_millis(1);
        let _ = p.on_tick(Instant::now());
        assert!(matches!(p.role, Role::Candidate));
        assert_eq!(p.pending_proposals.len(), 2);
        // Second election (still no quorum): Candidate → Candidate.
        // Pending proposals drained to lost_proposals.
        p.election_deadline = Instant::now() - Duration::from_millis(1);
        let _ = p.on_tick(Instant::now());
        assert!(matches!(p.role, Role::Candidate));
        assert!(p.pending_proposals.is_empty());
        let lost = <RaftProtocol<String> as ConsensusProtocol<String>>::take_lost_proposals(&mut p);
        assert_eq!(lost, vec!["x".to_string(), "y".to_string()]);
        // Subsequent take returns empty.
        assert!(
            <RaftProtocol<String> as ConsensusProtocol<String>>::take_lost_proposals(&mut p)
                .is_empty()
        );
    }

    #[test]
    fn forward_chain_drops_after_max_hops() {
        // L4: a Forward arriving with hops near the cap is dropped instead of
        // re-forwarded indefinitely.
        use crate::message::{RaftMessage, MAX_FORWARD_HOPS};
        let mut p = raft::<String>(NodeId::new("a", 1), 3, RaftConfig::default());
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
        // A Forward that would chain to MAX_FORWARD_HOPS or higher must be dropped.
        let out = p.handle_message(
            NodeId::new("b", 1),
            RaftMessage::Forward {
                value: "ttl".into(),
                hops: MAX_FORWARD_HOPS - 1,
            },
        );
        assert!(out.is_empty(), "Forward at TTL should be dropped");
        assert!(p.pending_proposals.is_empty());
    }

    #[test]
    fn forward_chain_increments_hops_below_max() {
        // L4: a Forward below the hop cap is re-forwarded with hops + 1.
        use crate::message::RaftMessage;
        let mut p = raft::<String>(NodeId::new("a", 1), 3, RaftConfig::default());
        let leader = NodeId::new("c", 1);
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
        let out = p.handle_message(
            NodeId::new("b", 1),
            RaftMessage::Forward {
                value: "relay".into(),
                hops: 1,
            },
        );
        let forwarded = out.iter().find_map(|o| match (&o.target, &o.message) {
            (SendTarget::Peer(t), RaftMessage::Forward { value, hops }) if t == &leader => {
                Some((value.clone(), *hops))
            }
            _ => None,
        });
        assert_eq!(forwarded, Some(("relay".into(), 2)));
    }

    #[test]
    fn forward_serde_roundtrip_with_hops() {
        // L4: serialized Forward includes hops; older payloads without hops
        // deserialize to hops=0 thanks to #[serde(default)].
        use crate::message::{RaftMessage, WireVariant};
        let msg: WireVariant<String> = WireVariant::Raft(RaftMessage::Forward {
            value: "v".into(),
            hops: 2,
        });
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("\"hops\":2"));
        let decoded: WireVariant<String> = serde_json::from_str(&json).unwrap();
        match decoded {
            WireVariant::Raft(RaftMessage::Forward { value, hops }) => {
                assert_eq!(value, "v");
                assert_eq!(hops, 2);
            }
            _ => panic!("wrong variant"),
        }
        // Legacy payload without hops field.
        let legacy = "{\"Raft\":{\"Forward\":{\"value\":\"old\"}}}";
        let decoded: WireVariant<String> = serde_json::from_str(legacy).unwrap();
        match decoded {
            WireVariant::Raft(RaftMessage::Forward { value, hops }) => {
                assert_eq!(value, "old");
                assert_eq!(hops, 0);
            }
            _ => panic!("legacy decode failed"),
        }
    }

    #[test]
    fn commit_index_persists_when_advanced() {
        // M3: every commit_index advance flags pending_persist_commit_index;
        // drain_persist_intent surfaces it for flush_persist to write through.
        use crate::message::{LogEntry, RaftMessage};
        let mut p = raft::<String>(NodeId::new("f", 1), 3, RaftConfig::default());
        p.current_term = 1;
        let leader = NodeId::new("L", 1);
        let _ = p.handle_message(
            leader.clone(),
            RaftMessage::AppendEntries {
                term: 1,
                leader: leader.clone(),
                prev_log_index: None,
                prev_log_term: 0,
                entries: vec![LogEntry {
                    term: 1,
                    value: "v0".into(),
                }],
                leader_commit: Some(0),
            },
        );
        assert_eq!(p.commit_index, Some(0));
        let intent = p.drain_persist_intent();
        assert_eq!(intent.commit_index, Some(Some(0)));
        // Idempotent — a second drain returns no commit_index intent.
        let intent2 = p.drain_persist_intent();
        assert_eq!(intent2.commit_index, None);
    }

    #[test]
    fn restore_state_uses_max_of_persisted_and_decisions() {
        // M3: recovery reconciles persisted commit_index with the highest
        // decided slot, taking the max so a crash mid-write doesn't roll
        // commit_index back below an already-delivered decision.
        use crate::message::LogEntry;
        let mut p = raft::<String>(NodeId::new("a", 1), 3, RaftConfig::default());
        let log = (0..5)
            .map(|i| LogEntry {
                term: 1,
                value: format!("v{i}"),
            })
            .collect();
        p.restore_state(
            1,
            None,
            log,
            // Decisions cover slots 0..=2.
            vec![(0, "v0".into()), (1, "v1".into()), (2, "v2".into())],
            // But commit_index was persisted as 4 — so two extra slots were
            // committed by the cluster but not yet saved to decisions.
            Some(4),
        );
        assert_eq!(p.commit_index, Some(4));
        // last_applied is the decisions floor; on the next event-loop tick,
        // apply_committed_entries will deliver slots 3 and 4 (at-least-once).
        assert_eq!(p.last_applied, Some(2));
    }

    #[test]
    fn restore_state_caps_persisted_commit_index_at_log_end() {
        // M3 defense: a corrupted backing store reporting commit_index past
        // the end of the log must not panic apply_committed_entries.
        use crate::message::LogEntry;
        let mut p = raft::<String>(NodeId::new("a", 1), 3, RaftConfig::default());
        p.restore_state(
            1,
            None,
            vec![LogEntry {
                term: 1,
                value: "only".into(),
            }],
            vec![],
            Some(99),
        );
        assert_eq!(p.commit_index, Some(0));
    }

    #[test]
    fn single_node_resume_seeds_match_index_for_self() {
        // L5: a single-node cluster resuming as leader after restart must
        // seed match_index[self] so try_advance_commit operates correctly.
        use crate::message::LogEntry;
        let me = NodeId::new("solo", 1);
        let mut p = raft::<String>(me.clone(), 1, RaftConfig::default());
        let log = vec![
            LogEntry {
                term: 1,
                value: "a".into(),
            },
            LogEntry {
                term: 1,
                value: "b".into(),
            },
        ];
        p.restore_state(1, Some(me.clone()), log, vec![(0, "a".into())], None);
        assert!(matches!(p.role, Role::Leader));
        assert_eq!(p.match_index.get(&me).copied(), Some(Some(1)));
    }

    #[test]
    fn become_follower_clears_pending_persist_log_from() {
        // L2: a leader that stepped down (e.g., via term-bump) must not carry
        // its leader-side persistence intent into follower state. The
        // entries themselves stay in self.log; if a new leader's AE
        // conflicts they get truncated, otherwise re-flagged when verified.
        use crate::message::LogEntry;
        let me = NodeId::new("a", 1);
        let mut p = raft::<String>(me.clone(), 3, RaftConfig::default());
        p.role = Role::Leader;
        p.current_term = 3;
        p.leader = Some(me.clone());
        p.log.push(LogEntry {
            term: 3,
            value: "x".into(),
        });
        p.pending_persist_log_from = Some(0);
        // Step down via a higher-term message.
        p.become_follower(5, None);
        assert!(matches!(p.role, Role::Follower));
        assert!(
            p.pending_persist_log_from.is_none(),
            "leader-side persistence intent should be cleared on step-down"
        );
    }

    #[test]
    fn follower_reports_conflict_index_on_log_gap() {
        use crate::message::RaftMessage;
        let mut p = raft::<String>(NodeId::new("a", 1), 3, RaftConfig::default());
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
        let mut p = raft::<String>(NodeId::new("a", 1), 3, RaftConfig::default());
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
        let mut p = raft::<String>(me.clone(), 3, RaftConfig::default());
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
    fn leader_uses_conflict_term_to_skip_shared_prefix() {
        use crate::message::{LogEntry, RaftMessage};
        let me = NodeId::new("a", 1);
        let b = NodeId::new("b", 1);
        let mut p = raft::<String>(me.clone(), 3, RaftConfig::default());
        p.role = Role::Leader;
        p.current_term = 7;
        p.leader = Some(me.clone());
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
                term: 1,
                value: "c".into(),
            },
            LogEntry {
                term: 7,
                value: "d".into(),
            },
            LogEntry {
                term: 7,
                value: "e".into(),
            },
        ];
        p.next_index.insert(b.clone(), 5);
        let out = p.handle_message(
            b.clone(),
            RaftMessage::AppendEntriesResponse {
                term: 7,
                success: false,
                match_index: None,
                conflict_term: Some(1),
                conflict_index: Some(0),
            },
        );
        assert_eq!(p.next_index.get(&b), Some(&3));
        match &out[0].message {
            RaftMessage::AppendEntries {
                prev_log_index: Some(2),
                entries,
                ..
            } => {
                assert_eq!(entries.len(), 2);
            }
            other => panic!("expected re-send from index 3, got {:?}", other),
        }
    }

    #[test]
    fn drain_persist_intent_returns_pending_state_and_clears_flags() {
        use crate::message::LogEntry;
        let mut p = raft::<String>(NodeId::new("a", 1), 3, RaftConfig::default());
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
        let mut p = raft::<String>(NodeId::new("a", 1), 3, RaftConfig::default());
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
        let mut p = raft::<String>(NodeId::new("a", 1), 3, RaftConfig::default());
        p.restore_state(
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
            None,
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
        let mut p = raft::<String>(NodeId::new("a", 1), 3, RaftConfig::default());
        p.restore_state(
            2,
            None,
            vec![LogEntry {
                term: 2,
                value: "x".into(),
            }],
            vec![],
            None,
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
        let mut p = raft::<String>(NodeId::new("a", 1), 3, RaftConfig::default());
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
            (SendTarget::Peer(t), RaftMessage::Forward { value, hops }) => {
                assert_eq!(t, &leader);
                assert_eq!(value, "hello");
                assert_eq!(*hops, 0);
            }
            (_, msg) => panic!("expected Forward to leader, got message {:?}", msg),
        }
        assert!(p.pending_proposals.is_empty());
    }

    #[test]
    fn follower_buffers_proposal_when_no_leader_known() {
        let mut p = raft::<String>(NodeId::new("a", 1), 3, RaftConfig::default());
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
        let mut p = raft::<String>(me.clone(), 3, RaftConfig::default());
        p.role = Role::Leader;
        p.current_term = 1;
        p.leader = Some(me.clone());
        let _ = p.handle_message(
            NodeId::new("b", 1),
            RaftMessage::Forward {
                value: "from-b".into(),
                hops: 0,
            },
        );
        assert_eq!(p.log.len(), 1);
        assert_eq!(p.log[0].value, "from-b");
    }

    #[test]
    fn pending_proposals_drained_as_forwards_when_leader_learned() {
        use crate::message::RaftMessage;
        let mut p = raft::<String>(NodeId::new("a", 1), 3, RaftConfig::default());
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
        let has_forward = out.iter().any(|o| {
            matches!(&o.message, RaftMessage::Forward { value, hops: 0 } if value == "buffered")
        });
        assert!(has_forward, "expected buffered proposal to be forwarded");
    }

    #[test]
    fn non_leader_buffers_received_forward_when_no_leader_known() {
        use crate::message::RaftMessage;
        let mut p = raft::<String>(NodeId::new("a", 1), 3, RaftConfig::default());
        // p.role is Follower, p.leader is None
        let out = p.handle_message(
            NodeId::new("b", 1),
            RaftMessage::Forward {
                value: "x".into(),
                hops: 0,
            },
        );
        assert!(out.is_empty());
        assert!(p.log.is_empty());
        assert_eq!(p.pending_proposals, vec!["x".to_string()]);
    }

    #[test]
    fn non_leader_chain_forwards_received_forward_to_known_leader() {
        use crate::message::RaftMessage;
        let mut p = raft::<String>(NodeId::new("a", 1), 3, RaftConfig::default());
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
                hops: 0,
            },
        );
        assert!(p.pending_proposals.is_empty());
        let forward_to_leader = out.iter().any(|o| {
            matches!(&o.target, SendTarget::Peer(t) if t == &leader)
                && matches!(&o.message, RaftMessage::Forward { value, hops: 1 } if value == "relay")
        });
        assert!(forward_to_leader, "expected chain-forward to known leader");
    }

    #[test]
    fn non_leader_buffers_forward_when_sender_is_believed_leader() {
        use crate::message::RaftMessage;
        let mut p = raft::<String>(NodeId::new("a", 1), 3, RaftConfig::default());
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
                hops: 0,
            },
        );
        assert!(out.is_empty());
        assert_eq!(p.pending_proposals, vec!["loop-guard".to_string()]);
    }

    #[test]
    fn single_node_raft_starts_as_follower_with_zero_term() {
        let p = raft::<String>(NodeId::new("solo", 1), 1, RaftConfig::default());
        assert!(matches!(p.role, Role::Follower));
        assert_eq!(p.current_term, 0);
        assert!(p.voted_for.is_none());
        assert!(p.leader.is_none());
        assert!(!p.pending_persist_term);
        assert!(!p.pending_persist_voted_for);
    }

    #[test]
    fn single_node_raft_commits_after_solo_election() {
        let mut p = raft::<String>(NodeId::new("solo", 1), 1, RaftConfig::default());
        let _ =
            <RaftProtocol<String> as ConsensusProtocol<String>>::propose(&mut p, "hello".into());
        assert!(matches!(p.role, Role::Follower));
        assert_eq!(p.pending_proposals, vec!["hello".to_string()]);
        p.election_deadline = Instant::now() - Duration::from_millis(1);
        let _ = p.on_tick(Instant::now());
        assert!(matches!(p.role, Role::Leader));
        assert_eq!(p.current_term, 1);
        assert_eq!(p.commit_index, Some(0));
        let decisions = p.take_decisions();
        assert_eq!(decisions.len(), 1);
        assert_eq!(decisions[0].slot, 0);
        assert_eq!(decisions[0].value, "hello");
    }

    #[test]
    fn three_node_raft_does_not_commit_immediately_on_propose() {
        let mut p = raft::<String>(NodeId::new("a", 1), 3, RaftConfig::default());
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
        let mut p = raft::<String>(me.clone(), 3, RaftConfig::default());
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
        let mut p = raft::<String>(me.clone(), 3, RaftConfig::default());
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
        let mut p = raft::<String>(me.clone(), 3, RaftConfig::default());
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
        let mut p = raft::<String>(NodeId::new("a", 1), 3, RaftConfig::default());
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
        let mut p = raft::<String>(NodeId::new("a", 1), 3, RaftConfig::default());
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
        let mut p = raft::<String>(NodeId::new("a", 1), 3, RaftConfig::default());
        let log = (0..5)
            .map(|i| LogEntry {
                term: 1,
                value: format!("v{i}"),
            })
            .collect();
        p.restore_state(
            1,
            None,
            log,
            vec![(0, "v0".into()), (1, "v1".into()), (2, "v2".into())],
            None,
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
        let mut p = raft::<String>(me.clone(), 3, RaftConfig::default());
        p.role = Role::Leader;
        p.leader = Some(me);
        assert!(!p.is_idle(), "leader is never idle");
    }

    #[test]
    fn is_idle_false_when_pending_proposals() {
        let mut p = raft::<String>(NodeId::new("a", 1), 3, RaftConfig::default());
        p.pending_proposals.push("x".into());
        assert!(!p.is_idle());
    }

    #[test]
    fn is_idle_false_when_log_nonempty() {
        use crate::message::LogEntry;
        let mut p = raft::<String>(NodeId::new("a", 1), 3, RaftConfig::default());
        p.log.push(LogEntry {
            term: 1,
            value: "x".into(),
        });
        assert!(!p.is_idle());
    }

    #[test]
    fn conflict_hint_uses_first_index_of_term() {
        use crate::message::{LogEntry, RaftMessage};
        let mut p = raft::<String>(NodeId::new("a", 1), 3, RaftConfig::default());
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
        let mut p = raft::<String>(me.clone(), 3, RaftConfig::default());
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
    fn single_node_raft_no_persist_intent_until_election() {
        let mut p = raft::<String>(NodeId::new("solo", 1), 1, RaftConfig::default());
        let intent = p.drain_persist_intent();
        assert!(intent.term.is_none());
        assert!(intent.voted_for.is_none());
        p.election_deadline = Instant::now() - Duration::from_millis(1);
        let _ = p.on_tick(Instant::now());
        let intent = p.drain_persist_intent();
        assert_eq!(intent.term, Some(1));
        assert_eq!(intent.voted_for, Some(Some(p.node_id.clone())));
    }

    #[test]
    fn single_node_recover_from_empty_storage_starts_as_follower() {
        let me = NodeId::new("solo", 1);
        let mut p = raft::<String>(me.clone(), 1, RaftConfig::default());
        p.restore_state(0, None, Vec::new(), Vec::new(), None);
        assert!(matches!(p.role, Role::Follower));
        assert_eq!(p.current_term, 0);
        assert!(p.voted_for.is_none());
        assert!(p.leader.is_none());
        let intent = p.drain_persist_intent();
        assert!(intent.term.is_none());
        assert!(intent.voted_for.is_none());
    }

    #[test]
    fn single_node_recover_preserves_persisted_term_and_log() {
        use crate::message::LogEntry;
        let me = NodeId::new("solo", 1);
        let mut p = raft::<String>(me.clone(), 1, RaftConfig::default());
        let log = vec![LogEntry {
            term: 1,
            value: "x".into(),
        }];
        p.restore_state(1, Some(me.clone()), log, vec![(0, "x".into())], None);
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
        let mut p = raft::<String>(me.clone(), 3, RaftConfig::default());
        p.role = Role::Leader;
        p.leader = Some(me.clone());
        p.restore_state(5, Some(me.clone()), Vec::new(), Vec::new(), None);
        assert!(matches!(p.role, Role::Follower));
        assert_eq!(p.leader, None);
        assert_eq!(p.current_term, 5);
    }

    #[test]
    fn old_leader_after_term_bump_chain_forwards() {
        use crate::message::RaftMessage;
        let me = NodeId::new("a", 1);
        let mut p = raft::<String>(me.clone(), 3, RaftConfig::default());
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
                hops: 0,
            },
        );
        assert!(p.log.is_empty());
        let forward_to_b = out.iter().any(|o| {
            matches!(&o.target, SendTarget::Peer(t) if t == &new_leader)
                && matches!(&o.message, RaftMessage::Forward { value, hops: 1 } if value == "delayed")
        });
        assert!(forward_to_b, "expected chain-forward to new leader");
    }

    #[test]
    fn stale_leader_steps_down_on_higher_term_heartbeat() {
        use crate::message::RaftMessage;
        let me = NodeId::new("a", 1);
        let mut old = raft::<String>(me.clone(), 3, RaftConfig::default());
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
        let mut p = raft::<String>(NodeId::new("a", 1), 3, RaftConfig::default());
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
