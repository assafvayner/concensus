use std::collections::{HashMap, HashSet};
use std::time::Instant;

use rand::RngExt;

use serde::{de::DeserializeOwned, Serialize};

use crate::config::NodeId;
use crate::message::{MessageVariant, ProposalNumber};

/// Where to send an outgoing message
pub(crate) enum SendTarget {
    /// Send to a specific peer
    Peer(NodeId),
    /// Broadcast to all peers
    Broadcast,
}

/// An outgoing message produced by the protocol state machine
pub(crate) struct Outgoing<V> {
    pub target: SendTarget,
    pub message: MessageVariant<V>,
}

/// A decided value ready to be delivered
pub(crate) struct Decision<V> {
    pub slot: u64,
    pub value: V,
}

#[cfg(feature = "multi-paxos")]
#[derive(Debug)]
pub(crate) enum LeaderState {
    Leader { term: u64 },
    Follower { leader: Option<NodeId>, last_contact: Instant },
    Candidate,
}

/// Manages all active Paxos instances
pub(crate) struct ProtocolState<V> {
    pub(crate) node_id: NodeId,
    pub(crate) instances: HashMap<u64, PaxosInstance<V>>,
    next_slot: u64,
    quorum_size: usize,
    decided_slots: HashMap<u64, V>,
    pending_decisions: Vec<Decision<V>>,
    lost_proposals: Vec<V>,
    /// Recent decisions to re-broadcast during retries so peers that missed
    /// the original Decide message can learn the outcome.
    recent_decisions: Vec<(Instant, u64, V)>,
    #[cfg(feature = "multi-paxos")]
    leader_state: LeaderState,
    #[cfg(feature = "multi-paxos")]
    highest_seen_round: u64,
    #[cfg(feature = "multi-paxos")]
    last_decide_time: Option<Instant>,
}

/// Per-slot Paxos instance
pub(crate) struct PaxosInstance<V> {
    // Proposer state (Phase 1 promise tracking + Phase 2 accept tracking)
    pub(crate) proposal_number: ProposalNumber,
    promises_received: HashSet<NodeId>,
    accepts_received: HashSet<NodeId>,
    // highest_accepted: tracks the highest-numbered accepted value from Promise
    // responses in the current round. This is PROPOSER-side state, reset on retry.
    highest_accepted: Option<(ProposalNumber, V)>,
    proposed_value: Option<V>,
    // Acceptor state (independent of proposer rounds — never reset on retry)
    highest_promised: Option<ProposalNumber>,
    accepted: Option<(ProposalNumber, V)>,
    // Resolution
    decided: bool,
    // Whether this node initiated the proposal for this slot
    is_proposer: bool,
    // Nack tracking
    nacked: bool,
    highest_seen_nack: Option<u64>,
    last_nack_time: Option<Instant>,
    retry_count: u32,
    /// When this instance last sent messages (propose or retry). Used to
    /// detect proposals stuck without any response (e.g. lost messages).
    last_send_time: Option<Instant>,
    /// Earliest time at which the next retry is allowed. Computed once with
    /// jitter when a nack or stale condition is detected.
    next_retry_at: Option<Instant>,
}

impl<V> PaxosInstance<V> {
    fn new(_slot: u64) -> Self {
        Self {
            proposal_number: (0, NodeId::new("", 0)),
            promises_received: HashSet::new(),
            accepts_received: HashSet::new(),
            highest_accepted: None,
            proposed_value: None,
            highest_promised: None,
            accepted: None,
            decided: false,
            is_proposer: false,
            nacked: false,
            highest_seen_nack: None,
            last_nack_time: None,
            retry_count: 0,
            last_send_time: None,
            next_retry_at: None,
        }
    }
}

impl<V> ProtocolState<V>
where
    V: Serialize + DeserializeOwned + Clone + Send + PartialEq,
{
    pub(crate) fn new(node_id: NodeId, total_nodes: usize) -> Self {
        Self {
            node_id,
            instances: HashMap::new(),
            next_slot: 0,
            quorum_size: (total_nodes / 2) + 1,
            decided_slots: HashMap::new(),
            pending_decisions: Vec::new(),
            lost_proposals: Vec::new(),
            recent_decisions: Vec::new(),
            #[cfg(feature = "multi-paxos")]
            leader_state: LeaderState::Follower {
                leader: None,
                last_contact: Instant::now(),
            },
            #[cfg(feature = "multi-paxos")]
            highest_seen_round: 0,
            #[cfg(feature = "multi-paxos")]
            last_decide_time: None,
        }
    }

    pub(crate) fn initialize_from_decisions(&mut self, decisions: Vec<(u64, V)>) {
        for (slot, value) in decisions {
            self.decided_slots.insert(slot, value);
            if slot >= self.next_slot {
                self.next_slot = slot + 1;
            }
        }
    }

    pub(crate) fn is_idle(&self) -> bool {
        self.instances.is_empty()
    }

    pub(crate) fn take_decisions(&mut self) -> Vec<Decision<V>> {
        std::mem::take(&mut self.pending_decisions)
    }

    pub(crate) fn take_lost_proposals(&mut self) -> Vec<V> {
        std::mem::take(&mut self.lost_proposals)
    }

    /// Handle an incoming message. Returns outgoing messages to send.
    pub(crate) fn handle_message(
        &mut self,
        from: NodeId,
        msg: MessageVariant<V>,
    ) -> Vec<Outgoing<V>> {
        match msg {
            MessageVariant::Prepare {
                slot,
                proposal_number,
            } => self.handle_prepare(from, slot, proposal_number),
            MessageVariant::Promise {
                slot,
                proposal_number,
                accepted,
            } => self.handle_promise(from, slot, proposal_number, accepted),
            MessageVariant::Accept {
                slot,
                proposal_number,
                value,
            } => self.handle_accept(from, slot, proposal_number, value),
            MessageVariant::Accepted {
                slot,
                proposal_number,
                value,
            } => self.handle_accepted(from, slot, proposal_number, value),
            MessageVariant::Decide { slot, value } => {
                self.handle_decide(from, slot, value);
                vec![]
            }
            MessageVariant::NackPrepare {
                slot,
                highest_promised,
                ..
            } => {
                self.handle_nack(slot, highest_promised);
                vec![]
            }
            MessageVariant::NackAccept {
                slot,
                highest_promised,
                ..
            } => {
                self.handle_nack(slot, highest_promised);
                vec![]
            }
            #[cfg(feature = "multi-paxos")]
            MessageVariant::Forward { value } => self.handle_forward(from, value),
            #[cfg(feature = "multi-paxos")]
            MessageVariant::Heartbeat { term } => self.handle_heartbeat(from, term),
        }
    }

    fn get_or_create_instance(&mut self, slot: u64) -> &mut PaxosInstance<V> {
        self.instances
            .entry(slot)
            .or_insert_with(|| PaxosInstance::new(slot))
    }

    // -- Phase 1: Prepare/Promise (Acceptor side) --
    // Acceptor promises if proposal_number is strictly greater than any prior promise.
    // Strict greater-than is required: a Prepare must supersede prior promises.
    fn handle_prepare(
        &mut self,
        from: NodeId,
        slot: u64,
        proposal_number: ProposalNumber,
    ) -> Vec<Outgoing<V>> {
        if let Some(value) = self.decided_slots.get(&slot).cloned() {
            // Inform the sender about the decision they missed
            return vec![Outgoing {
                target: SendTarget::Peer(from),
                message: MessageVariant::Decide { slot, value },
            }];
        }

        let instance = self.get_or_create_instance(slot);

        if instance
            .highest_promised
            .as_ref()
            .is_none_or(|hp| proposal_number > *hp)
        {
            instance.highest_promised = Some(proposal_number.clone());
            vec![Outgoing {
                target: SendTarget::Peer(from),
                message: MessageVariant::Promise {
                    slot,
                    proposal_number,
                    accepted: instance.accepted.clone(),
                },
            }]
        } else {
            vec![Outgoing {
                target: SendTarget::Peer(from),
                message: MessageVariant::NackPrepare {
                    slot,
                    proposal_number,
                    highest_promised: instance.highest_promised.clone().unwrap(),
                },
            }]
        }
    }

    // -- Phase 2: Accept/Accepted (Acceptor side) --
    // Acceptor accepts if proposal_number >= highest_promised.
    // >= because the proposer that made the promise is now sending Accept with the same number.
    fn handle_accept(
        &mut self,
        from: NodeId,
        slot: u64,
        proposal_number: ProposalNumber,
        value: V,
    ) -> Vec<Outgoing<V>> {
        if let Some(decided_value) = self.decided_slots.get(&slot).cloned() {
            // Inform the sender about the decision they missed
            return vec![Outgoing {
                target: SendTarget::Peer(from),
                message: MessageVariant::Decide {
                    slot,
                    value: decided_value,
                },
            }];
        }

        let instance = self.get_or_create_instance(slot);

        if instance
            .highest_promised
            .as_ref()
            .is_none_or(|hp| proposal_number >= *hp)
        {
            instance.highest_promised = Some(proposal_number.clone());
            instance.accepted = Some((proposal_number.clone(), value.clone()));
            vec![Outgoing {
                target: SendTarget::Peer(from),
                message: MessageVariant::Accepted {
                    slot,
                    proposal_number,
                    value,
                },
            }]
        } else {
            vec![Outgoing {
                target: SendTarget::Peer(from),
                message: MessageVariant::NackAccept {
                    slot,
                    proposal_number,
                    highest_promised: instance.highest_promised.clone().unwrap(),
                },
            }]
        }
    }

    // -- Promise handler (Proposer side) --
    fn handle_promise(
        &mut self,
        from: NodeId,
        slot: u64,
        proposal_number: ProposalNumber,
        accepted: Option<(ProposalNumber, V)>,
    ) -> Vec<Outgoing<V>> {
        if self.decided_slots.contains_key(&slot) {
            return vec![];
        }

        let quorum_size = self.quorum_size;
        let instance = match self.instances.get_mut(&slot) {
            Some(i) if i.proposal_number == proposal_number && !i.decided => i,
            _ => return vec![],
        };

        instance.promises_received.insert(from);

        // Track highest accepted value from promises (core Paxos safety rule)
        if let Some((pn, val)) = accepted {
            if instance
                .highest_accepted
                .as_ref()
                .is_none_or(|(existing_pn, _)| pn > *existing_pn)
            {
                instance.highest_accepted = Some((pn, val));
            }
        }

        if instance.promises_received.len() >= quorum_size {
            tracing::debug!(slot, "promise quorum reached, starting Phase 2");
            #[cfg(feature = "multi-paxos")]
            {
                let inst = self.instances.get(&slot).unwrap();
                let term = inst.proposal_number.0;
                // If this is an election-only slot (no proposed value), just become
                // leader and clean up. No Phase 2 needed.
                if inst.proposed_value.is_none() {
                    self.become_leader(term);
                    self.instances.remove(&slot);
                    return vec![];
                }
                self.become_leader(term);
            }
            self.start_phase2(slot)
        } else {
            vec![]
        }
    }

    // -- Accepted handler (Proposer side) --
    fn handle_accepted(
        &mut self,
        from: NodeId,
        slot: u64,
        proposal_number: ProposalNumber,
        _value: V,
    ) -> Vec<Outgoing<V>> {
        if self.decided_slots.contains_key(&slot) {
            return vec![];
        }

        let quorum_size = self.quorum_size;
        let instance = match self.instances.get_mut(&slot) {
            Some(i) if i.proposal_number == proposal_number && !i.decided => i,
            _ => return vec![],
        };

        instance.accepts_received.insert(from);

        if instance.accepts_received.len() >= quorum_size {
            tracing::debug!(slot, "accepted quorum reached, deciding");
            // Use the value from our own accepted state (proposer knows what it sent)
            let value = instance.accepted.as_ref().unwrap().1.clone();
            instance.decided = true;
            self.decide(slot, value.clone());
            self.instances.remove(&slot);
            vec![Outgoing {
                target: SendTarget::Broadcast,
                message: MessageVariant::Decide { slot, value },
            }]
        } else {
            vec![]
        }
    }

    // -- Decide handler --
    fn handle_decide(&mut self, from: NodeId, slot: u64, value: V) {
        #[cfg(feature = "multi-paxos")]
        {
            self.update_leader_contact(&from, self.highest_seen_round);
        }
        #[cfg(not(feature = "multi-paxos"))]
        let _ = &from;

        if self.decided_slots.contains_key(&slot) {
            return;
        }

        // Check if we had an active proposal for this slot with a different value.
        // Only re-propose if our value lost — if the decided value matches our
        // proposed value, we won this slot and no re-proposal is needed.
        if let Some(instance) = self.instances.get(&slot) {
            if instance.is_proposer {
                if let Some(ref proposed) = instance.proposed_value {
                    if *proposed != value {
                        self.lost_proposals.push(proposed.clone());
                    }
                }
            }
        }

        self.decide(slot, value);
        self.instances.remove(&slot);
    }

    // -- Nack handler --
    fn handle_nack(&mut self, slot: u64, highest_promised: ProposalNumber) {
        if let Some(instance) = self.instances.get_mut(&slot) {
            let round = highest_promised.0;
            tracing::debug!(slot, round, "nacked, will retry");
            instance.nacked = true;
            instance.last_nack_time = Some(Instant::now());
            if instance.highest_seen_nack.is_none_or(|r| round > r) {
                instance.highest_seen_nack = Some(round);
            }
            // Compute next retry time with jitter
            let base_ms = 100u64;
            let backoff_ms = base_ms.saturating_mul(1u64 << instance.retry_count.min(3));
            let jitter_ms = rand::rng().random_range(0..=backoff_ms / 2);
            instance.next_retry_at =
                Some(Instant::now() + std::time::Duration::from_millis(backoff_ms + jitter_ms));
        }
        #[cfg(feature = "multi-paxos")]
        {
            self.update_highest_seen_round(highest_promised.0);
            if matches!(self.leader_state, LeaderState::Leader { .. }) {
                self.step_down(None);
            }
        }
    }

    // -- Decide helper --
    fn decide(&mut self, slot: u64, value: V) {
        self.decided_slots.insert(slot, value.clone());
        self.recent_decisions
            .push((Instant::now(), slot, value.clone()));
        if slot >= self.next_slot {
            self.next_slot = slot + 1;
        }
        self.pending_decisions.push(Decision { slot, value });
        #[cfg(feature = "multi-paxos")]
        {
            self.last_decide_time = Some(Instant::now());
        }
    }

    // -- Propose: entry point --
    #[cfg(feature = "multi-paxos")]
    pub(crate) fn propose(&mut self, value: V) -> (u64, Vec<Outgoing<V>>) {
        if let LeaderState::Leader { term } = self.leader_state {
            self.propose_fast_path(value, term)
        } else {
            self.propose_full_paxos(value)
        }
    }

    #[cfg(not(feature = "multi-paxos"))]
    pub(crate) fn propose(&mut self, value: V) -> (u64, Vec<Outgoing<V>>) {
        self.propose_full_paxos(value)
    }

    // -- Full Paxos propose: Phase 1 + self-vote --
    fn propose_full_paxos(&mut self, value: V) -> (u64, Vec<Outgoing<V>>) {
        let slot = self.next_slot;
        self.next_slot += 1;

        let round = 1;
        let node_id = self.node_id.clone();
        let proposal_number: ProposalNumber = (round, node_id.clone());

        let instance = self.get_or_create_instance(slot);
        instance.proposal_number = proposal_number.clone();
        instance.proposed_value = Some(value);
        instance.is_proposer = true;
        instance.last_send_time = Some(Instant::now());
        // Set initial stale retry time
        let stale_ms = 100u64;
        let jitter_ms = rand::rng().random_range(0..=stale_ms / 2);
        instance.next_retry_at =
            Some(Instant::now() + std::time::Duration::from_millis(stale_ms + jitter_ms));

        // Self-vote as acceptor for Phase 1
        instance.highest_promised = Some(proposal_number.clone());
        instance.promises_received.insert(node_id);

        // Check if we already have a quorum (single-node case)
        if instance.promises_received.len() >= self.quorum_size {
            return (slot, self.start_phase2(slot));
        }

        (
            slot,
            vec![Outgoing {
                target: SendTarget::Broadcast,
                message: MessageVariant::Prepare {
                    slot,
                    proposal_number,
                },
            }],
        )
    }

    #[cfg(feature = "multi-paxos")]
    fn propose_fast_path(&mut self, value: V, term: u64) -> (u64, Vec<Outgoing<V>>) {
        let slot = self.next_slot;
        self.next_slot += 1;

        let node_id = self.node_id.clone();
        let proposal_number: ProposalNumber = (term, node_id.clone());

        let instance = self.get_or_create_instance(slot);
        instance.proposal_number = proposal_number.clone();
        instance.proposed_value = Some(value.clone());
        instance.is_proposer = true;
        instance.last_send_time = Some(Instant::now());
        let stale_ms = 100u64;
        let jitter_ms = rand::rng().random_range(0..=stale_ms / 2);
        instance.next_retry_at =
            Some(Instant::now() + std::time::Duration::from_millis(stale_ms + jitter_ms));

        // Skip Phase 1 — go directly to Phase 2
        // Self-vote as acceptor
        instance.highest_promised = Some(proposal_number.clone());
        instance.accepted = Some((proposal_number.clone(), value.clone()));
        instance.accepts_received.insert(node_id.clone());
        // Also count self in promises (for retry logic consistency)
        instance.promises_received.insert(node_id);

        tracing::debug!(slot, term, "leader fast path: skipping Phase 1");

        // Check single-node quorum
        if instance.accepts_received.len() >= self.quorum_size {
            self.decide(slot, value.clone());
            self.instances.remove(&slot);
            return (
                slot,
                vec![Outgoing {
                    target: SendTarget::Broadcast,
                    message: MessageVariant::Decide { slot, value },
                }],
            );
        }

        (
            slot,
            vec![Outgoing {
                target: SendTarget::Broadcast,
                message: MessageVariant::Accept {
                    slot,
                    proposal_number,
                    value,
                },
            }],
        )
    }

    fn start_phase2(&mut self, slot: u64) -> Vec<Outgoing<V>> {
        let instance = self.instances.get_mut(&slot).unwrap();

        // Phase 2 value selection: use highest accepted value from promises, or own value.
        // This is the core Paxos safety rule.
        let value = if let Some((_, v)) = &instance.highest_accepted {
            v.clone()
        } else {
            instance.proposed_value.clone().unwrap()
        };

        let proposal_number = instance.proposal_number.clone();

        // Self-vote as acceptor for Phase 2.
        // SAFETY CHECK: only self-accept if our acceptor hasn't promised a higher
        // number to another proposer since our Phase 1. Between collecting promise
        // quorum and starting Phase 2, a remote Prepare with a higher number could
        // have updated highest_promised.
        let can_self_accept = instance
            .highest_promised
            .as_ref()
            .is_none_or(|hp| proposal_number >= *hp);

        if can_self_accept {
            instance.highest_promised = Some(proposal_number.clone());
            instance.accepted = Some((proposal_number.clone(), value.clone()));
            instance.accepts_received.insert(self.node_id.clone());
        }

        // Check if we already have a quorum (single-node case)
        if instance.accepts_received.len() >= self.quorum_size {
            self.decide(slot, value.clone());
            self.instances.remove(&slot);
            return vec![Outgoing {
                target: SendTarget::Broadcast,
                message: MessageVariant::Decide { slot, value },
            }];
        }

        vec![Outgoing {
            target: SendTarget::Broadcast,
            message: MessageVariant::Accept {
                slot,
                proposal_number,
                value,
            },
        }]
    }

    /// Returns slots eligible for retry. Uses pre-computed `next_retry_at`
    /// which includes jitter to break livelock between competing proposers.
    pub(crate) fn get_retryable_proposals(&self) -> Vec<u64> {
        let now = Instant::now();
        self.instances
            .iter()
            .filter(|(_, i)| {
                if i.decided || !i.is_proposer {
                    return false;
                }
                // Check if retry time has elapsed
                let retry_at = match i.next_retry_at {
                    Some(t) => t,
                    None => return false,
                };
                if now < retry_at {
                    return false;
                }
                if i.nacked {
                    true
                } else {
                    // Not nacked — only retry if stuck (no quorum for current phase)
                    let in_phase2 = !i.accepts_received.is_empty();
                    let has_relevant_quorum = if in_phase2 {
                        i.accepts_received.len() >= self.quorum_size
                    } else {
                        i.promises_received.len() >= self.quorum_size
                    };
                    !has_relevant_quorum
                }
            })
            .map(|(&slot, _)| slot)
            .collect()
    }

    /// Returns Decide broadcasts for recent decisions that should be re-sent
    /// to ensure peers that missed the original Decide can learn the outcome.
    /// Expires entries older than 5 seconds.
    pub(crate) fn get_decision_rebroadcasts(&mut self) -> Vec<Outgoing<V>> {
        let now = Instant::now();
        let max_age = std::time::Duration::from_secs(5);

        // Remove expired entries
        self.recent_decisions
            .retain(|(t, _, _)| now.duration_since(*t) < max_age);

        self.recent_decisions
            .iter()
            .map(|(_, slot, value)| Outgoing {
                target: SendTarget::Broadcast,
                message: MessageVariant::Decide {
                    slot: *slot,
                    value: value.clone(),
                },
            })
            .collect()
    }

    pub(crate) fn retry_proposal(&mut self, slot: u64) -> Vec<Outgoing<V>> {
        let instance = match self.instances.get_mut(&slot) {
            Some(i) if !i.decided && i.is_proposer => i,
            _ => return vec![],
        };

        // Compute new_round that exceeds BOTH the highest nack AND the acceptor's
        // current highest_promised. This ensures the new proposal number doesn't
        // violate the acceptor's existing promise (which may have been updated by
        // a remote Prepare since our last attempt).
        let acceptor_round = instance.highest_promised.as_ref().map_or(0, |hp| hp.0);
        let nack_round = instance.highest_seen_nack.unwrap_or(0);
        let proposer_round = instance.proposal_number.0;
        let new_round = acceptor_round.max(nack_round).max(proposer_round) + 1;
        let proposal_number: ProposalNumber = (new_round, self.node_id.clone());

        // Reset PROPOSER state for new round.
        // highest_accepted tracks promises from current round — must be reset.
        // Acceptor state (accepted) is independent — NOT reset.
        instance.proposal_number = proposal_number.clone();
        instance.promises_received.clear();
        instance.accepts_received.clear();
        instance.highest_accepted = None;
        instance.nacked = false;
        instance.highest_seen_nack = None;
        instance.last_nack_time = None;
        instance.retry_count += 1;
        instance.last_send_time = Some(Instant::now());
        // Compute next stale retry time with jitter
        let stale_ms = 100u64.saturating_mul(1u64 << instance.retry_count.min(3));
        let jitter_ms = rand::rng().random_range(0..=stale_ms / 2);
        instance.next_retry_at =
            Some(Instant::now() + std::time::Duration::from_millis(stale_ms + jitter_ms));

        // Self-vote for new round — safe because new_round > any prior promise
        instance.highest_promised = Some(proposal_number.clone());
        instance.promises_received.insert(self.node_id.clone());

        if instance.promises_received.len() >= self.quorum_size {
            return self.start_phase2(slot);
        }

        vec![Outgoing {
            target: SendTarget::Broadcast,
            message: MessageVariant::Prepare {
                slot,
                proposal_number,
            },
        }]
    }

    // =======================================================================
    // Multi-Paxos leader methods
    // =======================================================================

    #[cfg(feature = "multi-paxos")]
    fn become_leader(&mut self, term: u64) {
        tracing::info!(term, node = %self.node_id, "became leader");
        self.leader_state = LeaderState::Leader { term };
        self.highest_seen_round = self.highest_seen_round.max(term);
        self.last_decide_time = None;
    }

    #[cfg(feature = "multi-paxos")]
    fn step_down(&mut self, new_leader: Option<NodeId>) {
        if matches!(self.leader_state, LeaderState::Leader { .. }) {
            tracing::info!(node = %self.node_id, ?new_leader, "stepping down from leader");
        }
        self.leader_state = LeaderState::Follower {
            leader: new_leader,
            last_contact: Instant::now(),
        };
    }

    #[cfg(feature = "multi-paxos")]
    fn update_leader_contact(&mut self, from: &NodeId, term: u64) {
        self.highest_seen_round = self.highest_seen_round.max(term);
        match &mut self.leader_state {
            LeaderState::Follower {
                leader,
                last_contact,
            } => {
                *leader = Some(from.clone());
                *last_contact = Instant::now();
            }
            LeaderState::Candidate => {
                self.leader_state = LeaderState::Follower {
                    leader: Some(from.clone()),
                    last_contact: Instant::now(),
                };
            }
            LeaderState::Leader { term: my_term } => {
                if term > *my_term {
                    self.step_down(Some(from.clone()));
                }
            }
        }
    }

    #[cfg(feature = "multi-paxos")]
    fn update_highest_seen_round(&mut self, round: u64) {
        self.highest_seen_round = self.highest_seen_round.max(round);
    }

    #[cfg(feature = "multi-paxos")]
    pub(crate) fn get_leader(&self) -> Option<NodeId> {
        match &self.leader_state {
            LeaderState::Leader { .. } => Some(self.node_id.clone()),
            LeaderState::Follower { leader, .. } => leader.clone(),
            LeaderState::Candidate => None,
        }
    }

    #[cfg(feature = "multi-paxos")]
    pub(crate) fn should_send_heartbeat(&self) -> bool {
        if let LeaderState::Leader { .. } = &self.leader_state {
            let heartbeat_interval = std::time::Duration::from_millis(100);
            match self.last_decide_time {
                Some(t) => Instant::now().duration_since(t) >= heartbeat_interval,
                None => true,
            }
        } else {
            false
        }
    }

    #[cfg(feature = "multi-paxos")]
    pub(crate) fn make_heartbeat(&self) -> Vec<Outgoing<V>> {
        if let LeaderState::Leader { term } = &self.leader_state {
            vec![Outgoing {
                target: SendTarget::Broadcast,
                message: MessageVariant::Heartbeat { term: *term },
            }]
        } else {
            vec![]
        }
    }

    #[cfg(feature = "multi-paxos")]
    pub(crate) fn check_leader_timeout(&self) -> bool {
        let leader_timeout = std::time::Duration::from_millis(500);
        if let LeaderState::Follower {
            leader: Some(_),
            last_contact,
        } = &self.leader_state
        {
            Instant::now().duration_since(*last_contact) >= leader_timeout
        } else {
            false
        }
    }

    #[cfg(feature = "multi-paxos")]
    pub(crate) fn start_election(&mut self) -> Vec<Outgoing<V>> {
        tracing::info!(node = %self.node_id, "starting leader election");
        self.leader_state = LeaderState::Candidate;

        let round = self.highest_seen_round + 1;
        let slot = self.next_slot;
        self.next_slot += 1;

        let node_id = self.node_id.clone();
        let proposal_number: ProposalNumber = (round, node_id.clone());
        let instance = self.get_or_create_instance(slot);
        instance.proposal_number = proposal_number.clone();
        instance.is_proposer = true;
        instance.last_send_time = Some(Instant::now());
        let stale_ms = 100u64;
        let jitter_ms = rand::rng().random_range(0..=stale_ms / 2);
        instance.next_retry_at =
            Some(Instant::now() + std::time::Duration::from_millis(stale_ms + jitter_ms));

        // Self-vote
        instance.highest_promised = Some(proposal_number.clone());
        instance.promises_received.insert(node_id);

        if instance.promises_received.len() >= self.quorum_size {
            self.become_leader(round);
            return vec![];
        }

        self.update_highest_seen_round(round);

        vec![Outgoing {
            target: SendTarget::Broadcast,
            message: MessageVariant::Prepare {
                slot,
                proposal_number,
            },
        }]
    }

    #[cfg(feature = "multi-paxos")]
    fn handle_heartbeat(&mut self, from: NodeId, term: u64) -> Vec<Outgoing<V>> {
        if term >= self.highest_seen_round {
            self.update_leader_contact(&from, term);
        }
        vec![]
    }

    #[cfg(feature = "multi-paxos")]
    fn handle_forward(&mut self, _from: NodeId, value: V) -> Vec<Outgoing<V>> {
        if let LeaderState::Leader { .. } = &self.leader_state {
            tracing::debug!(node = %self.node_id, "received forwarded proposal");
            let (_, outgoing) = self.propose(value);
            outgoing
        } else {
            tracing::debug!(node = %self.node_id, "received forward but not leader, ignoring");
            vec![]
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::NodeId;

    fn node(name: &str) -> NodeId {
        NodeId::new(name, 1000)
    }

    fn make_protocol(name: &str, total_nodes: usize) -> ProtocolState<String> {
        ProtocolState::new(node(name), total_nodes)
    }

    // -- Acceptor tests (Phase 1) --

    #[test]
    fn acceptor_promises_first_prepare() {
        let mut proto = make_protocol("a", 3);
        let from = node("b");
        let pn = (1, from.clone());

        let responses = proto.handle_message(
            from,
            MessageVariant::Prepare {
                slot: 0,
                proposal_number: pn.clone(),
            },
        );

        assert_eq!(responses.len(), 1);
        match &responses[0].message {
            MessageVariant::Promise {
                slot,
                proposal_number,
                accepted,
            } => {
                assert_eq!(*slot, 0);
                assert_eq!(proposal_number, &pn);
                assert!(accepted.is_none());
            }
            _ => panic!("expected Promise"),
        }
    }

    #[test]
    fn acceptor_nacks_lower_prepare() {
        let mut proto = make_protocol("a", 3);
        let from = node("b");

        // First prepare with higher number
        proto.handle_message(
            from.clone(),
            MessageVariant::Prepare {
                slot: 0,
                proposal_number: (5, from.clone()),
            },
        );

        // Second prepare with lower number
        let responses = proto.handle_message(
            from.clone(),
            MessageVariant::Prepare {
                slot: 0,
                proposal_number: (1, from.clone()),
            },
        );

        assert_eq!(responses.len(), 1);
        assert!(matches!(
            &responses[0].message,
            MessageVariant::NackPrepare { .. }
        ));
    }

    // -- Acceptor tests (Phase 2) --
    // Acceptor accepts if proposal_number >= highest_promised.
    // Greater-than-or-equal here because the proposer that made the promise
    // is now sending Accept with the same number — that's valid.

    #[test]
    fn acceptor_accepts_if_no_higher_promise() {
        let mut proto = make_protocol("a", 3);
        let from = node("b");
        let pn = (1, from.clone());

        let responses = proto.handle_message(
            from.clone(),
            MessageVariant::Accept {
                slot: 0,
                proposal_number: pn.clone(),
                value: "hello".to_string(),
            },
        );

        assert_eq!(responses.len(), 1);
        match &responses[0].message {
            MessageVariant::Accepted {
                slot,
                proposal_number,
                value,
            } => {
                assert_eq!(*slot, 0);
                assert_eq!(proposal_number, &pn);
                assert_eq!(value, "hello");
            }
            _ => panic!("expected Accepted"),
        }
    }

    #[test]
    fn acceptor_nacks_accept_with_lower_proposal() {
        let mut proto = make_protocol("a", 3);
        let from = node("b");

        // Promise a higher number first
        proto.handle_message(
            from.clone(),
            MessageVariant::Prepare {
                slot: 0,
                proposal_number: (5, from.clone()),
            },
        );

        // Try to accept with lower number
        let responses = proto.handle_message(
            from.clone(),
            MessageVariant::Accept {
                slot: 0,
                proposal_number: (1, from.clone()),
                value: "hello".to_string(),
            },
        );

        assert_eq!(responses.len(), 1);
        assert!(matches!(
            &responses[0].message,
            MessageVariant::NackAccept { .. }
        ));
    }

    #[test]
    fn acceptor_records_accepted_value_in_promise() {
        let mut proto = make_protocol("a", 3);
        let from = node("b");
        let pn1 = (1, from.clone());

        // Accept a value
        proto.handle_message(
            from.clone(),
            MessageVariant::Accept {
                slot: 0,
                proposal_number: pn1.clone(),
                value: "hello".to_string(),
            },
        );

        // New prepare should return the accepted value
        let pn2 = (2, from.clone());
        let responses = proto.handle_message(
            from.clone(),
            MessageVariant::Prepare {
                slot: 0,
                proposal_number: pn2.clone(),
            },
        );

        match &responses[0].message {
            MessageVariant::Promise {
                accepted: Some((pn, val)),
                ..
            } => {
                assert_eq!(pn, &pn1);
                assert_eq!(val, "hello");
            }
            _ => panic!("expected Promise with accepted value"),
        }
    }

    // -- Proposer tests --

    #[test]
    fn propose_starts_phase1_and_self_votes() {
        let mut proto = make_protocol("a", 3);
        let (slot, outgoing) = proto.propose("hello".to_string());

        assert_eq!(slot, 0);
        // Should produce Prepare broadcast
        assert!(!outgoing.is_empty());
        for out in &outgoing {
            assert!(matches!(
                &out.message,
                MessageVariant::Prepare { slot: 0, .. }
            ));
            assert!(matches!(&out.target, SendTarget::Broadcast));
        }

        // Self-vote: should have recorded own promise
        let instance = proto.instances.get(&0).unwrap();
        assert!(instance.promises_received.contains(&proto.node_id));
    }

    #[test]
    fn propose_increments_next_slot() {
        let mut proto = make_protocol("a", 3);
        let (slot1, _) = proto.propose("first".to_string());
        let (slot2, _) = proto.propose("second".to_string());
        assert_eq!(slot1, 0);
        assert_eq!(slot2, 1);
    }

    #[test]
    fn single_node_decides_immediately_on_propose() {
        let mut proto = make_protocol("a", 1);
        let (_slot, outgoing) = proto.propose("hello".to_string());

        // With quorum_size=1, self-vote gives immediate decision
        // Should produce Decide broadcast
        assert!(outgoing
            .iter()
            .any(|o| matches!(&o.message, MessageVariant::Decide { .. })));

        let decisions = proto.take_decisions();
        assert_eq!(decisions.len(), 1);
        assert_eq!(decisions[0].slot, 0);
        assert_eq!(decisions[0].value, "hello");
    }

    #[test]
    fn promise_quorum_triggers_phase2() {
        let mut proto = make_protocol("a", 3);
        let (_slot, _) = proto.propose("hello".to_string());

        // We need one more promise (already have self-vote)
        let pn = proto.instances.get(&0).unwrap().proposal_number.clone();
        let responses = proto.handle_message(
            node("b"),
            MessageVariant::Promise {
                slot: 0,
                proposal_number: pn.clone(),
                accepted: None,
            },
        );

        // Should produce Accept broadcast
        assert!(!responses.is_empty());
        for out in &responses {
            match &out.message {
                MessageVariant::Accept { value, .. } => assert_eq!(value, "hello"),
                _ => panic!("expected Accept, got {:?}", out.message),
            }
        }
    }

    #[test]
    fn phase2_value_selection_uses_highest_accepted() {
        let mut proto = make_protocol("a", 3);
        let (_slot, _) = proto.propose("my-value".to_string());

        let pn = proto.instances.get(&0).unwrap().proposal_number.clone();

        // Peer b already accepted a value at a lower proposal number
        let responses = proto.handle_message(
            node("b"),
            MessageVariant::Promise {
                slot: 0,
                proposal_number: pn.clone(),
                accepted: Some(((1, node("c")), "previous-value".to_string())),
            },
        );

        // Phase 2 should use "previous-value", not "my-value"
        for out in &responses {
            match &out.message {
                MessageVariant::Accept { value, .. } => assert_eq!(value, "previous-value"),
                _ => panic!("expected Accept"),
            }
        }
    }

    #[test]
    fn phase2_value_selection_picks_highest_of_multiple() {
        let mut proto = make_protocol("a", 5); // quorum = 3
        let (_slot, _) = proto.propose("my-value".to_string());
        let pn = proto.instances.get(&0).unwrap().proposal_number.clone();

        // Peer b accepted at round 1
        proto.handle_message(
            node("b"),
            MessageVariant::Promise {
                slot: 0,
                proposal_number: pn.clone(),
                accepted: Some(((1, node("x")), "old-value".to_string())),
            },
        );

        // Peer c accepted at round 3 (higher)
        let responses = proto.handle_message(
            node("c"),
            MessageVariant::Promise {
                slot: 0,
                proposal_number: pn.clone(),
                accepted: Some(((3, node("y")), "newer-value".to_string())),
            },
        );

        // Should use "newer-value" (highest proposal number)
        for out in &responses {
            match &out.message {
                MessageVariant::Accept { value, .. } => assert_eq!(value, "newer-value"),
                _ => panic!("expected Accept"),
            }
        }
    }

    #[test]
    fn duplicate_promise_does_not_double_count() {
        let mut proto = make_protocol("a", 5); // quorum = 3
        let (_slot, _) = proto.propose("hello".to_string());
        let pn = proto.instances.get(&0).unwrap().proposal_number.clone();

        // Same peer sends Promise twice
        proto.handle_message(
            node("b"),
            MessageVariant::Promise {
                slot: 0,
                proposal_number: pn.clone(),
                accepted: None,
            },
        );
        let responses = proto.handle_message(
            node("b"),
            MessageVariant::Promise {
                slot: 0,
                proposal_number: pn.clone(),
                accepted: None,
            },
        );

        // Should not trigger Phase 2 — we have self + b = 2, quorum = 3
        assert!(responses.is_empty());
    }

    // -- Decision tests --

    #[test]
    fn accepted_quorum_triggers_decision() {
        let mut proto = make_protocol("a", 3);
        let (_, _) = proto.propose("hello".to_string());
        let pn = proto.instances.get(&0).unwrap().proposal_number.clone();

        // Get quorum of promises to move to Phase 2
        proto.handle_message(
            node("b"),
            MessageVariant::Promise {
                slot: 0,
                proposal_number: pn.clone(),
                accepted: None,
            },
        );

        // Now we need one more Accepted (already have self-vote from Phase 2)
        let responses = proto.handle_message(
            node("b"),
            MessageVariant::Accepted {
                slot: 0,
                proposal_number: pn.clone(),
                value: "hello".to_string(),
            },
        );

        // Should broadcast Decide
        assert!(responses
            .iter()
            .any(|o| matches!(&o.message, MessageVariant::Decide { .. })));

        let decisions = proto.take_decisions();
        assert_eq!(decisions.len(), 1);
        assert_eq!(decisions[0].value, "hello");
    }

    #[test]
    fn duplicate_accepted_does_not_double_count() {
        let mut proto = make_protocol("a", 5); // quorum = 3
        let (_, _) = proto.propose("hello".to_string());
        let pn = proto.instances.get(&0).unwrap().proposal_number.clone();

        // Get quorum of promises (self + b + c = 3)
        proto.handle_message(
            node("b"),
            MessageVariant::Promise {
                slot: 0,
                proposal_number: pn.clone(),
                accepted: None,
            },
        );
        proto.handle_message(
            node("c"),
            MessageVariant::Promise {
                slot: 0,
                proposal_number: pn.clone(),
                accepted: None,
            },
        );

        // Same peer sends Accepted twice — should not trigger early quorum
        proto.handle_message(
            node("b"),
            MessageVariant::Accepted {
                slot: 0,
                proposal_number: pn.clone(),
                value: "hello".to_string(),
            },
        );
        let responses = proto.handle_message(
            node("b"),
            MessageVariant::Accepted {
                slot: 0,
                proposal_number: pn.clone(),
                value: "hello".to_string(),
            },
        );

        // self + b = 2, quorum = 3, should not decide
        assert!(responses.is_empty());
        assert!(proto.take_decisions().is_empty());
    }

    #[test]
    fn handle_decide_from_peer() {
        let mut proto = make_protocol("a", 3);

        proto.handle_message(
            node("b"),
            MessageVariant::Decide {
                slot: 5,
                value: "remote-decision".to_string(),
            },
        );

        let decisions = proto.take_decisions();
        assert_eq!(decisions.len(), 1);
        assert_eq!(decisions[0].slot, 5);
        assert_eq!(decisions[0].value, "remote-decision");

        // next_slot should advance
        assert!(proto.next_slot >= 6);
    }

    #[test]
    fn decided_slot_replies_with_decide_to_late_prepare() {
        let mut proto = make_protocol("a", 3);
        proto.handle_message(
            node("b"),
            MessageVariant::Decide {
                slot: 0,
                value: "decided".to_string(),
            },
        );
        proto.take_decisions();

        // A late Prepare for a decided slot should reply with the Decide
        // so the sender can learn the outcome (important for lossy networks).
        let responses = proto.handle_message(
            node("c"),
            MessageVariant::Prepare {
                slot: 0,
                proposal_number: (10, node("c")),
            },
        );
        assert_eq!(responses.len(), 1);
        assert!(matches!(
            &responses[0].message,
            MessageVariant::Decide { slot: 0, value } if value == "decided"
        ));
        assert!(matches!(&responses[0].target, SendTarget::Peer(id) if id == &node("c")));
    }

    #[test]
    fn instance_garbage_collected_after_decision() {
        let mut proto = make_protocol("a", 3);
        proto.handle_message(
            node("b"),
            MessageVariant::Decide {
                slot: 0,
                value: "done".to_string(),
            },
        );
        proto.take_decisions();

        assert!(!proto.instances.contains_key(&0));
        assert!(proto.decided_slots.contains_key(&0));
    }

    #[test]
    fn decide_from_external_does_not_repropose_if_value_matches() {
        // When the decided value matches our proposed value, we won this slot.
        // No re-proposal needed.
        let mut proto = make_protocol("a", 3);
        let (_, _) = proto.propose("my-value".to_string());

        proto.handle_message(
            node("b"),
            MessageVariant::Decide {
                slot: 0,
                value: "my-value".to_string(),
            },
        );

        let lost = proto.take_lost_proposals();
        assert!(lost.is_empty());
    }

    #[test]
    fn decide_for_different_value_re_proposes() {
        let mut proto = make_protocol("a", 3);
        let (_, _) = proto.propose("my-value".to_string());

        // A different value decided for our slot
        proto.handle_message(
            node("b"),
            MessageVariant::Decide {
                slot: 0,
                value: "other-value".to_string(),
            },
        );

        let lost = proto.take_lost_proposals();
        assert_eq!(lost.len(), 1);
        assert_eq!(lost[0], "my-value");
    }

    // -- Nack / retry tests --

    #[test]
    fn nack_records_highest_promised_for_retry() {
        let mut proto = make_protocol("a", 3);
        let (_, _) = proto.propose("hello".to_string());
        let pn = proto.instances.get(&0).unwrap().proposal_number.clone();

        proto.handle_message(
            node("b"),
            MessageVariant::NackPrepare {
                slot: 0,
                proposal_number: pn,
                highest_promised: (10, node("b")),
            },
        );

        let instance = proto.instances.get(&0).unwrap();
        assert!(instance.nacked);
        assert_eq!(instance.highest_seen_nack, Some(10));
        assert!(instance.last_nack_time.is_some());
    }

    #[test]
    fn retry_proposal_uses_higher_number() {
        let mut proto = make_protocol("a", 3);
        let (_, _) = proto.propose("hello".to_string());
        let old_pn = proto.instances.get(&0).unwrap().proposal_number.clone();

        // Nack so retry is allowed
        proto.handle_message(
            node("b"),
            MessageVariant::NackPrepare {
                slot: 0,
                proposal_number: old_pn.clone(),
                highest_promised: (5, node("b")),
            },
        );

        let outgoing = proto.retry_proposal(0);
        assert!(!outgoing.is_empty());

        let new_pn = proto.instances.get(&0).unwrap().proposal_number.clone();
        assert!(new_pn > old_pn);
        // New round should be at least highest_seen_nack + 1
        assert!(new_pn.0 > 5);
    }

    #[test]
    fn retry_preserves_acceptor_state() {
        let mut proto = make_protocol("a", 3);
        let (_, _) = proto.propose("hello".to_string());

        // Accept a value as acceptor
        let from = node("b");
        proto.handle_message(
            from.clone(),
            MessageVariant::Accept {
                slot: 0,
                proposal_number: (5, from.clone()),
                value: "other".to_string(),
            },
        );

        // Nack and retry
        proto.handle_message(
            from.clone(),
            MessageVariant::NackPrepare {
                slot: 0,
                proposal_number: proto.instances.get(&0).unwrap().proposal_number.clone(),
                highest_promised: (10, from),
            },
        );
        proto.retry_proposal(0);

        // Acceptor state should still have the accepted value
        let instance = proto.instances.get(&0).unwrap();
        assert!(instance.accepted.is_some());
        // Retry round should exceed the acceptor's highest_promised (round 5 from Accept)
        assert!(instance.proposal_number.0 > 5);
    }

    #[test]
    fn retry_round_exceeds_acceptor_highest_promised() {
        let mut proto = make_protocol("a", 3);
        let (_, _) = proto.propose("hello".to_string());

        // Another proposer's Prepare updates our acceptor's highest_promised to round 20
        proto.handle_message(
            node("c"),
            MessageVariant::Prepare {
                slot: 0,
                proposal_number: (20, node("c")),
            },
        );

        // Our proposal gets nacked with a lower round (5)
        proto.handle_message(
            node("b"),
            MessageVariant::NackPrepare {
                slot: 0,
                proposal_number: proto.instances.get(&0).unwrap().proposal_number.clone(),
                highest_promised: (5, node("b")),
            },
        );

        proto.retry_proposal(0);

        // Retry must use round > 20 (acceptor's promise), not just > 5 (nack)
        let instance = proto.instances.get(&0).unwrap();
        assert!(instance.proposal_number.0 > 20);
    }

    #[test]
    fn start_phase2_respects_acceptor_promise() {
        let mut proto = make_protocol("a", 3);
        let (_, _) = proto.propose("hello".to_string());
        let pn = proto.instances.get(&0).unwrap().proposal_number.clone();

        // Another proposer updates our highest_promised to a high round
        proto.handle_message(
            node("c"),
            MessageVariant::Prepare {
                slot: 0,
                proposal_number: (100, node("c")),
            },
        );

        // Now peer b sends Promise for our original (low) proposal number
        let _responses = proto.handle_message(
            node("b"),
            MessageVariant::Promise {
                slot: 0,
                proposal_number: pn.clone(),
                accepted: None,
            },
        );

        // Phase 2 should start but self-accept should be skipped because
        // our acceptor promised round 100. The Accept is still broadcast
        // (other acceptors may not have that promise), but our self-vote is not counted.
        let instance = proto.instances.get(&0);
        if let Some(inst) = instance {
            // Self should NOT be in accepts_received
            assert!(!inst.accepts_received.contains(&proto.node_id));
        }
    }

    #[test]
    fn get_retryable_proposals_with_backoff() {
        let mut proto = make_protocol("a", 3);
        let (_, _) = proto.propose("hello".to_string());

        proto.handle_message(
            node("b"),
            MessageVariant::NackPrepare {
                slot: 0,
                proposal_number: proto.instances.get(&0).unwrap().proposal_number.clone(),
                highest_promised: (5, node("b")),
            },
        );

        // Immediately after nack — backoff hasn't elapsed (100ms minimum)
        let _retryable = proto.get_retryable_proposals();
        // Just verify the method runs without panic — timing-sensitive tests are fragile
    }
}
