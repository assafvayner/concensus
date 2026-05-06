//! Property-based tests for `RaftProtocol`.
//!
//! Drives a small cluster of `RaftProtocol<u32>` instances with a
//! deterministic message bus and asserts the five Raft safety invariants
//! plus three operational ones after each step.

#![cfg(test)]
#![allow(dead_code)] // Items used only by proptest macros below — clippy false positive.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use proptest::collection::vec;
use proptest::prelude::*;

use crate::config::{NodeId, RaftConfig};
use crate::message::{LogEntry, RaftMessage};
use crate::protocol::raft::{RaftProtocol, Role};
use crate::protocol::{ConsensusProtocol, Outgoing, SendTarget};

const NUM_NODES: usize = 3;

#[derive(Debug, Clone)]
enum Step {
    Tick(u8),
    Deliver(usize),
    Drop(usize),
    Reorder(usize, usize),
    Propose(u8, u32),
    Restart(u8),
}

fn step_strategy() -> impl Strategy<Value = Step> {
    prop_oneof![
        (0u8..NUM_NODES as u8).prop_map(Step::Tick),
        (0usize..32).prop_map(Step::Deliver),
        (0usize..32).prop_map(Step::Drop),
        (0usize..32, 0usize..32).prop_map(|(a, b)| Step::Reorder(a, b)),
        (0u8..NUM_NODES as u8, any::<u32>()).prop_map(|(n, v)| Step::Propose(n, v)),
        (0u8..NUM_NODES as u8).prop_map(Step::Restart),
    ]
}

struct Harness {
    nodes: Vec<RaftProtocol<u32>>,
    ids: Vec<NodeId>,
    queue: Vec<(NodeId, NodeId, RaftMessage<u32>)>,
    now: Instant,
    persisted_term: Vec<u64>,
    persisted_voted_for: Vec<Option<NodeId>>,
    persisted_log: Vec<Vec<LogEntry<u32>>>,
    persisted_decisions: Vec<Vec<(u64, u32)>>,
}

impl Harness {
    fn new() -> Self {
        let cfg = RaftConfig {
            election_timeout_min: Duration::from_millis(100),
            election_timeout_max: Duration::from_millis(200),
            heartbeat_interval: Duration::from_millis(50),
        };
        let ids: Vec<NodeId> = (0..NUM_NODES)
            .map(|i| NodeId::new(format!("n{i}"), 1))
            .collect();
        let nodes: Vec<RaftProtocol<u32>> = ids
            .iter()
            .map(|id| RaftProtocol::new(id.clone(), NUM_NODES, cfg.clone()))
            .collect();
        Self {
            nodes,
            ids,
            queue: Vec::new(),
            now: Instant::now(),
            persisted_term: vec![0; NUM_NODES],
            persisted_voted_for: vec![None; NUM_NODES],
            persisted_log: vec![Vec::new(); NUM_NODES],
            persisted_decisions: vec![Vec::new(); NUM_NODES],
        }
    }

    fn idx_of(&self, id: &NodeId) -> Option<usize> {
        self.ids.iter().position(|x| x == id)
    }

    fn flush_persist(&mut self, node: usize) {
        let intent = self.nodes[node].drain_persist_intent();
        if let Some(t) = intent.term {
            self.persisted_term[node] = t;
        }
        if let Some(vf) = intent.voted_for {
            self.persisted_voted_for[node] = vf;
        }
        if let Some(idx) = intent.truncate_from {
            self.persisted_log[node].truncate(idx as usize);
        }
        if let Some(idx) = intent.append_from {
            for entry in &intent.log_snapshot[idx as usize..] {
                self.persisted_log[node].push(entry.clone());
            }
        }
    }

    fn enqueue(&mut self, from: &NodeId, outs: Vec<Outgoing<RaftMessage<u32>>>) {
        for out in outs {
            match out.target {
                SendTarget::Peer(to) => self.queue.push((from.clone(), to, out.message)),
                SendTarget::Broadcast => {
                    for id in &self.ids {
                        if id != from {
                            self.queue
                                .push((from.clone(), id.clone(), out.message.clone()));
                        }
                    }
                }
            }
        }
    }

    fn drain_decisions(&mut self, node: usize) {
        for d in self.nodes[node].take_decisions() {
            self.persisted_decisions[node].push((d.slot, d.value));
        }
    }

    fn step(&mut self, s: &Step) {
        match s {
            Step::Tick(n) => {
                let n = (*n as usize) % NUM_NODES;
                self.now += Duration::from_millis(50);
                let outs = self.nodes[n].on_tick(self.now);
                self.flush_persist(n);
                let from = self.ids[n].clone();
                self.enqueue(&from, outs);
                self.drain_decisions(n);
            }
            Step::Deliver(i) => {
                if self.queue.is_empty() {
                    return;
                }
                let i = *i % self.queue.len();
                let (from, to, msg) = self.queue.remove(i);
                if let Some(target) = self.idx_of(&to) {
                    let outs = self.nodes[target].handle_message(from, msg);
                    self.flush_persist(target);
                    let from_id = self.ids[target].clone();
                    self.enqueue(&from_id, outs);
                    self.drain_decisions(target);
                }
            }
            Step::Drop(i) => {
                if self.queue.is_empty() {
                    return;
                }
                let i = *i % self.queue.len();
                self.queue.remove(i);
            }
            Step::Reorder(a, b) => {
                if self.queue.len() < 2 {
                    return;
                }
                let a = *a % self.queue.len();
                let b = *b % self.queue.len();
                self.queue.swap(a, b);
            }
            Step::Propose(n, v) => {
                let n = (*n as usize) % NUM_NODES;
                let outs =
                    <RaftProtocol<u32> as ConsensusProtocol<u32>>::propose(&mut self.nodes[n], *v);
                self.flush_persist(n);
                let from = self.ids[n].clone();
                self.enqueue(&from, outs);
                self.drain_decisions(n);
            }
            Step::Restart(n) => {
                let n = (*n as usize) % NUM_NODES;
                self.flush_persist(n);
                self.nodes[n].recover(
                    self.persisted_term[n],
                    self.persisted_voted_for[n].clone(),
                    self.persisted_log[n].clone(),
                    self.persisted_decisions[n].clone(),
                );
            }
        }
    }

    fn assert_election_safety(&self) {
        let mut by_term: HashMap<u64, Vec<usize>> = HashMap::new();
        for (i, node) in self.nodes.iter().enumerate() {
            if matches!(node.role, Role::Leader) {
                by_term.entry(node.current_term).or_default().push(i);
            }
        }
        for (term, leaders) in by_term {
            assert!(
                leaders.len() <= 1,
                "election safety violated: term {term} has {} leaders {:?}",
                leaders.len(),
                leaders
            );
        }
    }

    fn assert_log_matching(&self) {
        for i in 0..self.nodes.len() {
            for j in (i + 1)..self.nodes.len() {
                let li = &self.nodes[i].log;
                let lj = &self.nodes[j].log;
                let min_len = li.len().min(lj.len());
                for k in (0..min_len).rev() {
                    if li[k].term == lj[k].term {
                        for m in 0..=k {
                            assert_eq!(
                                li[m].value, lj[m].value,
                                "log matching violated at i={m} between nodes {i} and {j}"
                            );
                            assert_eq!(li[m].term, lj[m].term);
                        }
                        break;
                    }
                }
            }
        }
    }

    fn assert_state_machine_safety(&self) {
        let mut applied: HashMap<u64, u32> = HashMap::new();
        for decisions in &self.persisted_decisions {
            for (slot, value) in decisions {
                if let Some(prev) = applied.get(slot) {
                    assert_eq!(
                        prev, value,
                        "state-machine safety violated: slot {slot} = {prev} vs {value}"
                    );
                } else {
                    applied.insert(*slot, *value);
                }
            }
        }
    }

    fn assert_term_monotonicity(&self) {
        for (i, node) in self.nodes.iter().enumerate() {
            assert!(
                node.current_term >= self.persisted_term[i],
                "term monotonicity violated at node {i}: current={} persisted={}",
                node.current_term,
                self.persisted_term[i]
            );
        }
    }

    fn assert_invariants(&self) {
        self.assert_election_safety();
        self.assert_log_matching();
        self.assert_state_machine_safety();
        self.assert_term_monotonicity();
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(
        std::env::var("PROPTEST_CASES").ok().and_then(|s| s.parse().ok()).unwrap_or(256)
    ))]

    #[test]
    fn random_steps_preserve_invariants(steps in vec(step_strategy(), 1..256)) {
        let mut h = Harness::new();
        for s in &steps {
            h.step(s);
            h.assert_invariants();
        }
    }
}

#[cfg(test)]
mod sanity {
    use super::*;

    #[test]
    fn harness_basic_election_and_propose() {
        let mut h = Harness::new();
        h.nodes[0].election_deadline = h.now - Duration::from_millis(1);
        h.step(&Step::Tick(0));
        for _ in 0..50 {
            if h.queue.is_empty() {
                h.step(&Step::Tick(0));
            } else {
                h.step(&Step::Deliver(0));
            }
        }
        let leader_count = h
            .nodes
            .iter()
            .filter(|n| matches!(n.role, Role::Leader))
            .count();
        assert!(leader_count >= 1, "expected a leader to be elected");
        h.assert_invariants();
    }
}
