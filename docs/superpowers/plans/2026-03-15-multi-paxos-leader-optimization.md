# Multi-Paxos Leader Optimization Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the Multi-Paxos leader optimization behind a `multi-paxos` feature flag so that a stable leader skips Phase 1, followers forward proposals to the leader, and leadership transfers via heartbeat-based failure detection.

**Architecture:** The feature is gated with `#[cfg(feature = "multi-paxos")]` throughout. `ProtocolState` gains leader-related fields (leader state, term, highest seen round). `propose()` gets a multi-paxos variant that skips Phase 1 when Leader. The node event loop gains forwarding logic and heartbeat/election checks on the existing 50ms retry tick. Two new message variants (`Forward`, `Heartbeat`) are added. All acceptor behavior is unchanged.

**Tech Stack:** Rust, tokio, serde, feature flags (`cfg`).

**Spec:** `docs/superpowers/specs/2026-03-15-multi-paxos-leader-optimization-design.md`

---

## Task 1: Feature flag and message types

**Files:**
- Modify: `daccord/Cargo.toml`
- Modify: `daccord/src/message.rs`

- [ ] **Step 1: Add `multi-paxos` feature flag**

In `daccord/Cargo.toml`, add to the `[features]` section:

```toml
multi-paxos = []
```

- [ ] **Step 2: Add `Forward` and `Heartbeat` message variants**

In `daccord/src/message.rs`, add two new variants to `MessageVariant<V>` gated behind the feature:

```rust
#[cfg(feature = "multi-paxos")]
Forward {
    value: V,
},
#[cfg(feature = "multi-paxos")]
Heartbeat {
    term: u64,
},
```

- [ ] **Step 3: Verify compilation with and without feature**

Run: `cargo build -p daccord` (without feature — should compile)
Run: `cargo build -p daccord --features multi-paxos` (with feature — should compile)

- [ ] **Step 4: Verify existing tests still pass**

Run: `cargo test -p daccord --all-features`

- [ ] **Step 5: Commit**

---

## Task 2: Leader state in ProtocolState

**Files:**
- Modify: `daccord/src/protocol.rs`

- [ ] **Step 1: Add `LeaderState` enum**

Add above `ProtocolState`, gated with `#[cfg(feature = "multi-paxos")]`:

```rust
#[cfg(feature = "multi-paxos")]
#[derive(Debug)]
pub(crate) enum LeaderState {
    Leader { term: u64 },
    Follower {
        leader: Option<NodeId>,
        last_contact: Instant,
    },
    Candidate,
}
```

- [ ] **Step 2: Add leader fields to `ProtocolState`**

Add these fields to the `ProtocolState<V>` struct, gated:

```rust
#[cfg(feature = "multi-paxos")]
leader_state: LeaderState,
/// The highest round number this node has seen from any source
/// (proposals, nacks, promises, decides). Used to compute the next
/// round when starting an election.
#[cfg(feature = "multi-paxos")]
highest_seen_round: u64,
/// When the leader last sent a Decide (used to decide if a heartbeat is needed).
#[cfg(feature = "multi-paxos")]
last_decide_time: Option<Instant>,
```

- [ ] **Step 3: Initialize leader fields in `ProtocolState::new()`**

In the `new()` constructor, add the initialization gated behind `#[cfg(feature = "multi-paxos")]`:

```rust
#[cfg(feature = "multi-paxos")]
leader_state: LeaderState::Follower {
    leader: None,
    last_contact: Instant::now(),
},
#[cfg(feature = "multi-paxos")]
highest_seen_round: 0,
#[cfg(feature = "multi-paxos")]
last_decide_time: None,
```

- [ ] **Step 4: Verify compilation**

Run: `cargo build -p daccord --features multi-paxos`
Run: `cargo test -p daccord --all-features`

- [ ] **Step 5: Commit**

---

## Task 3: Leader state transitions

**Files:**
- Modify: `daccord/src/protocol.rs`

This task adds the methods that transition leader state. These are called by the handlers in later tasks.

- [ ] **Step 1: Add `become_leader()` method**

```rust
#[cfg(feature = "multi-paxos")]
fn become_leader(&mut self, term: u64) {
    tracing::info!(term, node = %self.node_id, "became leader");
    self.leader_state = LeaderState::Leader { term };
    self.highest_seen_round = self.highest_seen_round.max(term);
    self.last_decide_time = None;
}
```

- [ ] **Step 2: Add `step_down()` method**

```rust
#[cfg(feature = "multi-paxos")]
fn step_down(&mut self, new_leader: Option<NodeId>) {
    if matches!(self.leader_state, LeaderState::Leader { .. }) {
        tracing::info!(
            node = %self.node_id,
            ?new_leader,
            "stepping down from leader"
        );
    }
    self.leader_state = LeaderState::Follower {
        leader: new_leader,
        last_contact: Instant::now(),
    };
}
```

- [ ] **Step 3: Add `update_leader_contact()` method**

Called when receiving a Decide or Heartbeat from the current leader:

```rust
#[cfg(feature = "multi-paxos")]
fn update_leader_contact(&mut self, from: &NodeId, term: u64) {
    self.highest_seen_round = self.highest_seen_round.max(term);
    match &mut self.leader_state {
        LeaderState::Follower { leader, last_contact } => {
            *leader = Some(from.clone());
            *last_contact = Instant::now();
        }
        LeaderState::Candidate => {
            // A leader exists — abandon election, become follower
            self.leader_state = LeaderState::Follower {
                leader: Some(from.clone()),
                last_contact: Instant::now(),
            };
        }
        LeaderState::Leader { term: my_term } => {
            if term > *my_term {
                // Higher term leader exists — step down
                self.step_down(Some(from.clone()));
            }
            // If term <= my_term, ignore (I'm the leader or co-equal)
        }
    }
}
```

- [ ] **Step 4: Add `update_highest_seen_round()` helper**

Called whenever we see a round number from any message:

```rust
#[cfg(feature = "multi-paxos")]
fn update_highest_seen_round(&mut self, round: u64) {
    self.highest_seen_round = self.highest_seen_round.max(round);
}
```

- [ ] **Step 5: Verify compilation and tests**

Run: `cargo test -p daccord --all-features`

- [ ] **Step 6: Commit**

---

## Task 4: Multi-Paxos propose (Phase 1 skip)

**Files:**
- Modify: `daccord/src/protocol.rs`

- [ ] **Step 1: Add multi-paxos `propose()` variant**

The existing `propose()` always starts Phase 1. Add a `#[cfg(feature = "multi-paxos")]` version that checks leader state first. Use conditional compilation to replace the method:

```rust
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
```

Rename the existing `propose()` body to `propose_full_paxos()` (used by both paths).

- [ ] **Step 2: Implement `propose_fast_path()`**

```rust
#[cfg(feature = "multi-paxos")]
fn propose_fast_path(&mut self, value: V, term: u64) -> (u64, Vec<Outgoing<V>>) {
    let slot = self.next_slot;
    self.next_slot += 1;

    let proposal_number: ProposalNumber = (term, self.node_id.clone());

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
    instance.accepts_received.insert(self.node_id.clone());

    // Also count self in promises (for retry logic consistency)
    instance.promises_received.insert(self.node_id.clone());

    // Check single-node quorum
    if instance.accepts_received.len() >= self.quorum_size {
        self.decide(slot, value.clone());
        self.instances.remove(&slot);
        return (slot, vec![Outgoing {
            target: SendTarget::Broadcast,
            message: MessageVariant::Decide { slot, value },
        }]);
    }

    (slot, vec![Outgoing {
        target: SendTarget::Broadcast,
        message: MessageVariant::Accept {
            slot,
            proposal_number,
            value,
        },
    }])
}
```

- [ ] **Step 3: Hook leader transition into `handle_promise()`**

When promise quorum is reached (existing code calls `start_phase2`), add multi-paxos leader promotion:

After the `tracing::debug!(slot, "promise quorum reached, starting Phase 2");` line, add:

```rust
#[cfg(feature = "multi-paxos")]
{
    let term = self.instances.get(&slot).unwrap().proposal_number.0;
    self.become_leader(term);
}
```

- [ ] **Step 4: Hook leader step-down into `handle_nack()`**

When a nack is received, if the node is Leader, step down:

At the end of `handle_nack()`, after the existing nack processing, add:

```rust
#[cfg(feature = "multi-paxos")]
{
    if matches!(self.leader_state, LeaderState::Leader { .. }) {
        self.step_down(None);
    }
    self.update_highest_seen_round(highest_promised.0);
}
```

- [ ] **Step 5: Hook leader contact into `handle_decide()`**

When receiving a Decide, update leader state. At the start of `handle_decide()`, before the existing logic, add:

```rust
#[cfg(feature = "multi-paxos")]
{
    // The sender of the Decide is (or was) the leader.
    // We don't have `from` in handle_decide — it needs to be passed in.
}
```

Note: `handle_decide` currently doesn't receive `from`. It's called from `handle_message()` which does have `from`. Modify `handle_decide` signature to accept `from: NodeId`:

```rust
fn handle_decide(&mut self, from: NodeId, slot: u64, value: V) {
    #[cfg(feature = "multi-paxos")]
    {
        // Extract term from the decide — we don't have it directly,
        // but the slot number and the existence of the decide proves
        // liveness. Use highest_seen_round as the term approximation.
        self.update_leader_contact(&from, self.highest_seen_round);
    }
    // ... existing logic unchanged ...
}
```

Update the call site in `handle_message()` to pass `from`:

```rust
MessageVariant::Decide { slot, value } => {
    self.handle_decide(from, slot, value);
    vec![]
}
```

- [ ] **Step 6: Verify compilation and existing tests**

Run: `cargo test -p daccord --all-features`
Run: `cargo test -p daccord` (without multi-paxos — ensure classic path unchanged)

- [ ] **Step 7: Commit**

---

## Task 5: Heartbeat and election logic

**Files:**
- Modify: `daccord/src/protocol.rs`

- [ ] **Step 1: Add `should_send_heartbeat()` method**

```rust
#[cfg(feature = "multi-paxos")]
pub(crate) fn should_send_heartbeat(&self) -> bool {
    if let LeaderState::Leader { .. } = &self.leader_state {
        let heartbeat_interval = std::time::Duration::from_millis(100);
        match self.last_decide_time {
            Some(t) => Instant::now().duration_since(t) >= heartbeat_interval,
            None => true, // Never sent a decide as leader, send heartbeat
        }
    } else {
        false
    }
}
```

- [ ] **Step 2: Add `make_heartbeat()` method**

```rust
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
```

- [ ] **Step 3: Record `last_decide_time` when deciding**

In the `decide()` helper method, add:

```rust
#[cfg(feature = "multi-paxos")]
{
    self.last_decide_time = Some(Instant::now());
}
```

- [ ] **Step 4: Add `check_leader_timeout()` method**

```rust
#[cfg(feature = "multi-paxos")]
pub(crate) fn check_leader_timeout(&self) -> bool {
    let leader_timeout = std::time::Duration::from_millis(500);
    if let LeaderState::Follower { leader: Some(_), last_contact } = &self.leader_state {
        Instant::now().duration_since(*last_contact) >= leader_timeout
    } else {
        false
    }
}
```

- [ ] **Step 5: Add `start_election()` method**

Called when a follower's leader timeout expires:

```rust
#[cfg(feature = "multi-paxos")]
pub(crate) fn start_election(&mut self) -> Vec<Outgoing<V>> {
    tracing::info!(node = %self.node_id, "starting leader election");
    self.leader_state = LeaderState::Candidate;

    // Propose a no-op or use the next slot with a round higher than any seen
    let round = self.highest_seen_round + 1;
    let slot = self.next_slot;
    self.next_slot += 1;

    let proposal_number: ProposalNumber = (round, self.node_id.clone());
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
    instance.promises_received.insert(self.node_id.clone());

    if instance.promises_received.len() >= self.quorum_size {
        // Single-node: become leader immediately
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
```

- [ ] **Step 6: Add `handle_heartbeat()` method**

```rust
#[cfg(feature = "multi-paxos")]
fn handle_heartbeat(&mut self, from: NodeId, term: u64) -> Vec<Outgoing<V>> {
    if term >= self.highest_seen_round {
        self.update_leader_contact(&from, term);
    }
    // Heartbeats don't produce responses
    vec![]
}
```

- [ ] **Step 7: Add `handle_forward()` method**

```rust
#[cfg(feature = "multi-paxos")]
fn handle_forward(&mut self, _from: NodeId, value: V) -> Vec<Outgoing<V>> {
    if let LeaderState::Leader { .. } = &self.leader_state {
        tracing::debug!(node = %self.node_id, "received forwarded proposal");
        let (_, outgoing) = self.propose(value);
        outgoing
    } else {
        // Not the leader — ignore (the sender will timeout and retry)
        tracing::debug!(node = %self.node_id, "received forward but not leader, ignoring");
        vec![]
    }
}
```

- [ ] **Step 8: Wire new message variants into `handle_message()`**

Add to the match in `handle_message()`:

```rust
#[cfg(feature = "multi-paxos")]
MessageVariant::Forward { value } => self.handle_forward(from, value),
#[cfg(feature = "multi-paxos")]
MessageVariant::Heartbeat { term } => self.handle_heartbeat(from, term),
```

- [ ] **Step 9: Verify compilation and tests**

Run: `cargo test -p daccord --all-features`
Run: `cargo test -p daccord` (classic paxos path)

- [ ] **Step 10: Commit**

---

## Task 6: Node event loop changes

**Files:**
- Modify: `daccord/src/node.rs`

- [ ] **Step 1: Add forwarding state to Node struct**

Under `#[cfg(feature = "multi-paxos")]`, add tracking for forwarded proposals:

```rust
#[cfg(feature = "multi-paxos")]
forwarded_proposals: Vec<(Instant, V)>,
```

Initialize to empty `Vec::new()` in `with_id_inner()`.

- [ ] **Step 2: Modify `handle_proposal()` for forwarding**

Replace the existing `handle_proposal` with a version that checks for forwarding under multi-paxos:

```rust
async fn handle_proposal(&mut self, value: V, senders: &[(NodeId, S)]) -> Result<(), NodeError> {
    #[cfg(feature = "multi-paxos")]
    {
        if let Some(leader_id) = self.protocol.get_leader() {
            if leader_id != self.node_id {
                // Forward to leader
                tracing::debug!(leader = %leader_id, "forwarding proposal to leader");
                let outgoing = vec![Outgoing {
                    target: SendTarget::Peer(leader_id),
                    message: MessageVariant::Forward { value: value.clone() },
                }];
                Self::send_outgoing(&self.node_id, &outgoing, senders).await;
                self.forwarded_proposals.push((Instant::now(), value));
                return Ok(());
            }
        }
    }

    // No leader or we ARE the leader — propose locally
    let (slot, outgoing) = self.protocol.propose(value);
    tracing::debug!(slot, "new proposal");
    Self::send_outgoing(&self.node_id, &outgoing, senders).await;
    self.process_decisions().await?;
    Ok(())
}
```

Add a `get_leader()` helper to `ProtocolState`:

```rust
#[cfg(feature = "multi-paxos")]
pub(crate) fn get_leader(&self) -> Option<NodeId> {
    match &self.leader_state {
        LeaderState::Leader { .. } => Some(self.node_id.clone()),
        LeaderState::Follower { leader, .. } => leader.clone(),
        LeaderState::Candidate => None,
    }
}
```

- [ ] **Step 3: Add heartbeat and election checks to `handle_retries()`**

At the end of `handle_retries()`, add multi-paxos logic:

```rust
#[cfg(feature = "multi-paxos")]
{
    // Send heartbeat if we're the leader and haven't sent a Decide recently
    if self.protocol.should_send_heartbeat() {
        let heartbeat = self.protocol.make_heartbeat();
        Self::send_outgoing(&self.node_id, &heartbeat, senders).await;
    }

    // Check for leader timeout — start election if leader is unresponsive
    if self.protocol.check_leader_timeout() {
        let outgoing = self.protocol.start_election();
        Self::send_outgoing(&self.node_id, &outgoing, senders).await;
    }

    // Check forwarded proposal timeouts (1 second)
    let forward_timeout = std::time::Duration::from_secs(1);
    let now = Instant::now();
    let timed_out: Vec<V> = self.forwarded_proposals
        .iter()
        .filter(|(t, _)| now.duration_since(*t) >= forward_timeout)
        .map(|(_, v)| v.clone())
        .collect();
    self.forwarded_proposals.retain(|(t, _)| now.duration_since(*t) < forward_timeout);

    for value in timed_out {
        tracing::debug!("forwarded proposal timed out, proposing directly");
        let (_, outgoing) = self.protocol.propose(value);
        Self::send_outgoing(&self.node_id, &outgoing, senders).await;
        self.process_decisions().await?;
    }
}
```

- [ ] **Step 4: Clean up forwarded proposals on Decide**

In `handle_incoming_bytes()`, after processing a Decide, remove any matching forwarded proposal. After `self.process_decisions().await?;`:

```rust
#[cfg(feature = "multi-paxos")]
{
    let decisions = &self.protocol.pending_decisions; // already taken, use the ones just processed
    // A simpler approach: just clear forwarded proposals when we see any decide,
    // since the forwarded value may have been decided under a different slot.
    // The timeout fallback handles the case where the forward was truly lost.
    // For now, we clear forwarded proposals that match a decided value.
    let decided_values: Vec<V> = self.protocol.take_decisions()
        .iter()
        .map(|d| d.value.clone())
        .collect();
    self.forwarded_proposals.retain(|(_, v)| {
        !decided_values.iter().any(|dv| dv == v)
    });
}
```

Actually, the simplest correct approach: in `process_decisions()`, check each decided value against `forwarded_proposals` and remove matches. Add this after the decision loop:

```rust
#[cfg(feature = "multi-paxos")]
{
    self.forwarded_proposals.retain(|(_, v)| {
        !decisions.iter().any(|d| d.value == *v)
    });
}
```

This requires capturing the decisions before they're consumed. Restructure `process_decisions` to keep a reference.

- [ ] **Step 5: Verify compilation and tests**

Run: `cargo test -p daccord --all-features`
Run: `cargo test -p daccord` (classic path)

- [ ] **Step 6: Commit**

---

## Task 7: Multi-Paxos integration tests

**Files:**
- Modify: `daccord-tests/Cargo.toml`
- Create: `daccord-tests/tests/multi_paxos.rs`

- [ ] **Step 1: Add multi-paxos feature to test crate**

In `daccord-tests/Cargo.toml`:

```toml
[features]
multi-paxos = ["daccord/multi-paxos"]
```

- [ ] **Step 2: Create `multi_paxos.rs` test file**

This file is only compiled when the `multi-paxos` feature is active:

```rust
#![cfg(feature = "multi-paxos")]

mod helpers;

use std::collections::HashSet;
use helpers::{
    assert_consistent_decisions, assert_safety_invariant,
    collect_decisions, collect_decisions_with_timeout,
    create_cluster, create_lossy_cluster,
};
use tokio::time::Duration;
```

- [ ] **Step 3: Write `leader_emerges_on_first_proposal` test**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn leader_emerges_on_first_proposal() {
    let mut cluster = create_cluster(3);
    cluster[0].handle.propose("first".to_string()).await.unwrap();

    // All nodes should decide
    let mut all = Vec::new();
    for node in &mut cluster {
        let decisions = collect_decisions(&mut node.decisions, 1).await;
        assert_eq!(decisions[0].value, "first");
        all.push(decisions);
    }
    assert_consistent_decisions(&all);
    for node in cluster { drop(node.handle); }
}
```

- [ ] **Step 4: Write `leader_fast_path_multiple_proposals` test**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn leader_fast_path_multiple_proposals() {
    let mut cluster = create_cluster(3);

    // First proposal establishes leadership via full Paxos
    cluster[0].handle.propose("first".to_string()).await.unwrap();
    for node in &mut cluster {
        collect_decisions(&mut node.decisions, 1).await;
    }

    // Subsequent proposals should use fast path (1 RT)
    let expected: HashSet<String> = (1..=10).map(|i| format!("v-{}", i)).collect();
    for v in &expected {
        cluster[0].handle.propose(v.clone()).await.unwrap();
    }

    let mut all = Vec::new();
    for node in &mut cluster {
        let decisions = collect_decisions(&mut node.decisions, 10).await;
        let values: HashSet<String> = decisions.iter().map(|d| d.value.clone()).collect();
        assert_eq!(values, expected);
        all.push(decisions);
    }
    assert_consistent_decisions(&all);
    for node in cluster { drop(node.handle); }
}
```

- [ ] **Step 5: Write `follower_forwards_to_leader` test**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn follower_forwards_to_leader() {
    let mut cluster = create_cluster(3);

    // Establish node-0 as leader
    cluster[0].handle.propose("establish".to_string()).await.unwrap();
    for node in &mut cluster {
        collect_decisions(&mut node.decisions, 1).await;
    }

    // Propose from node-1 (follower) — should be forwarded to leader
    cluster[1].handle.propose("from-follower".to_string()).await.unwrap();

    let mut all = Vec::new();
    for node in &mut cluster {
        let decisions = collect_decisions(&mut node.decisions, 1).await;
        assert_eq!(decisions[0].value, "from-follower");
        all.push(decisions);
    }
    assert_consistent_decisions(&all);
    for node in cluster { drop(node.handle); }
}
```

- [ ] **Step 6: Write `leader_crash_and_failover` test**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn leader_crash_and_failover() {
    let mut cluster = create_cluster(3);

    // Establish node-0 as leader
    cluster[0].handle.propose("before-crash".to_string()).await.unwrap();
    for node in &mut cluster {
        collect_decisions(&mut node.decisions, 1).await;
    }

    // Kill the leader (node-0)
    drop(cluster[0].handle.clone());
    cluster[0].run_handle.abort();

    // Wait for leader timeout (500ms) + election
    tokio::time::sleep(Duration::from_millis(800)).await;

    // Propose from node-1 — should trigger election or use new leader
    cluster[1].handle.propose("after-crash".to_string()).await.unwrap();

    // Nodes 1 and 2 should decide
    let timeout = Duration::from_secs(10);
    for node in cluster.iter_mut().skip(1) {
        let decisions = collect_decisions_with_timeout(&mut node.decisions, 1, timeout).await;
        assert_eq!(decisions[0].value, "after-crash");
    }

    for node in cluster { drop(node.handle); }
}
```

- [ ] **Step 7: Write `forwarding_fallback_no_leader` test**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn forwarding_fallback_no_leader() {
    // Fresh cluster — no leader established yet
    let mut cluster = create_cluster(3);

    // Propose from node-1 — no leader known, should fall back to full Paxos
    cluster[1].handle.propose("no-leader".to_string()).await.unwrap();

    let mut all = Vec::new();
    for node in &mut cluster {
        let decisions = collect_decisions(&mut node.decisions, 1).await;
        assert_eq!(decisions[0].value, "no-leader");
        all.push(decisions);
    }
    assert_consistent_decisions(&all);
    for node in cluster { drop(node.handle); }
}
```

- [ ] **Step 8: Write `heartbeat_keeps_leadership` test**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn heartbeat_keeps_leadership() {
    let mut cluster = create_cluster(3);

    // Establish leader
    cluster[0].handle.propose("establish".to_string()).await.unwrap();
    for node in &mut cluster {
        collect_decisions(&mut node.decisions, 1).await;
    }

    // Wait longer than leader timeout — heartbeats should keep leadership alive
    tokio::time::sleep(Duration::from_millis(800)).await;

    // Propose from follower — should still forward to original leader
    cluster[1].handle.propose("still-alive".to_string()).await.unwrap();

    let mut all = Vec::new();
    for node in &mut cluster {
        let decisions = collect_decisions(&mut node.decisions, 1).await;
        assert_eq!(decisions[0].value, "still-alive");
        all.push(decisions);
    }
    assert_consistent_decisions(&all);
    for node in cluster { drop(node.handle); }
}
```

- [ ] **Step 9: Write `safety_under_leader_transition` test**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn safety_under_leader_transition() {
    let mut cluster = create_lossy_cluster(3, 0.10);

    // Multiple proposers creating contention and leadership changes
    for (i, node) in cluster.iter().enumerate() {
        for j in 0..5 {
            node.handle.propose(format!("n{}-v{}", i, j)).await.unwrap();
        }
    }

    let mut all = Vec::new();
    for node in &mut cluster {
        let mut decisions = Vec::new();
        for _ in 0..15 {
            match tokio::time::timeout(Duration::from_secs(10), node.decisions.recv()).await {
                Ok(Some(d)) => decisions.push(d),
                _ => break,
            }
        }
        all.push(decisions);
    }

    assert_safety_invariant(&all);
    let total: usize = all.iter().map(|d| d.len()).sum();
    assert!(total > 0, "no decisions were made");
    for node in cluster { drop(node.handle); }
}
```

- [ ] **Step 10: Write `lossy_network_with_leader` test**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lossy_network_with_leader() {
    let mut cluster = create_lossy_cluster(3, 0.10);

    let expected: HashSet<String> = (0..5).map(|i| format!("v-{}", i)).collect();
    for v in &expected {
        cluster[0].handle.propose(v.clone()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let timeout = Duration::from_secs(30);
    let mut all = Vec::new();
    for node in &mut cluster {
        let decisions = collect_decisions_with_timeout(&mut node.decisions, 5, timeout).await;
        let values: HashSet<String> = decisions.iter().map(|d| d.value.clone()).collect();
        assert_eq!(values, expected);
        all.push(decisions);
    }
    assert_consistent_decisions(&all);
    for node in cluster { drop(node.handle); }
}
```

- [ ] **Step 11: Verify all multi-paxos tests pass**

Run: `cargo test -p daccord-tests --features multi-paxos --test multi_paxos -- --test-threads=1`

- [ ] **Step 12: Commit**

---

## Task 8: Run existing tests with multi-paxos feature

**Files:**
- No new files — just verification

The public API is unchanged, so all existing tests should pass transparently when the `multi-paxos` feature is enabled. The leader optimization just makes things faster, not different.

- [ ] **Step 1: Run all existing test suites with multi-paxos enabled**

```bash
cargo test -p daccord --all-features
cargo test -p daccord-tests --features multi-paxos --test cluster
cargo test -p daccord-tests --features multi-paxos --test adversarial_cluster -- --test-threads=1
cargo test -p daccord-tests --features multi-paxos --test larger_clusters -- --test-threads=1
cargo test -p daccord-tests --features multi-paxos --test delayed_cluster -- --test-threads=1
cargo test -p daccord-tests --features multi-paxos --test lifecycle
cargo test -p daccord-tests --features multi-paxos --test safety_invariants -- --test-threads=1
cargo test -p daccord-tests --features multi-paxos --test stress -- --test-threads=1
cargo test -p daccord-tests --features multi-paxos --test complex_values
cargo test -p daccord-tests --features multi-paxos --test recovery
```

- [ ] **Step 2: Fix any failures**

If any test fails, debug and fix. Common issues:
- Forwarding timeout too aggressive for slow tests
- Leader election interfering with concurrent proposal tests
- Heartbeat messages causing unexpected behavior in transport filter tests

- [ ] **Step 3: Commit any fixes**

---

## Task 9: Docker Compose demo with multi-paxos

**Files:**
- Modify: `daccord-demo/Cargo.toml`
- Modify: `daccord-demo/Dockerfile`
- Create: `daccord-demo/docker-compose.multi-paxos.yml`

- [ ] **Step 1: Add multi-paxos feature to demo crate**

In `daccord-demo/Cargo.toml`, add:

```toml
[features]
multi-paxos = ["daccord/multi-paxos"]
```

- [ ] **Step 2: Update Dockerfile to accept optional features**

Modify `daccord-demo/Dockerfile` to accept a build arg:

```dockerfile
# Stage 3: Builder - cache deps, then build
FROM chef AS builder
ARG FEATURES=""
RUN apt-get update && apt-get install -y protobuf-compiler && rm -rf /var/lib/apt/lists/*
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json -p daccord-demo
COPY . .
RUN if [ -n "$FEATURES" ]; then \
      cargo build --release -p daccord-demo --features "$FEATURES"; \
    else \
      cargo build --release -p daccord-demo; \
    fi
```

- [ ] **Step 3: Create `docker-compose.multi-paxos.yml`**

Based on `docker-compose.tcp.yml` but with the `FEATURES` build arg:

```yaml
x-node: &node-base
  build:
    context: ../..
    dockerfile: daccord-demo/Dockerfile
    args:
      FEATURES: multi-paxos
  environment: &node-env
    TRANSPORT: tcp
    BIND_ADDR: "0.0.0.0:9000"
    PEERS: "node-1=node-1:9000,node-2=node-2:9000,node-3=node-3:9000"
    GRPC_PORT: "50051"
    RUST_LOG: info
  healthcheck:
    test: ["CMD", "daccord-cli", "--addr", "localhost:50051", "health"]
    interval: 5s
    timeout: 3s
    retries: 10
    start_period: 10s
  networks:
    - consensus-net

services:
  node-1:
    <<: *node-base
    hostname: node-1
    ports:
      - "50051:50051"
  node-2:
    <<: *node-base
    hostname: node-2
    ports:
      - "50052:50051"
  node-3:
    <<: *node-base
    hostname: node-3
    ports:
      - "50053:50051"

networks:
  consensus-net:
    driver: bridge
```

Note: all 3 nodes expose their gRPC port so we can test failover by proposing to different nodes.

- [ ] **Step 4: Verify Docker build works**

Run from repo root:
```bash
cd daccord-demo
docker compose -f docker-compose.multi-paxos.yml build
```

- [ ] **Step 5: Manual test — start cluster, propose, verify**

```bash
docker compose -f docker-compose.multi-paxos.yml up -d
sleep 15
daccord-cli --addr localhost:50051 propose --value "alice"
daccord-cli --addr localhost:50051 propose --value "bob"
daccord-cli --addr localhost:50051 decisions
```

Verify output shows both values decided with consistent slots.

- [ ] **Step 6: Manual test — check leader election logs**

```bash
docker compose -f docker-compose.multi-paxos.yml logs | grep -i "leader"
```

Should show one node becoming leader.

- [ ] **Step 7: Manual test — leader failover**

```bash
docker compose -f docker-compose.multi-paxos.yml stop node-1
sleep 2
daccord-cli --addr localhost:50052 propose --value "after-failover"
daccord-cli --addr localhost:50052 decisions
docker compose -f docker-compose.multi-paxos.yml start node-1
sleep 5
daccord-cli --addr localhost:50051 decisions
```

- [ ] **Step 8: Shutdown and commit**

```bash
docker compose -f docker-compose.multi-paxos.yml down
```

- [ ] **Step 9: Commit**

---

## Verification

After all tasks complete:

```bash
# Classic Paxos still works (no feature)
cargo test -p daccord
cargo test -p daccord-tests

# Multi-Paxos works
cargo test -p daccord --features multi-paxos
cargo test -p daccord-tests --features multi-paxos --test multi_paxos -- --test-threads=1

# All existing tests pass with multi-paxos enabled
cargo test -p daccord-tests --features multi-paxos -- --test-threads=1

# Docker demo builds and runs
cd daccord-demo && docker compose -f docker-compose.multi-paxos.yml build
```
