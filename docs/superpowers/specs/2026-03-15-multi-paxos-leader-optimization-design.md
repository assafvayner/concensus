# Multi-Paxos Leader Optimization Design

## Goal

Implement the Multi-Paxos leader optimization behind a `multi-paxos` feature flag. When a stable leader is established, proposals skip Phase 1 (Prepare/Promise) and go directly to Phase 2 (Accept/Accepted), reducing the happy-path latency from 2 round trips to 1. Non-leader nodes forward proposals to the leader. Leadership is maintained via heartbeats and transfers via preemption.

This is a learning exercise — correctness and clarity over production hardening.

## Current State

Every proposal goes through full Classic Paxos:
1. Phase 1: Proposer broadcasts Prepare, collects Promise quorum
2. Phase 2: Proposer broadcasts Accept, collects Accepted quorum
3. Proposer broadcasts Decide

Cost: 2 round trips per proposal. No leader concept. Any node can propose independently. Each slot is a completely independent Paxos instance.

## Design

### Leader State & Terms

Each node maintains leader-related state (feature-gated):

```rust
#[cfg(feature = "multi-paxos")]
enum LeaderState {
    Leader { term: u64 },
    Follower { leader: Option<NodeId>, last_contact: Instant },
    Candidate,
}
```

- **Term** is the round number from a successful Phase 1. It maps directly to the existing `ProposalNumber.0`.
- **Becoming leader:** A node transitions to `Leader` when it wins Phase 1 (quorum of Promises) for any slot. It records the winning round as its term.
- **Losing leadership:** A leader steps down to `Follower` when:
  - It receives a NackPrepare or NackAccept with a higher round
  - It sees a Decide or Heartbeat from another node with a higher term
- All nodes start as `Follower { leader: None, last_contact: now }`.

### Phase 1 Skip (Core Optimization)

When a node is `Leader` with term `R`:

- New proposals skip Phase 1 entirely. The leader allocates the next slot, self-votes as acceptor, and broadcasts `Accept(slot, (R, leader_id), value)`.
- **Safety argument:** Phase 1 establishes that no value has been accepted at a higher round for the slot. Since the leader won Phase 1 at round `R` and no higher round has been observed, any acceptor that promised round `R` will honor Accept messages at round `R` for new (empty) slots.
- If the Accept is nacked (a higher round exists), the leader falls back to full Paxos for that slot and steps down.

Acceptor behavior is completely unchanged. Acceptors don't need to know about leadership.

### Proposal Forwarding

When a `Follower` node with a known leader receives a `propose()` call:

1. Send a `Forward { value }` message to the leader via the existing transport.
2. The leader receives the Forward, treats it as a local `propose()`, and runs Phase 2.
3. The decision reaches all nodes via the normal Decide broadcast.

**Fallback conditions:**
- If `current_leader` is `None`, the node proposes directly via full Paxos (Phase 1 + Phase 2). This triggers leader election naturally.
- If no Decide arrives for a forwarded value within a timeout (e.g., 1 second), the forwarding node falls back to direct proposal via full Paxos.

The public API (`NodeHandle::propose()`) is unchanged. Forwarding is transparent to the application.

### Heartbeat & Failure Detection

**Leader sends heartbeats:**
- Decide broadcasts serve as implicit heartbeats (prove liveness and carry data).
- When idle (no Decides sent recently), the leader sends an explicit `Heartbeat { term }` message to all peers on a timer (100ms interval).
- The leader resets its heartbeat timer each time it sends a Decide.

**Followers detect failure:**
- Track `last_contact: Instant`, updated on receiving Decide or Heartbeat from the current leader.
- If `last_contact` exceeds the leader timeout (500ms), transition to `Candidate` and run Phase 1 with a round higher than any seen.

**Stale heartbeats:** A follower ignores Heartbeats with a term lower than its current known term.

**Timing constants:**
- Heartbeat interval: 100ms
- Leader timeout: 500ms (5x heartbeat interval)

The existing 50ms retry interval in the node event loop drives heartbeat checks — no new timer needed.

### New Message Types

```rust
#[cfg(feature = "multi-paxos")]
Forward { value: V }

#[cfg(feature = "multi-paxos")]
Heartbeat { term: u64 }
```

### Leader Transition Scenarios

**Clean startup:** All nodes start as `Follower` with no known leader. First `propose()` call has no leader to forward to, so the node runs full Paxos. It wins Phase 1, becomes `Leader`. Other nodes see its Decide and update `current_leader`. Subsequent proposals from any node get forwarded.

**Leader proposes multiple values:** Leader allocates consecutive slots, skipping Phase 1 for each. Broadcasts Accept directly. Each Decide resets followers' heartbeat timers.

**Leader crashes:** Followers stop receiving Decides/Heartbeats. After 500ms, one or more become `Candidate` and run Phase 1. First to win becomes new leader. Forwarded proposals pending at the old leader are lost — forwarding nodes' timeouts fire and they repropose via the new leader or full Paxos.

**Competing candidates:** Two followers timeout simultaneously, both run Phase 1. Normal Paxos contention resolution applies — one wins, one gets nacked and backs off with jittered retry.

**Network partition heals:** Old leader on minority side can't reach quorum, proposals stall. Majority side elects new leader at higher term. When partition heals, old leader's Accepts get nacked, it sees Decides from new leader and steps down.

## Code Changes

### Feature flag

`daccord/Cargo.toml`:
```toml
[features]
multi-paxos = []
```

### `protocol.rs`

- Add `LeaderState` enum and leader state fields to `ProtocolState` under `#[cfg(feature = "multi-paxos")]`
- Add `#[cfg(feature = "multi-paxos")]` version of `propose()` that checks leader state and skips Phase 1 when Leader
- Add handler for `Forward` messages (leader receives, calls propose)
- Add handler for `Heartbeat` messages (follower updates leader contact time)
- Update `handle_decide()`: under multi-paxos, record sender as current leader, reset contact time
- Update `handle_nack()`: under multi-paxos, step down if nacked with higher round while Leader
- Add `should_send_heartbeat()`: returns true if Leader and no Decide sent recently
- Add `make_heartbeat()`: produces outgoing Heartbeat message
- Add `check_leader_timeout()`: returns true if Follower and leader timeout expired
- Add `start_election()`: transition to Candidate, run Phase 1 for a new slot

### `node.rs`

- Modify `handle_proposal()`: under multi-paxos, if Follower with known leader, send Forward instead of proposing. Track forwarded proposals with timeout.
- Add heartbeat logic to retry interval tick: if `should_send_heartbeat()`, send it. If `check_leader_timeout()`, start election.
- Handle `Forward` in `handle_incoming_bytes()`: if Leader, call propose with forwarded value.
- Handle forwarding timeout: if no Decide for a forwarded value within 1 second, fall back to local full Paxos.

### `message.rs`

- Add `Forward { value: V }` and `Heartbeat { term: u64 }` variants to `MessageVariant` under `#[cfg(feature = "multi-paxos")]`

### No changes to

Transports, storage, config, error types, public API (`Node`, `NodeHandle`, `DecisionReceiver`, `Decided`).

## Testing

### Feature gating in test crate

`daccord-tests/Cargo.toml`:
```toml
[features]
multi-paxos = ["daccord/multi-paxos"]

[dev-dependencies]
daccord = { path = "../daccord", features = ["channel-transport", "test-support"] }
```

### Existing tests with multi-paxos

Run all existing test suites (cluster, adversarial_cluster, larger_clusters, delayed_cluster, lifecycle, safety_invariants, stress, complex_values, recovery) with the `multi-paxos` feature enabled. The public API is unchanged, so these should pass transparently. This provides strong regression coverage.

Create a feature-gated test file (or use `cfg` within existing test files) that re-runs the existing test helpers with multi-paxos enabled.

### New multi-paxos-specific tests

File: `daccord-tests/tests/multi_paxos.rs` (only compiled with `multi-paxos` feature)

1. **Leader emerges on first proposal** — 3-node cluster, propose from node 0, verify it becomes leader
2. **Leader fast path** — propose 10 sequential values from the leader, verify all decide (confirm Phase 1 is skipped after first)
3. **Follower forwards to leader** — propose from a non-leader node, verify consensus via forwarding
4. **Leader crash and failover** — leader proposes, then is killed. After timeout, follower becomes new leader
5. **Forwarding fallback on no leader** — fresh cluster, no leader known, proposal works via full Paxos
6. **Forwarding timeout fallback** — forward to leader, kill leader, forwarding node falls back to direct proposal
7. **Heartbeat keeps leadership** — idle leader sends heartbeats, followers don't trigger election
8. **Stale leader steps down** — old leader sees Decide from higher term, transitions to Follower
9. **Safety under leader transition** — propose during failover, verify no safety violations via `assert_safety_invariant`
10. **Lossy network with leader** — 10% loss, verify leader election and proposals still work

### Docker Compose demo with multi-paxos

The existing `daccord-demo` crate runs a 3-node cluster with gRPC CLI. It should work with multi-paxos enabled to validate the optimization in a realistic deployment.

**Changes to demo crate:**

- `daccord-demo/Cargo.toml`: Add `multi-paxos` feature that forwards to `daccord/multi-paxos`:
  ```toml
  [features]
  multi-paxos = ["daccord/multi-paxos"]
  ```
- Add a new `docker-compose.multi-paxos.yml` (or parameterize the existing ones) that builds the demo with `--features multi-paxos`. The simplest approach: a new compose file that sets a build arg, with the Dockerfile updated to accept an optional `FEATURES` arg passed to `cargo build --release --features "$FEATURES"`.

**Manual test plan (run in the demo directory):**

1. **Start cluster with multi-paxos:**
   ```bash
   docker compose -f docker-compose.multi-paxos.yml up --build -d
   ```

2. **Wait for healthy, then propose values:**
   ```bash
   daccord-cli --addr localhost:50051 propose --value "alice"
   daccord-cli --addr localhost:50051 propose --value "bob"
   daccord-cli --addr localhost:50051 propose --value "charlie"
   ```

3. **Verify decisions are consistent across nodes:**
   ```bash
   daccord-cli --addr localhost:50051 decisions
   ```

4. **Verify leader election occurred** by checking logs for leader state transitions:
   ```bash
   docker compose -f docker-compose.multi-paxos.yml logs | grep -i "leader"
   ```

5. **Test leader failover** — kill the leader node, propose from another node's gRPC port, verify consensus continues:
   ```bash
   docker compose -f docker-compose.multi-paxos.yml stop node-1
   # Propose via node-2 (port 50052 if exposed, or via docker exec)
   docker exec daccord-node-2-1 daccord-cli --addr localhost:50051 propose --value "after-failover"
   docker exec daccord-node-2-1 daccord-cli --addr localhost:50051 decisions
   ```

6. **Restart killed node, verify it catches up:**
   ```bash
   docker compose -f docker-compose.multi-paxos.yml start node-1
   sleep 5
   daccord-cli --addr localhost:50051 decisions
   ```

7. **Shutdown:**
   ```bash
   docker compose -f docker-compose.multi-paxos.yml down
   ```

**Logging:** Add `tracing::info!` calls for leader state transitions (became leader, stepped down, forwarding proposal, received forward) so they're visible in the Docker logs. These are gated behind `#[cfg(feature = "multi-paxos")]`.
