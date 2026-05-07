# Simulation Test Expansion Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expand the daccord-tests simulation suite to cover cluster sizes, transport filter combinations, storage recovery, graceful shutdown, and Paxos safety invariants that the current tests don't exercise.

**Architecture:** All new tests live in `crates/daccord-tests/tests/`. New test files are organized by concern: larger cluster sizes, combined transport filters, persistence/recovery, shutdown behavior, and Paxos invariant fuzzing. Shared helpers are added to the existing `helpers/mod.rs` and `helpers/transport_filters.rs`. No changes to the daccord library itself.

**Tech Stack:** Rust, tokio (multi-thread runtime), daccord crate with `channel-transport` + `test-support` features, existing transport filter wrappers (Lossy, Delayed, Reordering).

---

## Current Coverage Summary

**What IS tested (140 tests):**
- Protocol unit tests: all Paxos phases, nack/retry, backoff, value selection, garbage collection (42 tests)
- Node unit tests: creation, cloning, channel-full, single-node, three-node (5 tests)
- Message/config/error/storage/transport: serde, display, roundtrip, channel, TCP, UDS (40 tests)
- Integration cluster tests: 1/3-node clusters, sequential/concurrent proposals, dead node, rapid proposals (6 tests)
- Adversarial lossy tests: 1%/10%/20%/30% loss x single/multi/different-node proposals (11 tests)
- Transport filter unit tests: Lossy/Delayed/Reordering sender+receiver (19 tests)

**What is NOT tested (gaps this plan fills):**

1. **Cluster sizes beyond 3** — 4-node, 5-node, and 7-node clusters are never tested. Even-sized clusters like 4-node are particularly interesting because quorum is 3/4 (same fault tolerance as 3-node but with more message fanout), and 5/7-node clusters exercise higher quorum thresholds (3/5, 4/7).
2. **Combined transport filters** — Loss + delay + reordering never tested together. Real networks exhibit all three simultaneously.
3. **Delayed transport in cluster** — The `DelayedSender` is unit-tested but never used in an actual cluster test.
4. **Reordering transport in cluster** — The `ReorderingReceiver` is unit-tested but never used in an actual cluster test.
5. **Storage recovery** — `initialize_from_decisions` is called on startup but no test verifies a node can restart from persisted state and resume consensus.
6. **Graceful shutdown** — No test verifies that dropping all `NodeHandle` clones causes `node.run()` to return `Ok(())`.
7. **Receiver close / NoQuorum** — No integration test verifies that a node returns `NodeError::NoQuorum` when its transport receiver closes during active consensus.
8. **Paxos safety invariant assertion** — No test ever asserts the core safety property under adversarial conditions: "no two nodes decide different values for the same slot." The `assert_consistent_decisions` helper checks this but only across nodes that received all decisions. A stronger check would verify safety even with partial decision sets.
9. **Slot monotonicity** — No test verifies that slot numbers never go backwards or that `next_slot` advances correctly after receiving external decisions for higher slots.
10. **Large batch proposals** — The `rapid_concurrent_proposals` test sends 50 values but only on a lossless 3-node cluster. No test sends a large batch (100+) under adversarial conditions.
11. **Complex value types** — Every test uses `String` as the consensus value type. No test exercises serde with structured data (nested structs, enums, optional fields, vectors). The library is generic over `V: Serialize + DeserializeOwned + Clone + Send + PartialEq`, but this is never verified with anything beyond strings.

---

## Task 1: Cluster tests for 4-node, 5-node, and 7-node clusters

**Files:**
- Modify: `crates/daccord-tests/tests/helpers/mod.rs`
- Create: `crates/daccord-tests/tests/larger_clusters.rs`

No new cluster helper function is needed — `create_cluster(n)` and `create_lossy_cluster(n, rate)` already accept arbitrary `n`. This task just verifies they work at larger sizes. A `create_cluster_with_dead_nodes(n, dead_count)` helper is needed to generalize the existing `create_cluster_with_dead_node`.

### 4-node cluster rationale

A 4-node cluster has quorum = 3 (same as a 3-node cluster: `4/2 + 1 = 3`). This means it tolerates exactly 1 failure — same as a 3-node cluster — but with an extra node participating in message exchange. This is interesting because:
- The extra non-quorum participant still sends/receives messages, increasing protocol traffic
- A 4-node cluster with 1 dead node has 3 live nodes and needs quorum 3, so it's right at the edge
- Even-sized clusters are a known source of subtle bugs in consensus implementations

- [ ] **Step 1: Create `larger_clusters.rs` with 4-node basic consensus tests**

Create file `crates/daccord-tests/tests/larger_clusters.rs`:

```rust
mod helpers;

use std::collections::HashSet;
use helpers::{
    assert_consistent_decisions, collect_decisions, create_cluster,
    collect_decisions_with_timeout, create_lossy_cluster,
};
use tokio::time::Duration;

const TIMEOUT: Duration = Duration::from_secs(30);

// ---------------------------------------------------------------------------
// 4-node cluster (quorum = 3, tolerates 1 failure)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn four_node_single_value() {
    let mut cluster = create_cluster(4);
    cluster[0].handle.propose("hello".to_string()).await.unwrap();

    let mut all = Vec::new();
    for node in &mut cluster {
        let decisions = collect_decisions(&mut node.decisions, 1).await;
        assert_eq!(decisions[0].value, "hello");
        all.push(decisions);
    }
    assert_consistent_decisions(&all);
    for node in cluster { drop(node.handle); }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn four_node_multiple_proposals() {
    let mut cluster = create_cluster(4);
    let expected: HashSet<String> = (0..10).map(|i| format!("v-{}", i)).collect();
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn four_node_proposals_from_different_nodes() {
    let mut cluster = create_cluster(4);
    for (i, node) in cluster.iter().enumerate() {
        node.handle.propose(format!("from-{}", i)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let expected: HashSet<String> = (0..4).map(|i| format!("from-{}", i)).collect();
    let mut all = Vec::new();
    for node in &mut cluster {
        let decisions = collect_decisions_with_timeout(&mut node.decisions, 4, TIMEOUT).await;
        let values: HashSet<String> = decisions.iter().map(|d| d.value.clone()).collect();
        assert_eq!(values, expected);
        all.push(decisions);
    }
    assert_consistent_decisions(&all);
    for node in cluster { drop(node.handle); }
}
```

- [ ] **Step 2: Run 4-node tests to verify they pass**

Run: `cargo test -p daccord-tests --test larger_clusters -- four_node -v`

- [ ] **Step 3: Write 4-node fault tolerance tests**

The 4-node cluster tolerates exactly 1 dead node (quorum 3, so 3 live is just enough). Test both the passing case (1 dead) and note that 2 dead would NOT have quorum.

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn four_node_one_dead_still_decides() {
    // quorum = 3, 3 live nodes — just enough
    let mut cluster = create_cluster_with_dead_nodes(4, 1);
    cluster[0].handle.propose("surviving".to_string()).await.unwrap();

    let mut all = Vec::new();
    for node in cluster.iter_mut().take(3) {
        let decisions = collect_decisions(&mut node.decisions, 1).await;
        assert_eq!(decisions[0].value, "surviving");
        all.push(decisions);
    }
    assert_consistent_decisions(&all);
    for node in cluster { drop(node.handle); node.run_handle.abort(); }
}
```

This requires the `create_cluster_with_dead_nodes(n, dead_count)` helper (see Step 8).

- [ ] **Step 4: Write 4-node lossy test**

Test that the even-sized cluster works under message loss:

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn four_node_lossy_10pct_multiple_proposals() {
    let mut cluster = create_lossy_cluster(4, 0.10);
    let expected: HashSet<String> = (0..10).map(|i| format!("v-{}", i)).collect();
    for v in &expected {
        cluster[0].handle.propose(v.clone()).await.unwrap();
    }

    let mut all = Vec::new();
    for node in &mut cluster {
        let decisions = collect_decisions_with_timeout(&mut node.decisions, 10, TIMEOUT).await;
        let values: HashSet<String> = decisions.iter().map(|d| d.value.clone()).collect();
        assert_eq!(values, expected);
        all.push(decisions);
    }
    assert_consistent_decisions(&all);
    for node in cluster { drop(node.handle); }
}
```

- [ ] **Step 5: Write 5-node tests (single value, multiple proposals, proposals from different nodes)**

```rust
// ---------------------------------------------------------------------------
// 5-node cluster (quorum = 3, tolerates 2 failures)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn five_node_single_value() {
    let mut cluster = create_cluster(5);
    cluster[0].handle.propose("hello".to_string()).await.unwrap();

    let mut all = Vec::new();
    for node in &mut cluster {
        let decisions = collect_decisions(&mut node.decisions, 1).await;
        assert_eq!(decisions[0].value, "hello");
        all.push(decisions);
    }
    assert_consistent_decisions(&all);
    for node in cluster { drop(node.handle); }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn five_node_multiple_proposals() {
    let mut cluster = create_cluster(5);
    let expected: HashSet<String> = (0..10).map(|i| format!("v-{}", i)).collect();
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn five_node_proposals_from_different_nodes() {
    let mut cluster = create_cluster(5);
    for (i, node) in cluster.iter().enumerate() {
        node.handle.propose(format!("from-{}", i)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let expected: HashSet<String> = (0..5).map(|i| format!("from-{}", i)).collect();
    let mut all = Vec::new();
    for node in &mut cluster {
        let decisions = collect_decisions_with_timeout(&mut node.decisions, 5, TIMEOUT).await;
        let values: HashSet<String> = decisions.iter().map(|d| d.value.clone()).collect();
        assert_eq!(values, expected);
        all.push(decisions);
    }
    assert_consistent_decisions(&all);
    for node in cluster { drop(node.handle); }
}
```

- [ ] **Step 6: Write 7-node single value and multiple proposals tests**

```rust
// ---------------------------------------------------------------------------
// 7-node cluster (quorum = 4, tolerates 3 failures)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn seven_node_single_value() {
    let mut cluster = create_cluster(7);
    cluster[0].handle.propose("hello".to_string()).await.unwrap();

    let mut all = Vec::new();
    for node in &mut cluster {
        let decisions = collect_decisions(&mut node.decisions, 1).await;
        assert_eq!(decisions[0].value, "hello");
        all.push(decisions);
    }
    assert_consistent_decisions(&all);
    for node in cluster { drop(node.handle); }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn seven_node_multiple_proposals() {
    let mut cluster = create_cluster(7);
    let expected: HashSet<String> = (0..10).map(|i| format!("v-{}", i)).collect();
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

- [ ] **Step 7: Write fault tolerance tests for 5-node and 4-node dead-node clusters**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn five_node_two_dead_still_decides() {
    // quorum = 3, 3 live nodes — just enough
    let mut cluster = create_cluster_with_dead_nodes(5, 2);
    cluster[0].handle.propose("surviving".to_string()).await.unwrap();

    let mut all = Vec::new();
    for node in cluster.iter_mut().take(3) {
        let decisions = collect_decisions(&mut node.decisions, 1).await;
        assert_eq!(decisions[0].value, "surviving");
        all.push(decisions);
    }
    assert_consistent_decisions(&all);
    for node in cluster { drop(node.handle); node.run_handle.abort(); }
}
```

- [ ] **Step 8: Add `create_cluster_with_dead_nodes` helper**

Add to `helpers/mod.rs` — a generalization of the existing `create_cluster_with_dead_node` that takes a `dead_count` parameter instead of hardcoding the last node. The last `dead_count` nodes are dead (receivers dropped, nodes not spawned).

- [ ] **Step 9: Run all tests, commit**

Run: `cargo test -p daccord-tests --test larger_clusters -v`
Run: `cargo test -p daccord-tests --test cluster -v` (existing tests still pass)

---

## Task 2: Combined transport filter cluster helpers

**Files:**
- Modify: `crates/daccord-tests/tests/helpers/mod.rs` — add `create_delayed_cluster` and `create_reordering_cluster` helpers

- [ ] **Step 1: Add `create_delayed_cluster` helper**

Add to `helpers/mod.rs`:

```rust
use transport_filters::{DelayedSender, ReorderingSender, ReorderingReceiver};

pub fn create_delayed_cluster(n: usize, min_ms: u64, max_ms: u64) -> Vec<ClusterNode> {
    // Same pattern as create_lossy_cluster but wraps senders with DelayedSender
    // Uses channel(64) as inner, wraps with DelayedSender::with_range
}
```

Wire it identically to `create_lossy_cluster` but with `DelayedSender::with_range(tx, Duration::from_millis(min_ms), Duration::from_millis(max_ms))` wrapping each sender. Pass raw `ChannelReceiver` to nodes (same rationale as lossy — wrapping the receiver interferes with the node's select loop).

- [ ] **Step 2: Add `create_reordering_cluster` helper**

```rust
pub fn create_reordering_cluster(n: usize, window_ms: u64, batch_size: usize) -> Vec<ClusterNode> {
    // Wraps receivers with ReorderingReceiver, senders with ReorderingSender (passthrough)
}
```

Unlike lossy/delayed where we only wrap senders, the `ReorderingReceiver` does NOT internally loop-and-drop like `LossyReceiver` did — it collects a batch, shuffles, and returns one at a time. This means it DOES yield back to the tokio select loop between messages, so it's safe to wrap the receiver side here. Use `ReorderingSender` (passthrough) for senders since reordering is receive-side only.

- [ ] **Step 3: Add `create_combined_cluster` helper**

A helper that composes lossy + delayed senders:

```rust
pub fn create_lossy_delayed_cluster(
    n: usize,
    drop_rate: f64,
    min_delay_ms: u64,
    max_delay_ms: u64,
) -> Vec<ClusterNode> {
    // Creates channel, wraps with LossySender, then wraps THAT with DelayedSender
    // LossySender<DelayedSender<ChannelSender>> — drop check happens first, then delay
    // (or DelayedSender<LossySender<ChannelSender>> — delay first, then drop — either is fine)
}
```

- [ ] **Step 4: Run existing tests to verify helpers compile**

Run: `cargo test -p daccord-tests --test cluster`

- [ ] **Step 5: Commit**

---

## Task 3: Delayed and reordering cluster tests

**Files:**
- Create: `crates/daccord-tests/tests/delayed_cluster.rs`

- [ ] **Step 1: Write delayed single-value test**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delayed_0_50ms_single_value() {
    let mut cluster = create_delayed_cluster(3, 0, 50);
    cluster[0].handle.propose("hello".to_string()).await.unwrap();
    // collect with generous timeout — delays add up
    let mut all = Vec::new();
    for node in &mut cluster {
        let decisions = collect_decisions_with_timeout(&mut node.decisions, 1, TIMEOUT).await;
        assert_eq!(decisions[0].value, "hello");
        all.push(decisions);
    }
    assert_consistent_decisions(&all);
    for node in cluster { drop(node.handle); }
}
```

- [ ] **Step 2: Write delayed multiple proposals test**

10 proposals with 0–50ms delay per message. All should eventually decide.

- [ ] **Step 3: Write reordering single-value and multiple-proposals tests**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reordered_single_value() {
    let mut cluster = create_reordering_cluster(3, 10, 5);
    // ... same pattern
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reordered_multiple_proposals() {
    let mut cluster = create_reordering_cluster(3, 10, 5);
    // 10 proposals, all should decide with consistent slot-value mapping
}
```

- [ ] **Step 4: Write combined lossy+delayed test**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lossy_10pct_delayed_0_30ms_single_value() {
    let mut cluster = create_lossy_delayed_cluster(3, 0.10, 0, 30);
    // ...
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lossy_10pct_delayed_0_30ms_multiple_proposals() {
    let mut cluster = create_lossy_delayed_cluster(3, 0.10, 0, 30);
    // 10 proposals, all decided
}
```

- [ ] **Step 5: Run all tests, commit**

Run: `cargo test -p daccord-tests --test delayed_cluster --test-threads=1 -v`

---

## Task 4: Storage recovery tests

**Files:**
- Create: `crates/daccord-tests/tests/recovery.rs`
- Modify: `crates/daccord-tests/tests/helpers/mod.rs` — add shared-storage cluster helper

This tests that a node can be stopped after deciding some values, then restarted with the same storage, and continue participating in consensus correctly. The key behavior: `initialize_from_decisions` loads prior decisions into `decided_slots` and advances `next_slot`, so the restarted node won't re-propose for already-decided slots.

- [ ] **Step 1: Add `SharedMemoryStorage` wrapper**

In `helpers/mod.rs`, create a storage wrapper that clones an `Arc<Mutex<MemoryStorage>>` so the same storage survives across node restarts:

```rust
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct SharedMemoryStorage<V> {
    inner: Arc<Mutex<MemoryStorage<V>>>,
}
```

Implement `Storage<V>` for it by delegating to the inner `MemoryStorage` through the mutex.

- [ ] **Step 2: Add helper to create a single node with shared storage**

```rust
pub fn create_node_with_storage(
    id: NodeId,
    peers: Vec<PeerInfo<ChannelSender>>,
    receiver: ChannelReceiver,
    storage: SharedMemoryStorage<String>,
) -> ClusterNode { ... }
```

- [ ] **Step 3: Write recovery test — node restarts and sees prior decisions**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn node_recovers_from_storage_and_continues() {
    // 1. Create 3-node cluster with shared storage for node-0
    // 2. Propose "value-a", wait for all 3 nodes to decide
    // 3. Drop node-0's handle and abort its run task (simulates crash)
    // 4. Create new channels for node-0, reconnect to nodes 1 and 2
    // 5. Create new node-0 with SAME shared storage
    // 6. Propose "value-b" from node-1
    // 7. All 3 nodes (including restarted node-0) should decide "value-b"
    //    at slot 1 (not slot 0, which was already decided)
}
```

This verifies: (a) storage persists across restarts, (b) `next_slot` advances past stored decisions, (c) new proposals get new slot numbers.

- [ ] **Step 4: Write recovery test — restarted node doesn't re-decide old slots**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recovered_node_skips_decided_slots() {
    // Similar to above, but verify the restarted node's decision receiver
    // only emits the NEW decision, not the old one it loaded from storage.
}
```

- [ ] **Step 5: Run tests, commit**

Run: `cargo test -p daccord-tests --test recovery -v`

---

## Task 5: Graceful shutdown and error condition tests

**Files:**
- Create: `crates/daccord-tests/tests/lifecycle.rs`

- [ ] **Step 1: Write graceful shutdown test — dropping handles stops node**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropping_all_handles_causes_clean_shutdown() {
    let cluster = create_cluster(3);

    // Collect the run handles
    let mut run_handles: Vec<_> = cluster.into_iter().map(|n| {
        drop(n.handle);
        drop(n.decisions);
        n.run_handle
    }).collect();

    // All nodes should exit with Ok(())
    for handle in &mut run_handles {
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            handle
        ).await;
        assert!(result.is_ok(), "node did not shut down within timeout");
        assert!(result.unwrap().unwrap().is_ok(), "node exited with error");
    }
}
```

- [ ] **Step 2: Write test — shutdown during active consensus**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_during_active_proposal() {
    let mut cluster = create_cluster(3);
    // Propose a value
    cluster[0].handle.propose("in-flight".to_string()).await.unwrap();
    // Immediately drop all handles (don't wait for decision)
    let run_handles: Vec<_> = cluster.into_iter().map(|n| {
        drop(n.handle);
        drop(n.decisions);
        n.run_handle
    }).collect();

    // Nodes should still shut down cleanly (not hang)
    for handle in run_handles {
        let result = tokio::time::timeout(Duration::from_secs(5), handle).await;
        assert!(result.is_ok(), "node hung after handle drop during active consensus");
    }
}
```

- [ ] **Step 3: Write test — single node shutdown**

```rust
#[tokio::test]
async fn single_node_shuts_down_on_handle_drop() {
    let cluster = create_cluster(1);
    let run_handle = cluster.into_iter().next().map(|n| {
        drop(n.handle);
        drop(n.decisions);
        n.run_handle
    }).unwrap();

    let result = tokio::time::timeout(Duration::from_secs(5), run_handle).await;
    assert!(result.is_ok());
    assert!(result.unwrap().unwrap().is_ok());
}
```

- [ ] **Step 4: Run tests, commit**

Run: `cargo test -p daccord-tests --test lifecycle -v`

---

## Task 6: Paxos safety invariant fuzzing

**Files:**
- Create: `crates/daccord-tests/tests/safety_invariants.rs`
- Modify: `crates/daccord-tests/tests/helpers/mod.rs` — add `assert_safety_invariant` helper

This is the most important task. It verifies the core Paxos guarantee: **no two nodes ever decide different values for the same slot**, even under adversarial network conditions with concurrent proposers.

- [ ] **Step 1: Add `assert_safety_invariant` helper**

In `helpers/mod.rs`, add a stronger safety check that works with partial decision sets (some nodes may not have received all decisions):

```rust
/// The core Paxos safety invariant: for any slot, all nodes that decided
/// that slot must have decided the SAME value. Unlike assert_consistent_decisions
/// which requires all nodes to have the same number of decisions, this check
/// tolerates partial sets — it only checks slots that multiple nodes decided.
pub fn assert_safety_invariant(all_decisions: &[Vec<Decided<String>>]) {
    let mut slot_values: HashMap<u64, String> = HashMap::new();
    for (node_idx, decisions) in all_decisions.iter().enumerate() {
        for d in decisions {
            if let Some(existing) = slot_values.get(&d.slot) {
                assert_eq!(
                    &d.value, existing,
                    "SAFETY VIOLATION: node {} decided slot {} = {:?}, \
                     but another node decided slot {} = {:?}",
                    node_idx, d.slot, d.value, d.slot, existing
                );
            } else {
                slot_values.insert(d.slot, d.value.clone());
            }
        }
    }
}
```

- [ ] **Step 2: Write concurrent multi-proposer safety test under loss**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn safety_under_loss_with_concurrent_proposers() {
    // 5-node cluster, 15% message loss
    // All 5 nodes propose simultaneously
    // Collect whatever decisions arrive within 30s
    // Assert safety invariant on ALL collected decisions
    let mut cluster = create_lossy_cluster(5, 0.15);

    for (i, node) in cluster.iter().enumerate() {
        node.handle.propose(format!("val-{}", i)).await.unwrap();
    }

    let mut all = Vec::new();
    for node in &mut cluster {
        // Collect up to 5 decisions, but don't fail if fewer arrive
        let mut decisions = Vec::new();
        for _ in 0..5 {
            match tokio::time::timeout(
                Duration::from_secs(10),
                node.decisions.recv()
            ).await {
                Ok(Some(d)) => decisions.push(d),
                _ => break,
            }
        }
        all.push(decisions);
    }

    assert_safety_invariant(&all);
    for node in cluster { drop(node.handle); }
}
```

- [ ] **Step 3: Write 4-node safety test under loss**

Even-sized clusters deserve dedicated safety verification since quorum math is different:

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn safety_four_node_under_loss_with_concurrent_proposers() {
    // 4-node cluster, 15% loss, all 4 propose simultaneously
    // quorum = 3, so needs 3/4 to agree
    let mut cluster = create_lossy_cluster(4, 0.15);

    for (i, node) in cluster.iter().enumerate() {
        node.handle.propose(format!("val-{}", i)).await.unwrap();
    }

    let mut all = Vec::new();
    for node in &mut cluster {
        let mut decisions = Vec::new();
        for _ in 0..4 {
            match tokio::time::timeout(
                Duration::from_secs(10),
                node.decisions.recv()
            ).await {
                Ok(Some(d)) => decisions.push(d),
                _ => break,
            }
        }
        all.push(decisions);
    }

    assert_safety_invariant(&all);
    for node in cluster { drop(node.handle); }
}
```

- [ ] **Step 5: Write rapid-fire multi-proposer safety test**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn safety_under_rapid_concurrent_proposals() {
    // 3-node cluster, 10% loss
    // Each node rapidly proposes 10 values (30 total)
    // Collect whatever arrives, assert safety
    let mut cluster = create_lossy_cluster(3, 0.10);

    for (i, node) in cluster.iter().enumerate() {
        for j in 0..10 {
            node.handle.propose(format!("n{}-v{}", i, j)).await.unwrap();
        }
    }

    let mut all = Vec::new();
    for node in &mut cluster {
        let mut decisions = Vec::new();
        for _ in 0..30 {
            match tokio::time::timeout(Duration::from_secs(10), node.decisions.recv()).await {
                Ok(Some(d)) => decisions.push(d),
                _ => break,
            }
        }
        all.push(decisions);
    }

    assert_safety_invariant(&all);
    // Verify at least SOME decisions were made
    let total: usize = all.iter().map(|d| d.len()).sum();
    assert!(total > 0, "no decisions were made at all");
    for node in cluster { drop(node.handle); }
}
```

- [ ] **Step 6: Write safety test under combined adversarial conditions**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn safety_under_loss_and_delay() {
    // 5-node cluster, 10% loss + 0-30ms delay
    // 3 proposers, 5 values each
    let mut cluster = create_lossy_delayed_cluster(5, 0.10, 0, 30);
    for (i, node) in cluster.iter().take(3).enumerate() {
        for j in 0..5 {
            node.handle.propose(format!("n{}-v{}", i, j)).await.unwrap();
        }
    }

    let mut all = Vec::new();
    for node in &mut cluster {
        let mut decisions = Vec::new();
        for _ in 0..15 {
            match tokio::time::timeout(Duration::from_secs(15), node.decisions.recv()).await {
                Ok(Some(d)) => decisions.push(d),
                _ => break,
            }
        }
        all.push(decisions);
    }

    assert_safety_invariant(&all);
    for node in cluster { drop(node.handle); }
}
```

- [ ] **Step 7: Write slot uniqueness invariant test**

No node should ever report two different values for the same slot:

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn no_duplicate_slots_per_node() {
    let mut cluster = create_lossy_cluster(3, 0.10);
    for v in 0..20 {
        cluster[0].handle.propose(format!("v-{}", v)).await.unwrap();
    }

    for node in &mut cluster {
        let decisions = collect_decisions_with_timeout(
            &mut node.decisions, 20, Duration::from_secs(30)
        ).await;
        let mut seen_slots: HashMap<u64, String> = HashMap::new();
        for d in &decisions {
            if let Some(prev) = seen_slots.insert(d.slot, d.value.clone()) {
                panic!(
                    "node received duplicate slot {}: {:?} and {:?}",
                    d.slot, prev, d.value
                );
            }
        }
    }
    for node in cluster { drop(node.handle); }
}
```

- [ ] **Step 8: Run all safety tests, commit**

Run: `cargo test -p daccord-tests --test safety_invariants --test-threads=1 -v`

---

## Task 7: Large batch and stress tests

**Files:**
- Create: `crates/daccord-tests/tests/stress.rs`

- [ ] **Step 1: Write 100-proposal lossless stress test**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hundred_proposals_lossless() {
    let mut cluster = create_unbounded_cluster(3);
    let n = 100;
    let expected: HashSet<String> = (0..n).map(|i| format!("v-{}", i)).collect();
    for v in &expected {
        cluster[0].handle.propose(v.clone()).await.unwrap();
    }

    let mut all = Vec::new();
    for node in &mut cluster {
        let decisions = collect_decisions_with_timeout(
            &mut node.decisions, n, Duration::from_secs(30)
        ).await;
        let values: HashSet<String> = decisions.iter().map(|d| d.value.clone()).collect();
        assert_eq!(values, expected);
        all.push(decisions);
    }
    assert_consistent_decisions(&all);
    for node in cluster { drop(node.handle); }
}
```

- [ ] **Step 2: Write 100-proposal lossy stress test (5% loss)**

Same as above but with `create_lossy_cluster(3, 0.05)`. Lower loss rate for reliability at scale.

- [ ] **Step 3: Write 5-node 50-proposal lossy stress test**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn five_node_fifty_proposals_lossy() {
    let mut cluster = create_lossy_cluster(5, 0.05);
    let n = 50;
    let expected: HashSet<String> = (0..n).map(|i| format!("v-{}", i)).collect();
    for v in &expected {
        cluster[0].handle.propose(v.clone()).await.unwrap();
    }

    let mut all = Vec::new();
    for node in &mut cluster {
        let decisions = collect_decisions_with_timeout(
            &mut node.decisions, n, Duration::from_secs(60)
        ).await;
        let values: HashSet<String> = decisions.iter().map(|d| d.value.clone()).collect();
        assert_eq!(values, expected);
        all.push(decisions);
    }
    assert_consistent_decisions(&all);
    for node in cluster { drop(node.handle); }
}
```

- [ ] **Step 4: Run stress tests, commit**

Run: `cargo test -p daccord-tests --test stress --test-threads=1 -v`

---

## Task 8: Complex value type tests

**Files:**
- Create: `crates/daccord-tests/tests/complex_values.rs`
- Modify: `crates/daccord-tests/tests/helpers/mod.rs` — add generic cluster helper

All existing tests use `String` as the consensus value type. This doesn't exercise serde with structured data, nor does it verify that the library works with realistic application payloads. The `Node` generic bound is `V: Serialize + DeserializeOwned + Clone + Send + PartialEq + 'static`, so any struct meeting those bounds should work — but we've never tested it.

- [ ] **Step 1: Define a complex value type**

In `complex_values.rs`, define a struct that represents a realistic application payload — something with nested fields, optional values, and a Vec:

```rust
mod helpers;

use std::collections::{HashMap, HashSet};
use serde::{Serialize, Deserialize};
use daccord::{
    channel, ChannelReceiver, ChannelSender, Decided, DecisionReceiver,
    MemoryStorage, Node, NodeHandle, NodeId, PeerInfo,
};
use tokio::task::JoinHandle;
use tokio::time::{timeout, Duration};

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
struct Operation {
    kind: OpKind,
    key: String,
    value: Option<String>,
    tags: Vec<String>,
    metadata: OperationMeta,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
enum OpKind {
    Set,
    Delete,
    BatchUpdate,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
struct OperationMeta {
    timestamp: u64,
    origin: String,
}
```

- [ ] **Step 2: Write cluster helper generic over value type**

Since the existing helpers in `mod.rs` are hardcoded to `String`, write a small local helper in the test file that creates a 3-node cluster generic over the value type. Alternatively, add a generic `create_cluster_typed<V>` to `helpers/mod.rs`:

```rust
// In helpers/mod.rs
pub fn create_cluster_typed<V>(n: usize) -> Vec<ClusterNodeTyped<V>>
where
    V: Serialize + DeserializeOwned + Clone + Send + PartialEq + 'static,
{
    // Same as create_cluster but uses Node<V, ...> instead of Node<String, ...>
}

pub struct ClusterNodeTyped<V> {
    pub handle: NodeHandle<V>,
    pub decisions: DecisionReceiver<V>,
    pub run_handle: JoinHandle<Result<(), daccord::NodeError>>,
    pub id: NodeId,
}
```

If adding generics to the shared helpers is too invasive, a self-contained helper local to the test file is fine — the goal is testing the library, not the helper code.

- [ ] **Step 3: Write single structured-value consensus test**

```rust
fn make_op(key: &str, kind: OpKind, ts: u64) -> Operation {
    Operation {
        kind,
        key: key.to_string(),
        value: Some(format!("val-for-{}", key)),
        tags: vec!["consensus".into(), "test".into()],
        metadata: OperationMeta {
            timestamp: ts,
            origin: "test-node".into(),
        },
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn structured_value_single_proposal() {
    // Create 3-node cluster with Operation as value type
    // Propose a single Operation, verify all 3 nodes decide the same value
    // Assert the deserialized struct fields match exactly
    let op = make_op("user:42", OpKind::Set, 1000);
    // ... propose, collect, assert all nodes got identical Operation
}
```

- [ ] **Step 4: Write multiple structured-value proposals test**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn structured_value_multiple_proposals() {
    // Propose 5 different Operations with different OpKinds
    let ops = vec![
        make_op("user:1", OpKind::Set, 1000),
        make_op("user:2", OpKind::Delete, 1001),
        make_op("config:a", OpKind::BatchUpdate, 1002),
        make_op("user:1", OpKind::Set, 1003),  // same key, different ts
        make_op("session:x", OpKind::Delete, 1004),
    ];
    // Propose all, verify all 5 are decided across all nodes
    // Verify struct equality (not just string comparison)
}
```

- [ ] **Step 5: Write structured-value test with empty/none fields**

Test edge cases in serde: empty strings, None optionals, empty Vec:

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn structured_value_with_edge_case_fields() {
    let op = Operation {
        kind: OpKind::Delete,
        key: String::new(),           // empty key
        value: None,                   // no value
        tags: vec![],                  // empty tags
        metadata: OperationMeta {
            timestamp: 0,
            origin: String::new(),
        },
    };
    // Propose, verify all nodes decide identical struct with empty/None fields preserved
}
```

- [ ] **Step 6: Write structured-value test with large payload**

Test that a value with a large `tags` Vec (e.g. 100 tags) or long strings serializes/deserializes correctly through the consensus protocol:

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn structured_value_large_payload() {
    let op = Operation {
        kind: OpKind::BatchUpdate,
        key: "k".repeat(200),
        value: Some("v".repeat(500)),
        tags: (0..100).map(|i| format!("tag-{}", i)).collect(),
        metadata: OperationMeta {
            timestamp: u64::MAX,
            origin: "long-origin-name".repeat(10),
        },
    };
    // Propose, verify all nodes decide the exact same large struct
}
```

- [ ] **Step 7: Run tests, commit**

Run: `cargo test -p daccord-tests --test complex_values -v`

---

## Verification

After all tasks are complete:

```bash
# All existing tests still pass
cargo test -p daccord --all-features

# All cluster tests pass
cargo test -p daccord-tests --test cluster

# All adversarial tests pass (serial to avoid runtime contention)
cargo test -p daccord-tests --test adversarial_cluster --test-threads=1

# New tests pass
cargo test -p daccord-tests --test larger_clusters --test-threads=1
cargo test -p daccord-tests --test delayed_cluster --test-threads=1
cargo test -p daccord-tests --test recovery --test-threads=1
cargo test -p daccord-tests --test lifecycle
cargo test -p daccord-tests --test safety_invariants --test-threads=1
cargo test -p daccord-tests --test stress --test-threads=1
cargo test -p daccord-tests --test complex_values
```

## New test count estimate

| File | Tests |
|---|---|
| `larger_clusters.rs` | ~12 (4-node x4, 5-node x4, 7-node x2, dead-node fault tolerance x2) |
| `delayed_cluster.rs` | ~5 (delayed x2, reordered x2, combined x1) |
| `recovery.rs` | ~2 |
| `lifecycle.rs` | ~3 |
| `safety_invariants.rs` | ~6 (5-node lossy, 4-node lossy, rapid multi-proposer, combined adversarial, slot uniqueness, and the base safety helper) |
| `stress.rs` | ~3 |
| `complex_values.rs` | ~4 (single structured value, multiple ops, edge-case fields, large payload) |

**Total new tests: ~35**, bringing the project total from 140 to ~175.
