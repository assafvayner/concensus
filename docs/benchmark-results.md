# Paxos vs Multi-Paxos Benchmark Results

**Date:** 2026-03-15
**Hardware:** Apple Silicon (local), in-memory channel transport with simulated delays
**Configuration:** 3-node cluster, 20 sequential proposals per test, 4 tokio worker threads

## Latency (Sequential Proposals)

Each proposal is submitted and waited on before the next. Measures per-proposal consensus latency.

| Network Delay | Classic Paxos (mean) | Multi-Paxos (mean) | Speedup | Classic (total) | Multi-Paxos (total) |
|---|---|---|---|---|---|
| No delay | 0.19ms | 0.23ms | ~1x | 3.7ms | 4.5ms |
| 0-1ms (LAN) | 5.93ms | 3.36ms | **1.8x** | 118.7ms | 67.1ms |
| 2-5ms (cross-AZ) | 17.70ms | 9.51ms | **1.9x** | 354.0ms | 190.3ms |
| 10-25ms (cross-region) | 67.84ms | 36.34ms | **1.9x** | 1356.8ms | 726.9ms |
| 25-50ms (inter-continental) | 178.39ms | 86.33ms | **2.1x** | 3567.8ms | 1726.7ms |

### Key observations

- **With no simulated delay**, there's no meaningful difference — in-memory channels are so fast that the Phase 1 overhead is negligible.
- **With network delays, Multi-Paxos is ~2x faster**, which is exactly the expected theoretical improvement: Classic Paxos needs 2 round trips (Prepare/Promise + Accept/Accepted), Multi-Paxos needs 1 (Accept/Accepted only, Phase 1 skipped).
- The speedup is consistent across all delay profiles (1.8x–2.1x), confirming the optimization works as designed.
- The first proposal in Multi-Paxos pays the full Phase 1 cost (leader election), but subsequent proposals benefit from the fast path.

## Throughput (Concurrent Proposals)

All proposals fired at once, then all decisions collected. Measures aggregate throughput.

| Network Delay | Classic Paxos | Multi-Paxos | Speedup |
|---|---|---|---|
| No delay | 7,886 proposals/sec | 13,806 proposals/sec | **1.8x** |
| 0-1ms (LAN) | 1,941 proposals/sec | 1,510 proposals/sec | 0.8x |
| 2-5ms (cross-AZ) | 790 proposals/sec | 735 proposals/sec | ~1x |
| 10-25ms (cross-region) | 200 proposals/sec | 189 proposals/sec | ~1x |
| 25-50ms (inter-continental) | 56 proposals/sec | 56 proposals/sec | ~1x |

### Key observations

- **Throughput shows less difference than latency** because when proposals are batched concurrently, Classic Paxos can pipeline multiple slots in parallel — each slot's Phase 1 overlaps with other slots' Phase 2.
- **With no delay**, Multi-Paxos is 1.8x faster on throughput because the Phase 1 skip eliminates message processing overhead even in the absence of network latency.
- **With delays**, throughput is similar because both modes pipeline effectively. The bottleneck shifts to network delay per round trip, and both modes need at least 1 round trip.
- The slight throughput regression at 0-1ms delay may be due to forwarding overhead or heartbeat traffic.

## Where Multi-Paxos Wins

The Multi-Paxos optimization shines for **sequential latency-sensitive workloads** where each operation waits for the previous one to complete:

- Database transactions (commit wait)
- Leader-based state machine replication
- Lock acquisition via consensus

For **throughput-oriented workloads** where many proposals are in flight simultaneously, the improvement is smaller because Classic Paxos already pipelines across slots.

## How to reproduce

```bash
# Classic Paxos
cargo test -p daccord-tests --test benchmark -- --nocapture --test-threads=1

# Multi-Paxos
cargo test -p daccord-tests --features multi-paxos --test benchmark -- --nocapture --test-threads=1
```
