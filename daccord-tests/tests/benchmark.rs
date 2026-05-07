//! Benchmark comparing Classic Paxos vs Multi-Paxos under various network delays.
//!
//! Run without multi-paxos (classic):
//!   cargo test -p daccord-tests --test benchmark -- --nocapture --test-threads=1
//!
//! Run with multi-paxos:
//!   cargo test -p daccord-tests --features multi-paxos --test benchmark -- --nocapture --test-threads=1
//!
//! The tests print timing results to stdout. Use --nocapture to see them.

mod helpers;

use helpers::{collect_decisions_with_timeout, create_cluster, create_delayed_cluster};
use std::collections::HashSet;
use tokio::time::{Duration, Instant};

const NUM_PROPOSALS: usize = 20;
const DECISION_TIMEOUT: Duration = Duration::from_secs(30);

fn mode_label() -> &'static str {
    if cfg!(feature = "multi-paxos") {
        "multi-paxos"
    } else {
        "classic-paxos"
    }
}

/// Measures per-proposal latency: time from propose() to decision received.
/// Returns (individual latencies, total elapsed).
async fn measure_sequential_latencies(
    cluster: &mut [helpers::ClusterNode],
    count: usize,
) -> (Vec<Duration>, Duration) {
    let mut latencies = Vec::with_capacity(count);
    let total_start = Instant::now();

    for i in 0..count {
        let start = Instant::now();
        cluster[0].handle.propose(format!("v-{}", i)).await.unwrap();
        // Wait for proposer node to decide
        let _decided = tokio::time::timeout(DECISION_TIMEOUT, cluster[0].decisions.recv())
            .await
            .expect("timed out waiting for decision")
            .expect("decision channel closed");
        latencies.push(start.elapsed());
    }

    let total = total_start.elapsed();

    // Drain remaining decisions from other nodes so they don't interfere
    for node in cluster.iter_mut().skip(1) {
        for _ in 0..count {
            let _ = tokio::time::timeout(Duration::from_secs(5), node.decisions.recv()).await;
        }
    }

    (latencies, total)
}

/// Measures throughput: fire all proposals, then collect all decisions.
/// Returns total elapsed time.
async fn measure_throughput(cluster: &mut [helpers::ClusterNode], count: usize) -> Duration {
    let start = Instant::now();

    // Fire all proposals as fast as possible
    for i in 0..count {
        cluster[0].handle.propose(format!("v-{}", i)).await.unwrap();
    }

    // Collect all decisions from all nodes
    let expected: HashSet<String> = (0..count).map(|i| format!("v-{}", i)).collect();
    for node in cluster.iter_mut() {
        let decisions =
            collect_decisions_with_timeout(&mut node.decisions, count, DECISION_TIMEOUT).await;
        let values: HashSet<String> = decisions.iter().map(|d| d.value.clone()).collect();
        assert_eq!(values, expected, "not all proposals decided on node");
    }

    start.elapsed()
}

fn print_latency_stats(label: &str, latencies: &[Duration], total: Duration) {
    let count = latencies.len();
    let mut sorted: Vec<f64> = latencies.iter().map(|d| d.as_secs_f64() * 1000.0).collect();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());

    let min = sorted[0];
    let max = sorted[count - 1];
    let median = sorted[count / 2];
    let mean: f64 = sorted.iter().sum::<f64>() / count as f64;
    let p99 = sorted[(count as f64 * 0.99) as usize];

    // First vs rest comparison (first proposal pays Phase 1 cost in multi-paxos)
    let first = sorted[0];
    let rest_mean: f64 = if count > 1 {
        sorted[1..].iter().sum::<f64>() / (count - 1) as f64
    } else {
        first
    };

    eprintln!("  [{label}]");
    eprintln!("    proposals:  {count}");
    eprintln!("    total:      {:.1}ms", total.as_secs_f64() * 1000.0);
    eprintln!("    min:        {min:.2}ms");
    eprintln!("    max:        {max:.2}ms");
    eprintln!("    median:     {median:.2}ms");
    eprintln!("    mean:       {mean:.2}ms");
    eprintln!("    p99:        {p99:.2}ms");
    eprintln!("    first:      {first:.2}ms");
    eprintln!("    rest mean:  {rest_mean:.2}ms");
}

fn print_throughput_stats(label: &str, count: usize, total: Duration) {
    let total_ms = total.as_secs_f64() * 1000.0;
    let per_proposal = total_ms / count as f64;
    let proposals_per_sec = count as f64 / total.as_secs_f64();

    eprintln!("  [{label}]");
    eprintln!("    proposals:      {count}");
    eprintln!("    total:          {total_ms:.1}ms");
    eprintln!("    per proposal:   {per_proposal:.2}ms");
    eprintln!("    throughput:     {proposals_per_sec:.0} proposals/sec");
}

// ===========================================================================
// Benchmark: No network delay (baseline)
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bench_latency_no_delay() {
    let mode = mode_label();
    eprintln!("\n=== Latency Benchmark: no delay ({mode}) ===");

    let mut cluster = create_cluster(3);
    let (latencies, total) = measure_sequential_latencies(&mut cluster, NUM_PROPOSALS).await;
    print_latency_stats(&format!("3-node no-delay {mode}"), &latencies, total);

    for node in cluster {
        drop(node.handle);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bench_throughput_no_delay() {
    let mode = mode_label();
    eprintln!("\n=== Throughput Benchmark: no delay ({mode}) ===");

    let mut cluster = create_cluster(3);
    let total = measure_throughput(&mut cluster, NUM_PROPOSALS).await;
    print_throughput_stats(&format!("3-node no-delay {mode}"), NUM_PROPOSALS, total);

    for node in cluster {
        drop(node.handle);
    }
}

// ===========================================================================
// Benchmark: 1ms network delay (LAN-like)
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bench_latency_1ms_delay() {
    let mode = mode_label();
    eprintln!("\n=== Latency Benchmark: 0-1ms delay ({mode}) ===");

    let mut cluster = create_delayed_cluster(3, 0, 1);
    let (latencies, total) = measure_sequential_latencies(&mut cluster, NUM_PROPOSALS).await;
    print_latency_stats(&format!("3-node 0-1ms {mode}"), &latencies, total);

    for node in cluster {
        drop(node.handle);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bench_throughput_1ms_delay() {
    let mode = mode_label();
    eprintln!("\n=== Throughput Benchmark: 0-1ms delay ({mode}) ===");

    let mut cluster = create_delayed_cluster(3, 0, 1);
    let total = measure_throughput(&mut cluster, NUM_PROPOSALS).await;
    print_throughput_stats(&format!("3-node 0-1ms {mode}"), NUM_PROPOSALS, total);

    for node in cluster {
        drop(node.handle);
    }
}

// ===========================================================================
// Benchmark: 5ms network delay (cross-AZ)
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bench_latency_5ms_delay() {
    let mode = mode_label();
    eprintln!("\n=== Latency Benchmark: 2-5ms delay ({mode}) ===");

    let mut cluster = create_delayed_cluster(3, 2, 5);
    let (latencies, total) = measure_sequential_latencies(&mut cluster, NUM_PROPOSALS).await;
    print_latency_stats(&format!("3-node 2-5ms {mode}"), &latencies, total);

    for node in cluster {
        drop(node.handle);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bench_throughput_5ms_delay() {
    let mode = mode_label();
    eprintln!("\n=== Throughput Benchmark: 2-5ms delay ({mode}) ===");

    let mut cluster = create_delayed_cluster(3, 2, 5);
    let total = measure_throughput(&mut cluster, NUM_PROPOSALS).await;
    print_throughput_stats(&format!("3-node 2-5ms {mode}"), NUM_PROPOSALS, total);

    for node in cluster {
        drop(node.handle);
    }
}

// ===========================================================================
// Benchmark: 25ms network delay (cross-region)
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bench_latency_25ms_delay() {
    let mode = mode_label();
    eprintln!("\n=== Latency Benchmark: 10-25ms delay ({mode}) ===");

    let mut cluster = create_delayed_cluster(3, 10, 25);
    let (latencies, total) = measure_sequential_latencies(&mut cluster, NUM_PROPOSALS).await;
    print_latency_stats(&format!("3-node 10-25ms {mode}"), &latencies, total);

    for node in cluster {
        drop(node.handle);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bench_throughput_25ms_delay() {
    let mode = mode_label();
    eprintln!("\n=== Throughput Benchmark: 10-25ms delay ({mode}) ===");

    let mut cluster = create_delayed_cluster(3, 10, 25);
    let total = measure_throughput(&mut cluster, NUM_PROPOSALS).await;
    print_throughput_stats(&format!("3-node 10-25ms {mode}"), NUM_PROPOSALS, total);

    for node in cluster {
        drop(node.handle);
    }
}

// ===========================================================================
// Benchmark: 50ms network delay (inter-continental)
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bench_latency_50ms_delay() {
    let mode = mode_label();
    eprintln!("\n=== Latency Benchmark: 25-50ms delay ({mode}) ===");

    let mut cluster = create_delayed_cluster(3, 25, 50);
    let (latencies, total) = measure_sequential_latencies(&mut cluster, NUM_PROPOSALS).await;
    print_latency_stats(&format!("3-node 25-50ms {mode}"), &latencies, total);

    for node in cluster {
        drop(node.handle);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bench_throughput_50ms_delay() {
    let mode = mode_label();
    eprintln!("\n=== Throughput Benchmark: 25-50ms delay ({mode}) ===");

    let mut cluster = create_delayed_cluster(3, 25, 50);
    let total = measure_throughput(&mut cluster, NUM_PROPOSALS).await;
    print_throughput_stats(&format!("3-node 25-50ms {mode}"), NUM_PROPOSALS, total);

    for node in cluster {
        drop(node.handle);
    }
}
