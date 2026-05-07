//! Cluster-level invariant checker for Raft tests.
//!
//! Polls all nodes' decision streams and asserts state-machine safety + a
//! liveness budget. Use as a sidecar in long-running scenarios.

#![allow(dead_code)]

use std::collections::HashMap;
use std::time::Duration;

use daccord::Decided;

use super::ClusterNode;

pub struct RaftClusterInvariantChecker {
    seen: HashMap<u64, String>,
    per_node_count: Vec<u64>,
}

impl RaftClusterInvariantChecker {
    pub fn new(num_nodes: usize) -> Self {
        Self {
            seen: HashMap::new(),
            per_node_count: vec![0; num_nodes],
        }
    }

    /// Drain pending decisions from each node and verify safety.
    /// Returns the total decisions observed in this poll.
    pub fn poll(&mut self, cluster: &mut [ClusterNode]) -> usize {
        let mut polled = 0;
        for (i, node) in cluster.iter_mut().enumerate() {
            while let Ok(d) = node.decisions.try_recv() {
                self.observe(i, &d);
                polled += 1;
            }
        }
        polled
    }

    fn observe(&mut self, node_idx: usize, d: &Decided<String>) {
        if node_idx < self.per_node_count.len() {
            self.per_node_count[node_idx] += 1;
        }
        if let Some(prev) = self.seen.get(&d.slot) {
            assert_eq!(
                prev, &d.value,
                "STATE-MACHINE SAFETY VIOLATION: slot {} = {} on node {}, prev seen {}",
                d.slot, d.value, node_idx, prev
            );
        } else {
            self.seen.insert(d.slot, d.value.clone());
        }
    }

    pub fn total_decided(&self) -> usize {
        self.seen.len()
    }

    pub fn per_node_counts(&self) -> &[u64] {
        &self.per_node_count
    }

    /// Run an observation window: poll every `interval` for `total` duration.
    /// Returns true if `total_decided` strictly increased over the window.
    pub async fn assert_liveness_window(
        &mut self,
        cluster: &mut [ClusterNode],
        total: Duration,
        interval: Duration,
    ) -> bool {
        let start = self.total_decided();
        let deadline = tokio::time::Instant::now() + total;
        while tokio::time::Instant::now() < deadline {
            self.poll(cluster);
            tokio::time::sleep(interval).await;
        }
        self.total_decided() > start
    }
}
