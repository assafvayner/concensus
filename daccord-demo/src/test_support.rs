//! In-process test helpers for spinning up a demo node + gRPC service over
//! channel transport.
//!
//! This module is gated behind the `test-support` feature so production
//! builds don't pull in the channel transport or test-only plumbing.

use std::sync::Arc;

use daccord::{
    channel, ChannelReceiver, ChannelSender, Node, NodeHandle, NodeId, PaxosConfig,
    PaxosMemoryStorage, PeerInfo,
};
use tokio::task::JoinHandle;

use crate::service::{collect_decisions, Algorithm, DecisionLog, DECISION_BROADCAST_CAPACITY};

/// Channel capacity used for the (single-node) self-loopback message
/// transport. The node never writes to it in a single-node cluster, so the
/// exact value is only relevant if a future multi-node test variant grows out
/// of this helper.
const TRANSPORT_CHANNEL_CAPACITY: usize = 64;

/// Handles produced by [`start_in_process_node`].
///
/// The caller owns each piece independently:
/// - `handle` — submit proposals via `handle.propose(...)`.
/// - `decisions` — shared in-memory log used by `ConsensusServiceImpl` to
///   answer `Watch`/`GetDecisions` queries.
/// - `algorithm` — which algorithm the node is configured to run.
/// - `node_task` — the spawned `Node::run` task; abort it for clean shutdown.
/// - `collector_task` — the task that drains decisions into the log; aborts
///   once `node_task` is gone.
pub struct InProcessNode {
    pub handle: NodeHandle<Vec<u8>>,
    pub decisions: Arc<DecisionLog>,
    pub algorithm: Algorithm,
    pub node_task: JoinHandle<Result<(), daccord::NodeError>>,
    pub collector_task: JoinHandle<()>,
}

impl InProcessNode {
    /// Abort the node and decision-collector tasks. Idempotent.
    pub fn shutdown(&self) {
        self.collector_task.abort();
        self.node_task.abort();
    }
}

/// Spin up a single-node Paxos cluster using channel transport.
///
/// The node has no peers (it's a one-node "cluster"), so quorum is 1 and any
/// proposal goes through immediately. This is the simplest possible end-to-end
/// setup for exercising the SDK ↔ demo gRPC round-trip in tests.
pub fn start_in_process_node(node_name: &str) -> InProcessNode {
    // Self-loopback channel transport. The node has no peers, so this
    // sender/receiver pair is never actually used to move bytes — but
    // `Node::run` still needs a receiver to listen on.
    let (_self_tx, self_rx): (ChannelSender, ChannelReceiver) = channel(TRANSPORT_CHANNEL_CAPACITY);

    let node_id = NodeId::new(node_name, 0);
    let storage = PaxosMemoryStorage::<Vec<u8>>::new();
    let peers: Vec<PeerInfo<ChannelSender>> = Vec::new();
    let (node, handle, decision_rx) =
        Node::paxos_with_id(node_id, PaxosConfig::default(), peers, self_rx, storage);

    let name = node_name.to_string();
    let node_task = tokio::spawn(node.run());

    let decisions = Arc::new(DecisionLog::new(DECISION_BROADCAST_CAPACITY));
    let collector_task = tokio::spawn(collect_decisions(decision_rx, decisions.clone(), name));

    InProcessNode {
        handle,
        decisions,
        algorithm: Algorithm::Paxos,
        node_task,
        collector_task,
    }
}
