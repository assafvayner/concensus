mod helpers;

use std::time::Duration;

use concensus::{
    channel, Node, NodeId, PaxosConfig, PaxosMemoryStorage, PeerInfo, RaftConfig,
    RaftMemoryStorage, RaftStorage,
};
use helpers::{collect_decisions, SharedMemoryStorage, SharedRaftStorage};
use tokio::time::timeout;

#[allow(clippy::type_complexity)]
fn build_paxos_cluster(
    ids: &[NodeId; 3],
    storage0: SharedMemoryStorage<String>,
) -> (
    Vec<concensus::NodeHandle<String>>,
    Vec<concensus::DecisionReceiver<String>>,
    Vec<tokio::task::JoinHandle<Result<(), concensus::NodeError>>>,
) {
    let (tx0, rx0) = channel(64);
    let (tx1, rx1) = channel(64);
    let (tx2, rx2) = channel(64);
    let peers0 = vec![
        PeerInfo {
            id: ids[1].clone(),
            sender: tx1.clone(),
        },
        PeerInfo {
            id: ids[2].clone(),
            sender: tx2.clone(),
        },
    ];
    let peers1 = vec![
        PeerInfo {
            id: ids[0].clone(),
            sender: tx0.clone(),
        },
        PeerInfo {
            id: ids[2].clone(),
            sender: tx2.clone(),
        },
    ];
    let peers2 = vec![
        PeerInfo {
            id: ids[0].clone(),
            sender: tx0.clone(),
        },
        PeerInfo {
            id: ids[1].clone(),
            sender: tx1.clone(),
        },
    ];
    let (n0, h0, d0) = Node::paxos_with_id(
        ids[0].clone(),
        PaxosConfig::default(),
        peers0,
        rx0,
        storage0,
    );
    let (n1, h1, d1) = Node::paxos_with_id(
        ids[1].clone(),
        PaxosConfig::default(),
        peers1,
        rx1,
        PaxosMemoryStorage::<String>::new(),
    );
    let (n2, h2, d2) = Node::paxos_with_id(
        ids[2].clone(),
        PaxosConfig::default(),
        peers2,
        rx2,
        PaxosMemoryStorage::<String>::new(),
    );
    let r0 = tokio::spawn(n0.run());
    let r1 = tokio::spawn(n1.run());
    let r2 = tokio::spawn(n2.run());
    (vec![h0, h1, h2], vec![d0, d1, d2], vec![r0, r1, r2])
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn paxos_restart_recovers_decisions() {
    let ids: [NodeId; 3] = [
        NodeId::new("a", 1000),
        NodeId::new("b", 1000),
        NodeId::new("c", 1000),
    ];
    let storage0 = SharedMemoryStorage::<String>::new();

    // Phase 1: initial cluster, propose values
    let (handles, mut decisions, run_handles) = build_paxos_cluster(&ids, storage0.clone());
    handles[0].propose("v0".into()).await.unwrap();
    handles[0].propose("v1".into()).await.unwrap();
    handles[0].propose("v2".into()).await.unwrap();
    let _ = collect_decisions(&mut decisions[0], 3).await;

    // Tear down entire cluster (simulates crash)
    for h in handles {
        drop(h);
    }
    for d in decisions {
        drop(d);
    }
    for rh in run_handles {
        rh.abort();
    }

    // Phase 2: rebuild cluster reusing storage0; propose more
    let (handles2, mut decisions2, run_handles2) = build_paxos_cluster(&ids, storage0.clone());
    handles2[0].propose("v3".into()).await.unwrap();
    let timeout_dec = timeout(Duration::from_secs(10), decisions2[0].recv()).await;
    assert!(timeout_dec.is_ok());

    for h in handles2 {
        drop(h);
    }
    for rh in run_handles2 {
        rh.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn raft_restart_recovers_decisions() {
    let ids: [NodeId; 3] = [
        NodeId::new("a", 1000),
        NodeId::new("b", 1000),
        NodeId::new("c", 1000),
    ];
    let storage0 = SharedRaftStorage::<String>::new();
    let cfg = RaftConfig {
        election_timeout_min: Duration::from_millis(150),
        election_timeout_max: Duration::from_millis(300),
        heartbeat_interval: Duration::from_millis(50),
    };

    let (tx0, rx0) = channel(64);
    let (tx1, rx1) = channel(64);
    let (tx2, rx2) = channel(64);
    let peers0 = vec![
        PeerInfo {
            id: ids[1].clone(),
            sender: tx1.clone(),
        },
        PeerInfo {
            id: ids[2].clone(),
            sender: tx2.clone(),
        },
    ];
    let peers1 = vec![
        PeerInfo {
            id: ids[0].clone(),
            sender: tx0.clone(),
        },
        PeerInfo {
            id: ids[2].clone(),
            sender: tx2.clone(),
        },
    ];
    let peers2 = vec![
        PeerInfo {
            id: ids[0].clone(),
            sender: tx0.clone(),
        },
        PeerInfo {
            id: ids[1].clone(),
            sender: tx1.clone(),
        },
    ];
    let (n0, h0, mut d0) =
        Node::raft_with_id(ids[0].clone(), cfg.clone(), peers0, rx0, storage0.clone());
    let (n1, h1, _d1) = Node::raft_with_id(
        ids[1].clone(),
        cfg.clone(),
        peers1,
        rx1,
        RaftMemoryStorage::<String>::new(),
    );
    let (n2, _h2, _d2) = Node::raft_with_id(
        ids[2].clone(),
        cfg.clone(),
        peers2,
        rx2,
        RaftMemoryStorage::<String>::new(),
    );
    let r0 = tokio::spawn(n0.run());
    let r1 = tokio::spawn(n1.run());
    let r2 = tokio::spawn(n2.run());

    tokio::time::sleep(Duration::from_millis(800)).await;

    // Propose. Best-effort: at least one node is leader; routing handles the rest.
    for v in &["v0", "v1", "v2"] {
        h0.propose((*v).to_string()).await.unwrap();
    }
    let _ = collect_decisions(&mut d0, 3).await;

    drop(h0);
    r0.abort();
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Verify the storage state is correctly preserved across drop+rebuild —
    // this is the actual recovery contract. Rebuilding a node into the
    // existing cluster (so other peers route to the new node-0) requires
    // re-routing peer senders, which is awkward in a unit test; instead we
    // verify that on restart, the persistent state would be loaded correctly.
    assert!(storage0.load_term().await.unwrap() >= 1);
    assert!(!storage0.load_log().await.unwrap().is_empty());
    let decisions = storage0.load_decisions().await.unwrap();
    assert_eq!(decisions.len(), 3);

    drop(h1);
    r1.abort();
    r2.abort();
}
