#![cfg(feature = "duckdb")]
//! Recovery tests for the DuckDB-backed [`PaxosStorage`] / [`RaftStorage`]
//! implementations.
//!
//! The trait-level tests exercise round-trip persistence across a `drop` +
//! reopen of the same file (or `:memory:` instance). The cluster-level tests
//! mirror `parity_recovery.rs` but route node 0's persistence through a
//! DuckDB file on a `tempfile::TempDir`.

mod helpers;

use std::path::PathBuf;
use std::time::Duration;

use daccord::{
    channel, DuckdbPaxosStorage, DuckdbRaftStorage, LogEntry, Node, NodeId, PaxosConfig,
    PaxosMemoryStorage, PaxosStorage, PeerInfo, RaftConfig, RaftMemoryStorage, RaftStorage,
};
use helpers::collect_decisions;
use tempfile::TempDir;
use tokio::time::timeout;

fn fresh_path(dir: &TempDir, name: &str) -> PathBuf {
    dir.path().join(name)
}

// ============================================================================
// Trait-level round-trip tests
// ============================================================================

#[tokio::test]
async fn paxos_decisions_persist_across_reopen() {
    let dir = TempDir::new().unwrap();
    let path = fresh_path(&dir, "paxos.duckdb");

    {
        let mut s = DuckdbPaxosStorage::<String>::open(&path).unwrap();
        s.save_decision(0, "a".into()).await.unwrap();
        s.save_decision(1, "b".into()).await.unwrap();
    }

    let s = DuckdbPaxosStorage::<String>::open(&path).unwrap();
    let mut decs = s.load_decisions().await.unwrap();
    decs.sort_by_key(|(slot, _)| *slot);
    assert_eq!(decs, vec![(0, "a".into()), (1, "b".into())]);
}

#[tokio::test]
async fn paxos_save_overwrites_slot() {
    let dir = TempDir::new().unwrap();
    let path = fresh_path(&dir, "paxos.duckdb");

    {
        let mut s = DuckdbPaxosStorage::<String>::open(&path).unwrap();
        s.save_decision(0, "first".into()).await.unwrap();
        s.save_decision(0, "second".into()).await.unwrap();
    }

    let s = DuckdbPaxosStorage::<String>::open(&path).unwrap();
    let decs = s.load_decisions().await.unwrap();
    assert_eq!(decs, vec![(0, "second".into())]);
}

#[tokio::test]
async fn paxos_in_memory_independent_per_instance() {
    let mut a = DuckdbPaxosStorage::<String>::open_in_memory().unwrap();
    let b = DuckdbPaxosStorage::<String>::open_in_memory().unwrap();
    a.save_decision(0, "x".into()).await.unwrap();
    assert!(b.load_decisions().await.unwrap().is_empty());
    assert_eq!(
        a.load_decisions().await.unwrap(),
        vec![(0, "x".to_string())]
    );
}

#[tokio::test]
async fn raft_term_voted_for_persist_across_reopen() {
    let dir = TempDir::new().unwrap();
    let path = fresh_path(&dir, "raft.duckdb");

    let nid = NodeId::new("a", 42);
    {
        let mut s = DuckdbRaftStorage::<String>::open(&path).unwrap();
        s.save_term(7).await.unwrap();
        s.save_voted_for(Some(nid.clone())).await.unwrap();
    }

    {
        let s = DuckdbRaftStorage::<String>::open(&path).unwrap();
        assert_eq!(s.load_term().await.unwrap(), 7);
        assert_eq!(s.load_voted_for().await.unwrap(), Some(nid.clone()));
    }

    {
        let mut s = DuckdbRaftStorage::<String>::open(&path).unwrap();
        s.save_voted_for(None).await.unwrap();
    }

    let s = DuckdbRaftStorage::<String>::open(&path).unwrap();
    assert!(s.load_voted_for().await.unwrap().is_none());
    assert_eq!(s.load_term().await.unwrap(), 7);
}

#[tokio::test]
async fn raft_log_append_load_roundtrip() {
    let dir = TempDir::new().unwrap();
    let path = fresh_path(&dir, "raft.duckdb");

    {
        let mut s = DuckdbRaftStorage::<String>::open(&path).unwrap();
        s.append_log(&[
            LogEntry {
                term: 1,
                value: "a".into(),
            },
            LogEntry {
                term: 1,
                value: "b".into(),
            },
        ])
        .await
        .unwrap();
        s.append_log(&[LogEntry {
            term: 2,
            value: "c".into(),
        }])
        .await
        .unwrap();
    }

    let s = DuckdbRaftStorage::<String>::open(&path).unwrap();
    let log = s.load_log().await.unwrap();
    assert_eq!(log.len(), 3);
    assert_eq!(log[0].term, 1);
    assert_eq!(log[0].value, "a");
    assert_eq!(log[1].term, 1);
    assert_eq!(log[1].value, "b");
    assert_eq!(log[2].term, 2);
    assert_eq!(log[2].value, "c");
}

#[tokio::test]
async fn raft_log_truncate_from() {
    let dir = TempDir::new().unwrap();
    let path = fresh_path(&dir, "raft.duckdb");

    {
        let mut s = DuckdbRaftStorage::<String>::open(&path).unwrap();
        let entries: Vec<LogEntry<String>> = (0..5)
            .map(|i| LogEntry {
                term: 1,
                value: format!("v{i}"),
            })
            .collect();
        s.append_log(&entries).await.unwrap();
        s.truncate_log_from(2).await.unwrap();
    }

    let s = DuckdbRaftStorage::<String>::open(&path).unwrap();
    let log = s.load_log().await.unwrap();
    assert_eq!(log.len(), 2);
    assert_eq!(log[0].value, "v0");
    assert_eq!(log[1].value, "v1");
}

#[tokio::test]
async fn raft_truncate_past_end_is_noop() {
    let mut s = DuckdbRaftStorage::<String>::open_in_memory().unwrap();
    s.append_log(&[LogEntry {
        term: 1,
        value: "a".into(),
    }])
    .await
    .unwrap();
    s.truncate_log_from(100).await.unwrap();
    assert_eq!(s.load_log().await.unwrap().len(), 1);
}

#[tokio::test]
async fn raft_commit_index_persist_and_clear() {
    let dir = TempDir::new().unwrap();
    let path = fresh_path(&dir, "raft.duckdb");

    {
        let mut s = DuckdbRaftStorage::<String>::open(&path).unwrap();
        assert!(s.load_commit_index().await.unwrap().is_none());
        s.save_commit_index(Some(7)).await.unwrap();
    }

    {
        let s = DuckdbRaftStorage::<String>::open(&path).unwrap();
        assert_eq!(s.load_commit_index().await.unwrap(), Some(7));
    }

    {
        let mut s = DuckdbRaftStorage::<String>::open(&path).unwrap();
        s.save_commit_index(None).await.unwrap();
    }

    let s = DuckdbRaftStorage::<String>::open(&path).unwrap();
    assert!(s.load_commit_index().await.unwrap().is_none());
}

#[tokio::test]
async fn paxos_and_raft_share_same_db_file() {
    let dir = TempDir::new().unwrap();
    let path = fresh_path(&dir, "shared.duckdb");

    {
        let mut p = DuckdbPaxosStorage::<String>::open(&path).unwrap();
        p.save_decision(5, "paxos-only".into()).await.unwrap();
    }
    {
        let mut r = DuckdbRaftStorage::<String>::open(&path).unwrap();
        r.save_term(11).await.unwrap();
        r.append_log(&[LogEntry {
            term: 11,
            value: "raft-only".into(),
        }])
        .await
        .unwrap();
        r.save_decision(9, "raft-decided".into()).await.unwrap();
    }

    {
        let p = DuckdbPaxosStorage::<String>::open(&path).unwrap();
        let decs = p.load_decisions().await.unwrap();
        assert_eq!(decs, vec![(5, "paxos-only".to_string())]);
    }
    {
        let r = DuckdbRaftStorage::<String>::open(&path).unwrap();
        assert_eq!(r.load_term().await.unwrap(), 11);
        let log = r.load_log().await.unwrap();
        assert_eq!(log.len(), 1);
        assert_eq!(log[0].value, "raft-only");
        let decs = r.load_decisions().await.unwrap();
        assert_eq!(decs, vec![(9, "raft-decided".to_string())]);
    }
}

// ============================================================================
// Cluster-level recovery tests
// ============================================================================

#[allow(clippy::type_complexity)]
fn build_paxos_cluster_with_duckdb(
    ids: &[NodeId; 3],
    storage0: DuckdbPaxosStorage<String>,
) -> (
    Vec<daccord::NodeHandle<String>>,
    Vec<daccord::DecisionReceiver<String>>,
    Vec<tokio::task::JoinHandle<Result<(), daccord::NodeError>>>,
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
async fn paxos_cluster_restart_recovers_decisions_via_duckdb() {
    let dir = TempDir::new().unwrap();
    let path = fresh_path(&dir, "paxos-cluster.duckdb");
    let ids: [NodeId; 3] = [
        NodeId::new("a", 1000),
        NodeId::new("b", 1000),
        NodeId::new("c", 1000),
    ];

    // Phase 1: initial cluster, propose values.
    {
        let storage0 = DuckdbPaxosStorage::<String>::open(&path).unwrap();
        let (handles, mut decisions, run_handles) = build_paxos_cluster_with_duckdb(&ids, storage0);
        handles[0].propose("v0".into()).await.unwrap();
        handles[0].propose("v1".into()).await.unwrap();
        handles[0].propose("v2".into()).await.unwrap();
        let _ = collect_decisions(&mut decisions[0], 3).await;

        for h in handles {
            drop(h);
        }
        for d in decisions {
            drop(d);
        }
        for rh in run_handles {
            rh.abort();
        }
    }

    // Verify the file persisted what node 0 saw.
    {
        let storage = DuckdbPaxosStorage::<String>::open(&path).unwrap();
        let decs = storage.load_decisions().await.unwrap();
        assert_eq!(decs.len(), 3);
    }

    // Phase 2: rebuild cluster against the same file, propose another value.
    // Decision delivery is at-least-once across restart, so the channel may
    // replay any of slots 0..=2 before surfacing the new proposal. Drain until
    // we see slot 3 — a broken recovery that failed to seed `next_slot` would
    // land "v3" at slot 0 and we'd time out here.
    let storage0 = DuckdbPaxosStorage::<String>::open(&path).unwrap();
    let (handles2, mut decisions2, run_handles2) = build_paxos_cluster_with_duckdb(&ids, storage0);
    handles2[0].propose("v3".into()).await.unwrap();
    let resumed = timeout(Duration::from_secs(10), async {
        loop {
            let d = decisions2[0]
                .recv()
                .await
                .expect("decision channel closed");
            if d.slot == 3 {
                break d;
            }
        }
    })
    .await
    .expect("timed out waiting for slot 3 decision");
    assert_eq!(resumed.value, "v3");

    for h in handles2 {
        drop(h);
    }
    for rh in run_handles2 {
        rh.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn raft_cluster_restart_persists_state_via_duckdb() {
    let dir = TempDir::new().unwrap();
    let path = fresh_path(&dir, "raft-cluster.duckdb");
    let ids: [NodeId; 3] = [
        NodeId::new("a", 1000),
        NodeId::new("b", 1000),
        NodeId::new("c", 1000),
    ];
    let cfg = RaftConfig {
        election_timeout_min: Duration::from_millis(150),
        election_timeout_max: Duration::from_millis(300),
        heartbeat_interval: Duration::from_millis(50),
    };

    {
        let storage0 = DuckdbRaftStorage::<String>::open(&path).unwrap();
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
            Node::raft_with_id(ids[0].clone(), cfg.clone(), peers0, rx0, storage0);
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

        for v in &["v0", "v1", "v2"] {
            h0.propose((*v).to_string()).await.unwrap();
        }
        let _ = collect_decisions(&mut d0, 3).await;

        drop(h0);
        drop(h1);
        r0.abort();
        r1.abort();
        r2.abort();
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // Reopen the file fresh and assert the persisted state covers the
    // recovery contract: term advanced, log non-empty, decisions equal to
    // proposed count.
    let storage = DuckdbRaftStorage::<String>::open(&path).unwrap();
    assert!(storage.load_term().await.unwrap() >= 1);
    assert!(!storage.load_log().await.unwrap().is_empty());
    let decisions = storage.load_decisions().await.unwrap();
    assert_eq!(decisions.len(), 3);
}
