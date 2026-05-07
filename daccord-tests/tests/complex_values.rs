mod helpers;

use std::collections::HashSet;

use helpers::create_cluster_typed;
use serde::{Deserialize, Serialize};
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
    let mut cluster = create_cluster_typed::<Operation>(3);

    let op = make_op("alpha", OpKind::Set, 1);
    cluster[0].handle.propose(op.clone()).await.unwrap();

    let mut decisions = Vec::new();
    for node in &mut cluster {
        let decided = timeout(Duration::from_secs(5), node.decisions.recv())
            .await
            .unwrap()
            .unwrap();
        decisions.push(decided.value);
    }

    for d in &decisions {
        assert_eq!(d, &op);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn structured_value_multiple_proposals() {
    let mut cluster = create_cluster_typed::<Operation>(3);

    let ops = vec![
        make_op("key-0", OpKind::Set, 10),
        make_op("key-1", OpKind::Delete, 20),
        make_op("key-2", OpKind::BatchUpdate, 30),
        make_op("key-3", OpKind::Set, 40),
        make_op("key-4", OpKind::Delete, 50),
    ];

    for op in &ops {
        cluster[0].handle.propose(op.clone()).await.unwrap();
    }

    let expected: HashSet<Operation> = ops.into_iter().collect();

    for node in &mut cluster {
        let mut node_decisions = HashSet::new();
        for _ in 0..5 {
            let decided = timeout(Duration::from_secs(5), node.decisions.recv())
                .await
                .unwrap()
                .unwrap();
            node_decisions.insert(decided.value);
        }
        assert_eq!(node_decisions, expected);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn structured_value_with_edge_case_fields() {
    let mut cluster = create_cluster_typed::<Operation>(3);

    let op = Operation {
        kind: OpKind::Set,
        key: String::new(),
        value: None,
        tags: vec![],
        metadata: OperationMeta {
            timestamp: 0,
            origin: String::new(),
        },
    };

    cluster[0].handle.propose(op.clone()).await.unwrap();

    for node in &mut cluster {
        let decided = timeout(Duration::from_secs(5), node.decisions.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(decided.value, op);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn structured_value_large_payload() {
    let mut cluster = create_cluster_typed::<Operation>(3);

    let op = Operation {
        kind: OpKind::BatchUpdate,
        key: "k".repeat(200),
        value: Some("v".repeat(500)),
        tags: (0..100).map(|i| format!("tag-{}", i)).collect(),
        metadata: OperationMeta {
            timestamp: u64::MAX,
            origin: "large-payload-test".into(),
        },
    };

    cluster[0].handle.propose(op.clone()).await.unwrap();

    for node in &mut cluster {
        let decided = timeout(Duration::from_secs(5), node.decisions.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(decided.value, op);
    }
}
