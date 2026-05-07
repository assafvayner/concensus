#![allow(dead_code)]

use std::process::{Command, Stdio};
use std::time::Duration;

use concensus_demo::ConsensusServiceClient;
use concensus_demo::{GetDecisionsRequest, HealthRequest, ProposeRequest};

pub const COMPOSE_FILE: &str = "docker-compose.raft.yml";

pub struct DockerCluster {
    pub nodes: Vec<NodeAddr>,
    composed: bool,
}

pub struct NodeAddr {
    pub container: String,
    pub grpc: String,
}

impl DockerCluster {
    pub fn up_3node_raft() -> Self {
        let status = Command::new("docker")
            .args(["compose", "-f", COMPOSE_FILE, "up", "--build", "-d"])
            .current_dir(env!("CARGO_MANIFEST_DIR"))
            .status()
            .expect("docker compose up failed");
        assert!(
            status.success(),
            "docker compose up exit={:?}",
            status.code()
        );
        Self {
            nodes: vec![
                NodeAddr {
                    container: "concensus-demo-node-1-1".into(),
                    grpc: "localhost:50051".into(),
                },
                NodeAddr {
                    container: "concensus-demo-node-2-1".into(),
                    grpc: String::new(),
                },
                NodeAddr {
                    container: "concensus-demo-node-3-1".into(),
                    grpc: String::new(),
                },
            ],
            composed: true,
        }
    }

    pub async fn wait_healthy(&self, timeout: Duration) {
        let deadline = tokio::time::Instant::now() + timeout;
        while tokio::time::Instant::now() < deadline {
            if self.health_ok().await {
                return;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        panic!("cluster did not become healthy within {timeout:?}");
    }

    pub async fn health_ok(&self) -> bool {
        for n in &self.nodes {
            if n.grpc.is_empty() {
                continue;
            }
            if !health_one(&n.grpc).await {
                return false;
            }
        }
        true
    }

    pub fn stop_node(&self, idx: usize) {
        let status = Command::new("docker")
            .args([
                "compose",
                "-f",
                COMPOSE_FILE,
                "stop",
                &format!("node-{}", idx + 1),
            ])
            .current_dir(env!("CARGO_MANIFEST_DIR"))
            .status()
            .expect("docker compose stop failed");
        assert!(status.success());
    }

    pub fn start_node(&self, idx: usize) {
        let status = Command::new("docker")
            .args([
                "compose",
                "-f",
                COMPOSE_FILE,
                "start",
                &format!("node-{}", idx + 1),
            ])
            .current_dir(env!("CARGO_MANIFEST_DIR"))
            .status()
            .expect("docker compose start failed");
        assert!(status.success());
    }

    pub fn disconnect_node(&self, idx: usize) {
        let status = Command::new("docker")
            .args([
                "network",
                "disconnect",
                "concensus-demo_consensus-net",
                &self.nodes[idx].container,
            ])
            .status()
            .expect("docker network disconnect failed");
        assert!(status.success());
    }

    pub fn connect_node(&self, idx: usize) {
        let status = Command::new("docker")
            .args([
                "network",
                "connect",
                "concensus-demo_consensus-net",
                &self.nodes[idx].container,
            ])
            .status()
            .expect("docker network connect failed");
        assert!(status.success());
    }
}

impl Drop for DockerCluster {
    fn drop(&mut self) {
        if self.composed {
            let _ = Command::new("docker")
                .args(["compose", "-f", COMPOSE_FILE, "down", "-v"])
                .current_dir(env!("CARGO_MANIFEST_DIR"))
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
        }
    }
}

async fn health_one(addr: &str) -> bool {
    let url = format!("http://{addr}");
    let mut client = match ConsensusServiceClient::connect(url).await {
        Ok(c) => c,
        Err(_) => return false,
    };
    client.health(HealthRequest {}).await.is_ok()
}

pub async fn propose(addr: &str, value: &str) -> Result<(), String> {
    let url = format!("http://{addr}");
    let mut client = ConsensusServiceClient::connect(url)
        .await
        .map_err(|e| e.to_string())?;
    client
        .propose(ProposeRequest {
            value: value.into(),
        })
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}

pub async fn fetch_decisions(addr: &str) -> Result<Vec<(u64, String)>, String> {
    let url = format!("http://{addr}");
    let mut client = ConsensusServiceClient::connect(url)
        .await
        .map_err(|e| e.to_string())?;
    let resp = client
        .get_decisions(GetDecisionsRequest {})
        .await
        .map_err(|e| e.to_string())?;
    Ok(resp
        .into_inner()
        .decisions
        .into_iter()
        .map(|d| (d.slot, d.value))
        .collect())
}
