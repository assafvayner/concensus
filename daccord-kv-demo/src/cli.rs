//! `daccord-kv-cli` — small CLI client for the KV demo's gRPC service.

use clap::{Parser, Subcommand};

pub mod kv_proto {
    tonic::include_proto!("daccord.kv.v1");
}

use kv_proto::kv_service_client::KvServiceClient;
use kv_proto::{GetRequest, HealthRequest, PutRequest, StatusRequest};

#[derive(Parser)]
#[command(
    name = "daccord-kv-cli",
    about = "CLI client for the daccord-kv-node gRPC API"
)]
struct Cli {
    /// gRPC server address (e.g. http://localhost:50051 or localhost:50051).
    #[arg(long)]
    addr: String,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Insert or replace the value for a key.
    Put {
        key: String,
        /// Value is sent as raw UTF-8 bytes.
        value: String,
    },
    /// Read the current value for a key.
    Get { key: String },
    /// Check node liveness.
    Health,
    /// Print detailed node state.
    Status,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    let addr = if cli.addr.starts_with("http://") || cli.addr.starts_with("https://") {
        cli.addr.clone()
    } else {
        format!("http://{}", cli.addr)
    };

    let mut client = KvServiceClient::connect(addr).await.unwrap_or_else(|e| {
        eprintln!("failed to connect: {}", e);
        std::process::exit(1);
    });

    match cli.command {
        Command::Put { key, value } => {
            match client
                .put(PutRequest {
                    key,
                    value: value.into_bytes(),
                })
                .await
            {
                Ok(response) => {
                    let resp = response.into_inner();
                    if resp.was_new {
                        println!("ok (new write)");
                    } else {
                        println!("ok (replaced existing value)");
                    }
                }
                Err(status) => {
                    eprintln!("error: {} ({})", status.message(), status.code());
                    std::process::exit(1);
                }
            }
        }
        Command::Get { key } => match client.get(GetRequest { key }).await {
            Ok(response) => {
                let resp = response.into_inner();
                if resp.found {
                    match std::str::from_utf8(&resp.value) {
                        Ok(s) => println!("{}", s),
                        Err(_) => {
                            // Not valid UTF-8 — print hex so the user sees something useful.
                            for b in resp.value {
                                print!("{:02x}", b);
                            }
                            println!();
                        }
                    }
                } else {
                    eprintln!("(absent)");
                    std::process::exit(2);
                }
            }
            Err(status) => {
                eprintln!("error: {} ({})", status.message(), status.code());
                std::process::exit(1);
            }
        },
        Command::Health => match client.health(HealthRequest {}).await {
            Ok(response) => {
                let r = response.into_inner();
                println!("ok node={} algorithm={}", r.node_name, r.algorithm);
            }
            Err(status) => {
                eprintln!("error: {} ({})", status.message(), status.code());
                std::process::exit(1);
            }
        },
        Command::Status => match client.status(StatusRequest {}).await {
            Ok(response) => {
                let s = response.into_inner();
                println!("node_id:      {}", s.node_id);
                println!("algorithm:    {}", s.algorithm);
                println!("role:         {}", s.role);
                println!("term:         {}", s.term);
                println!(
                    "leader_id:    {}",
                    if s.leader_id.is_empty() {
                        "<none>"
                    } else {
                        &s.leader_id
                    }
                );
                println!("log_len:      {}", s.log_len);
                match s.commit_index {
                    Some(c) => println!("commit_index: {}", c),
                    None => println!("commit_index: <none>"),
                }
                match s.last_applied {
                    Some(a) => println!("last_applied: {}", a),
                    None => println!("last_applied: <none>"),
                }
            }
            Err(status) => {
                eprintln!("error: {} ({})", status.message(), status.code());
                std::process::exit(1);
            }
        },
    }
}
