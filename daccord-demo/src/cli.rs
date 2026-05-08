use clap::{Parser, Subcommand};

pub mod consensus_proto {
    tonic::include_proto!("daccord.v1");
}

use consensus_proto::consensus_service_client::ConsensusServiceClient;
use consensus_proto::{
    GetDecisionsRequest, HealthRequest, ProposeRequest, StatusRequest, WatchRequest,
};

#[derive(Parser)]
#[command(name = "daccord-cli", about = "CLI client for daccord-node gRPC API")]
struct Cli {
    /// gRPC server address (e.g. http://localhost:50051)
    #[arg(long)]
    addr: String,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Propose a value for consensus.
    Propose {
        /// Payload to propose; sent as raw UTF-8 bytes.
        #[arg(long)]
        payload: String,
    },
    /// List decided values from a starting index.
    Decisions {
        /// Slot index to start at (default 0).
        #[arg(long, default_value_t = 0)]
        from: u64,
        /// Maximum number of decisions to return (server default if unset).
        #[arg(long)]
        limit: Option<u32>,
    },
    /// Stream decisions as they are decided.
    Watch {
        /// Slot index to start at (default 0).
        #[arg(long, default_value_t = 0)]
        from: u64,
    },
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

    let mut client = ConsensusServiceClient::connect(addr)
        .await
        .unwrap_or_else(|e| {
            eprintln!("failed to connect: {}", e);
            std::process::exit(1);
        });

    match cli.command {
        Command::Propose { payload } => {
            match client
                .propose(ProposeRequest {
                    payload: payload.into_bytes(),
                })
                .await
            {
                Ok(response) => {
                    let resp = response.into_inner();
                    println!("decided in slot {}", resp.slot);
                }
                Err(status) => {
                    eprintln!("error: {} ({})", status.message(), status.code());
                    std::process::exit(1);
                }
            }
        }
        Command::Decisions { from, limit } => {
            match client
                .get_decisions(GetDecisionsRequest {
                    start_index: from,
                    limit,
                })
                .await
            {
                Ok(response) => {
                    let resp = response.into_inner();
                    if resp.decisions.is_empty() {
                        println!("no decisions in range");
                    } else {
                        println!("{:<8} PAYLOAD", "SLOT");
                        for d in resp.decisions {
                            println!("{:<8} {}", d.slot, String::from_utf8_lossy(&d.payload));
                        }
                    }
                    println!("next_index: {}", resp.next_index);
                }
                Err(status) => {
                    eprintln!("error: {} ({})", status.message(), status.code());
                    std::process::exit(1);
                }
            }
        }
        Command::Watch { from } => {
            let stream = match client.watch(WatchRequest { start_index: from }).await {
                Ok(resp) => resp.into_inner(),
                Err(status) => {
                    eprintln!("error: {} ({})", status.message(), status.code());
                    std::process::exit(1);
                }
            };
            let mut stream = stream;
            loop {
                match stream.message().await {
                    Ok(Some(d)) => {
                        println!("slot {}: {}", d.slot, String::from_utf8_lossy(&d.payload));
                    }
                    Ok(None) => break,
                    Err(status) => {
                        eprintln!("stream error: {} ({})", status.message(), status.code());
                        std::process::exit(1);
                    }
                }
            }
        }
        Command::Health => match client.health(HealthRequest {}).await {
            Ok(response) => {
                println!("{}", response.into_inner().status);
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
