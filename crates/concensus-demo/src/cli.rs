use clap::{Parser, Subcommand};

pub mod consensus_proto {
    tonic::include_proto!("consensus");
}

use consensus_proto::consensus_service_client::ConsensusServiceClient;
use consensus_proto::{GetDecisionsRequest, HealthRequest, ProposeRequest};

#[derive(Parser)]
#[command(name = "concensus-cli", about = "CLI client for concensus-node gRPC API")]
struct Cli {
    /// gRPC server address (e.g. http://localhost:50051)
    #[arg(long)]
    addr: String,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Propose a value for consensus
    Propose {
        /// The value to propose
        #[arg(long)]
        value: String,
    },
    /// List all decided values
    Decisions,
    /// Check node health
    Health,
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
        Command::Propose { value } => {
            match client.propose(ProposeRequest { value }).await {
                Ok(response) => {
                    println!("{}", response.into_inner().status);
                }
                Err(status) => {
                    eprintln!("error: {} ({})", status.message(), status.code());
                    std::process::exit(1);
                }
            }
        }
        Command::Decisions => {
            match client.get_decisions(GetDecisionsRequest {}).await {
                Ok(response) => {
                    let decisions = response.into_inner().decisions;
                    if decisions.is_empty() {
                        println!("no decisions yet");
                    } else {
                        println!("{:<8} VALUE", "SLOT");
                        for d in decisions {
                            println!("{:<8} {}", d.slot, d.value);
                        }
                    }
                }
                Err(status) => {
                    eprintln!("error: {} ({})", status.message(), status.code());
                    std::process::exit(1);
                }
            }
        }
        Command::Health => {
            match client.health(HealthRequest {}).await {
                Ok(response) => {
                    println!("{}", response.into_inner().status);
                }
                Err(status) => {
                    eprintln!("error: {} ({})", status.message(), status.code());
                    std::process::exit(1);
                }
            }
        }
    }
}
