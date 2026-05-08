//! Smoke-test example for daccord-client.
//!
//! Connects to the configured endpoints, proposes 3 payloads concurrently,
//! then watches from index 0 and prints the first 3 decisions before
//! exiting.
//!
//! Usage:
//!     cargo run -p daccord-client --example echo -- http://127.0.0.1:50051

use std::env;

use bytes::Bytes;
use daccord_client::Client;
use futures::StreamExt;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let endpoints: Vec<String> = env::args().skip(1).collect();
    let endpoints = if endpoints.is_empty() {
        vec!["http://127.0.0.1:50051".to_string()]
    } else {
        endpoints
    };

    let client = Client::connect(endpoints).await?;

    let propose_handles: Vec<_> = (0..3u8)
        .map(|i| {
            let c = client.clone();
            tokio::spawn(async move {
                let payload = Bytes::from(format!("echo-{i}"));
                c.propose(payload).await
            })
        })
        .collect();

    for h in propose_handles {
        match h.await? {
            Ok(d) => println!("proposed: slot={} payload={:?}", d.slot, d.payload),
            Err(e) => eprintln!("propose error: {e}"),
        }
    }

    let watch = client.watch(0);
    tokio::pin!(watch);

    let mut seen = 0usize;
    while let Some(item) = watch.next().await {
        match item {
            Ok(d) => {
                println!("watched: slot={} payload={:?}", d.slot, d.payload);
                seen += 1;
                if seen >= 3 {
                    break;
                }
            }
            Err(e) => {
                eprintln!("watch error: {e}");
                break;
            }
        }
    }

    Ok(())
}
