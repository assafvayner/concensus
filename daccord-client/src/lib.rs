#![allow(clippy::result_large_err)]
//! Client SDK for the daccord consensus service.
//!
//! Wraps the `daccord.v1.ConsensusService` gRPC interface with ergonomic,
//! high-level Rust types and helpers (auto-reconnect Watch, propose retry,
//! typed Status).
//!
//! # Example
//!
//! ```no_run
//! use bytes::Bytes;
//! use daccord_client::Client;
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let client = Client::connect(vec!["http://127.0.0.1:50051".to_string()]).await?;
//! let decision = client.propose(Bytes::from_static(b"hello")).await?;
//! println!("decided slot {} payload {:?}", decision.slot, decision.payload);
//! # Ok(())
//! # }
//! ```

mod client;
mod error;
mod types;
mod watch;

mod proto {
    tonic::include_proto!("daccord.v1");
}

use bytes::Bytes;
use futures::stream::Stream;

pub use client::Client;
pub use error::Error;
pub use types::{Algorithm, ClusterStatus, Decision, Role};

impl Client {
    /// Stream decisions starting at `start_index`, auto-reconnecting on
    /// transient errors.
    ///
    /// The returned stream surfaces decisions in slot order. On disconnect
    /// or server-side "watch lagged" (`ResourceExhausted`), the client
    /// transparently reconnects from `last_yielded_slot + 1`.
    pub fn watch(&self, start_index: u64) -> impl Stream<Item = Result<Decision, Error>> {
        watch::watch_stream(self.clone(), start_index)
    }

    /// Convenience wrapper around [`Client::propose`] that accepts any
    /// type convertible into `Bytes`.
    pub async fn propose_into(&self, payload: impl Into<Bytes>) -> Result<Decision, Error> {
        self.propose(payload.into()).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn connect_with_no_endpoints_errors() {
        let err = Client::connect(vec![]).await.unwrap_err();
        assert!(matches!(err, Error::NoEndpoints));
    }

    #[tokio::test]
    async fn connect_with_invalid_endpoint_errors() {
        let err = Client::connect(vec!["not a url".to_string()])
            .await
            .unwrap_err();
        assert!(
            matches!(err, Error::InvalidEndpoint(_)),
            "expected InvalidEndpoint, got: {err:?}"
        );
    }

    #[test]
    fn algorithm_parses_known_values() {
        assert_eq!(Algorithm::from_str("paxos").unwrap(), Algorithm::Paxos);
        assert_eq!(Algorithm::from_str("raft").unwrap(), Algorithm::Raft);
        assert!(Algorithm::from_str("zab").is_err());
    }

    #[test]
    fn role_parses_known_values() {
        assert_eq!(Role::from_str("follower").unwrap(), Role::Follower);
        assert_eq!(Role::from_str("candidate").unwrap(), Role::Candidate);
        assert_eq!(Role::from_str("leader").unwrap(), Role::Leader);
        assert_eq!(Role::from_str("n/a").unwrap(), Role::NotApplicable);
        assert!(Role::from_str("dictator").is_err());
    }

    #[test]
    fn error_display_formats() {
        let e = Error::NoEndpoints;
        assert_eq!(e.to_string(), "no endpoints configured");
        let e = Error::InvalidEndpoint("foo".into());
        assert_eq!(e.to_string(), "invalid endpoint: foo");
        let e = Error::Invalid("bar".into());
        assert_eq!(e.to_string(), "invalid response: bar");
    }
}
