//! A Paxos consensus library with pluggable transports and storage.
//!
//! This crate implements the Classic Paxos (Multi-Paxos) consensus protocol,
//! allowing a cluster of nodes to agree on an ordered sequence of values.
//! Each decided value is assigned to a numbered slot, and all nodes in the
//! cluster converge on the same slot-to-value mapping.
//!
//! # Architecture
//!
//! The library is organized around a few core abstractions:
//!
//! - [`Node`] — the consensus participant that runs the Paxos event loop
//! - [`NodeHandle`] — a cloneable handle for submitting proposals to a running node
//! - [`DecisionReceiver`] — a channel receiver for consuming decided values
//! - [`Storage`] — a trait for persisting decisions (with [`MemoryStorage`] provided)
//! - [`MessageSender`] / [`MessageReceiver`] — traits for inter-node communication
//!
//! # Quick Start
//!
//! ```rust,no_run
//! use concensus::{Node, NodeId, MemoryStorage, PeerInfo};
//! # // This example requires a transport, shown conceptually
//! # fn main() {}
//! ```
//!
//! 1. Choose a transport (TCP, UDS, or in-memory channels)
//! 2. Create [`PeerInfo`] entries for each remote node
//! 3. Construct a [`Node`] with peers, a receiver, and storage
//! 4. Spawn [`Node::run`] on a tokio task
//! 5. Use the returned [`NodeHandle`] to propose values
//! 6. Read decided values from the [`DecisionReceiver`]
//!
//! # Feature Flags
//!
//! | Feature | Description |
//! |---------|-------------|
//! | `channel-transport` | In-memory bounded/unbounded channel transport for testing |
//! | `tcp-transport` | TCP transport with length-prefixed framing and reconnection |
//! | `uds-transport` | Unix domain socket transport (same framing as TCP) |
//! | `test-support` | Enables [`Node::with_id`] for deterministic node identity in tests |

pub mod config;
pub mod error;
pub(crate) mod message;
pub mod node;
pub(crate) mod protocol;
pub mod storage;
pub mod transport;

pub use config::{NodeId, PeerInfo};
pub use error::{NodeError, ProposeError, StorageError, TransportError};
pub use node::{Decided, DecisionReceiver, Node, NodeHandle};
pub use storage::{MemoryStorage, Storage};
pub use transport::{MessageReceiver, MessageSender};

/// In-memory channel transport for testing. Requires the `channel-transport` feature.
#[cfg(feature = "channel-transport")]
pub use transport::channel::{self as channel, channel, unbounded_channel, ChannelReceiver, ChannelSender};

/// TCP transport types. Requires the `tcp-transport` feature.
#[cfg(feature = "tcp-transport")]
pub use transport::tcp::{TcpReceiver, TcpSender, TcpTransport};

/// Unix domain socket transport types. Requires the `uds-transport` feature.
#[cfg(feature = "uds-transport")]
pub use transport::uds::{UdsReceiver, UdsSender, UdsTransport};
