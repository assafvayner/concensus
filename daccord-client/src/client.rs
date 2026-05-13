use std::time::Duration;

use bytes::Bytes;
use tonic::transport::{Channel, Endpoint};

use crate::error::Error;
use crate::proto::consensus_service_client::ConsensusServiceClient;
use crate::proto::{GetDecisionsRequest, ProposeRequest, StatusRequest, WatchRequest};
use crate::types::{ClusterStatus, Decision};

const PROPOSE_RETRY_BACKOFF: Duration = Duration::from_millis(100);

#[derive(Clone, Debug)]
pub struct Client {
    endpoints: Vec<Endpoint>,
    inner: ConsensusServiceClient<Channel>,
}

impl Client {
    /// Construct a new client connected to one of the provided endpoints.
    ///
    /// All endpoints are parsed; the resulting tonic [`Channel`] is built as
    /// a load-balanced channel over the parsed endpoints (lazy connect).
    pub async fn connect(endpoints: Vec<String>) -> Result<Self, Error> {
        if endpoints.is_empty() {
            return Err(Error::NoEndpoints);
        }

        let mut parsed = Vec::with_capacity(endpoints.len());
        for raw in endpoints {
            let ep = Endpoint::from_shared(raw.clone())
                .map_err(|e| Error::InvalidEndpoint(format!("{raw}: {e}")))?;
            parsed.push(ep);
        }

        let channel = Channel::balance_list(parsed.clone().into_iter());
        let inner = ConsensusServiceClient::new(channel);

        Ok(Self {
            endpoints: parsed,
            inner,
        })
    }

    /// Connect a fresh single-endpoint client to the given endpoint.
    /// Used internally for the propose retry path so we can target a
    /// specific replica on `Unavailable`.
    async fn connect_single(ep: &Endpoint) -> Result<ConsensusServiceClient<Channel>, Error> {
        let channel = ep.connect().await.map_err(Error::Transport)?;
        Ok(ConsensusServiceClient::new(channel))
    }

    /// Submit a payload for consensus.
    ///
    /// Retries once on `Unavailable` against the next configured endpoint
    /// (after a small backoff) and once on `Aborted` (proposal superseded)
    /// against the original channel. All other failures are surfaced.
    pub async fn propose(&self, payload: Bytes) -> Result<Decision, Error> {
        let req = || ProposeRequest {
            payload: payload.clone(),
        };

        let mut client = self.inner.clone();
        let first = client.propose(tonic::Request::new(req())).await;

        let resp = match first {
            Ok(r) => r,
            Err(status) => match status.code() {
                tonic::Code::Unavailable => {
                    // Best-effort: if only one endpoint is configured, surfaces
                    // the original `Unavailable` error rather than retrying
                    // against the same endpoint.
                    if self.endpoints.len() < 2 {
                        return Err(Error::Rpc(status));
                    }
                    tokio::time::sleep(PROPOSE_RETRY_BACKOFF).await;
                    let ep = &self.endpoints[1];
                    let mut alt = Self::connect_single(ep).await?;
                    alt.propose(tonic::Request::new(req())).await?
                }
                tonic::Code::Aborted => {
                    tokio::time::sleep(PROPOSE_RETRY_BACKOFF).await;
                    client.propose(tonic::Request::new(req())).await?
                }
                _ => return Err(Error::Rpc(status)),
            },
        };

        let inner = resp.into_inner();
        Ok(Decision {
            slot: inner.slot,
            payload: inner.payload,
        })
    }

    /// Pull a page of decisions starting at `start_index`.
    pub async fn get_decisions(
        &self,
        start_index: u64,
        limit: Option<u32>,
    ) -> Result<(Vec<Decision>, u64), Error> {
        let mut client = self.inner.clone();
        let resp = client
            .get_decisions(tonic::Request::new(GetDecisionsRequest {
                start_index,
                limit,
            }))
            .await?;
        let inner = resp.into_inner();
        let decisions = inner
            .decisions
            .into_iter()
            .map(|d| Decision {
                slot: d.slot,
                payload: d.payload,
            })
            .collect();
        Ok((decisions, inner.next_index))
    }

    /// Fetch the current status snapshot from the connected node.
    pub async fn status(&self) -> Result<ClusterStatus, Error> {
        let mut client = self.inner.clone();
        let resp = client.status(tonic::Request::new(StatusRequest {})).await?;
        let inner = resp.into_inner();

        let leader_id = if inner.leader_id.is_empty() {
            None
        } else {
            Some(inner.leader_id)
        };

        Ok(ClusterStatus {
            node_id: inner.node_id,
            algorithm: inner.algorithm.parse()?,
            role: inner.role.parse()?,
            term: inner.term,
            leader_id,
            log_len: inner.log_len,
            commit_index: inner.commit_index,
            last_applied: inner.last_applied,
        })
    }

    /// Open a fresh server-streaming Watch starting at `start_index`.
    /// The returned stream is the raw tonic stream; the auto-reconnect
    /// wrapper lives in [`crate::watch`].
    pub(crate) async fn open_watch(
        &self,
        start_index: u64,
    ) -> Result<tonic::Streaming<crate::proto::Decision>, Error> {
        let mut client = self.inner.clone();
        let resp = client
            .watch(tonic::Request::new(WatchRequest { start_index }))
            .await?;
        Ok(resp.into_inner())
    }
}
