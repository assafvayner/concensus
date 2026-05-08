use std::time::Duration;

use futures::stream::Stream;
use tokio_stream::StreamExt;

use crate::client::Client;
use crate::error::Error;
use crate::types::Decision;

const RECONNECT_BACKOFF: Duration = Duration::from_millis(200);

/// Build an auto-reconnecting Watch stream.
///
/// Behavior:
/// - Tracks the last yielded `slot`.
/// - On any error from the underlying tonic stream, or on
///   `Code::ResourceExhausted` (server-side "watch lagged"), waits a small
///   backoff and reopens the watch with `start_index = last_yielded + 1`
///   (or the originally requested `start_index` if nothing has been yielded
///   yet).
/// - Non-transient errors that are not `Unavailable` / `ResourceExhausted`
///   (e.g. `InvalidArgument`, `PermissionDenied`) are surfaced to the
///   caller and terminate the stream.
pub(crate) fn watch_stream(
    client: Client,
    start_index: u64,
) -> impl Stream<Item = Result<Decision, Error>> {
    async_stream::try_stream! {
        let mut last_yielded: Option<u64> = None;

        loop {
            let next_index = match last_yielded {
                Some(slot) => slot.saturating_add(1),
                None => start_index,
            };

            let stream = match client.open_watch(next_index).await {
                Ok(s) => s,
                Err(Error::Rpc(status)) => {
                    if is_transient(status.code()) {
                        tokio::time::sleep(RECONNECT_BACKOFF).await;
                        continue;
                    } else {
                        Err(Error::Rpc(status))?;
                        unreachable!();
                    }
                }
                Err(e) => {
                    Err(e)?;
                    unreachable!();
                }
            };

            tokio::pin!(stream);

            loop {
                match stream.next().await {
                    Some(Ok(msg)) => {
                        let decision = Decision {
                            slot: msg.slot,
                            payload: msg.payload,
                        };
                        last_yielded = Some(decision.slot);
                        yield decision;
                    }
                    Some(Err(status)) => {
                        if is_transient(status.code()) {
                            tracing::debug!(
                                code = ?status.code(),
                                "watch stream transient error, reconnecting"
                            );
                            tokio::time::sleep(RECONNECT_BACKOFF).await;
                            break;
                        } else {
                            Err(Error::Rpc(status))?;
                            unreachable!();
                        }
                    }
                    None => {
                        tracing::debug!("watch stream closed by server, reconnecting");
                        tokio::time::sleep(RECONNECT_BACKOFF).await;
                        break;
                    }
                }
            }
        }
    }
}

fn is_transient(code: tonic::Code) -> bool {
    matches!(
        code,
        tonic::Code::Unavailable | tonic::Code::ResourceExhausted | tonic::Code::DeadlineExceeded
    )
}
