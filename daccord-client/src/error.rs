use thiserror::Error;

/// Errors returned by [`Client`](crate::Client) operations.
#[derive(Debug, Error)]
pub enum Error {
    /// Failure establishing the underlying gRPC transport (e.g. invalid URI,
    /// TLS configuration).
    #[error("transport error: {0}")]
    Transport(#[from] tonic::transport::Error),
    /// The server returned a non-OK gRPC status. The wrapped [`tonic::Status`]
    /// preserves the gRPC code (`Unavailable`, `Aborted`, ...) for callers
    /// that want to inspect it.
    #[error("rpc error: {0}")]
    Rpc(#[from] tonic::Status),
    /// One of the endpoint strings passed to [`Client::connect`](crate::Client::connect)
    /// is not a valid gRPC URI.
    #[error("invalid endpoint: {0}")]
    InvalidEndpoint(String),
    /// [`Client::connect`](crate::Client::connect) was called with an empty endpoint list.
    #[error("no endpoints configured")]
    NoEndpoints,
    /// The server returned a syntactically valid response that the client
    /// could not interpret (e.g. an unknown `algorithm` or `role` string).
    #[error("invalid response: {0}")]
    Invalid(String),
}
