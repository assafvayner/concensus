use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("transport error: {0}")]
    Transport(#[from] tonic::transport::Error),
    #[error("rpc error: {0}")]
    Rpc(#[from] tonic::Status),
    #[error("invalid endpoint: {0}")]
    InvalidEndpoint(String),
    #[error("no endpoints configured")]
    NoEndpoints,
    #[error("invalid response: {0}")]
    Invalid(String),
}
