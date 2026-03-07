//! This module contains logic for connecting to an external Relayer and Block Engine.
//! The Relayer acts as an external TPU and TPU Forward socket while the Block Engine
//! is tasked with streaming high value bundles to the validator. The validator can run
//! in one of 3 modes:
//!     1. Connected to Relayer and Block Engine.
//!         - This is the ideal mode as it increases the probability of building the most profitable blocks.
//!     2. Connected only to Relayer.
//!         - A validator may choose to run in this mode if the main concern is to offload ingress traffic deduplication and sig-verification.
//!     3. Connected only to Block Engine.
//!         - Running in this mode means pending transactions are not exposed to external actors. This mode is ideal if the validator wishes
//!           to accept bundles while maintaining some level of privacy for in-flight transactions.

mod auth;
pub mod block_engine_stage;
pub mod fetch_stage_manager;
pub mod relayer_stage;

use {
    std::{
        net::{AddrParseError, SocketAddr},
        result,
        time::Duration,
    },
    thiserror::Error,
    tonic::{
        transport::Endpoint,
        Status,
    },
};

type Result<T> = result::Result<T, ProxyError>;
type HeartbeatEvent = (SocketAddr, SocketAddr);

#[derive(Error, Debug)]
pub enum ProxyError {
    #[error("GrpcError: code={code}, message={message}")]
    GrpcError { code: tonic::Code, message: String },

    #[error("stream disconnected")]
    GrpcStreamDisconnected,

    #[error("heartbeat error")]
    HeartbeatChannelError,

    #[error("heartbeat expired")]
    HeartbeatExpired,

    #[error("error forwarding packet to banking stage")]
    PacketForwardError,

    #[error("missing tpu config: {0:?}")]
    MissingTpuSocket(String),

    #[error("invalid socket address: {0:?}")]
    InvalidSocketAddress(#[from] AddrParseError),

    #[error("AuthenticationConnectionTimeout")]
    AuthenticationConnectionTimeout,

    #[error("AuthenticationTimeout")]
    AuthenticationTimeout,

    #[error("AuthenticationConnectionError: {0:?}")]
    AuthenticationConnectionError(String),

    #[error("BlockEngineConfigChanged")]
    BlockEngineConfigChanged,

    #[error("BamEnabled")]
    BamEnabled,

    #[error("BlockEngineConnectionTimeout")]
    BlockEngineConnectionTimeout,

    #[error("BlockEngineEndpointError: {0:?}")]
    BlockEngineEndpointError(String),

    #[error("BlockEngineConnectionError: {0:?}")]
    BlockEngineConnectionError(Box<tonic::transport::Error>),

    #[error("BlockEngineRequestError: code={code}, message={message}")]
    BlockEngineRequestError { code: tonic::Code, message: String },

    #[error("RelayerConnectionTimeout")]
    RelayerConnectionTimeout,

    #[error("RelayerTimeout")]
    RelayerEngineTimeout,

    #[error("RelayerConnectionError: {0:?}")]
    RelayerConnectionError(String),

    #[error("AuthenticationError: code={code}, message={message}")]
    AuthenticationError { code: tonic::Code, message: String },

    #[error("AuthenticationPermissionDenied")]
    AuthenticationPermissionDenied,

    #[error("BadAuthenticationToken: {0:?}")]
    BadAuthenticationToken(String),

    #[error("MethodTimeout: {0:?}")]
    MethodTimeout(String),

    #[error("MethodError: code={code}, message={message}")]
    MethodError { code: tonic::Code, message: String },
}

const SANITIZED_MESSAGE_LIMIT: usize = 128;

fn sanitize_status_message_for_influx(input: &str) -> String {
    input
        .chars()
        .filter(|c| matches!(c, 'a'..='z' | 'A'..='Z' | '0'..='9' | '_' | '-' | ' '))
        .take(SANITIZED_MESSAGE_LIMIT)
        .collect()
}

fn endpoint_from_url(
    url: &str,
    invalid_error: impl FnOnce() -> ProxyError,
    tls_error: impl FnOnce() -> ProxyError,
) -> Result<Endpoint> {
    let mut endpoint = Endpoint::from_shared(url.to_owned())
        .map_err(|_| invalid_error())?
        .tcp_keepalive(Some(Duration::from_secs(60)));
    if url.starts_with("https") {
        endpoint = endpoint
            .tls_config(tonic::transport::ClientTlsConfig::new())
            .map_err(|_| tls_error())?;
    }
    Ok(endpoint)
}

impl From<Status> for ProxyError {
    fn from(status: Status) -> Self {
        ProxyError::GrpcError {
            code: status.code(),
            message: sanitize_status_message_for_influx(status.message()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sanitize_status_message_for_influx() {
        let input = r#"Relayer is unhealthy!!! @@@ Please "try" \again\ when {it's} [healthy]."#;
        let expected = "Relayer is unhealthy  Please try again when its healthy";
        assert_eq!(sanitize_status_message_for_influx(input), expected);
    }
}
