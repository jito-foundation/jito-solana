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
    },
    thiserror::Error,
    tonic::Status,
};

type Result<T> = result::Result<T, ProxyError>;
type HeartbeatEvent = (SocketAddr, SocketAddr);

#[derive(Error, Debug)]
pub enum ProxyError {
    #[error("grpc error: {0}")]
    GrpcError(#[from] Status),

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

    #[error("invalid gRPC data: {0:?}")]
    InvalidData(String),

    #[error("timeout: {0:?}")]
    ConnectionError(#[from] tonic::transport::Error),

    #[error("AuthenticationConnectionTimeout")]
    AuthenticationConnectionTimeout,

    #[error("AuthenticationTimeout")]
    AuthenticationTimeout,

    #[error("AuthenticationConnectionError: {0:?}")]
    AuthenticationConnectionError(String),

    #[error("BlockEngineConnectionTimeout")]
    BlockEngineConnectionTimeout,

    #[error("BlockEngineTimeout")]
    BlockEngineTimeout,

    #[error("BlockEngineConnectionError: {0:?}")]
    BlockEngineConnectionError(String),

    #[error("RelayerConnectionTimeout")]
    RelayerConnectionTimeout,

    #[error("RelayerTimeout")]
    RelayerEngineTimeout,

    #[error("RelayerConnectionError: {0:?}")]
    RelayerConnectionError(String),

    #[error("AuthenticationError: {0:?}")]
    AuthenticationError(String),

    #[error("AuthenticationPermissionDenied")]
    AuthenticationPermissionDenied,

    #[error("BadAuthenticationToken: {0:?}")]
    BadAuthenticationToken(String),

    #[error("MethodTimeout: {0:?}")]
    MethodTimeout(String),

    #[error("MethodError: {0:?}")]
    MethodError(String),
}
