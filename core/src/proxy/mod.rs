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
    tonic::{transport::Endpoint, Status},
};

type Result<T> = result::Result<T, ProxyError>;
type HeartbeatEvent = (SocketAddr, SocketAddr);

/// A config object shared by both Relayer and Block Engine clients.
#[derive(Clone, Debug)]
pub struct BackendConfig {
    /// Address to the external auth-service responsible for generating access tokens.
    pub auth_service_endpoint: Endpoint,

    /// Primary backend endpoint.
    pub backend_endpoint: Endpoint,

    /// If set then it will be assumed the backend verified packets so signature verification will be bypassed in the validator.
    pub trust_packets: bool,
}

impl Default for BackendConfig {
    fn default() -> Self {
        Self {
            auth_service_endpoint: Endpoint::from_shared("http://127.0.0.1:5000").unwrap(),
            backend_endpoint: Endpoint::from_shared("http://127.0.0.1:3000").unwrap(),
            trust_packets: false,
        }
    }
}

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

    #[error("shutdown")]
    Shutdown,

    #[error("invalid gRPC data: {0:?}")]
    InvalidData(String),
}
