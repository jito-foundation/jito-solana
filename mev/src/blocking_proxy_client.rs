use {
    crate::proto::validator_interface::{
        validator_interface_client::ValidatorInterfaceClient, GetTpuConfigsRequest,
        SubscribePacketsRequest, SubscribePacketsResponse,
    },
    crossbeam_channel::{unbounded, Receiver},
    solana_sdk::{pubkey::Pubkey, signature::Signature},
    std::net::{AddrParseError, IpAddr, Ipv4Addr, SocketAddr},
    thiserror::Error,
    tokio::runtime::{Builder, Runtime},
    tonic::{
        codegen::{http::uri::InvalidUri, InterceptedService},
        metadata::MetadataValue,
        service::Interceptor,
        transport::{Channel, Endpoint, Error},
        Status,
    },
};

type ValidatorInterfaceClientType =
    ValidatorInterfaceClient<InterceptedService<Channel, AuthenticationInjector>>;

type SubscribePacketsReceiver =
    Receiver<std::result::Result<Option<SubscribePacketsResponse>, Status>>;

pub struct BlockingProxyClient {
    rt: Runtime,
    client: ValidatorInterfaceClientType,
}

#[derive(Error, Debug)]
pub enum ProxyError {
    #[error("bad uri error: {0}")]
    BadUrl(#[from] InvalidUri),
    #[error("connecting error: {0}")]
    ConnectionError(#[from] Error),
    #[error("grpc error: {0}")]
    GrpcError(#[from] Status),
    #[error("missing tpu socket: {0}")]
    MissingTpuSocket(String),
    #[error("invalid tpu socket: {0}")]
    BadTpuSocket(#[from] AddrParseError),
}

pub type ProxyResult<T> = std::result::Result<T, ProxyError>;

/// Blocking interface to the validator interface server
impl BlockingProxyClient {
    pub fn new(
        validator_interface_address: &str,
        auth_interceptor: &AuthenticationInjector,
    ) -> ProxyResult<Self> {
        let rt = Builder::new_multi_thread().enable_all().build().unwrap();
        let channel =
            rt.block_on(Endpoint::from_shared(validator_interface_address.to_string())?.connect())?;
        let client = ValidatorInterfaceClient::with_interceptor(channel, auth_interceptor.clone());
        Ok(Self { rt, client })
    }

    pub fn fetch_tpu_config(&mut self) -> ProxyResult<(SocketAddr, SocketAddr)> {
        let tpu_configs = self
            .rt
            .block_on(self.client.get_tpu_configs(GetTpuConfigsRequest {}))?
            .into_inner();

        let tpu_addr = tpu_configs
            .tpu
            .ok_or_else(|| ProxyError::MissingTpuSocket("tpu".into()))?;
        let tpu_forward_addr = tpu_configs
            .tpu_forward
            .ok_or_else(|| ProxyError::MissingTpuSocket("tpu_fwd".into()))?;

        let tpu_ip = IpAddr::from(tpu_addr.ip.parse::<Ipv4Addr>()?);
        let tpu_forward_ip = IpAddr::from(tpu_forward_addr.ip.parse::<Ipv4Addr>()?);

        let tpu_socket = SocketAddr::new(tpu_ip, tpu_addr.port as u16);
        let tpu_forward_socket = SocketAddr::new(tpu_forward_ip, tpu_forward_addr.port as u16);

        Ok((tpu_socket, tpu_forward_socket))
    }

    pub fn subscribe_packets(&mut self) -> ProxyResult<SubscribePacketsReceiver> {
        let mut packet_subscription = self
            .rt
            .block_on(self.client.subscribe_packets(SubscribePacketsRequest {}))?
            .into_inner();

        let (sender, receiver) = unbounded();
        self.rt.spawn(async move {
            loop {
                let msg = packet_subscription.message().await;
                let error = msg.is_err();
                if sender.send(msg).is_err() || error {
                    break;
                }
            }
        });

        Ok(receiver)
    }
}

#[derive(Clone)]
pub struct AuthenticationInjector {
    msg: Vec<u8>,
    sig: Signature,
    pubkey: Pubkey,
}

impl AuthenticationInjector {
    pub fn new(msg: Vec<u8>, sig: Signature, pubkey: Pubkey) -> Self {
        AuthenticationInjector { msg, sig, pubkey }
    }
}

impl Interceptor for AuthenticationInjector {
    fn call(
        &mut self,
        mut request: tonic::Request<()>,
    ) -> std::result::Result<tonic::Request<()>, Status> {
        request.metadata_mut().append_bin(
            "public-key-bin",
            MetadataValue::from_bytes(&self.pubkey.to_bytes()),
        );
        request.metadata_mut().append_bin(
            "message-bin",
            MetadataValue::from_bytes(self.msg.as_slice()),
        );
        request.metadata_mut().append_bin(
            "signature-bin",
            MetadataValue::from_bytes(self.sig.as_ref()),
        );
        Ok(request)
    }
}
