use {
    crate::relayer_stage::RelayerStageError,
    crossbeam_channel::{unbounded, Receiver},
    futures_util::stream,
    jito_protos::proto::validator_interface::{
        validator_interface_client::ValidatorInterfaceClient, GetTpuConfigsRequest,
        PacketStreamMsg, SubscribeBundlesRequest, SubscribeBundlesResponse,
    },
    solana_sdk::{pubkey::Pubkey, signature::Signature},
    std::{
        fs::File,
        io::{self, Read},
        net::{AddrParseError, IpAddr, Ipv4Addr, SocketAddr},
    },
    thiserror::Error,
    tokio::runtime::{Builder, Runtime},
    tonic::{
        codegen::{http::uri::InvalidUri, InterceptedService},
        metadata::MetadataValue,
        service::Interceptor,
        transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Error},
        Code, Status,
    },
};

type ValidatorInterfaceClientType =
    ValidatorInterfaceClient<InterceptedService<Channel, AuthInterceptor>>;

type SubscribePacketsReceiver = Receiver<Result<Option<PacketStreamMsg>, Status>>;

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

    #[error("missing tls cert: {0}")]
    MissingTlsCert(#[from] io::Error),
}

pub type ProxyResult<T> = Result<T, ProxyError>;

/// Blocking interface to the validator interface server
impl BlockingProxyClient {
    pub fn new(address: String, auth_interceptor: AuthInterceptor) -> ProxyResult<Self> {
        let rt = Builder::new_multi_thread().enable_all().build().unwrap();
        let mut validator_interface_endpoint = Endpoint::from_shared(address.clone())?;

        if address.as_str().contains("https") {
            let mut buf = Vec::new();
            File::open("/etc/ssl/certs/jito_ca.pem")?.read_to_end(&mut buf)?;
            validator_interface_endpoint = validator_interface_endpoint.tls_config(
                ClientTlsConfig::new()
                    .domain_name("jito.wtf")
                    .ca_certificate(Certificate::from_pem(buf)),
            )?;
        }

        let channel = rt.block_on(validator_interface_endpoint.connect())?;
        let client = ValidatorInterfaceClient::with_interceptor(channel, auth_interceptor);

        info!("connected to relayer at {}", address);

        Ok(Self { rt, client })
    }

    pub fn maybe_retryable_auth(e: &RelayerStageError, current_token: &[u8]) -> Option<String> {
        if let RelayerStageError::GrpcError(status) = e {
            if status.code() != Code::Unauthenticated {
                return None;
            }

            let msg = status.message().split_whitespace().collect::<Vec<&str>>();
            if msg.len() != 2 {
                return None;
            }

            if msg[0] != "token" {
                return None;
            }

            if msg[1].as_bytes() != current_token {
                Some(msg[1].to_string())
            } else {
                None
            }
        } else {
            None
        }
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

    pub fn start_bi_directional_packet_stream(&mut self) -> ProxyResult<SubscribePacketsReceiver> {
        let mut packet_subscription = self
            .rt
            .block_on(
                self.client
                    .start_bi_directional_packet_stream(stream::iter(vec![])),
            )?
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

    pub fn subscribe_bundles(
        &mut self,
    ) -> ProxyResult<Receiver<Result<Option<SubscribeBundlesResponse>, Status>>> {
        let mut bundle_subscription = self
            .rt
            .block_on(self.client.subscribe_bundles(SubscribeBundlesRequest {}))?
            .into_inner();

        let (sender, receiver) = unbounded();
        self.rt.spawn(async move {
            loop {
                let msg = bundle_subscription.message().await;
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
pub struct AuthInterceptor {
    msg: Vec<u8>,
    sig: Signature,
    pubkey: Pubkey,
}

impl AuthInterceptor {
    pub fn new(msg: Vec<u8>, sig: Signature, pubkey: Pubkey) -> Self {
        AuthInterceptor { msg, sig, pubkey }
    }

    pub fn set_msg(&mut self, new_msg: Vec<u8>) {
        self.msg = new_msg;
    }

    pub fn get_msg(&self) -> &Vec<u8> {
        &self.msg
    }
}

impl Interceptor for AuthInterceptor {
    fn call(&mut self, mut request: tonic::Request<()>) -> Result<tonic::Request<()>, Status> {
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
