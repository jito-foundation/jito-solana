use {
    std::time::Duration,
    tonic::transport::{ClientTlsConfig, Endpoint},
};

const TCP_KEEPALIVE: Duration = Duration::from_secs(60);

pub(crate) fn endpoint_from_url(url: &str) -> Result<Endpoint, tonic::transport::Error> {
    endpoint_from_url_with_tls_config(url, ClientTlsConfig::new().with_enabled_roots())
}

pub(crate) fn endpoint_from_url_with_error<E>(
    url: &str,
    invalid_error: impl FnOnce() -> E,
    tls_error: impl FnOnce() -> E,
) -> Result<Endpoint, E> {
    endpoint_from_url_with_tls_config_and_error(
        url,
        ClientTlsConfig::new().with_enabled_roots(),
        invalid_error,
        tls_error,
    )
}

fn endpoint_from_url_with_tls_config(
    url: &str,
    tls_config: ClientTlsConfig,
) -> Result<Endpoint, tonic::transport::Error> {
    let mut endpoint = Endpoint::from_shared(url.to_owned())?.tcp_keepalive(Some(TCP_KEEPALIVE));
    if url.starts_with("https") {
        endpoint = endpoint.tls_config(tls_config)?;
    }
    Ok(endpoint)
}

fn endpoint_from_url_with_tls_config_and_error<E>(
    url: &str,
    tls_config: ClientTlsConfig,
    invalid_error: impl FnOnce() -> E,
    tls_error: impl FnOnce() -> E,
) -> Result<Endpoint, E> {
    let mut endpoint = Endpoint::from_shared(url.to_owned())
        .map_err(|_| invalid_error())?
        .tcp_keepalive(Some(TCP_KEEPALIVE));
    if url.starts_with("https") {
        endpoint = endpoint.tls_config(tls_config).map_err(|_| tls_error())?;
    }
    Ok(endpoint)
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        jito_protos::proto::auth::{
            GenerateAuthChallengeRequest, GenerateAuthChallengeResponse, GenerateAuthTokensRequest,
            GenerateAuthTokensResponse, RefreshAccessTokenRequest, RefreshAccessTokenResponse,
            auth_service_client::AuthServiceClient,
            auth_service_server::{AuthService, AuthServiceServer},
        },
        rcgen::{CertifiedKey, generate_simple_self_signed},
        tokio::time::{Duration, timeout},
        tonic::{
            Request, Response, Status,
            transport::{Certificate, Identity, Server, ServerTlsConfig},
        },
    };

    #[derive(Default)]
    struct ProbeAuthService;

    #[tonic::async_trait]
    impl AuthService for ProbeAuthService {
        async fn generate_auth_challenge(
            &self,
            _request: Request<GenerateAuthChallengeRequest>,
        ) -> Result<Response<GenerateAuthChallengeResponse>, Status> {
            Ok(Response::new(GenerateAuthChallengeResponse {
                challenge: "tls-ok".to_string(),
            }))
        }

        async fn generate_auth_tokens(
            &self,
            _request: Request<GenerateAuthTokensRequest>,
        ) -> Result<Response<GenerateAuthTokensResponse>, Status> {
            Err(Status::unimplemented("not needed for TLS probe"))
        }

        async fn refresh_access_token(
            &self,
            _request: Request<RefreshAccessTokenRequest>,
        ) -> Result<Response<RefreshAccessTokenResponse>, Status> {
            Err(Status::unimplemented("not needed for TLS probe"))
        }
    }

    fn localhost_cert() -> (String, String) {
        let CertifiedKey { cert, signing_key } =
            generate_simple_self_signed(["localhost".to_string()]).unwrap();
        (cert.pem(), signing_key.serialize_pem())
    }

    async fn start_tls_probe_server(cert_pem: String, key_pem: String) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            Server::builder()
                .tls_config(ServerTlsConfig::new().identity(Identity::from_pem(cert_pem, key_pem)))
                .unwrap()
                .add_service(AuthServiceServer::new(ProbeAuthService))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        format!("https://localhost:{}", addr.port())
    }

    #[tokio::test]
    async fn endpoint_connects_with_configured_tls_roots() {
        let (cert_pem, key_pem) = localhost_cert();
        let url = start_tls_probe_server(cert_pem.clone(), key_pem).await;
        let endpoint = endpoint_from_url_with_tls_config(
            &url,
            ClientTlsConfig::new().ca_certificate(Certificate::from_pem(cert_pem)),
        )
        .unwrap();

        let channel = timeout(Duration::from_secs(5), endpoint.connect())
            .await
            .unwrap()
            .unwrap();
        let mut client = AuthServiceClient::new(channel);
        let response = client
            .generate_auth_challenge(GenerateAuthChallengeRequest::default())
            .await
            .unwrap()
            .into_inner();

        assert_eq!(response.challenge, "tls-ok");
    }

    #[tokio::test]
    async fn endpoint_rejects_tls_without_configured_roots() {
        let (cert_pem, key_pem) = localhost_cert();
        let url = start_tls_probe_server(cert_pem, key_pem).await;
        let endpoint = endpoint_from_url_with_tls_config(&url, ClientTlsConfig::new()).unwrap();

        let result = timeout(Duration::from_secs(5), endpoint.connect())
            .await
            .unwrap();

        assert!(
            result.is_err(),
            "TLS connection unexpectedly succeeded without configured roots"
        );
    }
}
