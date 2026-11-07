use {
    std::time::Duration,
    tonic::transport::{ClientTlsConfig, Endpoint},
};

const TCP_KEEPALIVE: Duration = Duration::from_secs(60);

pub(crate) fn endpoint_from_url(url: &str) -> Result<Endpoint, tonic::transport::Error> {
    endpoint_from_url_with_tls_config(url, ClientTlsConfig::new().with_enabled_roots())
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

    // Self-signed for `localhost`, valid until 2126. Generated once for these tests only.
    const LOCALHOST_CERT_PEM: &str = "\
-----BEGIN CERTIFICATE-----
MIIBkzCCATigAwIBAgIUPHjJfWnCorCz23kjfpSUBe0plqUwCgYIKoZIzj0EAwIw
FDESMBAGA1UEAwwJbG9jYWxob3N0MCAXDTI2MTEwNzA1MzQzNloYDzIxMjYxMDE0
MDUzNDM2WjAUMRIwEAYDVQQDDAlsb2NhbGhvc3QwWTATBgcqhkjOPQIBBggqhkjO
PQMBBwNCAATQAGvtXceGym5oFZ6v1+wmuF0AFt7dFjbM62CHmc++iReKhOtQ1WmO
XIKSqrVAPYjstqh40akSHCjwTG6iKgwwo2YwZDAdBgNVHQ4EFgQUUdjGCpc8xhHU
tCdl5aw9Bug3wEkwHwYDVR0jBBgwFoAUUdjGCpc8xhHUtCdl5aw9Bug3wEkwFAYD
VR0RBA0wC4IJbG9jYWxob3N0MAwGA1UdEwEB/wQCMAAwCgYIKoZIzj0EAwIDSQAw
RgIhAJbeLEu94kxUEu5K4br2HltVMEw2A4/BJ4UrmmVQiyBZAiEA8RFoIvYJaLEK
JNbHP9PXNBVUIvFzbiHdIefmOhz/iVM=
-----END CERTIFICATE-----
";
    const LOCALHOST_KEY_PEM: &str = "\
-----BEGIN PRIVATE KEY-----
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQg552SooAK8VEMHHjc
fhJPpSbrjmv0jJGgVtuATFWID2ehRANCAATQAGvtXceGym5oFZ6v1+wmuF0AFt7d
FjbM62CHmc++iReKhOtQ1WmOXIKSqrVAPYjstqh40akSHCjwTG6iKgww
-----END PRIVATE KEY-----
";

    fn localhost_cert() -> (String, String) {
        (
            LOCALHOST_CERT_PEM.to_string(),
            LOCALHOST_KEY_PEM.to_string(),
        )
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

    #[tokio::test]
    async fn endpoint_builders_load_default_tls_roots() {
        const CHILD_ENV: &str = "SOLANA_TLS_ROOTS_TEST_CHILD";

        // Isolate the native root-store environment from other tests.
        if std::env::var_os(CHILD_ENV).is_none() {
            let cert_file = tempfile::NamedTempFile::new().unwrap();
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "tonic_endpoint::tests::endpoint_builders_load_default_tls_roots",
                    "--nocapture",
                ])
                .env(CHILD_ENV, "1")
                .env("SSL_CERT_FILE", cert_file.path())
                .env_remove("SSL_CERT_DIR")
                .status()
                .unwrap();
            assert!(
                status.success(),
                "TLS root-store subprocess failed: {status}"
            );
            return;
        }

        let (cert_pem, key_pem) = localhost_cert();
        std::fs::write(std::env::var_os("SSL_CERT_FILE").unwrap(), &cert_pem).unwrap();
        let url = start_tls_probe_server(cert_pem, key_pem).await;

        let endpoint = endpoint_from_url(&url).unwrap();
        timeout(Duration::from_secs(5), endpoint.connect())
            .await
            .expect("timed out connecting to local TLS server")
            .expect("production endpoint failed to load TLS roots");
    }
}
