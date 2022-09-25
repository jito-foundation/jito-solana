use {
    chrono::Utc,
    jito_protos::proto::auth::{
        auth_service_client::AuthServiceClient, GenerateAuthChallengeRequest,
        GenerateAuthTokensRequest, RefreshAccessTokenRequest, Role, Token,
    },
    solana_gossip::cluster_info::ClusterInfo,
    solana_sdk::signature::{Keypair, Signer},
    std::{
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc, Mutex,
        },
        time::Duration,
    },
    tokio::time::sleep,
    tonic::{service::Interceptor, transport::Channel, Request, Status},
};

/// Interceptor responsible for adding the access token to request headers.
pub(crate) struct AuthInterceptor {
    /// The token added to each request header.
    access_token: Arc<Mutex<Token>>,
}

impl AuthInterceptor {
    pub(crate) fn new(access_token: Arc<Mutex<Token>>) -> Self {
        Self { access_token }
    }
}

impl Interceptor for AuthInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        request.metadata_mut().insert(
            "authorization",
            format!("Bearer {}", self.access_token.lock().unwrap().value)
                .parse()
                .unwrap(),
        );

        Ok(request)
    }
}

/// Contains collection of utility functions responsible for generating and refreshing new tokens.
pub(crate) mod token_manager {
    use {super::*, crate::proxy::ProxyError, tonic::transport::Endpoint};

    /// Control loop responsible for making sure access and refresh tokens are updated.
    pub(crate) async fn auth_tokens_update_loop(
        auth_service_endpoint: Endpoint,
        access_token: Arc<Mutex<Token>>,
        cluster_info: Arc<ClusterInfo>,
        exit: Arc<AtomicBool>,
    ) {
        const RETRY_INTERVAL: Duration = Duration::from_secs(5);
        const SLEEP_INTERVAL: Duration = Duration::from_secs(60);

        let mut num_refresh_loop_errors: u64 = 0;
        let mut num_connect_errors: u64 = 0;
        while !exit.load(Ordering::Relaxed) {
            sleep(RETRY_INTERVAL).await;

            match auth_service_endpoint.connect().await {
                Ok(channel) => {
                    if let Err(e) = auth_tokens_update_loop_helper(
                        AuthServiceClient::new(channel),
                        auth_service_endpoint.uri().to_string(),
                        (access_token.clone(), Token::default()),
                        cluster_info.clone(),
                        SLEEP_INTERVAL,
                        exit.clone(),
                    )
                    .await
                    {
                        num_refresh_loop_errors += 1;
                        datapoint_error!(
                            "auth_tokens_update_loop-refresh_loop_error",
                            ("url", auth_service_endpoint.uri().to_string(), String),
                            ("count", num_refresh_loop_errors, i64),
                            ("error", e.to_string(), String)
                        );
                    }
                }
                Err(e) => {
                    num_connect_errors += 1;
                    datapoint_error!(
                        "auth_tokens_update_loop-refresh_connect_error",
                        ("url", auth_service_endpoint.uri().to_string(), String),
                        ("count", num_connect_errors, i64),
                        ("error", e.to_string(), String)
                    );
                }
            }
        }
    }

    /// Responsible for keeping generating and refreshing the access token.
    async fn auth_tokens_update_loop_helper(
        mut auth_service_client: AuthServiceClient<Channel>,
        url: String,
        (access_token, mut refresh_token): (Arc<Mutex<Token>>, Token),
        cluster_info: Arc<ClusterInfo>,
        sleep_interval: Duration,
        exit: Arc<AtomicBool>,
    ) -> crate::proxy::Result<()> {
        const REFRESH_WITHIN_SECS: i64 = 300;
        let mut num_full_refreshes = 0;
        let mut num_refresh_access_token = 0;

        while !exit.load(Ordering::Relaxed) {
            let access_token_expiry: i64 = access_token
                .lock()
                .unwrap()
                .expires_at_utc
                .as_ref()
                .map(|ts| ts.seconds)
                .unwrap_or_default();
            let refresh_token_expiry = refresh_token
                .expires_at_utc
                .as_ref()
                .map(|ts| ts.seconds)
                .unwrap_or_default();

            let now = Utc::now().timestamp();

            let should_refresh_access = access_token_expiry.checked_sub(now).ok_or_else(|| {
                ProxyError::InvalidData("Received invalid access_token expiration".to_string())
            })? <= REFRESH_WITHIN_SECS;
            let should_generate_new_tokens =
                refresh_token_expiry.checked_sub(now).ok_or_else(|| {
                    ProxyError::InvalidData("Received invalid refresh_token expiration".to_string())
                })? <= REFRESH_WITHIN_SECS;

            match (should_refresh_access, should_generate_new_tokens) {
                // Generate new tokens if the refresh_token is close to being expired.
                (_, true) => {
                    let kp = cluster_info.keypair().clone();

                    let (new_access_token, new_refresh_token) =
                        generate_auth_tokens(&mut auth_service_client, kp.as_ref()).await?;

                    *access_token.lock().unwrap() = new_access_token.clone();
                    refresh_token = new_refresh_token;

                    num_full_refreshes += 1;
                    datapoint_info!(
                        "auth_tokens_update_loop-tokens_generated",
                        ("url", url, String),
                        ("count", num_full_refreshes, i64),
                    );
                }
                // Invoke the refresh_access_token method if the access_token is close to being expired.
                (true, _) => {
                    let new_access_token =
                        refresh_access_token(&mut auth_service_client, refresh_token.clone())
                            .await?;
                    *access_token.lock().unwrap() = new_access_token;

                    num_refresh_access_token += 1;
                    datapoint_info!(
                        "auth_tokens_update_loop-refresh_access_token",
                        ("url", url, String),
                        ("count", num_refresh_access_token, i64),
                    );
                }
                // Sleep and do nothing if neither token is close to expired,
                (false, false) => sleep(sleep_interval).await,
            }
        }

        Ok(())
    }

    /// Invokes the refresh_access_token gRPC method.
    /// Returns a new access_token.
    async fn refresh_access_token(
        auth_service_client: &mut AuthServiceClient<Channel>,
        refresh_token: Token,
    ) -> crate::proxy::Result<Token> {
        match auth_service_client
            .refresh_access_token(RefreshAccessTokenRequest {
                refresh_token: refresh_token.value,
            })
            .await
        {
            Ok(resp) => get_validated_token(resp.into_inner().access_token),
            Err(e) => Err(ProxyError::GrpcError(e)),
        }
    }

    /// Generates an auth challenge then generates and returns validated auth tokens.
    async fn generate_auth_tokens(
        auth_service_client: &mut AuthServiceClient<Channel>,
        // used to sign challenges
        keypair: &Keypair,
    ) -> crate::proxy::Result<(
        Token, /* access_token */
        Token, /* refresh_token */
    )> {
        let challenge_response = auth_service_client
            .generate_auth_challenge(GenerateAuthChallengeRequest {
                role: Role::Validator as i32,
                pubkey: keypair.pubkey().as_ref().to_vec(),
            })
            .await?;

        let formatted_challenge = format!(
            "{}-{}",
            keypair.pubkey(),
            challenge_response.into_inner().challenge
        );
        let signed_challenge = keypair
            .sign_message(formatted_challenge.as_bytes())
            .as_ref()
            .to_vec();

        let auth_tokens = auth_service_client
            .generate_auth_tokens(GenerateAuthTokensRequest {
                challenge: formatted_challenge,
                client_pubkey: keypair.pubkey().as_ref().to_vec(),
                signed_challenge,
            })
            .await?;

        let inner = auth_tokens.into_inner();
        let access_token = get_validated_token(inner.access_token)?;
        let refresh_token = get_validated_token(inner.refresh_token)?;

        Ok((access_token, refresh_token))
    }

    /// An invalid token is one where any of its fields are None or the token itself is None.
    /// Performs the necessary validations on the auth tokens before returning,
    /// i.e. it is safe to call .unwrap() on the token fields from the call-site.
    fn get_validated_token(maybe_token: Option<Token>) -> crate::proxy::Result<Token> {
        let token = maybe_token
            .ok_or_else(|| ProxyError::InvalidData("received a null token".to_string()))?;
        if token.expires_at_utc.is_none() {
            Err(ProxyError::InvalidData(
                "expires_at_utc field is null".to_string(),
            ))
        } else {
            Ok(token)
        }
    }
}
