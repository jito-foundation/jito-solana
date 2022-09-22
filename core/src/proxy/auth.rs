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
        retry_interval: Duration,
        exit: Arc<AtomicBool>,
    ) {
        while !exit.load(Ordering::Relaxed) {
            match auth_service_endpoint.connect().await {
                Ok(channel) => {
                    if let Err(e) = auth_tokens_update_loop_helper(
                        AuthServiceClient::new(channel),
                        (access_token.clone(), Token::default()),
                        cluster_info.clone(),
                        Duration::from_secs(10),
                        exit.clone(),
                    )
                    .await
                    {
                        error!("auth_refresh_loop error: {:?}", e);
                        sleep(retry_interval).await;
                    }
                }
                Err(e) => {
                    error!(
                        "error connecting to auth service url: {} error: {}",
                        auth_service_endpoint.uri(),
                        e
                    );
                    sleep(retry_interval).await;
                }
            }
        }
    }

    /// Responsible for keeping generating and refreshing the access token.
    async fn auth_tokens_update_loop_helper(
        mut auth_service_client: AuthServiceClient<Channel>,
        (access_token, mut refresh_token): (Arc<Mutex<Token>>, Token),
        cluster_info: Arc<ClusterInfo>,
        sleep_interval: Duration,
        exit: Arc<AtomicBool>,
    ) -> crate::proxy::Result<()> {
        while !exit.load(Ordering::Relaxed) {
            let access_token_expiry: i64 = {
                if let Some(ts) = access_token.lock().unwrap().expires_at_utc.as_ref() {
                    ts.seconds
                } else {
                    0
                }
            };
            let refresh_token_expiry: i64 = {
                if let Some(ts) = refresh_token.expires_at_utc.as_ref() {
                    ts.seconds
                } else {
                    0
                }
            };

            let now = Utc::now().timestamp();
            let should_refresh_access = {
                let delta = access_token_expiry.checked_sub(now);
                if delta.is_none() {
                    return Err(ProxyError::InvalidData(
                        "Received invalid access_token expiration".to_string(),
                    ));
                }
                delta.unwrap() <= 300
            };
            let should_generate_new_tokens = {
                let delta = refresh_token_expiry.checked_sub(now);
                if delta.is_none() {
                    return Err(ProxyError::InvalidData(
                        "Received invalid refresh_token expiration".to_string(),
                    ));
                }
                delta.unwrap() <= 300
            };

            match (should_refresh_access, should_generate_new_tokens) {
                // Generate new tokens if the refresh_token is close to being expired.
                (_, true) => {
                    let kp = cluster_info.keypair().clone();
                    match generate_auth_tokens(&mut auth_service_client, kp.as_ref()).await {
                        Ok((new_access_token, new_refresh_token)) => {
                            *access_token.lock().unwrap() = new_access_token.clone();
                            refresh_token = new_refresh_token;
                        }
                        Err(e) => {
                            return Err(e);
                        }
                    }
                }
                // Invoke the refresh_access_token method if the access_token is close to being expired.
                (true, _) => {
                    match refresh_access_token(&mut auth_service_client, refresh_token.clone())
                        .await
                    {
                        Ok(new_access_token) => {
                            *access_token.lock().unwrap() = new_access_token;
                        }
                        Err(e) => return Err(e),
                    }
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
            Ok(resp) => validate_token(resp.into_inner().access_token).map_err(|e| {
                error!("invalid access_token");
                e
            }),
            Err(e) => {
                debug!("error refreshing access token: {}", e);
                Err(ProxyError::GrpcError(e))
            }
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
        let challenge = match auth_service_client
            .generate_auth_challenge(GenerateAuthChallengeRequest {
                role: Role::Validator as i32,
                pubkey: keypair.pubkey().as_ref().to_vec(),
            })
            .await
        {
            Ok(resp) => Ok(format!(
                "{}-{}",
                keypair.pubkey(),
                resp.into_inner().challenge
            )),
            Err(e) => {
                debug!("error generating auth challenge: {}", e);
                Err(ProxyError::GrpcError(e))
            }
        }?;

        let signed_challenge = keypair.sign_message(challenge.as_bytes()).as_ref().to_vec();
        match auth_service_client
            .generate_auth_tokens(GenerateAuthTokensRequest {
                challenge,
                client_pubkey: keypair.pubkey().as_ref().to_vec(),
                signed_challenge,
            })
            .await
        {
            Ok(resp) => {
                let inner = resp.into_inner();

                let access_token = validate_token(inner.access_token).map_err(|e| {
                    error!("invalid access_token");
                    e
                })?;
                let refresh_token = validate_token(inner.refresh_token).map_err(|e| {
                    error!("invalid access_token");
                    e
                })?;

                Ok((access_token, refresh_token))
            }
            Err(e) => {
                debug!("error generating auth tokens: {}", e);
                Err(ProxyError::GrpcError(e))
            }
        }
    }

    /// An invalid token is one where any of its fields are None or the token itself is None.
    /// Performs the necessary validations on the auth tokens before returning,
    /// i.e. it is safe to call .unwrap() on the token fields from the call-site.
    fn validate_token(maybe_token: Option<Token>) -> crate::proxy::Result<Token> {
        match maybe_token {
            Some(token) => {
                if token.expires_at_utc.is_none() {
                    Err(ProxyError::InvalidData(
                        "expires_at_utc field is null".to_string(),
                    ))
                } else {
                    Ok(token)
                }
            }
            None => Err(ProxyError::InvalidData("received a null token".to_string())),
        }
    }
}
