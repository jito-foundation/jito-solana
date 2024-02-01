use {
    crate::proxy::ProxyError,
    chrono::Utc,
    jito_protos::proto::auth::{
        auth_service_client::AuthServiceClient, GenerateAuthChallengeRequest,
        GenerateAuthTokensRequest, RefreshAccessTokenRequest, Role, Token,
    },
    solana_gossip::cluster_info::ClusterInfo,
    solana_sdk::signature::{Keypair, Signer},
    std::{
        sync::{Arc, Mutex},
        time::Duration,
    },
    tokio::time::timeout,
    tonic::{service::Interceptor, transport::Channel, Code, Request, Status},
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

/// Generates an auth challenge then generates and returns validated auth tokens.
pub async fn generate_auth_tokens(
    auth_service_client: &mut AuthServiceClient<Channel>,
    // used to sign challenges
    keypair: &Keypair,
) -> crate::proxy::Result<(
    Token, /* access_token */
    Token, /* refresh_token */
)> {
    debug!("generate_auth_challenge");
    let challenge_response = auth_service_client
        .generate_auth_challenge(GenerateAuthChallengeRequest {
            role: Role::Validator as i32,
            pubkey: keypair.pubkey().as_ref().to_vec(),
        })
        .await
        .map_err(|e: Status| {
            if e.code() == Code::PermissionDenied {
                ProxyError::AuthenticationPermissionDenied
            } else {
                ProxyError::AuthenticationError(e.to_string())
            }
        })?;

    let formatted_challenge = format!(
        "{}-{}",
        keypair.pubkey(),
        challenge_response.into_inner().challenge
    );

    let signed_challenge = keypair
        .sign_message(formatted_challenge.as_bytes())
        .as_ref()
        .to_vec();

    debug!(
        "formatted_challenge: {} signed_challenge: {:?}",
        formatted_challenge, signed_challenge
    );

    debug!("generate_auth_tokens");
    let auth_tokens = auth_service_client
        .generate_auth_tokens(GenerateAuthTokensRequest {
            challenge: formatted_challenge,
            client_pubkey: keypair.pubkey().as_ref().to_vec(),
            signed_challenge,
        })
        .await
        .map_err(|e| ProxyError::AuthenticationError(e.to_string()))?;

    let inner = auth_tokens.into_inner();
    let access_token = get_validated_token(inner.access_token)?;
    let refresh_token = get_validated_token(inner.refresh_token)?;

    Ok((access_token, refresh_token))
}

/// Tries to refresh the access token or run full-reauth if needed.
pub async fn maybe_refresh_auth_tokens(
    auth_service_client: &mut AuthServiceClient<Channel>,
    access_token: &Arc<Mutex<Token>>,
    refresh_token: &Token,
    cluster_info: &Arc<ClusterInfo>,
    connection_timeout: &Duration,
    refresh_within_s: u64,
) -> crate::proxy::Result<(
    Option<Token>, // access token
    Option<Token>, // refresh token
)> {
    let access_token_expiry: u64 = access_token
        .lock()
        .unwrap()
        .expires_at_utc
        .as_ref()
        .map(|ts| ts.seconds as u64)
        .unwrap_or_default();
    let refresh_token_expiry: u64 = refresh_token
        .expires_at_utc
        .as_ref()
        .map(|ts| ts.seconds as u64)
        .unwrap_or_default();

    let now = Utc::now().timestamp() as u64;

    let should_refresh_access =
        access_token_expiry.checked_sub(now).unwrap_or_default() <= refresh_within_s;
    let should_generate_new_tokens =
        refresh_token_expiry.checked_sub(now).unwrap_or_default() <= refresh_within_s;

    if should_generate_new_tokens {
        let kp = cluster_info.keypair().clone();

        let (new_access_token, new_refresh_token) = timeout(
            *connection_timeout,
            generate_auth_tokens(auth_service_client, kp.as_ref()),
        )
        .await
        .map_err(|_| ProxyError::MethodTimeout("generate_auth_tokens".to_string()))?
        .map_err(|e| ProxyError::MethodError(e.to_string()))?;

        return Ok((Some(new_access_token), Some(new_refresh_token)));
    } else if should_refresh_access {
        let new_access_token = timeout(
            *connection_timeout,
            refresh_access_token(auth_service_client, refresh_token),
        )
        .await
        .map_err(|_| ProxyError::MethodTimeout("refresh_access_token".to_string()))?
        .map_err(|e| ProxyError::MethodError(e.to_string()))?;

        return Ok((Some(new_access_token), None));
    }

    Ok((None, None))
}

pub async fn refresh_access_token(
    auth_service_client: &mut AuthServiceClient<Channel>,
    refresh_token: &Token,
) -> crate::proxy::Result<Token> {
    let response = auth_service_client
        .refresh_access_token(RefreshAccessTokenRequest {
            refresh_token: refresh_token.value.clone(),
        })
        .await
        .map_err(|e| ProxyError::AuthenticationError(e.to_string()))?;
    get_validated_token(response.into_inner().access_token)
}

/// An invalid token is one where any of its fields are None or the token itself is None.
/// Performs the necessary validations on the auth tokens before returning,
/// i.e. it is safe to call .unwrap() on the token fields from the call-site.
fn get_validated_token(maybe_token: Option<Token>) -> crate::proxy::Result<Token> {
    let token = maybe_token
        .ok_or_else(|| ProxyError::BadAuthenticationToken("received a null token".to_string()))?;
    if token.expires_at_utc.is_none() {
        Err(ProxyError::BadAuthenticationToken(
            "expires_at_utc field is null".to_string(),
        ))
    } else {
        Ok(token)
    }
}
