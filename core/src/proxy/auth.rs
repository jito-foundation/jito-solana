use {
    crate::proxy::{ProxyError, sanitize_status_message_for_influx},
    arc_swap::ArcSwap,
    chrono::Utc,
    jito_protos::proto::auth::{
        GenerateAuthChallengeRequest, GenerateAuthTokensRequest, RefreshAccessTokenRequest, Role,
        Token, auth_service_client::AuthServiceClient,
    },
    solana_gossip::cluster_info::ClusterInfo,
    solana_keypair::Keypair,
    solana_pubkey::Pubkey,
    solana_signer::Signer,
    std::{sync::Arc, time::Duration},
    tokio::time::timeout,
    tonic::{
        Code, Request, Status,
        service::Interceptor,
        transport::{Channel, Endpoint},
    },
};

pub(crate) struct AuthRefreshState {
    auth_client: AuthServiceClient<Channel>,
    access_token: Arc<ArcSwap<Token>>,
    refresh_token: Token,
    identity: Pubkey,
}

impl AuthRefreshState {
    pub(crate) fn interceptor(&self) -> AuthInterceptor {
        AuthInterceptor {
            access_token: self.access_token.clone(),
        }
    }

    pub(crate) fn validate_identity(&self, identity: Pubkey) -> crate::proxy::Result<()> {
        if identity != self.identity {
            return Err(ProxyError::AuthenticationConnectionError(
                "validator identity changed".to_string(),
            ));
        }
        Ok(())
    }

    pub(crate) async fn maybe_refresh(
        &mut self,
        cluster_info: &ClusterInfo,
        connection_timeout: &Duration,
        refresh_within_s: u64,
    ) -> crate::proxy::Result<(bool, bool)> {
        let refresh_deadline = Utc::now()
            .timestamp()
            .saturating_add_unsigned(refresh_within_s);

        if expires_by(&self.refresh_token, refresh_deadline) {
            let keypair = cluster_info.keypair();
            // Close the identity-rotation race between the outer check and reauthentication.
            self.validate_identity(keypair.pubkey())?;
            let (new_access_token, new_refresh_token) = timeout(
                *connection_timeout,
                generate_auth_tokens(&mut self.auth_client, keypair.as_ref()),
            )
            .await
            .map_err(|_| ProxyError::MethodTimeout("generate_auth_tokens".to_string()))?
            .map_err(|e| ProxyError::MethodError {
                code: tonic::Code::Unknown,
                message: sanitize_status_message_for_influx(&e.to_string()),
            })?;
            self.access_token.store(Arc::new(new_access_token));
            self.refresh_token = new_refresh_token;
            Ok((true, true))
        } else if expires_by(&self.access_token.load(), refresh_deadline) {
            let new_access_token = timeout(
                *connection_timeout,
                refresh_access_token(&mut self.auth_client, &self.refresh_token),
            )
            .await
            .map_err(|_| ProxyError::MethodTimeout("refresh_access_token".to_string()))?
            .map_err(|e| ProxyError::MethodError {
                code: tonic::Code::Unknown,
                message: sanitize_status_message_for_influx(&e.to_string()),
            })?;
            self.access_token.store(Arc::new(new_access_token));
            Ok((true, false))
        } else {
            Ok((false, false))
        }
    }
}

fn expires_by(token: &Token, deadline: i64) -> bool {
    // Keep protobuf seconds signed so already-expired timestamps cannot wrap into the future.
    token
        .expires_at_utc
        .as_ref()
        .is_none_or(|ts| ts.seconds <= deadline)
}

/// Interceptor responsible for adding the access token to request headers.
pub(crate) struct AuthInterceptor {
    /// The token added to each request header.
    access_token: Arc<ArcSwap<Token>>,
}

impl Interceptor for AuthInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        request.metadata_mut().insert(
            "authorization",
            format!("Bearer {}", self.access_token.load().value)
                .parse()
                .map_err(|_| Status::invalid_argument("Failed to parse authorization token"))?,
        );

        Ok(request)
    }
}

pub(crate) async fn auth_client_from_endpoint(
    endpoint: &Endpoint,
    connection_timeout: &Duration,
    keypair: &Keypair,
) -> crate::proxy::Result<AuthRefreshState> {
    let mut auth_client = AuthServiceClient::new(
        timeout(*connection_timeout, endpoint.connect())
            .await
            .map_err(|_| ProxyError::AuthenticationConnectionTimeout)?
            .map_err(|err| {
                ProxyError::AuthenticationConnectionError(sanitize_status_message_for_influx(
                    &err.to_string(),
                ))
            })?,
    );
    let (access_token, refresh_token) = timeout(
        *connection_timeout,
        generate_auth_tokens(&mut auth_client, keypair),
    )
    .await
    .map_err(|_| ProxyError::AuthenticationTimeout)??;
    Ok(AuthRefreshState {
        auth_client,
        access_token: Arc::new(ArcSwap::from_pointee(access_token)),
        refresh_token,
        identity: keypair.pubkey(),
    })
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
                ProxyError::AuthenticationError {
                    code: e.code(),
                    message: sanitize_status_message_for_influx(e.message()),
                }
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

    debug!("formatted_challenge: {formatted_challenge} signed_challenge: {signed_challenge:?}",);

    let auth_tokens = auth_service_client
        .generate_auth_tokens(GenerateAuthTokensRequest {
            challenge: formatted_challenge,
            client_pubkey: keypair.pubkey().as_ref().to_vec(),
            signed_challenge,
        })
        .await
        .map_err(|e| ProxyError::AuthenticationError {
            code: e.code(),
            message: sanitize_status_message_for_influx(e.message()),
        })?;

    let inner = auth_tokens.into_inner();
    let access_token = get_validated_token(inner.access_token)?;
    let refresh_token = get_validated_token(inner.refresh_token)?;

    Ok((access_token, refresh_token))
}

async fn refresh_access_token(
    auth_service_client: &mut AuthServiceClient<Channel>,
    refresh_token: &Token,
) -> crate::proxy::Result<Token> {
    let response = auth_service_client
        .refresh_access_token(RefreshAccessTokenRequest {
            refresh_token: refresh_token.value.clone(),
        })
        .await
        .map_err(|e| ProxyError::AuthenticationError {
            code: e.code(),
            message: sanitize_status_message_for_influx(e.message()),
        })?;
    get_validated_token(response.into_inner().access_token)
}

/// Reject malformed auth responses before publishing a token to the interceptor.
fn get_validated_token(maybe_token: Option<Token>) -> crate::proxy::Result<Token> {
    let token = maybe_token
        .ok_or_else(|| ProxyError::BadAuthenticationToken("received a null token".to_string()))?;
    if token.value.is_empty() {
        Err(ProxyError::BadAuthenticationToken(
            "token value is empty".to_string(),
        ))
    } else if token.expires_at_utc.is_none() {
        Err(ProxyError::BadAuthenticationToken(
            "expires_at_utc field is null".to_string(),
        ))
    } else {
        Ok(token)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn token(value: &str, expires_at: i64) -> Token {
        let mut token = Token {
            value: value.to_string(),
            ..Token::default()
        };
        token.expires_at_utc.get_or_insert_default().seconds = expires_at;
        token
    }

    #[test]
    fn expiry_is_signed_and_inclusive() {
        for (expires_at, expected) in [(-1, true), (10, true), (11, false)] {
            assert_eq!(expires_by(&token("token", expires_at), 10), expected);
        }
    }

    #[test]
    fn token_requires_value_and_expiry() {
        assert!(get_validated_token(Some(token("token", 1))).is_ok());
        let mut missing_expiry = token("token", 1);
        missing_expiry.expires_at_utc = None;
        for invalid in [None, Some(token("", 1)), Some(missing_expiry)] {
            assert!(get_validated_token(invalid).is_err());
        }
    }
}
