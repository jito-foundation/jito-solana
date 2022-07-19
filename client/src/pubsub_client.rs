use {
    crate::{
        rpc_config::{
            RpcAccountInfoConfig, RpcBlockSubscribeConfig, RpcBlockSubscribeFilter,
            RpcProgramAccountsConfig, RpcSignatureSubscribeConfig, RpcTransactionLogsConfig,
            RpcTransactionLogsFilter,
        },
        rpc_filter,
        rpc_response::{
            Response as RpcResponse, RpcBlockUpdate, RpcKeyedAccount, RpcLogsResponse,
            RpcSignatureResult, RpcVote, SlotInfo, SlotUpdate,
        },
    },
    crossbeam_channel::{unbounded, Receiver, Sender},
    log::*,
    serde::de::DeserializeOwned,
    serde_json::{
        json,
        value::Value::{Number, Object},
        Map, Value,
    },
    solana_account_decoder::UiAccount,
    solana_sdk::{clock::Slot, pubkey::Pubkey, signature::Signature},
    std::{
        marker::PhantomData,
        net::TcpStream,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc, RwLock,
        },
        thread::{sleep, JoinHandle},
        time::Duration,
    },
    thiserror::Error,
    tungstenite::{connect, stream::MaybeTlsStream, Message, WebSocket},
    url::{ParseError, Url},
};

#[derive(Debug, Error)]
pub enum PubsubClientError {
    #[error("url parse error")]
    UrlParseError(#[from] ParseError),

    #[error("unable to connect to server")]
    ConnectionError(#[from] tungstenite::Error),

    #[error("json parse error")]
    JsonParseError(#[from] serde_json::error::Error),

    #[error("unexpected message format: {0}")]
    UnexpectedMessageError(String),

    #[error("request error: {0}")]
    RequestError(String),
}

pub struct PubsubClientSubscription<T>
where
    T: DeserializeOwned,
{
    message_type: PhantomData<T>,
    operation: &'static str,
    socket: Arc<RwLock<WebSocket<MaybeTlsStream<TcpStream>>>>,
    subscription_id: u64,
    t_cleanup: Option<JoinHandle<()>>,
    exit: Arc<AtomicBool>,
}

impl<T> Drop for PubsubClientSubscription<T>
where
    T: DeserializeOwned,
{
    fn drop(&mut self) {
        self.send_unsubscribe()
            .unwrap_or_else(|_| warn!("unable to unsubscribe from websocket"));
        self.socket
            .write()
            .unwrap()
            .close(None)
            .unwrap_or_else(|_| warn!("unable to close websocket"));
    }
}

impl<T> PubsubClientSubscription<T>
where
    T: DeserializeOwned,
{
    fn send_subscribe(
        writable_socket: &Arc<RwLock<WebSocket<MaybeTlsStream<TcpStream>>>>,
        body: String,
    ) -> Result<u64, PubsubClientError> {
        writable_socket
            .write()
            .unwrap()
            .write_message(Message::Text(body))?;
        let message = writable_socket.write().unwrap().read_message()?;
        Self::extract_subscription_id(message)
    }

    fn extract_subscription_id(message: Message) -> Result<u64, PubsubClientError> {
        let message_text = &message.into_text()?;
        let json_msg: Map<String, Value> = serde_json::from_str(message_text)?;

        if let Some(Number(x)) = json_msg.get("result") {
            if let Some(x) = x.as_u64() {
                return Ok(x);
            }
        }
        // TODO: Add proper JSON RPC response/error handling...
        Err(PubsubClientError::UnexpectedMessageError(format!(
            "{:?}",
            json_msg
        )))
    }

    pub fn send_unsubscribe(&self) -> Result<(), PubsubClientError> {
        let method = format!("{}Unsubscribe", self.operation);
        self.socket
            .write()
            .unwrap()
            .write_message(Message::Text(
                json!({
                "jsonrpc":"2.0","id":1,"method":method,"params":[self.subscription_id]
                })
                .to_string(),
            ))
            .map_err(|err| err.into())
    }

    fn get_version(
        writable_socket: &Arc<RwLock<WebSocket<MaybeTlsStream<TcpStream>>>>,
    ) -> Result<semver::Version, PubsubClientError> {
        writable_socket
            .write()
            .unwrap()
            .write_message(Message::Text(
                json!({
                    "jsonrpc":"2.0","id":1,"method":"getVersion",
                })
                .to_string(),
            ))?;
        let message = writable_socket.write().unwrap().read_message()?;
        let message_text = &message.into_text()?;
        let json_msg: Map<String, Value> = serde_json::from_str(message_text)?;

        if let Some(Object(version_map)) = json_msg.get("result") {
            if let Some(node_version) = version_map.get("solana-core") {
                let node_version = semver::Version::parse(
                    node_version.as_str().unwrap_or_default(),
                )
                .map_err(|e| {
                    PubsubClientError::RequestError(format!(
                        "failed to parse cluster version: {}",
                        e
                    ))
                })?;
                return Ok(node_version);
            }
        }
        // TODO: Add proper JSON RPC response/error handling...
        Err(PubsubClientError::UnexpectedMessageError(format!(
            "{:?}",
            json_msg
        )))
    }

    fn read_message(
        writable_socket: &Arc<RwLock<WebSocket<MaybeTlsStream<TcpStream>>>>,
    ) -> Result<T, PubsubClientError> {
        let message = writable_socket.write().unwrap().read_message()?;
        let message_text = &message.into_text().unwrap();
        let json_msg: Map<String, Value> = serde_json::from_str(message_text)?;

        if let Some(Object(params)) = json_msg.get("params") {
            if let Some(result) = params.get("result") {
                let x: T = serde_json::from_value::<T>(result.clone()).unwrap();
                return Ok(x);
            }
        }

        // TODO: Add proper JSON RPC response/error handling...
        Err(PubsubClientError::UnexpectedMessageError(format!(
            "{:?}",
            json_msg
        )))
    }

    pub fn shutdown(&mut self) -> std::thread::Result<()> {
        if self.t_cleanup.is_some() {
            info!("websocket thread - shutting down");
            self.exit.store(true, Ordering::Relaxed);
            let x = self.t_cleanup.take().unwrap().join();
            info!("websocket thread - shut down.");
            x
        } else {
            warn!("websocket thread - already shut down.");
            Ok(())
        }
    }
}

pub type PubsubLogsClientSubscription = PubsubClientSubscription<RpcResponse<RpcLogsResponse>>;
pub type LogsSubscription = (
    PubsubLogsClientSubscription,
    Receiver<RpcResponse<RpcLogsResponse>>,
);

pub type PubsubSlotClientSubscription = PubsubClientSubscription<SlotInfo>;
pub type SlotsSubscription = (PubsubSlotClientSubscription, Receiver<SlotInfo>);

pub type PubsubSignatureClientSubscription =
    PubsubClientSubscription<RpcResponse<RpcSignatureResult>>;
pub type SignatureSubscription = (
    PubsubSignatureClientSubscription,
    Receiver<RpcResponse<RpcSignatureResult>>,
);

pub type PubsubBlockClientSubscription = PubsubClientSubscription<RpcResponse<RpcBlockUpdate>>;
pub type BlockSubscription = (
    PubsubBlockClientSubscription,
    Receiver<RpcResponse<RpcBlockUpdate>>,
);

pub type PubsubProgramClientSubscription = PubsubClientSubscription<RpcResponse<RpcKeyedAccount>>;
pub type ProgramSubscription = (
    PubsubProgramClientSubscription,
    Receiver<RpcResponse<RpcKeyedAccount>>,
);

pub type PubsubAccountClientSubscription = PubsubClientSubscription<RpcResponse<UiAccount>>;
pub type AccountSubscription = (
    PubsubAccountClientSubscription,
    Receiver<RpcResponse<UiAccount>>,
);

pub type PubsubVoteClientSubscription = PubsubClientSubscription<RpcVote>;
pub type VoteSubscription = (PubsubVoteClientSubscription, Receiver<RpcVote>);

pub type PubsubRootClientSubscription = PubsubClientSubscription<Slot>;
pub type RootSubscription = (PubsubRootClientSubscription, Receiver<Slot>);

pub struct PubsubClient {}

fn connect_with_retry(
    url: Url,
) -> Result<WebSocket<MaybeTlsStream<TcpStream>>, tungstenite::Error> {
    let mut connection_retries = 5;
    loop {
        let result = connect(url.clone()).map(|(socket, _)| socket);
        if let Err(tungstenite::Error::Http(response)) = &result {
            if response.status() == reqwest::StatusCode::TOO_MANY_REQUESTS && connection_retries > 0
            {
                let mut duration = Duration::from_millis(500);
                if let Some(retry_after) = response.headers().get(reqwest::header::RETRY_AFTER) {
                    if let Ok(retry_after) = retry_after.to_str() {
                        if let Ok(retry_after) = retry_after.parse::<u64>() {
                            if retry_after < 120 {
                                duration = Duration::from_secs(retry_after);
                            }
                        }
                    }
                }

                connection_retries -= 1;
                debug!(
                    "Too many requests: server responded with {:?}, {} retries left, pausing for {:?}",
                    response, connection_retries, duration
                );

                sleep(duration);
                continue;
            }
        }
        return result;
    }
}

impl PubsubClient {
    pub fn account_subscribe(
        url: &str,
        pubkey: &Pubkey,
        config: Option<RpcAccountInfoConfig>,
    ) -> Result<AccountSubscription, PubsubClientError> {
        let url = Url::parse(url)?;
        let socket = connect_with_retry(url)?;
        let (sender, receiver) = unbounded();

        let socket = Arc::new(RwLock::new(socket));
        let socket_clone = socket.clone();
        let exit = Arc::new(AtomicBool::new(false));
        let exit_clone = exit.clone();
        let body = json!({
            "jsonrpc":"2.0",
            "id":1,
            "method":"accountSubscribe",
            "params":[
                pubkey.to_string(),
                config
            ]
        })
        .to_string();
        let subscription_id = PubsubAccountClientSubscription::send_subscribe(&socket_clone, body)?;

        let t_cleanup = std::thread::spawn(move || {
            Self::cleanup_with_sender(exit_clone, &socket_clone, sender)
        });

        let result = PubsubClientSubscription {
            message_type: PhantomData,
            operation: "account",
            socket,
            subscription_id,
            t_cleanup: Some(t_cleanup),
            exit,
        };

        Ok((result, receiver))
    }

    pub fn block_subscribe(
        url: &str,
        filter: RpcBlockSubscribeFilter,
        config: Option<RpcBlockSubscribeConfig>,
    ) -> Result<BlockSubscription, PubsubClientError> {
        let url = Url::parse(url)?;
        let socket = connect_with_retry(url)?;
        let (sender, receiver) = unbounded();

        let socket = Arc::new(RwLock::new(socket));
        let socket_clone = socket.clone();
        let exit = Arc::new(AtomicBool::new(false));
        let exit_clone = exit.clone();
        let body = json!({
            "jsonrpc":"2.0",
            "id":1,
            "method":"blockSubscribe",
            "params":[filter, config]
        })
        .to_string();

        let subscription_id = PubsubBlockClientSubscription::send_subscribe(&socket_clone, body)?;

        let t_cleanup = std::thread::spawn(move || {
            Self::cleanup_with_sender(exit_clone, &socket_clone, sender)
        });

        let result = PubsubClientSubscription {
            message_type: PhantomData,
            operation: "block",
            socket,
            subscription_id,
            t_cleanup: Some(t_cleanup),
            exit,
        };

        Ok((result, receiver))
    }

    pub fn logs_subscribe(
        url: &str,
        filter: RpcTransactionLogsFilter,
        config: RpcTransactionLogsConfig,
    ) -> Result<LogsSubscription, PubsubClientError> {
        let url = Url::parse(url)?;
        let socket = connect_with_retry(url)?;
        let (sender, receiver) = unbounded();

        let socket = Arc::new(RwLock::new(socket));
        let socket_clone = socket.clone();
        let exit = Arc::new(AtomicBool::new(false));
        let exit_clone = exit.clone();
        let body = json!({
            "jsonrpc":"2.0",
            "id":1,
            "method":"logsSubscribe",
            "params":[filter, config]
        })
        .to_string();

        let subscription_id = PubsubLogsClientSubscription::send_subscribe(&socket_clone, body)?;

        let t_cleanup = std::thread::spawn(move || {
            Self::cleanup_with_sender(exit_clone, &socket_clone, sender)
        });

        let result = PubsubClientSubscription {
            message_type: PhantomData,
            operation: "logs",
            socket,
            subscription_id,
            t_cleanup: Some(t_cleanup),
            exit,
        };

        Ok((result, receiver))
    }

    pub fn program_subscribe(
        url: &str,
        pubkey: &Pubkey,
        mut config: Option<RpcProgramAccountsConfig>,
    ) -> Result<ProgramSubscription, PubsubClientError> {
        let url = Url::parse(url)?;
        let socket = connect_with_retry(url)?;
        let (sender, receiver) = unbounded();

        let socket = Arc::new(RwLock::new(socket));
        let socket_clone = socket.clone();
        let exit = Arc::new(AtomicBool::new(false));
        let exit_clone = exit.clone();

        if let Some(ref mut config) = config {
            if let Some(ref mut filters) = config.filters {
                let node_version = PubsubProgramClientSubscription::get_version(&socket_clone).ok();
                // If node does not support the pubsub `getVersion` method, assume version is old
                // and filters should be mapped (node_version.is_none()).
                rpc_filter::maybe_map_filters(node_version, filters)
                    .map_err(PubsubClientError::RequestError)?;
            }
        }

        let body = json!({
            "jsonrpc":"2.0",
            "id":1,
            "method":"programSubscribe",
            "params":[
                pubkey.to_string(),
                config
            ]
        })
        .to_string();
        let subscription_id = PubsubProgramClientSubscription::send_subscribe(&socket_clone, body)?;

        let t_cleanup = std::thread::spawn(move || {
            Self::cleanup_with_sender(exit_clone, &socket_clone, sender)
        });

        let result = PubsubClientSubscription {
            message_type: PhantomData,
            operation: "program",
            socket,
            subscription_id,
            t_cleanup: Some(t_cleanup),
            exit,
        };

        Ok((result, receiver))
    }

    pub fn vote_subscribe(url: &str) -> Result<VoteSubscription, PubsubClientError> {
        let url = Url::parse(url)?;
        let socket = connect_with_retry(url)?;
        let (sender, receiver) = unbounded();

        let socket = Arc::new(RwLock::new(socket));
        let socket_clone = socket.clone();
        let exit = Arc::new(AtomicBool::new(false));
        let exit_clone = exit.clone();
        let body = json!({
            "jsonrpc":"2.0",
            "id":1,
            "method":"voteSubscribe",
        })
        .to_string();
        let subscription_id = PubsubVoteClientSubscription::send_subscribe(&socket_clone, body)?;

        let t_cleanup = std::thread::spawn(move || {
            Self::cleanup_with_sender(exit_clone, &socket_clone, sender)
        });

        let result = PubsubClientSubscription {
            message_type: PhantomData,
            operation: "vote",
            socket,
            subscription_id,
            t_cleanup: Some(t_cleanup),
            exit,
        };

        Ok((result, receiver))
    }

    pub fn root_subscribe(url: &str) -> Result<RootSubscription, PubsubClientError> {
        let url = Url::parse(url)?;
        let socket = connect_with_retry(url)?;
        let (sender, receiver) = unbounded();

        let socket = Arc::new(RwLock::new(socket));
        let socket_clone = socket.clone();
        let exit = Arc::new(AtomicBool::new(false));
        let exit_clone = exit.clone();
        let body = json!({
            "jsonrpc":"2.0",
            "id":1,
            "method":"rootSubscribe",
        })
        .to_string();
        let subscription_id = PubsubRootClientSubscription::send_subscribe(&socket_clone, body)?;

        let t_cleanup = std::thread::spawn(move || {
            Self::cleanup_with_sender(exit_clone, &socket_clone, sender)
        });

        let result = PubsubClientSubscription {
            message_type: PhantomData,
            operation: "root",
            socket,
            subscription_id,
            t_cleanup: Some(t_cleanup),
            exit,
        };

        Ok((result, receiver))
    }

    pub fn signature_subscribe(
        url: &str,
        signature: &Signature,
        config: Option<RpcSignatureSubscribeConfig>,
    ) -> Result<SignatureSubscription, PubsubClientError> {
        let url = Url::parse(url)?;
        let socket = connect_with_retry(url)?;
        let (sender, receiver) = unbounded();

        let socket = Arc::new(RwLock::new(socket));
        let socket_clone = socket.clone();
        let exit = Arc::new(AtomicBool::new(false));
        let exit_clone = exit.clone();
        let body = json!({
            "jsonrpc":"2.0",
            "id":1,
            "method":"signatureSubscribe",
            "params":[
                signature.to_string(),
                config
            ]
        })
        .to_string();
        let subscription_id =
            PubsubSignatureClientSubscription::send_subscribe(&socket_clone, body)?;

        let t_cleanup = std::thread::spawn(move || {
            Self::cleanup_with_sender(exit_clone, &socket_clone, sender)
        });

        let result = PubsubClientSubscription {
            message_type: PhantomData,
            operation: "signature",
            socket,
            subscription_id,
            t_cleanup: Some(t_cleanup),
            exit,
        };

        Ok((result, receiver))
    }

    pub fn slot_subscribe(url: &str) -> Result<SlotsSubscription, PubsubClientError> {
        let url = Url::parse(url)?;
        let socket = connect_with_retry(url)?;
        let (sender, receiver) = unbounded::<SlotInfo>();

        let socket = Arc::new(RwLock::new(socket));
        let socket_clone = socket.clone();
        let exit = Arc::new(AtomicBool::new(false));
        let exit_clone = exit.clone();
        let body = json!({
            "jsonrpc":"2.0",
            "id":1,
            "method":"slotSubscribe",
            "params":[]
        })
        .to_string();
        let subscription_id = PubsubSlotClientSubscription::send_subscribe(&socket_clone, body)?;

        let t_cleanup = std::thread::spawn(move || {
            Self::cleanup_with_sender(exit_clone, &socket_clone, sender)
        });

        let result = PubsubClientSubscription {
            message_type: PhantomData,
            operation: "slot",
            socket,
            subscription_id,
            t_cleanup: Some(t_cleanup),
            exit,
        };

        Ok((result, receiver))
    }

    pub fn slot_updates_subscribe(
        url: &str,
        handler: impl Fn(SlotUpdate) + Send + 'static,
    ) -> Result<PubsubClientSubscription<SlotUpdate>, PubsubClientError> {
        let url = Url::parse(url)?;
        let socket = connect_with_retry(url)?;

        let socket = Arc::new(RwLock::new(socket));
        let socket_clone = socket.clone();
        let exit = Arc::new(AtomicBool::new(false));
        let exit_clone = exit.clone();
        let body = json!({
            "jsonrpc":"2.0",
            "id":1,
            "method":"slotsUpdatesSubscribe",
            "params":[]
        })
        .to_string();
        let subscription_id = PubsubSlotClientSubscription::send_subscribe(&socket, body)?;

        let t_cleanup = std::thread::spawn(move || {
            Self::cleanup_with_handler(exit_clone, &socket_clone, handler)
        });

        Ok(PubsubClientSubscription {
            message_type: PhantomData,
            operation: "slotsUpdates",
            socket,
            subscription_id,
            t_cleanup: Some(t_cleanup),
            exit,
        })
    }

    fn cleanup_with_sender<T>(
        exit: Arc<AtomicBool>,
        socket: &Arc<RwLock<WebSocket<MaybeTlsStream<TcpStream>>>>,
        sender: Sender<T>,
    ) where
        T: DeserializeOwned + Send + 'static,
    {
        let handler = move |message| match sender.send(message) {
            Ok(_) => (),
            Err(err) => {
                info!("receive error: {:?}", err);
            }
        };
        Self::cleanup_with_handler(exit, socket, handler);
    }

    fn cleanup_with_handler<T, F>(
        exit: Arc<AtomicBool>,
        socket: &Arc<RwLock<WebSocket<MaybeTlsStream<TcpStream>>>>,
        handler: F,
    ) where
        T: DeserializeOwned,
        F: Fn(T) + Send + 'static,
    {
        loop {
            if exit.load(Ordering::Relaxed) {
                break;
            }

            match PubsubClientSubscription::read_message(socket) {
                Ok(message) => handler(message),
                Err(err) => {
                    info!("receive error: {:?}", err);
                    break;
                }
            }
        }

        info!("websocket - exited receive loop");
    }
}

#[cfg(test)]
mod tests {
    // see client-test/test/client.rs
}
