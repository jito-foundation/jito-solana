//! RPC interface to dynamically make changes to runtime plugins.

use {
    crossbeam_channel::Sender,
    jsonrpc_core::{BoxFuture, ErrorCode, MetaIoHandler, Metadata, Result as JsonRpcResult},
    jsonrpc_core_client::{transports::ipc, RpcError},
    jsonrpc_derive::rpc,
    jsonrpc_ipc_server::{
        tokio::{self, sync::oneshot::channel as oneshot_channel},
        RequestContext, ServerBuilder,
    },
    jsonrpc_server_utils::tokio::sync::oneshot::Sender as OneShotSender,
    log::*,
    solana_sdk::exit::Exit,
    std::{
        path::{Path, PathBuf},
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc, RwLock,
        },
    },
};

#[derive(Debug)]
pub enum RuntimePluginManagerRpcRequest {
    ReloadPlugin {
        name: String,
        config_file: String,
        response_sender: OneShotSender<JsonRpcResult<()>>,
    },
    UnloadPlugin {
        name: String,
        response_sender: OneShotSender<JsonRpcResult<()>>,
    },
    LoadPlugin {
        config_file: String,
        response_sender: OneShotSender<JsonRpcResult<String>>,
    },
    ListPlugins {
        response_sender: OneShotSender<JsonRpcResult<Vec<String>>>,
    },
}

#[rpc]
pub trait RuntimePluginAdminRpc {
    type Metadata;

    #[rpc(meta, name = "reloadPlugin")]
    fn reload_plugin(
        &self,
        meta: Self::Metadata,
        name: String,
        config_file: String,
    ) -> BoxFuture<JsonRpcResult<()>>;

    #[rpc(meta, name = "unloadPlugin")]
    fn unload_plugin(&self, meta: Self::Metadata, name: String) -> BoxFuture<JsonRpcResult<()>>;

    #[rpc(meta, name = "loadPlugin")]
    fn load_plugin(
        &self,
        meta: Self::Metadata,
        config_file: String,
    ) -> BoxFuture<JsonRpcResult<String>>;

    #[rpc(meta, name = "listPlugins")]
    fn list_plugins(&self, meta: Self::Metadata) -> BoxFuture<JsonRpcResult<Vec<String>>>;
}

#[derive(Clone)]
pub struct RuntimePluginAdminRpcRequestMetadata {
    pub rpc_request_sender: Sender<RuntimePluginManagerRpcRequest>,
    pub validator_exit: Arc<RwLock<Exit>>,
}

impl Metadata for RuntimePluginAdminRpcRequestMetadata {}

fn rpc_path(ledger_path: &Path) -> PathBuf {
    #[cfg(target_family = "windows")]
    {
        // More information about the wackiness of pipe names over at
        // https://docs.microsoft.com/en-us/windows/win32/ipc/pipe-names
        if let Some(ledger_filename) = ledger_path.file_name() {
            PathBuf::from(format!(
                "\\\\.\\pipe\\{}-runtime_plugin_admin.rpc",
                ledger_filename.to_string_lossy()
            ))
        } else {
            PathBuf::from("\\\\.\\pipe\\runtime_plugin_admin.rpc")
        }
    }
    #[cfg(not(target_family = "windows"))]
    {
        ledger_path.join("runtime_plugin_admin.rpc")
    }
}

/// Start the Runtime Plugin Admin RPC interface.
pub fn run(
    ledger_path: &Path,
    metadata: RuntimePluginAdminRpcRequestMetadata,
    plugin_exit: Arc<AtomicBool>,
) {
    let rpc_path = rpc_path(ledger_path);

    let event_loop = tokio::runtime::Builder::new_multi_thread()
        .thread_name("solRuntimePluginAdminRpc")
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();

    std::thread::Builder::new()
        .name("solAdminRpc".to_string())
        .spawn(move || {
            let mut io = MetaIoHandler::default();
            io.extend_with(RuntimePluginAdminRpcImpl.to_delegate());

            let validator_exit = metadata.validator_exit.clone();

            match ServerBuilder::with_meta_extractor(io, move |_req: &RequestContext| {
                metadata.clone()
            })
            .event_loop_executor(event_loop.handle().clone())
            .start(&format!("{}", rpc_path.display()))
            {
                Err(e) => {
                    error!("Unable to start runtime plugin admin rpc service: {e:?}, exiting");
                    validator_exit.write().unwrap().exit();
                }
                Ok(server) => {
                    info!("started runtime plugin admin rpc service!");
                    let close_handle = server.close_handle();
                    let c_plugin_exit = plugin_exit.clone();
                    validator_exit
                        .write()
                        .unwrap()
                        .register_exit(Box::new(move || {
                            close_handle.close();
                            c_plugin_exit.store(true, Ordering::Relaxed);
                        }));

                    server.wait();
                    plugin_exit.store(true, Ordering::Relaxed);
                }
            }
        })
        .unwrap();
}

pub struct RuntimePluginAdminRpcImpl;
impl RuntimePluginAdminRpc for RuntimePluginAdminRpcImpl {
    type Metadata = RuntimePluginAdminRpcRequestMetadata;

    fn reload_plugin(
        &self,
        meta: Self::Metadata,
        name: String,
        config_file: String,
    ) -> BoxFuture<JsonRpcResult<()>> {
        Box::pin(async move {
            let (response_sender, response_receiver) = oneshot_channel();

            if meta
                .rpc_request_sender
                .send(RuntimePluginManagerRpcRequest::ReloadPlugin {
                    name,
                    config_file,
                    response_sender,
                })
                .is_err()
            {
                error!("rpc_request_sender channel closed, exiting");
                meta.validator_exit.write().unwrap().exit();

                return Err(jsonrpc_core::Error {
                    code: ErrorCode::InternalError,
                    message: "Internal channel disconnected while sending the request".to_string(),
                    data: None,
                });
            }

            match response_receiver.await {
                Err(_) => {
                    error!("response_receiver channel closed, exiting");
                    meta.validator_exit.write().unwrap().exit();
                    Err(jsonrpc_core::Error {
                        code: ErrorCode::InternalError,
                        message: "Internal channel disconnected while awaiting the response"
                            .to_string(),
                        data: None,
                    })
                }
                Ok(resp) => resp,
            }
        })
    }

    fn unload_plugin(&self, meta: Self::Metadata, name: String) -> BoxFuture<JsonRpcResult<()>> {
        Box::pin(async move {
            let (response_sender, response_receiver) = oneshot_channel();

            if meta
                .rpc_request_sender
                .send(RuntimePluginManagerRpcRequest::UnloadPlugin {
                    name,
                    response_sender,
                })
                .is_err()
            {
                error!("rpc_request_sender channel closed, exiting");
                meta.validator_exit.write().unwrap().exit();

                return Err(jsonrpc_core::Error {
                    code: ErrorCode::InternalError,
                    message: "Internal channel disconnected while sending the request".to_string(),
                    data: None,
                });
            }

            match response_receiver.await {
                Err(_) => {
                    error!("response_receiver channel closed, exiting");
                    meta.validator_exit.write().unwrap().exit();
                    Err(jsonrpc_core::Error {
                        code: ErrorCode::InternalError,
                        message: "Internal channel disconnected while awaiting the response"
                            .to_string(),
                        data: None,
                    })
                }
                Ok(resp) => resp,
            }
        })
    }

    fn load_plugin(
        &self,
        meta: Self::Metadata,
        config_file: String,
    ) -> BoxFuture<JsonRpcResult<String>> {
        Box::pin(async move {
            let (response_sender, response_receiver) = oneshot_channel();

            if meta
                .rpc_request_sender
                .send(RuntimePluginManagerRpcRequest::LoadPlugin {
                    config_file,
                    response_sender,
                })
                .is_err()
            {
                error!("rpc_request_sender channel closed, exiting");
                meta.validator_exit.write().unwrap().exit();

                return Err(jsonrpc_core::Error {
                    code: ErrorCode::InternalError,
                    message: "Internal channel disconnected while sending the request".to_string(),
                    data: None,
                });
            }

            match response_receiver.await {
                Err(_) => {
                    error!("response_receiver channel closed, exiting");
                    meta.validator_exit.write().unwrap().exit();
                    Err(jsonrpc_core::Error {
                        code: ErrorCode::InternalError,
                        message: "Internal channel disconnected while awaiting the response"
                            .to_string(),
                        data: None,
                    })
                }
                Ok(resp) => resp,
            }
        })
    }

    fn list_plugins(&self, meta: Self::Metadata) -> BoxFuture<JsonRpcResult<Vec<String>>> {
        Box::pin(async move {
            let (response_sender, response_receiver) = oneshot_channel();

            if meta
                .rpc_request_sender
                .send(RuntimePluginManagerRpcRequest::ListPlugins { response_sender })
                .is_err()
            {
                error!("rpc_request_sender channel closed, exiting");
                meta.validator_exit.write().unwrap().exit();

                return Err(jsonrpc_core::Error {
                    code: ErrorCode::InternalError,
                    message: "Internal channel disconnected while sending the request".to_string(),
                    data: None,
                });
            }

            match response_receiver.await {
                Err(_) => {
                    error!("response_receiver channel closed, exiting");
                    meta.validator_exit.write().unwrap().exit();
                    Err(jsonrpc_core::Error {
                        code: ErrorCode::InternalError,
                        message: "Internal channel disconnected while awaiting the response"
                            .to_string(),
                        data: None,
                    })
                }
                Ok(resp) => resp,
            }
        })
    }
}

// Connect to the Runtime Plugin RPC interface
pub async fn connect(ledger_path: &Path) -> Result<gen_client::Client, RpcError> {
    let rpc_path = rpc_path(ledger_path);
    if !rpc_path.exists() {
        Err(RpcError::Client(format!(
            "{} does not exist",
            rpc_path.display()
        )))
    } else {
        ipc::connect::<_, gen_client::Client>(&format!("{}", rpc_path.display())).await
    }
}
