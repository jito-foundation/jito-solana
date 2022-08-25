use {
    crate::{
        runtime_plugin::RuntimePluginError,
        runtime_plugin_admin_rpc_service::RuntimePluginManagerRpcRequest,
        runtime_plugin_manager::RuntimePluginManager,
    },
    crossbeam_channel::Receiver,
    log::{error, info},
    solana_runtime::{bank_forks::BankForks, commitment::BlockCommitmentCache},
    std::{
        path::PathBuf,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc, RwLock,
        },
        thread::{self, JoinHandle},
        time::Duration,
    },
};

pub struct RuntimePluginService {
    plugin_manager: Arc<RwLock<RuntimePluginManager>>,
    rpc_thread: JoinHandle<()>,
}

impl RuntimePluginService {
    pub fn start(
        plugin_config_files: &[PathBuf],
        rpc_receiver: Receiver<RuntimePluginManagerRpcRequest>,
        bank_forks: Arc<RwLock<BankForks>>,
        block_commitment_cache: Arc<RwLock<BlockCommitmentCache>>,
        exit: Arc<AtomicBool>,
    ) -> Result<Self, RuntimePluginError> {
        let mut plugin_manager =
            RuntimePluginManager::new(bank_forks, block_commitment_cache, exit.clone());

        for config in plugin_config_files {
            let name = plugin_manager
                .load_plugin(config)
                .map_err(|e| RuntimePluginError::FailedToLoadPlugin(e.into()))?;
            info!("Loaded Runtime Plugin: {name}");
        }

        let plugin_manager = Arc::new(RwLock::new(plugin_manager));
        let rpc_thread =
            Self::start_rpc_request_handler(rpc_receiver, plugin_manager.clone(), exit);

        Ok(Self {
            plugin_manager,
            rpc_thread,
        })
    }

    pub fn join(self) {
        if let Err(e) = self.rpc_thread.join() {
            error!("error joining rpc thread: {e:?}");
        }
        self.plugin_manager.write().unwrap().unload_all_plugins();
    }

    fn start_rpc_request_handler(
        rpc_receiver: Receiver<RuntimePluginManagerRpcRequest>,
        plugin_manager: Arc<RwLock<RuntimePluginManager>>,
        exit: Arc<AtomicBool>,
    ) -> JoinHandle<()> {
        thread::Builder::new()
            .name("solRuntimePluginRpc".to_string())
            .spawn(move || {
                const TIMEOUT: Duration = Duration::from_secs(3);
                while !exit.load(Ordering::Relaxed) {
                    if let Ok(request) = rpc_receiver.recv_timeout(TIMEOUT) {
                        match request {
                            RuntimePluginManagerRpcRequest::ListPlugins { response_sender } => {
                                let plugin_list = plugin_manager.read().unwrap().list_plugins();
                                if response_sender.send(plugin_list).is_err() {
                                    error!("response_sender channel disconnected");
                                    return;
                                }
                            }
                            RuntimePluginManagerRpcRequest::ReloadPlugin {
                                ref name,
                                ref config_file,
                                response_sender,
                            } => {
                                let reload_result = plugin_manager
                                    .write()
                                    .unwrap()
                                    .reload_plugin(name, config_file);
                                if response_sender.send(reload_result).is_err() {
                                    error!("response_sender channel disconnected");
                                    return;
                                }
                            }
                            RuntimePluginManagerRpcRequest::LoadPlugin {
                                ref config_file,
                                response_sender,
                            } => {
                                let load_result =
                                    plugin_manager.write().unwrap().load_plugin(config_file);
                                if response_sender.send(load_result).is_err() {
                                    error!("response_sender channel disconnected");
                                    return;
                                }
                            }
                            RuntimePluginManagerRpcRequest::UnloadPlugin {
                                ref name,
                                response_sender,
                            } => {
                                let unload_result =
                                    plugin_manager.write().unwrap().unload_plugin(name);
                                if response_sender.send(unload_result).is_err() {
                                    error!("response_sender channel disconnected");
                                    return;
                                }
                            }
                        }
                    }
                }
                plugin_manager.write().unwrap().unload_all_plugins();
            })
            .unwrap()
    }
}
