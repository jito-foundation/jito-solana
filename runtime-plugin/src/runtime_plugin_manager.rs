use {
    crate::runtime_plugin::{PluginDependencies, RuntimePlugin},
    jsonrpc_core::{serde_json, ErrorCode, Result as JsonRpcResult},
    libloading::Library,
    log::*,
    solana_runtime::{bank_forks::BankForks, commitment::BlockCommitmentCache},
    std::{
        fs::File,
        io::Read,
        path::{Path, PathBuf},
        sync::{atomic::AtomicBool, Arc, RwLock},
    },
};

#[derive(thiserror::Error, Debug)]
pub enum RuntimePluginManagerError {
    #[error("Cannot open the the plugin config file")]
    CannotOpenConfigFile(String),

    #[error("Cannot read the the plugin config file")]
    CannotReadConfigFile(String),

    #[error("The config file is not in a valid Json format")]
    InvalidConfigFileFormat(String),

    #[error("Plugin library path is not specified in the config file")]
    LibPathNotSet,

    #[error("Invalid plugin path")]
    InvalidPluginPath,

    #[error("Cannot load plugin shared library")]
    PluginLoadError(String),

    #[error("The runtime plugin {0} is already loaded shared library")]
    PluginAlreadyLoaded(String),

    #[error("The RuntimePlugin on_load method failed")]
    PluginStartError(String),
}

pub struct RuntimePluginManager {
    plugins: Vec<Box<dyn RuntimePlugin>>,
    libs: Vec<Library>,
    bank_forks: Arc<RwLock<BankForks>>,
    block_commitment_cache: Arc<RwLock<BlockCommitmentCache>>,
    exit: Arc<AtomicBool>,
}

impl RuntimePluginManager {
    pub fn new(
        bank_forks: Arc<RwLock<BankForks>>,
        block_commitment_cache: Arc<RwLock<BlockCommitmentCache>>,
        exit: Arc<AtomicBool>,
    ) -> Self {
        Self {
            plugins: vec![],
            libs: vec![],
            bank_forks,
            block_commitment_cache,
            exit,
        }
    }

    /// This method allows dynamic loading of a runtime plugin.
    /// Adds to the existing list of loaded plugins.
    pub(crate) fn load_plugin(
        &mut self,
        plugin_config_path: impl AsRef<Path>,
    ) -> JsonRpcResult<String /* plugin name */> {
        // First load plugin
        let (mut new_plugin, new_lib, config_file) =
            load_plugin_from_config(plugin_config_path.as_ref()).map_err(|e| {
                jsonrpc_core::Error {
                    code: ErrorCode::InvalidRequest,
                    message: format!("Failed to load plugin: {e}"),
                    data: None,
                }
            })?;

        // Then see if a plugin with this name already exists, if so return Err.
        let name = new_plugin.name();
        if self.plugins.iter().any(|plugin| name.eq(plugin.name())) {
            return Err(jsonrpc_core::Error {
                code: ErrorCode::InvalidRequest,
                message: format!(
                    "There already exists a plugin named {} loaded. Did not load requested plugin",
                    name,
                ),
                data: None,
            });
        }

        new_plugin
            .on_load(
                config_file,
                PluginDependencies {
                    bank_forks: self.bank_forks.clone(),
                    block_commitment_cache: self.block_commitment_cache.clone(),
                    exit: self.exit.clone(),
                },
            )
            .map_err(|on_load_err| jsonrpc_core::Error {
                code: ErrorCode::InvalidRequest,
                message: format!(
                    "on_load method of plugin {} failed: {on_load_err}",
                    new_plugin.name()
                ),
                data: None,
            })?;

        self.plugins.push(new_plugin);
        self.libs.push(new_lib);

        Ok(name.to_string())
    }

    /// Unloads the plugins and loaded plugin libraries, making sure to fire
    /// their `on_plugin_unload()` methods so they can do any necessary cleanup.
    pub(crate) fn unload_all_plugins(&mut self) {
        (0..self.plugins.len()).for_each(|idx| {
            self.try_drop_plugin(idx);
        });
    }

    pub(crate) fn unload_plugin(&mut self, name: &str) -> JsonRpcResult<()> {
        // Check if any plugin names match this one
        let Some(idx) = self
            .plugins
            .iter()
            .position(|plugin| plugin.name().eq(name))
        else {
            // If we don't find one return an error
            return Err(jsonrpc_core::error::Error {
                code: ErrorCode::InvalidRequest,
                message: String::from("The plugin you requested to unload is not loaded"),
                data: None,
            });
        };

        // Unload and drop plugin and lib
        self.try_drop_plugin(idx);

        Ok(())
    }

    /// Reloads an existing plugin.
    pub(crate) fn reload_plugin(&mut self, name: &str, config_file: &str) -> JsonRpcResult<()> {
        // Check if any plugin names match this one
        let Some(idx) = self
            .plugins
            .iter()
            .position(|plugin| plugin.name().eq(name))
        else {
            // If we don't find one return an error
            return Err(jsonrpc_core::error::Error {
                code: ErrorCode::InvalidRequest,
                message: String::from("The plugin you requested to reload is not loaded"),
                data: None,
            });
        };

        self.try_drop_plugin(idx);

        // Try to load plugin, library
        // SAFETY: It is up to the validator to ensure this is a valid plugin library.
        let (mut new_plugin, new_lib, new_parsed_config_file) =
            load_plugin_from_config(config_file.as_ref()).map_err(|err| jsonrpc_core::Error {
                code: ErrorCode::InvalidRequest,
                message: err.to_string(),
                data: None,
            })?;

        // Attempt to on_load with new plugin
        match new_plugin.on_load(
            new_parsed_config_file,
            PluginDependencies {
                bank_forks: self.bank_forks.clone(),
                block_commitment_cache: self.block_commitment_cache.clone(),
                exit: self.exit.clone(),
            },
        ) {
            // On success, push plugin and library
            Ok(()) => {
                self.plugins.push(new_plugin);
                self.libs.push(new_lib);
                Ok(())
            }
            // On failure, return error
            Err(err) => Err(jsonrpc_core::error::Error {
                code: ErrorCode::InvalidRequest,
                message: format!(
                    "Failed to start new plugin (previous plugin was dropped!): {err}"
                ),
                data: None,
            }),
        }
    }

    pub(crate) fn list_plugins(&self) -> JsonRpcResult<Vec<String>> {
        Ok(self.plugins.iter().map(|p| p.name().to_owned()).collect())
    }

    fn try_drop_plugin(&mut self, idx: usize) {
        if idx < self.plugins.len() {
            let mut plugin = self.plugins.remove(idx);
            let lib = self.libs.remove(idx);
            drop(lib);
            plugin.on_unload();
        } else {
            error!("failed to drop plugin: index {idx} out of bounds");
        }
    }
}

fn load_plugin_from_config(
    plugin_config_path: &Path,
) -> Result<(Box<dyn RuntimePlugin>, Library, &str), RuntimePluginManagerError> {
    type PluginConstructor = unsafe fn() -> *mut dyn RuntimePlugin;
    use libloading::Symbol;

    let mut file = match File::open(plugin_config_path) {
        Ok(file) => file,
        Err(err) => {
            return Err(RuntimePluginManagerError::CannotOpenConfigFile(format!(
                "Failed to open the plugin config file {plugin_config_path:?}, error: {err:?}"
            )));
        }
    };

    let mut contents = String::new();
    if let Err(err) = file.read_to_string(&mut contents) {
        return Err(RuntimePluginManagerError::CannotReadConfigFile(format!(
            "Failed to read the plugin config file {plugin_config_path:?}, error: {err:?}"
        )));
    }

    let result: serde_json::Value = match json5::from_str(&contents) {
        Ok(value) => value,
        Err(err) => {
            return Err(RuntimePluginManagerError::InvalidConfigFileFormat(format!(
                "The config file {plugin_config_path:?} is not in a valid Json5 format, error: {err:?}"
            )));
        }
    };

    let libpath = result["libpath"]
        .as_str()
        .ok_or(RuntimePluginManagerError::LibPathNotSet)?;
    let mut libpath = PathBuf::from(libpath);
    if libpath.is_relative() {
        let config_dir = plugin_config_path.parent().ok_or_else(|| {
            RuntimePluginManagerError::CannotOpenConfigFile(format!(
                "Failed to resolve parent of {plugin_config_path:?}",
            ))
        })?;
        libpath = config_dir.join(libpath);
    }

    let config_file = plugin_config_path
        .as_os_str()
        .to_str()
        .ok_or(RuntimePluginManagerError::InvalidPluginPath)?;

    let (plugin, lib) = unsafe {
        let lib = Library::new(libpath)
            .map_err(|e| RuntimePluginManagerError::PluginLoadError(e.to_string()))?;
        let constructor: Symbol<PluginConstructor> = lib
            .get(b"_create_plugin")
            .map_err(|e| RuntimePluginManagerError::PluginLoadError(e.to_string()))?;
        (Box::from_raw(constructor()), lib)
    };

    Ok((plugin, lib, config_file))
}
