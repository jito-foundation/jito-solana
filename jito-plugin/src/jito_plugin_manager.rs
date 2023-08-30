use {
    crate::jito_plugin::{JitoPlugin, PluginDependencies},
    jsonrpc_core::{serde_json, ErrorCode, Result as JsonRpcResult},
    libloading::Library,
    log::*,
    solana_runtime::bank_forks::BankForks,
    std::{
        fs::File,
        io::Read,
        path::{Path, PathBuf},
        sync::{Arc, RwLock},
    },
};

#[derive(thiserror::Error, Debug)]
pub enum JitoPluginManagerError {
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

    #[error("The Jito plugin {0} is already loaded shared library")]
    PluginAlreadyLoaded(String),

    #[error("The JitoPlugin on_load method failed")]
    PluginStartError(String),
}

pub struct JitoPluginManager {
    plugin: Option<(Box<dyn JitoPlugin>, Library)>,
    bank_forks: Arc<RwLock<BankForks>>,
}

impl JitoPluginManager {
    /// This method allows dynamic loading of the Jito plugin.
    pub fn load_plugin(
        plugin_config_path: impl AsRef<Path>,
        bank_forks: &Arc<RwLock<BankForks>>,
    ) -> JsonRpcResult<Self> {
        // First load plugin
        let (mut jito_plugin, lib, config_file) =
            load_plugin_from_config(plugin_config_path.as_ref()).map_err(|e| {
                jsonrpc_core::Error {
                    code: ErrorCode::InvalidRequest,
                    message: format!("Failed to load plugin: {e}"),
                    data: None,
                }
            })?;

        jito_plugin
            .on_load(
                config_file,
                PluginDependencies {
                    bank_forks: bank_forks.clone(),
                },
            )
            .map_err(|on_load_err| jsonrpc_core::Error {
                code: ErrorCode::InvalidRequest,
                message: format!(
                    "on_load method of plugin {} failed: {on_load_err}",
                    jito_plugin.name()
                ),
                data: None,
            })?;

        Ok(Self {
            plugin: Some((jito_plugin, lib)),
            bank_forks: bank_forks.clone(),
        })
    }

    /// Unload the plugin and loaded plugin libraries, making sure to fire
    /// their `on_plugin_unload()` methods so they can do any necessary cleanup.
    pub fn unload(&mut self) {
        self.plugin.as_mut().map(|p| p.0.on_unload());
        self.plugin = None;
    }

    pub fn reload_plugin(&mut self, config_file: &str) -> JsonRpcResult<()> {
        *self = Self::load_plugin(config_file, &self.bank_forks)?;
        Ok(())
    }
}

fn load_plugin_from_config(
    plugin_config_path: &Path,
) -> Result<(Box<dyn JitoPlugin>, Library, &str), JitoPluginManagerError> {
    type PluginConstructor = unsafe fn() -> *mut dyn JitoPlugin;
    use libloading::Symbol;

    let mut file = match File::open(plugin_config_path) {
        Ok(file) => file,
        Err(err) => {
            return Err(JitoPluginManagerError::CannotOpenConfigFile(format!(
                "Failed to open the plugin config file {plugin_config_path:?}, error: {err:?}"
            )));
        }
    };

    let mut contents = String::new();
    if let Err(err) = file.read_to_string(&mut contents) {
        return Err(JitoPluginManagerError::CannotReadConfigFile(format!(
            "Failed to read the plugin config file {plugin_config_path:?}, error: {err:?}"
        )));
    }

    let result: serde_json::Value = match json5::from_str(&contents) {
        Ok(value) => value,
        Err(err) => {
            return Err(JitoPluginManagerError::InvalidConfigFileFormat(format!(
                "The config file {plugin_config_path:?} is not in a valid Json5 format, error: {err:?}"
            )));
        }
    };

    let libpath = result["libpath"]
        .as_str()
        .ok_or(JitoPluginManagerError::LibPathNotSet)?;
    let mut libpath = PathBuf::from(libpath);
    if libpath.is_relative() {
        let config_dir = plugin_config_path.parent().ok_or_else(|| {
            JitoPluginManagerError::CannotOpenConfigFile(format!(
                "Failed to resolve parent of {plugin_config_path:?}",
            ))
        })?;
        libpath = config_dir.join(libpath);
    }

    let config_file = plugin_config_path
        .as_os_str()
        .to_str()
        .ok_or(JitoPluginManagerError::InvalidPluginPath)?;

    let (plugin, lib) = unsafe {
        let lib = Library::new(libpath)
            .map_err(|e| JitoPluginManagerError::PluginLoadError(e.to_string()))?;
        let constructor: Symbol<PluginConstructor> = lib
            .get(b"_create_plugin")
            .map_err(|e| JitoPluginManagerError::PluginLoadError(e.to_string()))?;
        (Box::from_raw(constructor()), lib)
    };

    Ok((plugin, lib, config_file))
}
