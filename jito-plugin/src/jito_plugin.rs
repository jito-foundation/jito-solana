use {
    solana_runtime::bank_forks::BankForks,
    std::{
        any::Any,
        error,
        fmt::Debug,
        io,
        sync::{Arc, RwLock},
    },
    thiserror::Error,
};

pub type Result<T> = std::result::Result<T, JitoPluginError>;

/// Errors returned by plugin calls
#[derive(Error, Debug)]
pub enum JitoPluginError {
    /// Error opening the configuration file; for example, when the file
    /// is not found or when the validator process has no permission to read it.
    #[error("Error opening config file. Error detail: ({0}).")]
    ConfigFileOpenError(#[from] io::Error),

    /// Any custom error defined by the plugin.
    #[error("Plugin-defined custom error. Error message: ({0})")]
    Custom(Box<dyn error::Error + Send + Sync>),
}

pub struct PluginDependencies {
    pub bank_forks: Arc<RwLock<BankForks>>,
}

pub trait JitoPlugin: Any + Debug + Send + Sync {
    fn name(&self) -> &'static str;
    fn on_load(&mut self, config_file: &str, dependencies: PluginDependencies) -> Result<()>;
    fn on_unload(&mut self);
}
