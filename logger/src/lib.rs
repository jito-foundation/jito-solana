//! The `logger` module configures `env_logger`

use {
    lazy_static::lazy_static,
    std::sync::{Arc, RwLock},
};

lazy_static! {
    static ref LOGGER: Arc<RwLock<env_logger::Logger>> =
        Arc::new(RwLock::new(env_logger::Logger::from_default_env()));
}

struct LoggerShim {}

impl log::Log for LoggerShim {
    fn enabled(&self, metadata: &log::Metadata) -> bool {
        LOGGER.read().unwrap().enabled(metadata)
    }

    fn log(&self, record: &log::Record) {
        LOGGER.read().unwrap().log(record);
    }

    fn flush(&self) {}
}

fn replace_logger(logger: env_logger::Logger) {
    log::set_max_level(logger.filter());
    *LOGGER.write().unwrap() = logger;
    let _ = log::set_boxed_logger(Box::new(LoggerShim {}));
}

// Configures logging with a specific filter overriding RUST_LOG.  _RUST_LOG is used instead
// so if set it takes precedence.
// May be called at any time to re-configure the log filter
pub fn setup_with(filter: &str) {
    let logger =
        env_logger::Builder::from_env(env_logger::Env::new().filter_or("_RUST_LOG", filter))
            .format_timestamp_nanos()
            .build();
    replace_logger(logger);
}

// Configures logging with a default filter if RUST_LOG is not set
pub fn setup_with_default(filter: &str) {
    let logger = env_logger::Builder::from_env(env_logger::Env::new().default_filter_or(filter))
        .format_timestamp_nanos()
        .build();
    replace_logger(logger);
}

// Configures logging with the default filter "error" if RUST_LOG is not set
pub fn setup() {
    setup_with_default("error");
}

// Configures file logging with a default filter if RUST_LOG is not set
//
// NOTE: This does not work at the moment, pending the resolution of https://github.com/env-logger-rs/env_logger/issues/208
pub fn setup_file_with_default(_logfile: &str, _filter: &str) {
    // TODO (LB): fix Target::Pipe error
    // use std::fs::OpenOptions;
    // let file = OpenOptions::new()
    //     .write(true)
    //     .create(true)
    //     .append(true)
    //     .open(logfile)
    //     .unwrap();
    // let logger = env_logger::Builder::from_env(env_logger::Env::new().default_filter_or(filter))
    //     .format_timestamp_nanos()
    //     .target(env_logger::Target::Pipe(Box::new(file)))
    //     .build();
    // replace_logger(logger);
}
