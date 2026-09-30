#![allow(clippy::arithmetic_side_effects)]
pub use solana_test_validator as test_validator;
use {
    console::style,
    fd_lock::{RwLock, RwLockWriteGuard},
    indicatif::{ProgressDrawTarget, ProgressStyle},
    std::{
        borrow::Cow,
        fmt::Display,
        fs::{File, OpenOptions},
        path::Path,
        process::exit,
        time::Duration,
    },
};

pub mod admin_rpc_service;
pub mod bootstrap;
pub mod cli;
pub mod commands;
pub mod dashboard;
pub mod shred_receiver_addresses;

const DCOU_BUILD_ERROR: &str =
    "refusing to run agave-validator: compiled with dev-context-only-utils";
const DEBUG_BUILD_WARNING: &str =
    "agave-validator was compiled with debug assertions enabled and is not suitable for production";

fn check_production_build(dev_context_only_utils: bool) -> Result<(), &'static str> {
    if dev_context_only_utils {
        Err(DCOU_BUILD_ERROR)
    } else {
        Ok(())
    }
}

#[doc(hidden)]
pub fn check_production_validator_build() -> Result<(), &'static str> {
    if cfg!(debug_assertions) {
        eprintln!("Warning: {DEBUG_BUILD_WARNING}");
    }
    check_production_build(agave_feature_set::DEV_CONTEXT_ONLY_UTILS_ENABLED)
}

pub fn format_name_value(name: &str, value: &str) -> String {
    format!("{} {}", style(name).bold(), value)
}
/// Pretty print a "name value"
pub fn println_name_value(name: &str, value: &str) {
    println!("{}", format_name_value(name, value));
}

const SPINNER_TEMPLATE: &str = "{spinner:.green} {wide_msg}";
const MULTILINE_SPINNER_TEMPLATE: &str = "{spinner:.green} {msg}";

/// Creates a new process bar for processing that will take an unknown amount of time
pub fn new_spinner_progress_bar() -> ProgressBar {
    new_spinner_progress_bar_with_template(SPINNER_TEMPLATE)
}

/// Creates a spinner that preserves multiline messages instead of truncating them to one line.
pub(crate) fn new_multiline_spinner_progress_bar() -> ProgressBar {
    new_spinner_progress_bar_with_template(MULTILINE_SPINNER_TEMPLATE)
}

fn new_spinner_progress_bar_with_template(template: &str) -> ProgressBar {
    let progress_bar = indicatif::ProgressBar::new(42);
    progress_bar.set_draw_target(ProgressDrawTarget::stdout());
    progress_bar.set_style(spinner_progress_style(template));
    progress_bar.enable_steady_tick(Duration::from_millis(100));

    ProgressBar {
        progress_bar,
        is_term: console::Term::stdout().is_term(),
    }
}

fn spinner_progress_style(template: &str) -> ProgressStyle {
    ProgressStyle::default_spinner()
        .template(template)
        .expect("ProgressStyle::template direct input to be correct")
}

pub struct ProgressBar {
    progress_bar: indicatif::ProgressBar,
    is_term: bool,
}

impl ProgressBar {
    pub fn set_message<T: Into<Cow<'static, str>> + Display>(&self, msg: T) {
        if self.is_term {
            self.progress_bar.set_message(msg);
        } else {
            println!("{msg}");
        }
    }

    pub fn println<I: AsRef<str>>(&self, msg: I) {
        self.progress_bar.println(msg);
    }

    pub fn abandon_with_message<T: Into<Cow<'static, str>> + Display>(&self, msg: T) {
        if self.is_term {
            self.progress_bar.abandon_with_message(msg);
        } else {
            println!("{msg}");
        }
    }
}

pub fn ledger_lockfile(ledger_path: &Path) -> RwLock<File> {
    let lockfile = ledger_path.join("ledger.lock");
    fd_lock::RwLock::new(
        OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(lockfile)
            .unwrap(),
    )
}

pub fn lock_ledger<'lock>(
    ledger_path: &Path,
    ledger_lockfile: &'lock mut RwLock<File>,
) -> RwLockWriteGuard<'lock, File> {
    ledger_lockfile.try_write().unwrap_or_else(|_| {
        println!(
            "Error: Unable to lock {} directory. Check if another validator is running",
            ledger_path.display()
        );
        exit(1);
    })
}

#[cfg(test)]
mod tests {
    use super::{DCOU_BUILD_ERROR, check_production_build, check_production_validator_build};

    #[test]
    fn production_build_guard_accepts_release() {
        assert_eq!(check_production_build(false), Ok(()));
    }

    #[test]
    fn production_build_guard_rejects_dcou() {
        assert_eq!(check_production_build(true), Err(DCOU_BUILD_ERROR));
    }

    #[test]
    fn production_build_guard_rejects_unified_dcou_feature() {
        assert_eq!(check_production_validator_build(), Err(DCOU_BUILD_ERROR));
    }

    #[test]
    fn production_build_guard_accepts_custom_profile() {
        // Profile names and debug symbols are intentionally irrelevant. Custom
        // profiles are safe when DCOU is absent.
        assert_eq!(check_production_build(false), Ok(()));
    }
}
