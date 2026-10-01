use {
    assert_cmd::prelude::*,
    solana_keypair::{Keypair, write_keypair_file},
    std::{fs, process::Command},
    tempfile::TempDir,
};

const DCOU_BUILD_ERROR: &str =
    "refusing to run agave-validator: compiled with dev-context-only-utils";
const DCOU_OVERRIDE_WARNING: &str = "running agave-validator with dev-context-only-utils enabled";

fn validator_command(temp_dir: &TempDir) -> Command {
    let temp_dir_path = temp_dir.path();
    let id_json_path = temp_dir_path.join("id.json");
    let id_json_str = id_json_path.to_str().unwrap();
    let ledger_path = temp_dir_path.join("ledger");
    write_keypair_file(&Keypair::new(), id_json_str).unwrap();

    let mut cmd = Command::new(assert_cmd::cargo::cargo_bin!(env!("CARGO_PKG_NAME")));
    cmd.env_remove("RUST_LOG").args([
        "--identity",
        id_json_str,
        "--ledger",
        ledger_path.to_str().unwrap(),
        "--no-voting",
        "--no-xdp",
    ]);
    cmd
}

#[test]
fn test_dcou_build_refuses_validator_operations() {
    for operation in [None, Some("run"), Some("init")] {
        let temp_dir = TempDir::new().unwrap();
        let mut cmd = validator_command(&temp_dir);
        cmd.args(["--log", "-"]).env_remove("AGAVE_ALLOW_DCOU");
        if let Some(operation) = operation {
            cmd.arg(operation);
        }
        cmd.assert()
            .failure()
            .stdout(predicates::str::contains(DCOU_BUILD_ERROR));
        assert!(!temp_dir.path().join("ledger").exists());
    }
}

#[test]
fn test_dcou_build_rejects_invalid_override() {
    for value in ["", "0", "true", "01"] {
        let temp_dir = TempDir::new().unwrap();
        let mut cmd = validator_command(&temp_dir);
        cmd.args(["--log", "-"])
            .env("AGAVE_ALLOW_DCOU", value)
            .assert()
            .failure()
            .stdout(predicates::str::contains(DCOU_BUILD_ERROR));
    }
}

#[test]
fn test_use_the_same_path_for_accounts_and_snapshots() {
    let temp_dir = TempDir::new().unwrap();
    let temp_dir_str = temp_dir.path().to_str().unwrap();
    let mut cmd = validator_command(&temp_dir);
    cmd.env("AGAVE_ALLOW_DCOU", "1").args([
        "--log",
        "-",
        "--accounts",
        temp_dir_str,
        "--snapshots",
        temp_dir_str,
    ]);
    cmd.assert().failure().stderr(predicates::str::contains(
        "the --accounts and --snapshots paths must be unique",
    ));
}

#[test]
fn test_build_warnings_are_logged() {
    let temp_dir = TempDir::new().unwrap();
    let accounts_and_snapshots_path = temp_dir.path().join("accounts-and-snapshots");
    let log_path = temp_dir.path().join("validator.log");
    let mut cmd = validator_command(&temp_dir);
    cmd.env("AGAVE_ALLOW_DCOU", "1").args([
        "--log",
        log_path.to_str().unwrap(),
        "--accounts",
        accounts_and_snapshots_path.to_str().unwrap(),
        "--snapshots",
        accounts_and_snapshots_path.to_str().unwrap(),
    ]);
    cmd.assert().failure();

    let log = fs::read_to_string(log_path).unwrap();
    if cfg!(debug_assertions) {
        assert!(log.contains("compiled with debug assertions enabled"));
    }
    assert!(log.contains(DCOU_OVERRIDE_WARNING));
}

#[test]
fn test_dcou_override_warning_ignores_rust_log() {
    let temp_dir = TempDir::new().unwrap();
    let shared_path = temp_dir.path().join("shared");
    let mut cmd = validator_command(&temp_dir);
    cmd.env("AGAVE_ALLOW_DCOU", "1")
        .env("RUST_LOG", "solana=info")
        .args([
            "--log",
            "-",
            "--accounts",
            shared_path.to_str().unwrap(),
            "--snapshots",
            shared_path.to_str().unwrap(),
        ]);
    cmd.assert()
        .failure()
        .stderr(predicates::str::contains(DCOU_OVERRIDE_WARNING));
}
