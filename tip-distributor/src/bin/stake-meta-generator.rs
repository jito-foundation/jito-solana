//! This binary is responsible for generating a JSON file that contains meta-data about stake
//! & delegations given a ledger snapshot directory. The JSON file is structured as an array
//! of [StakeMeta] objects.

use {
    clap::Parser,
    log::*,
    solana_client::rpc_client::RpcClient,
    solana_sdk::{clock::Slot, pubkey::Pubkey},
    solana_tip_distributor::{self, stake_meta_generator_workflow::run_workflow},
    std::{
        fs::{self},
        path::PathBuf,
        process::exit,
    },
};

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Ledger path.
    #[clap(long, env, parse(try_from_str = Args::ledger_path_parser))]
    ledger_path: PathBuf,

    /// The tip-distribution program id.
    #[clap(long, env)]
    tip_distribution_program_id: Pubkey,

    /// Path to JSON file populated with the [StakeMetaCollection] object.
    #[clap(long, env)]
    out_path: String,

    /// The expected base58 encoded snapshot bank hash.
    #[clap(long, env)]
    snapshot_bank_hash: String,

    /// The expected snapshot slot.
    #[clap(long, env)]
    snapshot_slot: Slot,

    /// The RPC to fetch lamports from for the tip distribution accounts.
    #[clap(long, env)]
    rpc_url: String,
}

impl Args {
    fn ledger_path_parser(ledger_path: &str) -> Result<PathBuf, &'static str> {
        Ok(fs::canonicalize(&ledger_path).unwrap_or_else(|err| {
            error!("Unable to access ledger path '{}': {}", ledger_path, err);
            exit(1);
        }))
    }
}

fn main() {
    env_logger::init();
    info!("Starting stake-meta-generator...");

    let args: Args = Args::parse();

    let rpc_client = RpcClient::new(args.rpc_url);

    run_workflow(
        &args.ledger_path,
        args.snapshot_bank_hash,
        args.snapshot_slot,
        args.tip_distribution_program_id,
        args.out_path,
        rpc_client,
    )
    .expect("Workflow failed.");
}
