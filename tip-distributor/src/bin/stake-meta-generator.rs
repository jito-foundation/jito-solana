//! This binary is responsible for generating a JSON file that contains meta-data about stake
//! & delegations given a ledger snapshot directory. The JSON file is structured as an array
//! of [StakeMeta] objects.

use {
    clap::Parser,
    log::*,
    solana_sdk::{clock::Slot, pubkey::Pubkey},
    solana_tip_distributor::{self, stake_meta_generator_workflow::generate_stake_meta},
    std::{
        fs::{self},
        path::PathBuf,
        process::exit,
    },
};

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Ledger path, where you created the snapshot.
    #[clap(long, env, value_parser = Args::ledger_path_parser)]
    ledger_path: PathBuf,

    /// The tip-distribution program id.
    #[clap(long, env)]
    tip_distribution_program_id: Pubkey,

    /// The tip-payment program id.
    #[clap(long, env)]
    tip_payment_program_id: Pubkey,

    /// Path to JSON file populated with the [StakeMetaCollection] object.
    #[clap(long, env)]
    out_path: String,

    /// The expected snapshot slot.
    #[clap(long, env)]
    snapshot_slot: Slot,
}

impl Args {
    fn ledger_path_parser(ledger_path: &str) -> Result<PathBuf, &'static str> {
        Ok(fs::canonicalize(ledger_path).unwrap_or_else(|err| {
            error!("Unable to access ledger path '{}': {}", ledger_path, err);
            exit(1);
        }))
    }
}

fn main() {
    env_logger::init();
    info!("Starting stake-meta-generator...");

    let args: Args = Args::parse();

    if let Err(e) = generate_stake_meta(
        &args.ledger_path,
        &args.snapshot_slot,
        &args.tip_distribution_program_id,
        &args.out_path,
        &args.tip_payment_program_id,
    ) {
        error!("error producing stake-meta: {:?}", e);
    } else {
        info!("produced stake meta");
    }
}
