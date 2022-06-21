//! This binary generates a merkle tree for each [TipDistributionAccount]; they are derived
//! using a user provided [StakeMetaCollection] JSON file. The roots are then uploaded to their
//! their [TipDistributionAccount] as long as the provided keypair is the
//! `merkle_root_upload_authority`. All funds minus the validator's commission are distributed
//! to delegated accounts proportional to their amounts delegated.

use {
    clap::Parser, log::*, solana_sdk::signature::read_keypair_file,
    solana_tip_distributor::merkle_root_generator_workflow::run_workflow, std::path::PathBuf,
};

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// The path to the keypair used to sign the `upload_merkle_root` transactions.
    /// This will skip uploading to accounts where `merkle_root_upload_authority`
    /// does not match this keypair.
    #[clap(long, env)]
    path_to_my_keypair: PathBuf,

    /// Path to JSON file containing the [StakeMetaCollection] object.
    #[clap(long, env)]
    stake_meta_coll_path: PathBuf,

    /// The RPC to send transactions to.
    #[clap(long, env)]
    rpc_url: String,

    /// Path to JSON file to get populated with tree node data.
    #[clap(long, env)]
    out_path: String,

    /// Indicates whether or not to actually upload roots to their respective
    /// [TipDistributionAccount]'s  
    #[clap(long, env)]
    upload_roots: bool,
}

fn main() {
    env_logger::init();
    info!("Starting merkle-root-generator workflow...");

    let args: Args = Args::parse();

    let my_keypair =
        read_keypair_file(&args.path_to_my_keypair).expect("Failed to read keypair file.");

    run_workflow(
        args.stake_meta_coll_path,
        args.out_path,
        args.rpc_url,
        my_keypair,
        args.upload_roots,
    )
    .unwrap();
}
