//! This binary claims MEV tips.

use {
    clap::Parser,
    log::*,
    solana_sdk::pubkey::Pubkey,
    solana_tip_distributor::claim_mev_workflow::claim_mev_tips,
    std::{path::PathBuf, time::Duration},
};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to JSON file containing the [GeneratedMerkleTreeCollection] object.
    #[arg(long, env)]
    merkle_trees_path: PathBuf,

    /// RPC to send transactions through
    #[arg(long, env, default_value = "http://localhost:8899")]
    rpc_url: String,

    /// Tip distribution program ID
    #[arg(long, env)]
    tip_distribution_program_id: Pubkey,

    /// Path to keypair
    #[arg(long, env)]
    keypair_path: PathBuf,

    /// Number of unique connections to the RPC server
    #[arg(long, env, default_value_t = 100)]
    rpc_connection_count: u64,

    #[arg(long, env, default_value_t = 5)]
    max_loop_retries: u64,

    /// Limits how long before send loop runs before stopping. Defaults to 10 mins
    #[arg(long, env, default_value_t = 10*60)]
    max_loop_duration_secs: u64,

    /// Rate-limits the maximum number of requests per RPC connection
    #[arg(long, env, default_value_t = 200)]
    max_concurrent_rpc_reqs: usize,
}

#[tokio::main]
async fn main() {
    env_logger::init();
    let args: Args = Args::parse();
    info!("Starting to claim mev tips...");

    if let Err(e) = claim_mev_tips(
        &args.merkle_trees_path,
        args.rpc_url,
        args.rpc_connection_count,
        &args.tip_distribution_program_id,
        &args.keypair_path,
        args.max_loop_retries,
        Duration::from_secs(args.max_loop_duration_secs),
        args.max_concurrent_rpc_reqs,
    )
    .await
    {
        panic!("error claiming mev tips: {e:?}");
    }
    info!(
        "done claiming mev tips from file {:?}",
        args.merkle_trees_path
    );
}
