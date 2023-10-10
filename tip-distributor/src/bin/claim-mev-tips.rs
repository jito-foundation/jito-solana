//! This binary claims MEV tips.

use solana_metrics::{datapoint_error, datapoint_info};
use {
    clap::Parser,
    log::*,
    solana_sdk::pubkey::Pubkey,
    solana_tip_distributor::claim_mev_workflow::claim_mev_tips,
    std::{
        path::PathBuf,
        time::{Duration, Instant},
    },
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

    /// Number of unique connections to the RPC server for sending txns
    #[arg(long, env, default_value_t = 100)]
    rpc_send_connection_count: u64,

    /// Rate-limits the maximum number of requests per RPC connection
    #[arg(long, env, default_value_t = 200)]
    max_concurrent_rpc_get_reqs: usize,

    #[arg(long, env, default_value_t = 5)]
    max_loop_retries: u64,

    /// Limits how long before send loop runs before stopping. Defaults to 10 mins
    #[arg(long, env, default_value_t = 10*60)]
    max_loop_duration_secs: u64,
}

#[tokio::main]
async fn main() {
    env_logger::init();
    let args: Args = Args::parse();
    info!("Starting to claim mev tips...");
    let start = Instant::now();

    match claim_mev_tips(
        &args.merkle_trees_path,
        args.rpc_url,
        args.rpc_send_connection_count,
        args.max_concurrent_rpc_get_reqs,
        &args.tip_distribution_program_id,
        &args.keypair_path,
        args.max_loop_retries,
        Duration::from_secs(args.max_loop_duration_secs),
    )
    .await
    {
        Err(e) => datapoint_error!(
            "claim_mev_workflow-claim_error",
            ("error", 1, i64),
            ("err_str", e.to_string(), String),
            (
                "merkle_trees_path",
                args.merkle_trees_path.to_string_lossy(),
                String
            ),
            ("latency_us", start.elapsed().as_micros(), i64),
        ),
        Ok(()) => datapoint_info!(
            "claim_mev_workflow-claim_completion",
            (
                "merkle_trees_path",
                args.merkle_trees_path.to_string_lossy(),
                String
            ),
            ("latency_us", start.elapsed().as_micros(), i64),
        ),
    }
}
