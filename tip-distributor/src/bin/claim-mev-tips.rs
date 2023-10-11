//! This binary claims MEV tips.
use {
    clap::Parser,
    gethostname::gethostname,
    log::*,
    solana_metrics::{datapoint_error, datapoint_info, set_host_id},
    solana_sdk::{pubkey::Pubkey, signature::read_keypair_file},
    solana_tip_distributor::{
        claim_mev_workflow::{claim_mev_tips, ClaimMevError},
        read_json_from_file,
        reclaim_rent_workflow::reclaim_rent,
        GeneratedMerkleTreeCollection,
    },
    std::{
        path::PathBuf,
        sync::Arc,
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
    #[arg(long, env, default_value_t = 128)]
    rpc_send_connection_count: u64,

    /// Rate-limits the maximum number of GET requests per RPC connection
    #[arg(long, env, default_value_t = 256)]
    max_concurrent_rpc_get_reqs: usize,

    /// Number of retries for main claim send loop. Loop is time bounded.
    #[arg(long, env, default_value_t = 5)]
    max_loop_retries: u64,

    /// Limits how long before send loop runs before stopping. Defaults to 10 mins
    #[arg(long, env, default_value_t = 10 * 60)]
    max_loop_duration_secs: u64,

    /// Specifies whether to reclaim any rent.
    #[arg(long, env, default_value_t = true)]
    should_reclaim_rent: bool,

    /// Specifies whether to reclaim rent on behalf of validators from respective TDAs.
    #[arg(long, env)]
    should_reclaim_tdas: bool,

    /// The price to pay per compute unit aka "Priority Fee".
    #[arg(long, env, default_value_t = 1)]
    micro_lamports_per_compute_unit: u64,
}

#[tokio::main]
async fn main() -> Result<(), ClaimMevError> {
    env_logger::init();
    gethostname()
        .into_string()
        .map(set_host_id)
        .expect("set hostname");
    let args: Args = Args::parse();
    let keypair = Arc::new(read_keypair_file(&args.keypair_path).expect("read keypair file"));
    let merkle_trees: GeneratedMerkleTreeCollection =
        read_json_from_file(&args.merkle_trees_path).expect("read GeneratedMerkleTreeCollection");
    let max_loop_duration = Duration::from_secs(args.max_loop_duration_secs);

    info!(
        "Starting to claim mev tips for epoch: {}",
        merkle_trees.epoch
    );
    let start = Instant::now();

    match claim_mev_tips(
        merkle_trees.clone(),
        args.rpc_url.clone(),
        args.rpc_send_connection_count,
        args.max_concurrent_rpc_get_reqs,
        &args.tip_distribution_program_id,
        keypair.clone(),
        args.max_loop_retries,
        max_loop_duration,
        args.micro_lamports_per_compute_unit,
    )
    .await
    {
        Err(e) => {
            datapoint_error!(
                "claim_mev_workflow-claim_error",
                ("epoch", merkle_trees.epoch, i64),
                ("error", 1, i64),
                ("err_str", e.to_string(), String),
                (
                    "merkle_trees_path",
                    args.merkle_trees_path.to_string_lossy(),
                    String
                ),
                ("elapsed_us", start.elapsed().as_micros(), i64),
            );
            Err(e)
        }
        Ok(()) => {
            datapoint_info!(
                "claim_mev_workflow-claim_completion",
                ("epoch", merkle_trees.epoch, i64),
                (
                    "merkle_trees_path",
                    args.merkle_trees_path.to_string_lossy(),
                    String
                ),
                ("elapsed_us", start.elapsed().as_micros(), i64),
            );
            Ok(())
        }
    }?;

    if args.should_reclaim_rent {
        let start = Instant::now();
        match reclaim_rent(
            args.rpc_url,
            args.rpc_send_connection_count,
            args.tip_distribution_program_id,
            keypair,
            args.max_loop_retries,
            max_loop_duration,
            args.should_reclaim_tdas,
            args.micro_lamports_per_compute_unit,
        )
        .await
        {
            Err(e) => {
                datapoint_error!(
                    "claim_mev_workflow-reclaim_rent_error",
                    ("epoch", merkle_trees.epoch, i64),
                    ("error", 1, i64),
                    ("err_str", e.to_string(), String),
                    (
                        "merkle_trees_path",
                        args.merkle_trees_path.to_string_lossy(),
                        String
                    ),
                    ("elapsed_us", start.elapsed().as_micros(), i64),
                );
                Err(e)
            }
            Ok(()) => {
                datapoint_info!(
                    "claim_mev_workflow-reclaim_rent_completion",
                    ("epoch", merkle_trees.epoch, i64),
                    (
                        "merkle_trees_path",
                        args.merkle_trees_path.to_string_lossy(),
                        String
                    ),
                    ("elapsed_us", start.elapsed().as_micros(), i64),
                );
                Ok(())
            }
        }?;
    }
    solana_metrics::flush(); // sometimes last datapoint doesn't get emitted. this increases likelihood.
    Ok(())
}
