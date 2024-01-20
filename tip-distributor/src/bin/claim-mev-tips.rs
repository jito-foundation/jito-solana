//! This binary claims MEV tips.
use {
    clap::Parser,
    futures::future::join_all,
    gethostname::gethostname,
    log::*,
    solana_metrics::{datapoint_error, datapoint_info, set_host_id},
    solana_sdk::{
        pubkey::Pubkey,
        signature::{read_keypair_file, Keypair},
    },
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

    /// Limits how long before send loop runs before stopping
    #[arg(long, env, default_value_t = 60 * 60)]
    max_retry_duration_secs: u64,

    /// Specifies whether to reclaim any rent.
    #[arg(long, env, default_value_t = true)]
    should_reclaim_rent: bool,

    /// Specifies whether to reclaim rent on behalf of validators from respective TDAs.
    #[arg(long, env)]
    should_reclaim_tdas: bool,

    /// The price to pay for priority fee
    #[arg(long, env, default_value_t = 1)]
    micro_lamports: u64,
}

async fn start_mev_claim_process(
    merkle_trees: GeneratedMerkleTreeCollection,
    rpc_url: String,
    tip_distribution_program_id: Pubkey,
    signer: Arc<Keypair>,
    max_loop_duration: Duration,
    micro_lamports: u64,
) -> Result<(), ClaimMevError> {
    let start = Instant::now();

    match claim_mev_tips(
        &merkle_trees,
        rpc_url,
        tip_distribution_program_id,
        signer,
        max_loop_duration,
        micro_lamports,
    )
    .await
    {
        Err(e) => {
            datapoint_error!(
                "claim_mev_workflow-claim_error",
                ("epoch", merkle_trees.epoch, i64),
                ("error", 1, i64),
                ("err_str", e.to_string(), String),
                ("elapsed_us", start.elapsed().as_micros(), i64),
            );
            Err(e)
        }
        Ok(()) => {
            datapoint_info!(
                "claim_mev_workflow-claim_completion",
                ("epoch", merkle_trees.epoch, i64),
                ("elapsed_us", start.elapsed().as_micros(), i64),
            );
            Ok(())
        }
    }
}

async fn start_rent_claim(
    rpc_url: String,
    tip_distribution_program_id: Pubkey,
    signer: Arc<Keypair>,
    max_loop_duration: Duration,
    should_reclaim_tdas: bool,
    micro_lamports: u64,
    epoch: u64,
) -> Result<(), ClaimMevError> {
    let start = Instant::now();
    match reclaim_rent(
        rpc_url,
        tip_distribution_program_id,
        signer,
        max_loop_duration,
        should_reclaim_tdas,
        micro_lamports,
    )
    .await
    {
        Err(e) => {
            datapoint_error!(
                "claim_mev_workflow-reclaim_rent_error",
                ("epoch", epoch, i64),
                ("error", 1, i64),
                ("err_str", e.to_string(), String),
                ("elapsed_us", start.elapsed().as_micros(), i64),
            );
            Err(e)
        }
        Ok(()) => {
            datapoint_info!(
                "claim_mev_workflow-reclaim_rent_completion",
                ("epoch", epoch, i64),
                ("elapsed_us", start.elapsed().as_micros(), i64),
            );
            Ok(())
        }
    }
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
    let max_loop_duration = Duration::from_secs(args.max_retry_duration_secs);

    info!(
        "Starting to claim mev tips for epoch: {}",
        merkle_trees.epoch
    );
    let epoch = merkle_trees.epoch;

    let mut futs = vec![];
    futs.push(tokio::spawn(start_mev_claim_process(
        merkle_trees,
        args.rpc_url.clone(),
        args.tip_distribution_program_id,
        keypair.clone(),
        max_loop_duration,
        args.micro_lamports,
    )));
    if args.should_reclaim_rent {
        futs.push(tokio::spawn(start_rent_claim(
            args.rpc_url.clone(),
            args.tip_distribution_program_id,
            keypair.clone(),
            max_loop_duration,
            args.should_reclaim_tdas,
            args.micro_lamports,
            epoch,
        )));
    }
    let results = join_all(futs).await;
    solana_metrics::flush(); // sometimes last datapoint doesn't get emitted. this increases likelihood.
    for r in results {
        r.map_err(|e| ClaimMevError::UncaughtError { e: e.to_string() })??;
    }
    Ok(())
}
