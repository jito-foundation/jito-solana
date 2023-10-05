//! This binary claims MEV tips.

use {
    clap::Parser, log::*, solana_sdk::pubkey::Pubkey,
    solana_tip_distributor::claim_mev_workflow::claim_mev_tips, std::path::PathBuf,
};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to JSON file containing the [GeneratedMerkleTreeCollection] object.
    #[arg(long, env)]
    merkle_trees_path: PathBuf,

    /// RPC to send transactions through
    #[arg(long, env)]
    rpc_url: String,

    /// Tip distribution program ID
    #[arg(long, env)]
    tip_distribution_program_id: Pubkey,

    /// Path to keypair
    #[arg(long, env)]
    keypair_path: PathBuf,
}

#[tokio::main]
async fn main() {
    env_logger::init();
    let args: Args = Args::parse();
    info!("Starting to claim mev tips...");

    if let Err(e) = claim_mev_tips(
        &args.merkle_trees_path,
        &args.rpc_url,
        &args.tip_distribution_program_id,
        &args.keypair_path,
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
