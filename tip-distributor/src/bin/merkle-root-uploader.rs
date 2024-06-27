use {
    clap::Parser, log::info, solana_sdk::pubkey::Pubkey,
    solana_tip_distributor::merkle_root_upload_workflow::upload_merkle_root, std::path::PathBuf,
};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to JSON file containing the [StakeMetaCollection] object.
    #[arg(long, env)]
    merkle_root_path: PathBuf,

    /// The path to the keypair used to sign and pay for the `upload_merkle_root` transactions.
    #[arg(long, env)]
    keypair_path: PathBuf,

    /// The RPC to send transactions to.
    #[arg(long, env)]
    rpc_url: String,

    /// Tip distribution program ID
    #[arg(long, env)]
    tip_distribution_program_id: Pubkey,

    /// Rate-limits the maximum number of requests per RPC connection
    #[arg(long, env, default_value_t = 100)]
    max_concurrent_rpc_get_reqs: usize,

    /// Number of transactions to send to RPC at a time.
    #[arg(long, env, default_value_t = 64)]
    txn_send_batch_size: usize,

    /// Optional API key for the block engine
    #[arg(long, env)]
    block_engine_api_key: Option<String>,
}

#[tokio::main]
async fn main() {
    env_logger::init();

    let args: Args = Args::parse();

    info!("starting merkle root uploader...");
    if let Err(e) = upload_merkle_root(
        &args.merkle_root_path,
        &args.keypair_path,
        &args.rpc_url,
        &args.tip_distribution_program_id,
        args.max_concurrent_rpc_get_reqs,
        args.txn_send_batch_size,
        args.block_engine_api_key,
    )
    .await
    {
        panic!("failed to upload merkle roots: {:?}", e);
    }
    info!(
        "uploaded merkle roots from file {:?}",
        args.merkle_root_path
    );
}
