use {
    clap::Parser,
    log::info,
    solana_sdk::pubkey::Pubkey,
    solana_tip_distributor::merkle_root_upload_workflow::upload_merkle_root,
    std::{path::PathBuf, str::FromStr},
};

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Path to JSON file containing the [StakeMetaCollection] object.
    #[clap(long, env)]
    merkle_root_path: PathBuf,

    /// The path to the keypair used to sign and pay for the `upload_merkle_root` transactions.
    #[clap(long, env)]
    keypair_path: PathBuf,

    /// The RPC to send transactions to.
    #[clap(long, env)]
    rpc_url: String,

    /// Tip distribution program ID
    #[clap(long, env)]
    tip_distribution_program_id: String,
}

fn main() {
    env_logger::init();

    let args: Args = Args::parse();

    let tip_distribution_program_id = Pubkey::from_str(&args.tip_distribution_program_id)
        .expect("valid tip_distribution_program_id");

    info!("starting merkle root uploader...");
    if let Err(e) = upload_merkle_root(
        &args.merkle_root_path,
        &args.keypair_path,
        &args.rpc_url,
        &tip_distribution_program_id,
    ) {
        panic!("failed to upload merkle roots: {:?}", e);
    }
    info!(
        "uploaded merkle roots from file {:?}",
        args.merkle_root_path
    );
}
