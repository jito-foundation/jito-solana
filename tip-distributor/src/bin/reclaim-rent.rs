//! Reclaims rent from TDAs and Claim Status accounts.

use {
    clap::Parser,
    log::*,
    solana_client::nonblocking::rpc_client::RpcClient,
    solana_sdk::{pubkey::Pubkey, signature::read_keypair_file},
    solana_tip_distributor::reclaim_rent_workflow::reclaim_rent,
    std::{path::PathBuf, str::FromStr},
    tokio::runtime::Runtime,
};

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// RPC to send transactions through.
    #[clap(long, env)]
    rpc_url: String,

    /// Tip distribution program ID.
    #[clap(long, env)]
    tip_distribution_program_id: String,

    /// The keypair signing and paying for transactions.
    #[clap(long, env)]
    keypair_path: PathBuf,

    /// Specifies whether to reclaim rent on behalf of validators from respective TDAs.
    #[clap(long, env)]
    should_reclaim_tdas: bool,
}

fn main() {
    env_logger::init();

    info!("Starting to claim mev tips...");
    let args: Args = Args::parse();
    let tip_distribution_program_id = Pubkey::from_str(&args.tip_distribution_program_id)
        .expect("valid tip_distribution_program_id");

    let runtime = Runtime::new().unwrap();
    if let Err(e) = runtime.block_on(reclaim_rent(
        RpcClient::new(args.rpc_url),
        tip_distribution_program_id,
        read_keypair_file(&args.keypair_path).expect("read keypair file"),
        args.should_reclaim_tdas,
    )) {
        panic!("error reclaiming rent: {e:?}");
    }

    info!("done reclaiming all rent",);
}
