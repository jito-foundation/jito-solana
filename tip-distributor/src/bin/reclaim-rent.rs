//! Reclaims rent from TDAs and Claim Status accounts.

use {
    clap::Parser,
    log::*,
    solana_client::nonblocking::rpc_client::RpcClient,
    solana_sdk::{
        commitment_config::CommitmentConfig, pubkey::Pubkey, signature::read_keypair_file,
    },
    solana_tip_distributor::reclaim_rent_workflow::reclaim_rent,
    std::{path::PathBuf, str::FromStr, time::Duration},
    tokio::runtime::Runtime,
};

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// RPC to send transactions through.
    /// NOTE: This script uses getProgramAccounts, make sure you have added an account index
    /// for the tip_distribution_program_id on the RPC node.
    #[clap(long, env)]
    rpc_url: String,

    /// Tip distribution program ID.
    #[clap(long, env, value_parser = Pubkey::from_str)]
    tip_distribution_program_id: Pubkey,

    /// The keypair signing and paying for transactions.
    #[clap(long, env)]
    keypair_path: PathBuf,

    /// High timeout b/c of get_program_accounts call
    #[clap(long, env, default_value_t = 180)]
    rpc_timeout_secs: u64,

    /// Specifies whether to reclaim rent on behalf of validators from respective TDAs.
    #[clap(long, env)]
    should_reclaim_tdas: bool,
}

fn main() {
    env_logger::init();

    info!("Starting to claim mev tips...");
    let args: Args = Args::parse();

    let runtime = Runtime::new().unwrap();
    if let Err(e) = runtime.block_on(reclaim_rent(
        RpcClient::new_with_timeout_and_commitment(
            args.rpc_url,
            Duration::from_secs(args.rpc_timeout_secs),
            CommitmentConfig::confirmed(),
        ),
        args.tip_distribution_program_id,
        read_keypair_file(&args.keypair_path).expect("read keypair file"),
        args.should_reclaim_tdas,
    )) {
        panic!("error reclaiming rent: {e:?}");
    }

    info!("done reclaiming all rent",);
}
