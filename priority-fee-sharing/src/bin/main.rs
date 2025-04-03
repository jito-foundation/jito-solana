use clap::Parser;
use log::info;
use solana_clock::{Epoch, Slot, DEFAULT_MS_PER_SLOT};
use solana_pubkey::Pubkey;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::reward_type::RewardType;
use solana_sdk::signature::{read_keypair_file, Keypair, Signer};
use std::cmp::min;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;
use tokio::time::{interval, sleep, Interval};

//TODO
// Add in File IO for tracking TXs as well as recovery
// Add in recovery look that looks at validator history
// Make it so it can be run as a CLI or a subroutine of the validator

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Args {
    /// RPC URL to use
    #[arg(long)]
    url: String,

    /// Path to identity keypair
    #[arg(long)]
    keypair: PathBuf,

    /// Priority fee distribution program
    #[arg(long)]
    priority_fee_distribution_program: Pubkey,

    /// How frequently to check for new priority fees and distribute them
    #[arg(long)]
    period_seconds: u64,

    /// The minimum balance that should be left in the keypair to ensure it has
    /// enough to cover voting costs
    #[arg(long)]
    minimum_balance: u64,

    /// The commission rate for validators in bips. A 100% commission (10_000 bips)
    /// would mean the validator takes all the fees for themselves. That's the default
    /// value for safety reasons.
    #[arg(long, default_value_t = 10_000)]
    commission_bps: u64,
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let args: Args = Args::parse();

    let keypair = read_keypair_file(&args.keypair).unwrap(); // todo map error
    let rpc = RpcClient::new(args.url);

    share_priority_fees_loop(
        &keypair,
        &rpc,
        Duration::from_secs(args.period_seconds),
        args.commission_bps,
    )
    .await
}
