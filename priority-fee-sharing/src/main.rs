use clap::Parser;
use log::info;
use solana_clock::{Epoch, Slot};
use solana_pubkey::Pubkey;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::reward_type::RewardType;
use solana_sdk::signature::{read_keypair_file, Keypair, Signer};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;
use tokio::time::{interval, Interval};

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
}

enum BlockStats {
    Unprocessed,
    Skipped,
    Processed { priority_fee_lamports: u64 },
}

struct LeaderEpochInfo {
    leader: Pubkey,
    epoch: Epoch,
    block_stats: BTreeMap<u64, BlockStats>,
}

async fn share_priority_fees_loop(
    keypair: &Keypair,
    rpc_client: &RpcClient,
    mut refresh_interval: Interval,
) -> Result<(), anyhow::Error> {
    let epoch = rpc_client
        .get_epoch_info_with_commitment(CommitmentConfig::finalized())
        .await?;

    info!("Current epoch: {}", epoch.epoch);

    // Get leader schedule for the current epoch
    let leader_schedule = rpc_client
        .get_leader_schedule_with_commitment(
            Some(epoch.absolute_slot),
            CommitmentConfig::finalized(),
        )
        .await?
        .ok_or(anyhow::anyhow!(
            "Leader schedule for slot {} not available",
            epoch.absolute_slot
        ))?;

    let my_slots = leader_schedule
        .get(&keypair.pubkey().to_string())
        .ok_or(anyhow::anyhow!(
            "No leader slots found for {}",
            keypair.pubkey()
        ))?;

    let mut leader_epoch_info = LeaderEpochInfo {
        leader: keypair.pubkey(),
        epoch: epoch.epoch,
        block_stats: BTreeMap::from_iter(
            my_slots
                .into_iter()
                .map(|slot| (*slot as u64 + epoch.slot_index, BlockStats::Unprocessed)),
        ),
    };

    loop {
        refresh_interval.tick().await;

        let finalized_slot = rpc_client
            .get_slot_with_commitment(CommitmentConfig::finalized())
            .await?;

        let unprocessed_slots = leader_epoch_info
            .block_stats
            .iter()
            .filter(|(slot, block_stats)| {
                matches!(block_stats, BlockStats::Unprocessed) && **slot <= finalized_slot
            })
            .map(|(slot, _)| *slot)
            .collect::<Vec<_>>();
        if unprocessed_slots.is_empty() {
            continue;
        }

        info!(
            "fetching {} slots between {} and {}",
            unprocessed_slots.len(),
            unprocessed_slots.first().unwrap(),
            unprocessed_slots.last().unwrap()
        );
        for slot in unprocessed_slots {
            let block = rpc_client.get_block(slot).await?; // TODO: handle missing block and/or RPC error
            let fees = block
                .rewards
                .iter()
                .find(|r| r.reward_type == Some(RewardType::Fee))
                .unwrap()
                .lamports;
            *leader_epoch_info.block_stats.get_mut(&slot).unwrap() = BlockStats::Processed {
                priority_fee_lamports: fees as u64,
            };
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let args: Args = Args::parse();

    let keypair = read_keypair_file(&args.keypair).unwrap(); // todo map error
    let rpc = RpcClient::new(args.url);
    let refresh_interval = interval(Duration::from_secs(args.period_seconds));

    share_priority_fees_loop(&keypair, &rpc, refresh_interval).await
}
