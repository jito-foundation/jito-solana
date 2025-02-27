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

enum BlockStats {
    Unprocessed,
    Skipped,
    Processed { priority_fee_lamports: u64 },
}

struct LeaderStats {
    leader: Pubkey,
    epoch: Epoch,
    block_stats: BTreeMap<u64, BlockStats>,
}

impl LeaderStats {
    pub(crate) async fn refresh_unprocessed_blocks(
        &mut self,
        rpc: &RpcClient,
    ) -> Result<(), anyhow::Error> {
        // find any unprocessed blocks since the last refresh
        let finalized_slot = rpc
            .get_slot_with_commitment(CommitmentConfig::finalized())
            .await?;
        let unprocessed_slots = self
            .block_stats
            .iter()
            .filter(|(slot, block_stats)| {
                matches!(block_stats, BlockStats::Unprocessed) && **slot <= finalized_slot
            })
            .map(|(slot, _)| *slot)
            .collect::<Vec<_>>();
        if unprocessed_slots.is_empty() {
            return Ok(());
        }

        info!(
            "fetching {} slots between {} and {}",
            unprocessed_slots.len(),
            unprocessed_slots.first().unwrap(),
            unprocessed_slots.last().unwrap()
        );
        for slot in unprocessed_slots {
            // TODO: handle missing block and/or RPC error differently with BlockStats::Skipped
            let block = rpc.get_block(slot).await?;
            let fees = block
                .rewards
                .iter()
                .find(|r| r.reward_type == Some(RewardType::Fee))
                .unwrap()
                .lamports;
            *self.block_stats.get_mut(&slot).unwrap() = BlockStats::Processed {
                priority_fee_lamports: fees as u64,
            };
        }
        Ok(())
    }

    /// Returns the total fees earned from block rewards this epoch
    fn total_fees_this_epoch(&self) -> u64 {
        self.block_stats
            .iter()
            .filter_map(|(_, block_stats)| match block_stats {
                BlockStats::Processed {
                    priority_fee_lamports,
                } => Some(*priority_fee_lamports),
                _ => None,
            })
            .sum()
    }
}

impl LeaderStats {
    async fn populate(leader: Pubkey, rpc_client: &RpcClient) -> Result<Self, anyhow::Error> {
        let epoch = rpc_client
            .get_epoch_info_with_commitment(CommitmentConfig::finalized())
            .await?;

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
            .get(&leader.to_string())
            .ok_or(anyhow::anyhow!("No leader slots found for {}", leader))?;

        Ok(LeaderStats {
            leader,
            epoch: epoch.epoch,
            block_stats: BTreeMap::from_iter(
                my_slots
                    .into_iter()
                    .map(|slot| (*slot as u64 + epoch.slot_index, BlockStats::Unprocessed)),
            ),
        })
    }
}

async fn share_priority_fees_loop(
    keypair: &Keypair,
    rpc_client: &RpcClient,
    refresh_interval: Duration,
    commission_bps: u64,
) -> Result<(), anyhow::Error> {
    let mut leader_stats = LeaderStats::populate(keypair.pubkey(), rpc_client).await?;

    loop {
        let epoch_info = rpc_client
            .get_epoch_info_with_commitment(CommitmentConfig::finalized())
            .await?;

        // Handle epoch rollover
        if epoch_info.epoch != leader_stats.epoch {
            // TODO: handle epoch change w/ any last minute transfers
            leader_stats = LeaderStats::populate(keypair.pubkey(), rpc_client).await?;
        }

        // Refresh the fees for any unprocessed blocks
        leader_stats.refresh_unprocessed_blocks(rpc_client).await?;

        // Calculate how many lamports should be in the tip distribution account for priority fees
        // based on the total fees this epoch and the validator commission
        let total_fees_this_epoch = leader_stats.total_fees_this_epoch();
        let expected_lamports = total_fees_this_epoch * (10_000 - commission_bps) / 10_000;

        // don't transfer too many out to ensure there's enough to cover voting fees
        let validator_lamports = rpc_client
            .get_account_with_commitment(&keypair.pubkey(), CommitmentConfig::finalized())
            .await?
            .value
            .unwrap() // TODO: don't unwrap
            .lamports;

        // TODO (LB): read the tip distribution account and see how many lamports are in there
        // If it doesn't exist, need to create it and fund it with the minimum balance +

        // sleep for minimum of refresh_interval or time left in epoch (with some safety buffer)
        let num_slots_left_in_epoch = epoch_info.slots_in_epoch - epoch_info.slot_index;
        let time_left_ms = num_slots_left_in_epoch * DEFAULT_MS_PER_SLOT * 9 / 10;
        sleep(min(refresh_interval, Duration::from_millis(time_left_ms))).await;
    }
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
