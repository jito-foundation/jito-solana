use log::info;
use solana_clock::Epoch;
use solana_pubkey::Pubkey;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::reward_type::RewardType;
use std::collections::BTreeMap;

pub enum BlockStats {
    Unprocessed,
    Skipped,
    Processed { priority_fee_lamports: u64 },
}

pub struct LeaderStats {
    pub leader: Pubkey,
    pub epoch: Epoch,
    pub block_stats: BTreeMap<u64, BlockStats>,
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
    pub fn total_fees_this_epoch(&self) -> u64 {
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

    pub async fn populate(leader: Pubkey, rpc_client: &RpcClient) -> Result<Self, anyhow::Error> {
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
