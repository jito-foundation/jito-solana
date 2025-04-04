pub mod error;
pub mod fee_records;
pub mod leader_stats;

use anyhow::Result;
use log::info;
use solana_clock::DEFAULT_MS_PER_SLOT;
use solana_pubkey::Pubkey;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::signature::{Keypair, Signer};
use std::cmp::min;
use std::time::Duration;
use tokio::time::sleep;

use crate::leader_stats::LeaderStats;

/// Main function for sharing priority fees
pub async fn share_priority_fees_loop(
    keypair: &Keypair,
    rpc_client: &RpcClient,
    refresh_interval: Duration,
    commission_bps: u64,
    minimum_balance: u64,
    priority_fee_distribution_program: Pubkey,
    csv_path: Option<String>,
) -> Result<()> {
    let csv_path = csv_path.unwrap_or_else(|| "/tmp/priority_fees.csv".to_string());
    let mut leader_stats = LeaderStats::populate(keypair.pubkey(), rpc_client).await?;

    Ok(())

    // let fee_records = FeeRecordManager::new(csv_path.clone());
    // fee_records.Ok(())
    //

    // loop {
    //     let epoch_info = rpc_client
    //         .get_epoch_info_with_commitment(CommitmentConfig::finalized())
    //         .await?;

    //     // Handle epoch rollover
    //     if epoch_info.epoch != leader_stats.epoch {
    //         // TODO: handle epoch change w/ any last minute transfers
    //         leader_stats = LeaderStats::populate(keypair.pubkey(), rpc_client).await?;
    //     }

    //     // Refresh the fees for any unprocessed blocks
    //     leader_stats.refresh_unprocessed_blocks(rpc_client).await?;

    //     // Calculate how many lamports should be in the tip distribution account for priority fees
    //     // based on the total fees this epoch and the validator commission
    //     let total_fees_this_epoch = leader_stats.total_fees_this_epoch();
    //     let expected_lamports = total_fees_this_epoch * (10_000 - commission_bps) / 10_000;

    //     // don't transfer too many out to ensure there's enough to cover voting fees
    //     let validator_lamports = rpc_client
    //         .get_account_with_commitment(&keypair.pubkey(), CommitmentConfig::finalized())
    //         .await?
    //         .value
    //         .ok_or_else(|| anyhow::anyhow!("Failed to get account"))?
    //         .lamports;

    //     info!(
    //         "Epoch: {}, Total fees: {}, Expected distribution: {}, Validator balance: {}",
    //         epoch_info.epoch, total_fees_this_epoch, expected_lamports, validator_lamports
    //     );

    //     // TODO: Implement distribution logic
    //     // If validator_lamports - expected_lamports > minimum_balance,
    //     // then transfer expected_lamports to priority_fee_distribution_program

    //     // sleep for minimum of refresh_interval or time left in epoch (with some safety buffer)
    //     let num_slots_left_in_epoch = epoch_info.slots_in_epoch - epoch_info.slot_index;
    //     let time_left_ms = num_slots_left_in_epoch * DEFAULT_MS_PER_SLOT * 9 / 10;
    //     sleep(min(refresh_interval, Duration::from_millis(time_left_ms))).await;
    // }
}
