pub mod error;
pub mod fee_records;
pub mod leader_stats;

use anyhow::Result;
use fee_records::{FeeRecordState, FeeRecords};
use log::info;
use solana_clock::DEFAULT_MS_PER_SLOT;
use solana_pubkey::Pubkey;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::epoch_info::EpochInfo;
use solana_sdk::signature::{Keypair, Signer};
use std::cmp::min;
use std::time::Duration;
use tokio::time::sleep;

use crate::leader_stats::LeaderStats;

async fn sleep_ms(ms: u64) {
    sleep(Duration::from_millis(ms)).await;
}

async fn handle_epoch(
    rpc_client: &RpcClient,
    fee_records: &FeeRecords,
    validator_address: &Pubkey,
    running_epoch: u64,
) -> Result<(u64, u64)> {
    // Startup
    let epoch = rpc_client
        .get_epoch_info_with_commitment(CommitmentConfig::finalized())
        .await?;

    if running_epoch == epoch.epoch {
        return Ok((running_epoch, epoch.absolute_slot));
    }

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

    let validator_slots =
        leader_schedule
            .get(&validator_address.to_string())
            .ok_or(anyhow::anyhow!(
                "No leader slots found for {}",
                validator_address
            ))?;

    for slot in validator_slots {
        let result = fee_records.add_priority_fee_record(*slot as u64);
        if let Err(err) = result {
            eprintln!(
                "Error adding priority fee record for slot {}: {}",
                slot, err
            );
        }
    }

    Ok((epoch.epoch, epoch.absolute_slot))
}

async fn handle_unprocessed_blocks(
    rpc_client: &RpcClient,
    fee_records: &FeeRecords,
    validator_address: &Pubkey,
    running_slot: u64,
) -> Result<()> {
    let records = fee_records.get_records_by_state(FeeRecordState::Unprocessed)?;

    for record in records {
        // Try to fetch block and update
        if record.slot < running_slot {}
    }

    Ok(())
}

async fn handle_pending_blocks(
    rpc_client: &RpcClient,
    fee_records: &FeeRecords,
    validator_address: &Pubkey,
    running_epoch: u64,
    chunk_size: usize,
) -> Result<()> {
    let records = fee_records.get_records_by_state(FeeRecordState::ProcessedAndPending)?;

    for record_chunk in records.chunks(chunk_size) {
        // Try to send transactions
        // let ixs = vec![];
        for record in record_chunk {
            // ixs.push
        }

        // Send transactions
        // let result = rpc_client.send_transaction - skip preflight
    }

    Ok(())
}

/// Main function for sharing priority fees
pub async fn share_priority_fees_loop(
    rpc_url: &String,
    payer_keypair: &Keypair,
    validator_address: &Pubkey,
    commission_bps: u64,
    minimum_balance: u64,
    priority_fee_distribution_program: Pubkey,
    fee_record_db_path: String,
    chunk_size: usize,
) -> Result<()> {
    let rpc_client = RpcClient::new(rpc_url.clone());
    let fee_records = FeeRecords::new(fee_record_db_path.clone())?;

    let mut running_epoch = 0;
    let mut running_slot = 0;

    loop {
        // 1. Handle Epoch
        let result =
            handle_epoch(&rpc_client, &fee_records, validator_address, running_epoch).await;
        match result {
            Ok((epoch, slot)) => {
                running_epoch = epoch;
                running_slot = slot;
            }
            Err(err) => eprintln!("Error handling epoch: {}", err),
        }

        // 2. Handle unprocessed blocks
        let result =
            handle_unprocessed_blocks(&rpc_client, &fee_records, validator_address, running_slot)
                .await;
        if let Err(err) = result {
            eprintln!("Error handling unprocessed records: {}", err);
        }

        // 3. Handle pending blocks
        let result = handle_pending_blocks(
            &rpc_client,
            &fee_records,
            validator_address,
            running_epoch,
            chunk_size,
        )
        .await;
        if let Err(err) = result {
            eprintln!("Error handling pending blocks: {}", err);
        }

        sleep_ms(1000).await;
    }
}

// /// Main function for sharing priority fees
// pub async fn share_priority_fees_loop(
//     keypair: &Keypair,
//     rpc_client: &RpcClient,
//     refresh_interval: Duration,
//     commission_bps: u64,
//     minimum_balance: u64,
//     priority_fee_distribution_program: Pubkey,
//     csv_path: Option<String>,
// ) -> Result<()> {
//     let csv_path = csv_path.unwrap_or_else(|| "/tmp/priority_fees.csv".to_string());
//     let mut leader_stats = LeaderStats::populate(keypair.pubkey(), rpc_client).await?;
//     let fee_records = FeeRecordManager::new(csv_path.clone());

//     loop {
//         let epoch_info = rpc_client
//             .get_epoch_info_with_commitment(CommitmentConfig::finalized())
//             .await?;

//         // Handle epoch rollover
//         if epoch_info.epoch != leader_stats.epoch {
//             // TODO: handle epoch change w/ any last minute transfers
//             leader_stats = LeaderStats::populate(keypair.pubkey(), rpc_client).await?;
//         }

//         // Refresh the fees for any unprocessed blocks
//         leader_stats.refresh_unprocessed_blocks(rpc_client).await?;

//         // Calculate how many lamports should be in the tip distribution account for priority fees
//         // based on the total fees this epoch and the validator commission
//         let total_fees_this_epoch = leader_stats.total_fees_this_epoch();
//         let expected_lamports = total_fees_this_epoch * (10_000 - commission_bps) / 10_000;

//         // don't transfer too many out to ensure there's enough to cover voting fees
//         let validator_lamports = rpc_client
//             .get_account_with_commitment(&keypair.pubkey(), CommitmentConfig::finalized())
//             .await?
//             .value
//             .ok_or_else(|| anyhow::anyhow!("Failed to get account"))?
//             .lamports;

//         info!(
//             "Epoch: {}, Total fees: {}, Expected distribution: {}, Validator balance: {}",
//             epoch_info.epoch, total_fees_this_epoch, expected_lamports, validator_lamports
//         );

//         // TODO: Implement distribution logic
//         // If validator_lamports - expected_lamports > minimum_balance,
//         // then transfer expected_lamports to priority_fee_distribution_program

//         // sleep for minimum of refresh_interval or time left in epoch (with some safety buffer)
//         let num_slots_left_in_epoch = epoch_info.slots_in_epoch - epoch_info.slot_index;
//         let time_left_ms = num_slots_left_in_epoch * DEFAULT_MS_PER_SLOT * 9 / 10;
//         sleep(min(refresh_interval, Duration::from_millis(time_left_ms))).await;
//     }
// }
