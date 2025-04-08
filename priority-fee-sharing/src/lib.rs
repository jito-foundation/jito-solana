pub mod error;
pub mod fee_records;
pub mod leader_stats;

use anyhow::Result;
use fee_records::{FeeRecordEntry, FeeRecordState, FeeRecords};
use log::{error, info};
use solana_client::rpc_config::RpcSendTransactionConfig;
use solana_pubkey::Pubkey;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::instruction::Instruction;
use solana_sdk::native_token::lamports_to_sol;
use solana_sdk::reward_type::RewardType;
use solana_sdk::signature::Keypair;
use solana_sdk::signer::Signer;
use solana_sdk::transaction::Transaction;
use spl_memo::build_memo;
use std::time::Duration;
use tokio::time::sleep;

// 1s/block, 4 leader blocks in a row
const LEADER_SLOT_MS: u64 = 1000 * 4;

async fn sleep_ms(ms: u64) {
    sleep(Duration::from_millis(ms)).await;
}

async fn delay_past_leader_slot(rpc_client: &RpcClient, fee_records: &FeeRecords) -> Result<()> {
    loop {
        let epoch_info = rpc_client
            .get_epoch_info_with_commitment(CommitmentConfig::finalized())
            .await?;
        if fee_records.does_record_exsist(epoch_info.absolute_slot, epoch_info.epoch) {
            sleep_ms(LEADER_SLOT_MS).await;
            continue;
        }
        break;
    }

    Ok(())
}

async fn handle_epoch_and_leader_slot(
    rpc_client: &RpcClient,
    fee_records: &FeeRecords,
    validator_address: &Pubkey,
    running_epoch: u64,
) -> Result<(u64, u64)> {
    let epoch_info = rpc_client
        .get_epoch_info_with_commitment(CommitmentConfig::finalized())
        .await?;

    if running_epoch == epoch_info.epoch {
        return Ok((running_epoch, epoch_info.absolute_slot));
    }

    let leader_schedule = rpc_client
        .get_leader_schedule_with_commitment(
            Some(epoch_info.absolute_slot),
            CommitmentConfig::finalized(),
        )
        .await?
        .ok_or(anyhow::anyhow!(
            "Leader schedule for slot {} not available",
            epoch_info.absolute_slot
        ))?;

    let validator_slots =
        leader_schedule
            .get(&validator_address.to_string())
            .ok_or(anyhow::anyhow!(
                "No leader slots found for {}",
                validator_address
            ))?;

    for slot in validator_slots {
        let result = fee_records.add_priority_fee_record(*slot as u64, epoch_info.epoch);
        if let Err(err) = result {
            eprintln!(
                "Error adding priority fee record for slot {}: {}",
                slot, err
            );
        }
    }

    Ok((epoch_info.epoch, epoch_info.absolute_slot))
}

async fn handle_unprocessed_blocks(
    rpc_client: &RpcClient,
    fee_records: &FeeRecords,
    running_slot: u64,
    call_limit: usize,
) -> Result<()> {
    let records = fee_records.get_records_by_state(FeeRecordState::Unprocessed)?;

    delay_past_leader_slot(rpc_client, fee_records).await?;

    for record in records.iter().take(call_limit) {
        // Try to fetch block and update
        if record.slot <= running_slot {
            let block_result = rpc_client.get_block(record.slot).await;

            match block_result {
                Ok(block) => {
                    let priority_fee_lamports = block
                        .rewards
                        .iter()
                        .find(|r| r.reward_type == Some(RewardType::Fee))
                        .map(|r| r.lamports)
                        .unwrap_or(0);

                    info!(
                        "Recorded Priority Fees for {}: {}",
                        record.slot, priority_fee_lamports
                    );
                    let result = fee_records.process_record(
                        record.slot,
                        record.epoch,
                        priority_fee_lamports as u64,
                    );
                    if let Err(err) = result {
                        eprintln!(
                            "Error processing priority fee record for slot {}: {}",
                            record.slot, err
                        );
                    }
                }
                Err(e) => {
                    error!("Could not get block, {}", e);
                    let result = fee_records.skip_record(record.slot, record.epoch);
                    if let Err(err) = result {
                        eprintln!(
                            "Error skipping priority fee record for slot {}: {}",
                            record.slot, err
                        );
                    }
                }
            }
        }
    }

    Ok(())
}

async fn handle_pending_blocks(
    rpc_client: &RpcClient,
    fee_records: &FeeRecords,
    payer_keypair: &Keypair,
    validator_address: &Pubkey,
    chunk_size: usize,
    call_limit: usize,
) -> Result<()> {
    let records = fee_records.get_records_by_state(FeeRecordState::ProcessedAndPending)?;

    delay_past_leader_slot(rpc_client, fee_records).await?;

    let blockhash = rpc_client.get_latest_blockhash().await?;

    for record_chunk in records.chunks(chunk_size).take(call_limit) {
        // Try to send transactions
        let mut ixs: Vec<Instruction> = vec![];
        let mut records: Vec<FeeRecordEntry> = vec![];

        for record in record_chunk {
            let memo_text = format!(
                "Transfer {} lamports from {} for epoch {}",
                lamports_to_sol(record.priority_fee_lamports),
                validator_address,
                record.epoch
            );
            let memo_ix = build_memo(memo_text.as_bytes(), &[]); // No signer for the memo
            ixs.push(memo_ix);
            records.push(record.clone());
        }

        // Create TX
        let tx = Transaction::new_signed_with_payer(
            &ixs,
            Some(&payer_keypair.pubkey()),
            &[payer_keypair],
            blockhash,
        );

        let result = rpc_client
            .send_transaction_with_config(
                &tx,
                RpcSendTransactionConfig {
                    skip_preflight: true,
                    ..Default::default()
                },
            )
            .await;

        match result {
            Ok(sig) => {
                info!("Transaction sent: {}", sig);

                for record in records {
                    let result =
                        fee_records.complete_record(record.slot, record.epoch, &sig.to_string(), 0);
                    if let Err(err) = result {
                        eprintln!(
                            "Error processing priority fee record for slot {}: {}",
                            record.slot, err
                        );
                    }
                }
            }
            Err(err) => error!("Failed to send transaction: {:?}", err),
        }
    }

    Ok(())
}

/// Main function for sharing priority fees
pub async fn share_priority_fees_loop(
    rpc_client: &RpcClient,
    fee_records: &FeeRecords,
    payer_keypair: &Keypair,
    validator_address: &Pubkey,
    priority_fee_distribution_program: Pubkey,
    commission_bps: u64,
    minimum_balance: u64,
    chunk_size: usize,
    call_limit: usize,
) -> Result<()> {
    let mut running_epoch = 0;
    let mut running_slot = 0;

    loop {
        // 1. Handle Epoch
        let result = handle_epoch_and_leader_slot(
            &rpc_client,
            &fee_records,
            validator_address,
            running_epoch,
        )
        .await;
        match result {
            Ok((epoch, slot)) => {
                running_epoch = epoch;
                running_slot = slot;
            }
            Err(err) => eprintln!("Error handling epoch and leader slots: {}", err),
        }

        // 2. Handle unprocessed blocks
        let result =
            handle_unprocessed_blocks(&rpc_client, &fee_records, running_slot, call_limit).await;
        if let Err(err) = result {
            eprintln!("Error handling unprocessed records: {}", err);
        }

        // 3. Handle pending blocks
        let result = handle_pending_blocks(
            &rpc_client,
            &fee_records,
            payer_keypair,
            validator_address,
            chunk_size,
            call_limit,
        )
        .await;
        if let Err(err) = result {
            eprintln!("Error handling pending blocks: {}", err);
        }

        sleep_ms(LEADER_SLOT_MS).await;
    }
}
