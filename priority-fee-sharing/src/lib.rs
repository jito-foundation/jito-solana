pub mod error;
pub mod fee_records;

use anyhow::{anyhow, Result};
use fee_records::{FeeRecordEntry, FeeRecordState, FeeRecords};
use log::{error, info};
use solana_client::rpc_config::RpcSendTransactionConfig;
use solana_pubkey::Pubkey;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::compute_budget::ComputeBudgetInstruction;
use solana_sdk::instruction::AccountMeta;
use solana_sdk::instruction::Instruction;
use solana_sdk::native_token::lamports_to_sol;
use solana_sdk::reward_type::RewardType;
use solana_sdk::signature::read_keypair_file;
use solana_sdk::signature::Keypair;
use solana_sdk::signer::Signer;
use solana_sdk::transaction::Transaction;
use std::path::PathBuf;
use std::time::Duration;
use tokio::time::sleep;

// ------------------------- GLOBAL CONSTANTS -----------------------------
// 1s/block, 4 leader blocks in a row
const LEADER_SLOT_MS: u64 = 1000 * 4;
const MAX_BPS: u64 = 10_000;

// ------------------------- HELPER FUNCTIONS -----------------------------
fn calculate_share(priority_fee_lamports: u64, commission_bps: u64) -> Result<u64> {
    let priority_fee_lamports_bps = priority_fee_lamports
        .checked_mul(MAX_BPS)
        .ok_or_else(|| anyhow!("Overflow when calculating priority fee in basis points"))?;

    let amount_to_share_lamports_bps = priority_fee_lamports_bps
        .checked_mul(commission_bps)
        .ok_or_else(|| anyhow!("Overflow when calculating commission amount in basis points"))?;

    let amount_to_share_lamports = amount_to_share_lamports_bps
        .checked_div(MAX_BPS)
        .ok_or_else(|| anyhow!("Division by zero when calculating final share amount"))?;

    Ok(amount_to_share_lamports)
}

fn check_commission_percentage(commission_bps: u64) -> Result<()> {
    if commission_bps > MAX_BPS {
        error!(
            "Commission percentage must be less than or equal to {}",
            MAX_BPS
        );
        return Err(anyhow!(
            "Invalid commission percentage: {} cannot be larger than {}",
            commission_bps,
            MAX_BPS
        ));
    }
    Ok(())
}

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

async fn check_if_initialize_priority_fee_distribution_account_exsists(
    rpc_client: &RpcClient,
    validator_vote_address: &Pubkey,
    priority_fee_distribution_program: &Pubkey,
    running_epoch: u64,
) -> bool {
    let (priority_fee_distribution_account, _) = Pubkey::find_program_address(
        &[
            b"priority_fee_distribution",
            validator_vote_address.as_ref(),
            &running_epoch.to_le_bytes(),
        ],
        priority_fee_distribution_program,
    );

    let result = rpc_client
        .get_account(&priority_fee_distribution_account)
        .await;

    if result.is_err() {
        return false;
    }
    let account = result.unwrap();

    info!("Account: {:?}", account);
    true
}

fn create_initialize_priority_fee_distribution_account_ix(
    payer_keypair: &Keypair,
    validator_vote_address: &Pubkey,
    priority_fee_distribution_program: &Pubkey,
    commission_bps: u16,
    running_epoch: u64,
) -> Instruction {
    // Instruction {
    //     program_id,
    //     data:
    //         jito_priority_fee_distribution::instruction::InitializePriorityFeeDistributionAccount {
    //             merkle_root_upload_authority: merkle_root_upload_auth,
    //             validator_commission_bps: 0xBADD,
    //             bump: 0x55,
    //         }
    //         .data(),
    //     accounts:
    //         jito_priority_fee_distribution::accounts::InitializePriorityFeeDistributionAccount {
    //             config: config,
    //             system_program: system_program,
    //             priority_fee_distribution_account: priority_fee_distribution_account,
    //             signer: signer,
    //             validator_vote_account: validator_vote_account,
    //         }
    //         .to_account_metas(None),
    // }

    let mut data = Vec::with_capacity(256);
    // initialize_priority_fee_distribution_account
    let discriminator: [u8; 8] = [49, 128, 247, 162, 140, 2, 193, 87];
    data.extend_from_slice(&discriminator);

    // This should be set to the actual merkle root upload authority
    let merkle_root_upload_authority =
        Pubkey::from_str_const("2AxPPApUQWvo2JsB52iQC4gbEipAWjRvmnNyDHJgd6Pe");
    data.extend_from_slice(&merkle_root_upload_authority.to_bytes());

    data.extend_from_slice(&commission_bps.to_le_bytes());

    let (priority_fee_distribution_account, bump) = Pubkey::find_program_address(
        &[
            b"PF_DISTRIBUTION_ACCOUNT",
            validator_vote_address.as_ref(),
            &running_epoch.to_le_bytes(),
        ],
        priority_fee_distribution_program,
    );
    data.push(bump);

    // Get the config account PDA
    let (config, _) =
        Pubkey::find_program_address(&[b"CONFIG_ACCOUNT"], priority_fee_distribution_program);
    info!("Config {}", config);

    // // Create the instruction data: discriminator + args
    // let mut data = Vec::with_capacity(8 + 32 + 2 + 1);
    // data.extend_from_slice(&discriminator);
    // data.extend_from_slice(&merkle_root_upload_authority.to_bytes()); // Merkle Root Upload Authority
    // data.extend_from_slice(&commission_bps.to_le_bytes()); // Commission
    // data.extend_from_slice(&[bump]); // Bump as a single byte

    // List of accounts required for the instruction
    let accounts = vec![
        AccountMeta::new_readonly(config, false), // config
        AccountMeta::new(priority_fee_distribution_account, false), // priority_fee_distribution_account (writable)
        AccountMeta::new_readonly(*validator_vote_address, false),  // validator_vote_account
        AccountMeta::new(payer_keypair.pubkey(), true),             // signer (writable, signer)
        AccountMeta::new_readonly(solana_sdk::system_program::id(), false), // system_program
    ];

    // ACCOUNTS IN ORDER (exact order is important):
    // Account 0: Config - 1111111ogCyDbaRMvkdsHB3qfdyFYaG1WtRUAfdh (is_signer: false, is_writable: false)
    // Account 1: Priority Fee Distribution Account - 11111112cMQwSC9qirWGjZM6gLGwW69X22mqwLLGP (is_signer: false, is_writable: true)
    // Account 2: Validator Vote Account - 11111113R2cuenjG5nFubqX9Wzuukdin2YfGQVzu5 (is_signer: false, is_writable: false)
    // Account 3: Signer - 111111131h1vYVSYuKP6AhS86fbRdMw9XHiZAvAaj (is_signer: true, is_writable: true)
    // Account 4: System Program - 11111112D1oxKts8YPdTJRG5FzxTNpMtWmq8hkVx3 (is_signer: false, is_writable: false)

    let ix = Instruction {
        program_id: *priority_fee_distribution_program,
        accounts,
        data,
    };

    // Print the raw data bytes in various formats
    let serialized_data = ix.clone().data;

    // Print the raw bytes in decimal format
    info!("DATA RAW BYTES (DECIMAL):");
    let mut string = String::new();
    string.push('[');
    for (i, byte) in serialized_data.iter().enumerate() {
        if i > 0 {
            string.push_str(", ");
        }
        string.push_str(&byte.to_string());
    }
    string.push(']');
    info!("{}", string);

    ix
}

fn create_share_ix(
    payer_keypair: &Keypair,
    validator_vote_address: &Pubkey,
    priority_fee_distribution_program: &Pubkey,
    amount_to_share_lamports: u64,
    running_epoch: u64,
) -> Instruction {
    // Define the instruction discriminator for transfer_priority_fee_tips
    let discriminator: [u8; 8] = [195, 208, 218, 42, 198, 181, 69, 74];

    // Get the priority fee distribution account PDA
    let (priority_fee_distribution_account, _) = Pubkey::find_program_address(
        &[
            b"priority_fee_distribution",
            validator_vote_address.as_ref(),
            &running_epoch.to_le_bytes(),
        ],
        priority_fee_distribution_program,
    );

    // Get the config account PDA
    let (config, _) =
        Pubkey::find_program_address(&[b"CONFIG_ACCOUNT"], priority_fee_distribution_program);
    info!("Config {}", config);

    // Create the instruction data: discriminator + lamports amount
    let mut data = Vec::with_capacity(8 + 8);
    data.extend_from_slice(&discriminator);
    data.extend_from_slice(&amount_to_share_lamports.to_le_bytes());

    // List of accounts required for the instruction
    let accounts = vec![
        AccountMeta::new_readonly(config, false), // config
        AccountMeta::new(priority_fee_distribution_account, false), // priority_fee_distribution_account (writable)
        AccountMeta::new(payer_keypair.pubkey(), true),             // from (writable, signer)
        AccountMeta::new_readonly(solana_sdk::system_program::id(), false), // system_program
    ];

    let ix = Instruction {
        program_id: *priority_fee_distribution_program,
        accounts,
        data,
    };

    // Print the raw data bytes in various formats
    let serialized_data = ix.clone().data;

    // Print the raw bytes in decimal format
    info!("DATA RAW BYTES (DECIMAL):");
    let mut string = String::new();
    string.push('[');
    for (i, byte) in serialized_data.iter().enumerate() {
        if i > 0 {
            string.push_str(", ");
        }
        string.push_str(&byte.to_string());
    }
    string.push(']');
    info!("{}", string);

    ix
}

// ------------------------- BLOCK FUNCTIONS -----------------------------
async fn handle_epoch_and_leader_slot(
    rpc_client: &RpcClient,
    fee_records: &FeeRecords,
    validator_address: &Pubkey,
    running_epoch: u64,
) -> Result<(u64, u64)> {
    let epoch_info = rpc_client
        .get_epoch_info_with_commitment(CommitmentConfig::finalized())
        .await?;
    let epoch_start_slot = epoch_info.absolute_slot - epoch_info.slot_index;

    if running_epoch == epoch_info.epoch {
        return Ok((running_epoch, epoch_info.absolute_slot));
    }

    let leader_schedule = rpc_client
        .get_leader_schedule_with_commitment(
            Some(epoch_info.absolute_slot),
            CommitmentConfig::finalized(),
        )
        .await?
        .ok_or(anyhow!(
            "Leader schedule for slot {} not available",
            epoch_info.absolute_slot
        ))?;

    let validator_slots = leader_schedule
        .get(&validator_address.to_string())
        .ok_or(anyhow!("No leader slots found for {}", validator_address))?;

    for slot in validator_slots {
        let slot = *slot as u64 + epoch_start_slot;
        info!("Processing slot {} {}", slot, epoch_info.absolute_slot);
        let result = fee_records.add_priority_fee_record(slot, epoch_info.epoch);
        if let Err(err) = result {
            error!(
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

    let total_records = records.len();
    let filtered_records: Vec<FeeRecordEntry> = records
        .into_iter()
        .filter(|record| record.slot <= running_slot)
        .collect();
    let total_filtered_records = filtered_records.len();

    info!(
        "Processing unprocessed blocks: {} remaining to process {} left in epoch",
        total_filtered_records,
        total_records - total_filtered_records
    );
    for record in filtered_records.iter().take(call_limit) {
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
                        error!(
                            "Error processing priority fee record for slot {}: {}",
                            record.slot, err
                        );
                    }
                }
                Err(e) => {
                    error!("Could not get block, {}", e);
                    let result = fee_records.skip_record(record.slot, record.epoch);
                    if let Err(err) = result {
                        error!(
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
    priority_fee_distribution_program: &Pubkey,
    commission_bps: u64,
    minimum_balance_lamports: u64,
    chunk_size: usize,
    call_limit: usize,
    running_epoch: u64,
) -> Result<()> {
    let records = fee_records.get_records_by_state(FeeRecordState::ProcessedAndPending)?;

    delay_past_leader_slot(rpc_client, fee_records).await?;

    let mut balance_after_transfer = rpc_client.get_balance(&payer_keypair.pubkey()).await?;
    let blockhash = rpc_client.get_latest_blockhash().await?;

    let validator_vote_address =
        Pubkey::from_str_const("13sfDC74Rqz6i2cMWjY5wqyRaNdpdpRd75pGLYPzqs44");

    if !check_if_initialize_priority_fee_distribution_account_exsists(
        rpc_client,
        &validator_vote_address,
        priority_fee_distribution_program,
        running_epoch,
    )
    .await
    {
        info!("Creating PDA...");
        let ix = create_initialize_priority_fee_distribution_account_ix(
            payer_keypair,
            &validator_vote_address,
            priority_fee_distribution_program,
            commission_bps as u16,
            running_epoch,
        );
        let tx = Transaction::new_signed_with_payer(
            &[ix.clone()],
            Some(&payer_keypair.pubkey()),
            &[payer_keypair],
            blockhash,
        );

        info!("{:?}\n\n{:?}", ix, tx);

        let result = rpc_client
            .send_transaction_with_config(
                &tx,
                RpcSendTransactionConfig {
                    skip_preflight: false,
                    ..Default::default()
                },
            )
            .await;
        match result {
            Ok(sig) => {
                info!("Create PDA Transaction sent: {}", sig);
            }
            Err(err) => {
                return Err(anyhow!("Failed to send Create PDA transaction: {:?}", err,));
            }
        }
    }

    for record_chunk in records.chunks(chunk_size).take(call_limit) {
        // Try to send transactions
        let mut ixs: Vec<Instruction> = vec![];
        let mut records: Vec<FeeRecordEntry> = vec![];

        for record in record_chunk {
            let priority_fee_lamports: u64 = record.priority_fee_lamports;

            let amount_to_share_lamports = calculate_share(priority_fee_lamports, commission_bps)?;

            let share_ix = create_share_ix(
                payer_keypair,
                &validator_vote_address,
                priority_fee_distribution_program,
                amount_to_share_lamports,
                running_epoch,
            );
            ixs.push(share_ix);
            records.push(record.clone());

            balance_after_transfer =
                balance_after_transfer.saturating_sub(amount_to_share_lamports);
        }

        if balance_after_transfer < minimum_balance_lamports {
            return Err(anyhow!(
                "Minimum balance reached {}/{}",
                balance_after_transfer,
                minimum_balance_lamports
            ));
        }

        // Create TX
        info!("Creating TX...");
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
                    skip_preflight: false,
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
                        error!(
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

// ------------------------- MAIN FUNCTIONS -----------------------------
pub async fn share_priority_fees_loop(
    rpc_url: String,
    fee_records_db_path: PathBuf,
    priority_fee_keypair_path: PathBuf,
    validator_address: Pubkey,
    priority_fee_distribution_program: Pubkey,
    commission_bps: u64,
    minimum_balance_lamports: u64,
    chunk_size: usize,
    call_limit: usize,
) -> Result<()> {
    check_commission_percentage(commission_bps)?;

    let payer_keypair = read_keypair_file(priority_fee_keypair_path)
        .unwrap_or_else(|err| panic!("Failed to read payer keypair file: {}", err));

    let rpc_client = RpcClient::new(rpc_url);
    let fee_records = FeeRecords::new(fee_records_db_path)?;

    let mut running_epoch = 0;
    let mut running_slot = 0;

    loop {
        // 1. Handle Epoch
        info!(" -------- 1. HANDLE EPOCH AND LEADER SLOT -----------");
        let result = handle_epoch_and_leader_slot(
            &rpc_client,
            &fee_records,
            &validator_address,
            running_epoch,
        )
        .await;
        match result {
            Ok((epoch, slot)) => {
                running_epoch = epoch;
                running_slot = slot;
            }
            Err(err) => error!("Error handling epoch and leader slots: {}", err),
        }

        // 2. Handle unprocessed blocks
        info!(" -------- 2. HANDLE UNPROCESSED BLOCKS -----------");
        let result =
            handle_unprocessed_blocks(&rpc_client, &fee_records, running_slot, call_limit).await;
        if let Err(err) = result {
            error!("Error handling unprocessed records: {}", err);
        }

        // 3. Handle pending blocks
        info!(" -------- 3. HANDLE PENDING BLOCKS -----------");
        let result = handle_pending_blocks(
            &rpc_client,
            &fee_records,
            &payer_keypair,
            &validator_address,
            &priority_fee_distribution_program,
            commission_bps,
            minimum_balance_lamports,
            chunk_size,
            call_limit,
            running_epoch,
        )
        .await;
        if let Err(err) = result {
            error!("Error handling pending blocks: {}", err);
        }

        sleep_ms(LEADER_SLOT_MS).await;
    }
}
