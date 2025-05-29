pub mod error;
pub mod fee_records;

use anyhow::{anyhow, Result};
use fee_records::{FeeRecordEntry, FeeRecordState, FeeRecords};
use log::{debug, error, info, warn};
use solana_client::rpc_config::RpcSendTransactionConfig;
use solana_pubkey::Pubkey;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::compute_budget::ComputeBudgetInstruction;
use solana_sdk::epoch_info::EpochInfo;
use solana_sdk::instruction::AccountMeta;
use solana_sdk::instruction::Instruction;
use solana_sdk::native_token::lamports_to_sol;
use solana_sdk::reward_type::RewardType;
use solana_sdk::signature::read_keypair_file;
use solana_sdk::signature::Keypair;
use solana_sdk::signer::Signer;
use solana_sdk::transaction::Transaction;
use solana_sdk::vote::state::VoteState;
use std::path::PathBuf;
use std::time::Duration;
use tokio::time::sleep;

// ------------------------- GLOBAL CONSTANTS -----------------------------
// 1s/block, 4 leader blocks in a row
const LEADER_SLOT_MS: u64 = 1000 * 4;
const MAX_BPS: u64 = 10_000;

// ------------------------- HELPER STRUCTS -----------------------------
#[derive(Debug, Clone)]
pub struct PFEpochInfo {
    pub epoch: u64,
    pub slot: u64,
    pub slots_in_epoch: u64,
    pub slot_index: u64,
}

impl PFEpochInfo {
    pub fn new(epoch_info: EpochInfo) -> Self {
        Self {
            epoch: epoch_info.epoch,
            slot: epoch_info.absolute_slot,
            slots_in_epoch: epoch_info.slots_in_epoch,
            slot_index: epoch_info.slot_index,
        }
    }

    pub fn null() -> Self {
        Self {
            epoch: 0,
            slot: 0,
            slots_in_epoch: 0,
            slot_index: 0,
        }
    }

    pub fn first_slot_in_epoch(&self) -> u64 {
        self.slot - self.slot_index
    }

    pub fn last_slot_in_epoch(&self) -> u64 {
        self.first_slot_in_epoch() + self.slots_in_epoch
    }

    pub fn percentage_of_epoch(&self) -> f64 {
        self.slot_index as f64 / self.slots_in_epoch as f64 * 100.0
    }
}

// ------------------------- HELPER FUNCTIONS -----------------------------
fn calculate_share(priority_fee_lamports: u64, commission_bps: u64) -> Result<u64> {
    // Calculate the amount that goes to delegators (total minus commission)
    let delegator_share_bps = MAX_BPS
        .checked_sub(commission_bps)
        .ok_or_else(|| anyhow!("Invalid commission value exceeds maximum"))?;

    // Calculate the amount to share with delegators
    let amount_to_share_lamports_bps = priority_fee_lamports
        .checked_mul(delegator_share_bps)
        .ok_or_else(|| anyhow!("Overflow when calculating delegator share in basis points"))?;

    // Convert from basis points back to lamports
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

fn get_priority_fee_distribution_config_account_address(
    priority_fee_distribution_program: &Pubkey,
) -> (Pubkey, u8) {
    Pubkey::find_program_address(&[b"CONFIG_ACCOUNT"], priority_fee_distribution_program)
}

fn get_priority_fee_distribution_account_address(
    validator_vote_address: &Pubkey,
    priority_fee_distribution_program: &Pubkey,
    running_epoch: u64,
) -> (Pubkey, u8) {
    Pubkey::find_program_address(
        &[
            b"PF_DISTRIBUTION_ACCOUNT",
            validator_vote_address.as_ref(),
            &running_epoch.to_le_bytes(),
        ],
        priority_fee_distribution_program,
    )
}

async fn get_priority_fee_distribution_account_balance(
    rpc_client: &RpcClient,
    validator_vote_address: &Pubkey,
    priority_fee_distribution_program: &Pubkey,
    running_epoch: u64,
) -> Result<u64> {
    let (address, _) = get_priority_fee_distribution_account_address(
        validator_vote_address,
        priority_fee_distribution_program,
        running_epoch,
    );

    let balance = rpc_client.get_balance(&address).await?;

    Ok(balance)
}

async fn get_priority_fee_distribution_account_internal_balance(
    rpc_client: &RpcClient,
    validator_vote_address: &Pubkey,
    priority_fee_distribution_program: &Pubkey,
    running_epoch: u64,
) -> Result<u64> {
    let (address, _) = get_priority_fee_distribution_account_address(
        validator_vote_address,
        priority_fee_distribution_program,
        running_epoch,
    );

    let account = rpc_client.get_account(&address).await?;

    // The PriorityFeeDistributionAccount has a specific layout where total_lamports_transferred
    // is after several other fields. According to the IDL:
    // - 8 bytes: account discriminator
    // - 32 bytes: validator_vote_account (pubkey)
    // - 32 bytes: merkle_root_upload_authority (pubkey)
    // - Variable: merkle_root (option) - depends on whether it's set
    // - 8 bytes: epoch_created_at (u64)
    // - 2 bytes: validator_commission_bps (u16)
    // - 8 bytes: expires_at (u64)
    // - 8 bytes: total_lamports_transferred (u64) <- this is what we want
    // - 1 byte: bump (u8)

    // Check if the merkle_root is set (first byte after the two pubkeys)
    let merkle_root_offset = 8 + 32 + 32; // discriminator + validator_vote_account + merkle_root_upload_authority
    let has_merkle_root = account.data[merkle_root_offset] != 0;

    // Calculate offset to total_lamports_transferred
    let mut offset = merkle_root_offset + 1; // +1 for the option tag

    if has_merkle_root {
        // Merkle root is present, skip it:
        // - 32 bytes: root
        // - 8 bytes: max_total_claim
        // - 8 bytes: max_num_nodes
        // - 8 bytes: total_funds_claimed
        // - 8 bytes: num_nodes_claimed
        offset += 32 + 8 + 8 + 8 + 8;
    }

    // Skip the remaining fields before total_lamports_transferred
    // - 8 bytes: epoch_created_at
    // - 2 bytes: validator_commission_bps
    // - 8 bytes: expires_at
    offset += 8 + 2 + 8;

    // Now we're at total_lamports_transferred, read 8 bytes as u64
    let total_lamports_transferred = u64::from_le_bytes([
        account.data[offset],
        account.data[offset + 1],
        account.data[offset + 2],
        account.data[offset + 3],
        account.data[offset + 4],
        account.data[offset + 5],
        account.data[offset + 6],
        account.data[offset + 7],
    ]);

    Ok(total_lamports_transferred)
}

async fn get_validator_identity(
    rpc_client: &RpcClient,
    validator_vote_address: &Pubkey,
) -> Result<Pubkey> {
    let account_result = rpc_client.get_account(validator_vote_address).await;

    match account_result {
        Ok(account) => {
            let vote_state_result = VoteState::deserialize(&account.data);

            match vote_state_result {
                Ok(state) => {
                    return Ok(state.node_pubkey);
                }
                Err(e) => {
                    return Err(anyhow!("Could not parse Vote State: {:?}", e));
                }
            }
        }
        Err(e) => {
            return Err(anyhow!("Could not get Validator Idenity: {:?}", e));
        }
    }
}

async fn check_priority_fee_distribution_account_exsists(
    rpc_client: &RpcClient,
    validator_vote_address: &Pubkey,
    priority_fee_distribution_program: &Pubkey,
    running_epoch: u64,
) -> bool {
    let (priority_fee_distribution_account, _) = get_priority_fee_distribution_account_address(
        validator_vote_address,
        priority_fee_distribution_program,
        running_epoch,
    );

    let result = rpc_client
        .get_account(&priority_fee_distribution_account)
        .await;

    result.is_ok()
}

async fn check_or_create_fee_distribution_account(
    rpc_client: &RpcClient,
    payer_keypair: &Keypair,
    vote_authority_keypair: &Keypair,
    validator_vote_address: &Pubkey,
    merkle_root_upload_authority: &Pubkey,
    priority_fee_distribution_program: &Pubkey,
    commission_bps: u64,
    running_epoch: u64,
) -> Result<()> {
    let account_exsists = check_priority_fee_distribution_account_exsists(
        rpc_client,
        validator_vote_address,
        priority_fee_distribution_program,
        running_epoch,
    )
    .await;

    if account_exsists {
        return Ok(());
    }

    let blockhash = rpc_client.get_latest_blockhash().await?;

    let ix = create_initialize_priority_fee_distribution_account_ix(
        vote_authority_keypair,
        validator_vote_address,
        priority_fee_distribution_program,
        merkle_root_upload_authority,
        commission_bps as u16,
        running_epoch,
    );
    let tx = Transaction::new_signed_with_payer(
        &[ix.clone()],
        Some(&payer_keypair.pubkey()),
        &[vote_authority_keypair, payer_keypair],
        blockhash,
    );

    let result = rpc_client
        .send_and_confirm_transaction_with_spinner_and_config(
            &tx,
            CommitmentConfig::finalized(),
            RpcSendTransactionConfig {
                skip_preflight: true,
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

    return Ok(());
}

fn create_initialize_priority_fee_distribution_account_ix(
    vote_authority_keypair: &Keypair,
    validator_vote_address: &Pubkey,
    priority_fee_distribution_program: &Pubkey,
    merkle_root_upload_authority: &Pubkey,
    commission_bps: u16,
    running_epoch: u64,
) -> Instruction {
    // initialize_priority_fee_distribution_account
    let discriminator: [u8; 8] = [49, 128, 247, 162, 140, 2, 193, 87];

    let (priority_fee_distribution_account, bump) = get_priority_fee_distribution_account_address(
        validator_vote_address,
        priority_fee_distribution_program,
        running_epoch,
    );

    // Get the config account PDA
    let (config, _) =
        get_priority_fee_distribution_config_account_address(&priority_fee_distribution_program);

    // Create the instruction data: discriminator + args
    let mut data = Vec::with_capacity(8 + 32 + 2 + 1);
    data.extend_from_slice(&discriminator);
    data.extend_from_slice(&merkle_root_upload_authority.to_bytes()); // Merkle Root Upload Authority
    data.extend_from_slice(&commission_bps.to_le_bytes()); // Commission
    data.extend_from_slice(&[bump]); // Bump as a single byte

    // List of accounts required for the instruction
    let accounts = vec![
        AccountMeta::new_readonly(config, false), // config
        AccountMeta::new(priority_fee_distribution_account, false), // priority_fee_distribution_account (writable)
        AccountMeta::new_readonly(*validator_vote_address, false),  // validator_vote_account
        AccountMeta::new(vote_authority_keypair.pubkey(), true),    // signer (writable, signer)
        AccountMeta::new_readonly(solana_sdk::system_program::id(), false), // system_program
    ];

    Instruction {
        program_id: *priority_fee_distribution_program,
        accounts,
        data,
    }
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
    let (priority_fee_distribution_account, _) = get_priority_fee_distribution_account_address(
        validator_vote_address,
        priority_fee_distribution_program,
        running_epoch,
    );

    // Get the config account PDA
    let (config, _) =
        get_priority_fee_distribution_config_account_address(&priority_fee_distribution_program);

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

    Instruction {
        program_id: *priority_fee_distribution_program,
        accounts,
        data,
    }
}

// ------------------------- BLOCK FUNCTIONS -----------------------------
async fn handle_epoch_and_leader_slot(
    rpc_client: &RpcClient,
    fee_records: &FeeRecords,
    validator_vote_account: &Pubkey,
    validator_identity: &Pubkey,
    running_epoch_info: &PFEpochInfo,
) -> Result<(PFEpochInfo, bool)> {
    // epoch, absolute_slot, start_slot, end_slot, is_new_epoch
    let epoch_info = rpc_client
        .get_epoch_info_with_commitment(CommitmentConfig::finalized())
        .await?;
    let epoch_start_slot = epoch_info.absolute_slot - epoch_info.slot_index;

    if running_epoch_info.epoch == epoch_info.epoch {
        return Ok((PFEpochInfo::new(epoch_info), false));
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

    // Leader Schedules are found by identity
    let validator_slots = leader_schedule
        .get(&validator_identity.to_string())
        .ok_or(anyhow!("No leader slots found for {}", validator_identity))?;

    for slot in validator_slots {
        let slot = *slot as u64 + epoch_start_slot;
        info!("Processing slot {}", epoch_info.absolute_slot);
        let result = fee_records.add_priority_fee_record(
            slot,
            epoch_info.epoch,
            validator_vote_account,
            validator_identity,
        );
        if let Err(err) = result {
            error!(
                "Error adding priority fee record for slot {}: {}",
                slot, err
            );
        }
    }

    Ok((PFEpochInfo::new(epoch_info), true))
}

async fn handle_unprocessed_blocks(
    rpc_client: &RpcClient,
    fee_records: &FeeRecords,
    running_epoch_info: &PFEpochInfo,
) -> Result<()> {
    let records = fee_records.get_records_by_state(FeeRecordState::Unprocessed)?;

    delay_past_leader_slot(rpc_client, fee_records).await?;

    let total_records = records.len();
    let filtered_records: Vec<FeeRecordEntry> = records
        .into_iter()
        .filter(|record| record.slot <= running_epoch_info.slot)
        .collect();
    let total_filtered_records = filtered_records.len();

    info!(
        "Processing unprocessed blocks: {} remaining to process {} left in epoch",
        total_filtered_records,
        total_records - total_filtered_records
    );
    for record in filtered_records.iter() {
        // Try to fetch block and update
        if record.slot <= running_epoch_info.slot {
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
                        record.slot,
                        lamports_to_sol(priority_fee_lamports as u64)
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
                    warn!("Could not get block, {}", e);
                    // Only skip if the error indicates the slot was skipped or missing in long-term storage
                    let error_message = e.to_string();
                    if error_message.contains("was skipped, or missing in long-term storage") {
                        let result = fee_records.skip_record(record.slot, record.epoch);
                        if let Err(err) = result {
                            error!(
                                "Error skipping priority fee record for slot {}: {}",
                                record.slot, err
                            );
                        }
                    } else {
                        // For other errors (like "Transaction history is not available"),
                        // you might want to handle differently - perhaps retry, return the error, etc.
                        error!(
                            "Could not get block for slot {} - You may have to switch your RPC provider",
                            record.slot
                        );
                    }
                }
            }
        }
    }

    Ok(())
}

fn should_handle_pending_blocks(
    running_epoch_info: &PFEpochInfo,
    transfer_count: u64,
    transactions_per_epoch: u64,
) -> bool {
    let percentage_of_epoch = running_epoch_info.percentage_of_epoch();
    let percentage_per_transaction = 100.0 / transactions_per_epoch as f64;

    debug!("TX count: {}", transfer_count);
    if transfer_count == 0 {
        return true;
    }

    debug!(
        "{} > {} ({})",
        percentage_of_epoch,
        transfer_count as f64 * percentage_per_transaction,
        percentage_of_epoch > transfer_count as f64 * percentage_per_transaction
    );
    percentage_of_epoch > transfer_count as f64 * percentage_per_transaction
}

async fn handle_pending_blocks(
    rpc_client: &RpcClient,
    fee_records: &FeeRecords,
    payer_keypair: &Keypair,
    vote_authority_keypair: &Keypair,
    validator_vote_account: &Pubkey,
    priority_fee_distribution_program: &Pubkey,
    merkle_root_upload_authority: &Pubkey,
    commission_bps: u64,
    minimum_balance_lamports: u64,
    priority_fee_lamports: u64,
    transactions_per_epoch: u64,
    running_epoch_info: &PFEpochInfo,
    transfer_count: u64,
) -> Result<u64> {
    if !should_handle_pending_blocks(running_epoch_info, transfer_count, transactions_per_epoch) {
        info!("Not time to transfer");
        return Ok(transfer_count);
    }

    let records = fee_records.get_records_by_state(FeeRecordState::ProcessedAndPending)?;
    delay_past_leader_slot(rpc_client, fee_records).await?;

    // Check or create Priority Fee Distribution Account
    check_or_create_fee_distribution_account(
        rpc_client,
        payer_keypair,
        vote_authority_keypair,
        validator_vote_account,
        merkle_root_upload_authority,
        priority_fee_distribution_program,
        commission_bps,
        running_epoch_info.epoch,
    )
    .await?;

    let mut balance_after_transfer = rpc_client.get_balance(&payer_keypair.pubkey()).await?;
    let blockhash = rpc_client.get_latest_blockhash().await?;

    let mut records_to_transfer: Vec<FeeRecordEntry> = vec![];
    let mut amount_to_transfer: u64 = 0;
    for record in records {
        // Sanity Check
        if record.vote_account.ne(&validator_vote_account.to_string()) {
            info!(
                "Record is not for the correct validator {} != {}",
                record.vote_account, validator_vote_account
            );
            continue;
        }
        // Sanity Check
        if record.state != FeeRecordState::ProcessedAndPending {
            info!(
                "Record is not in the correct state {:?} != {:?}",
                record.state,
                FeeRecordState::ProcessedAndPending
            );
            continue;
        }

        let amount_to_share = match calculate_share(record.priority_fee_lamports, commission_bps) {
            Ok(amount) => amount,
            Err(err) => {
                info!("Error calculating share: {}", err);
                continue;
            }
        };

        balance_after_transfer = balance_after_transfer.saturating_sub(amount_to_share);
        if balance_after_transfer < minimum_balance_lamports {
            info!(
                "Balance after transfer would be below minimum balance {} < {}",
                balance_after_transfer, minimum_balance_lamports
            );
            break;
        }

        amount_to_transfer = amount_to_transfer.saturating_add(amount_to_share);
        records_to_transfer.push(record);
    }

    if records_to_transfer.is_empty() {
        info!("No records to transfer");
        return Ok(transfer_count);
    }

    if amount_to_transfer == 0 {
        info!("No amount to transfer");
        return Ok(transfer_count);
    }

    // Create TX
    let share_ix = create_share_ix(
        payer_keypair,
        &validator_vote_account,
        priority_fee_distribution_program,
        amount_to_transfer,
        running_epoch_info.epoch,
    );

    let tx = Transaction::new_signed_with_payer(
        &[
            ComputeBudgetInstruction::set_compute_unit_price(priority_fee_lamports),
            share_ix,
        ],
        Some(&payer_keypair.pubkey()),
        &[payer_keypair],
        blockhash,
    );

    let result = rpc_client
        .send_and_confirm_transaction_with_spinner_and_config(
            &tx,
            CommitmentConfig::finalized(),
            RpcSendTransactionConfig {
                skip_preflight: true,
                ..Default::default()
            },
        )
        .await;

    let slot_landed = rpc_client
        .get_slot_with_commitment(CommitmentConfig::finalized())
        .await
        .unwrap_or(running_epoch_info.slot);
    match result {
        Ok(sig) => {
            info!(
                "Share Transaction sent: {} ({}: {})",
                sig,
                slot_landed,
                lamports_to_sol(amount_to_transfer)
            );

            for record in records_to_transfer {
                let result = fee_records.complete_record(
                    record.slot,
                    record.epoch,
                    &sig.to_string(),
                    slot_landed,
                );
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

    Ok(transfer_count.saturating_add(1))
}

// ------------------------- MAIN FUNCTIONS -----------------------------
pub async fn share_priority_fees_loop(
    rpc_url: String,
    fee_records_db_path: PathBuf,
    priority_fee_payer_keypair_path: PathBuf,
    vote_authority_keypair_path: PathBuf,
    validator_vote_account: Pubkey,
    merkle_root_upload_authority: Pubkey,
    priority_fee_distribution_program: Pubkey,
    minimum_balance_lamports: u64,
    commission_bps: u64,
    priority_fee_lamports: u64,
    transactions_per_epoch: u64,
) -> Result<()> {
    check_commission_percentage(commission_bps)?;

    let payer_keypair = read_keypair_file(priority_fee_payer_keypair_path)
        .unwrap_or_else(|err| panic!("Failed to read payer keypair file: {}", err));

    let vote_authority_keypair = read_keypair_file(vote_authority_keypair_path)
        .unwrap_or_else(|err| panic!("Failed to read vote authority keypair file: {}", err));

    let rpc_client = RpcClient::new(rpc_url);
    let fee_records = FeeRecords::new(fee_records_db_path)?;

    let mut running_epoch_info = PFEpochInfo::null();
    let mut transfer_count = 0;

    let validator_identity = get_validator_identity(&rpc_client, &validator_vote_account).await?;

    loop {
        // 1. Handle Epoch
        info!(" -------- 1. HANDLE EPOCH AND LEADER SLOT -----------");
        let result = handle_epoch_and_leader_slot(
            &rpc_client,
            &fee_records,
            &validator_vote_account,
            &validator_identity,
            &running_epoch_info,
        )
        .await;
        match result {
            Ok((epoch_info, did_rollover)) => {
                running_epoch_info = epoch_info;
                if did_rollover {
                    transfer_count = 0;
                }
            }
            Err(err) => error!("Error handling epoch and leader slots: {}", err),
        }

        // 2. Handle unprocessed blocks
        info!(" -------- 2. HANDLE UNPROCESSED BLOCKS -----------");
        let result =
            handle_unprocessed_blocks(&rpc_client, &fee_records, &running_epoch_info).await;
        if let Err(err) = result {
            error!("Error handling unprocessed records: {}", err);
        }

        // 3. Handle pending blocks
        info!(" -------- 3. HANDLE PENDING BLOCKS -----------");
        let result = handle_pending_blocks(
            &rpc_client,
            &fee_records,
            &payer_keypair,
            &vote_authority_keypair,
            &validator_vote_account,
            &priority_fee_distribution_program,
            &merkle_root_upload_authority,
            commission_bps,
            minimum_balance_lamports,
            priority_fee_lamports,
            transactions_per_epoch,
            &running_epoch_info,
            transfer_count,
        )
        .await;
        match result {
            Ok(transfers) => transfer_count = transfers,
            Err(err) => error!("Error handling pending blocks: {}", err),
        }

        sleep_ms(LEADER_SLOT_MS).await;
    }
}

pub async fn print_out_priority_fee_distribution_information(
    rpc_url: String,
    validator_vote_account: Pubkey,
    priority_fee_distribution_program: Pubkey,
    epoch: Option<u64>,
) -> Result<()> {
    // Initialize RPC client
    let rpc_client = RpcClient::new(rpc_url);

    // Get the current epoch if not specified
    let running_epoch = match epoch {
        Some(e) => e,
        None => {
            let epoch_info = rpc_client
                .get_epoch_info_with_commitment(CommitmentConfig::finalized())
                .await?;
            epoch_info.epoch
        }
    };

    // Get the PriorityFeeDistributionAccount PDA address
    let (address, _) = get_priority_fee_distribution_account_address(
        &validator_vote_account,
        &priority_fee_distribution_program,
        running_epoch,
    );

    // Get the external balance (total SOL in the account)
    let external_balance = match get_priority_fee_distribution_account_balance(
        &rpc_client,
        &validator_vote_account,
        &priority_fee_distribution_program,
        running_epoch,
    )
    .await
    {
        Ok(balance) => balance,
        Err(e) => {
            println!("Error fetching account balance: {}", e);
            return Err(anyhow!("Failed to get account balance: {}", e));
        }
    };

    // Get the internal transferred balance
    let internal_balance = match get_priority_fee_distribution_account_internal_balance(
        &rpc_client,
        &validator_vote_account,
        &priority_fee_distribution_program,
        running_epoch,
    )
    .await
    {
        Ok(balance) => balance,
        Err(e) => {
            println!("Error fetching internal balance: {}", e);
            return Err(anyhow!("Failed to get internal balance: {}", e));
        }
    };

    // Use the existing lamports_to_sol function
    let external_balance_sol = solana_sdk::native_token::lamports_to_sol(external_balance);
    let internal_balance_sol = solana_sdk::native_token::lamports_to_sol(internal_balance);

    // Print the information
    println!(
        "Priority Fee Distribution Account Information for Epoch {}",
        running_epoch
    );
    println!("Address: {}", address);
    println!(
        "Current Account Balance: {} lamports ({} SOL)",
        external_balance, external_balance_sol
    );
    println!(
        "Total Lamports Transferred: {} lamports ({} SOL)",
        internal_balance, internal_balance_sol
    );

    if internal_balance > external_balance {
        let claimed_lamports = internal_balance - external_balance;
        let claimed_sol = solana_sdk::native_token::lamports_to_sol(claimed_lamports);
        println!(
            "Note: {} lamports ({} SOL) have been claimed by delegators",
            claimed_lamports, claimed_sol
        );
    }

    Ok(())
}
