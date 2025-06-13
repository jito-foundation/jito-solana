pub mod error;
pub mod fee_records;

use anyhow::{anyhow, Result};
use clap::ValueEnum;
use fee_records::{FeeRecordEntry, FeeRecordState, FeeRecords};
use log::warn;
use log::{error, info};
use solana_account::Account;
use solana_client::rpc_config::{RpcBlockConfig, RpcLeaderScheduleConfig, RpcSendTransactionConfig};
use solana_metrics::{datapoint_error, datapoint_info};
use solana_pubkey::Pubkey;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::compute_budget::ComputeBudgetInstruction;
use solana_sdk::epoch_info::EpochInfo;
use solana_sdk::instruction::AccountMeta;
use solana_sdk::instruction::Instruction;
use solana_sdk::native_token::lamports_to_sol;
use solana_sdk::pubkey;
use solana_sdk::reward_type::RewardType;
use solana_sdk::signature::read_keypair_file;
use solana_sdk::signature::Keypair;
use solana_sdk::signer::Signer;
use solana_sdk::transaction::Transaction;
use solana_sdk::vote::state::VoteState;
use std::{env, fmt};
use std::path::PathBuf;
use std::time::Duration;
use tokio::time::sleep;

// ------------------------- GLOBAL CONSTANTS -----------------------------
// 1s/block, 4 leader blocks in a row
const LEADER_SLOT_MS: u64 = 1000 * 4;
const MAX_BPS: u64 = 10_000;

// ------------------------- HELPER ENUMS -----------------------------
#[derive(ValueEnum, Debug, Clone)]
pub enum Cluster {
    Mainnet,
    Testnet,
    Localnet,
}

impl fmt::Display for Cluster {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Mainnet => write!(f, "mainnet"),
            Self::Testnet => write!(f, "testnet"),
            Self::Localnet => write!(f, "localnet"),
        }
    }
}


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

// ------------------------- VERIFY SETUP -----------------------------
pub async fn verify_setup(
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
    loop_timeout_ms: u64,
) -> Result<()> {
    // ---------------- RPC CHECK -------------------------
    let rpc_client = RpcClient::new(rpc_url);

    let epoch_info = match rpc_client.get_epoch_info().await {
        Ok(epoch_info) => {
            info!("✅ RPC able to get epoch info");
            epoch_info
        }
        Err(err) => {
            return Err(anyhow!(format!("❌ Failed to get epoch info: {}", err)));
        }
    };

    for i in 0..250 {
        let slot = epoch_info.absolute_slot - i;
        match get_rewards_safe(&rpc_client, slot).await {
            Ok((should_skip, _)) => {
                if !should_skip {
                    info!("✅ RPC able to get block");
                    break;
                }
            }
            Err(err) => {
                // - `Could not get block, RPC response error -32009: Slot 336212841 was skipped, or missing in long-term storage;` - This is OK
                // - `Could not get block, RPC response error -32011: Transaction history is not available from this node;` - This is not okay, and will need a new RPC
                // - `Could not get block, RPC response error -32015: Transaction version (0) is not supported by the requesting client. Please try the request again with the following configuration parameter: "maxSupportedTransactionVersion": 0;` - Not sure yet
                // - `Could not get block, RPC response error -32007: Slot 336638156 was skipped, or missing due to ledger jump to recent snapshot;` - Not sure yet
                // - `Could not get block, invalid type: null, expected struct UiConfirmedBlock` - Not sure yet
                return Err(anyhow!(format!("❌ Failed to get block: {}", err)));
            }
        }
    }

    // ---------------- DATABASE CHECK ---------------------
    let fee_records = match FeeRecords::new(fee_records_db_path) {
        Ok(fee_records) => {
            info!("✅ Database connected");
            fee_records
        }
        Err(err) => return Err(anyhow!(format!("❌ Database could not connect: {}", err))),
    };

    match fee_records.check_connection() {
        Ok(_) => info!("✅ Database checked"),
        Err(err) => return Err(anyhow!(format!("❌ Database check failed: {}", err))),
    }

    // ---------------- PAYER CHECK ---------------------
    let payer_keypair = match read_keypair_file(priority_fee_payer_keypair_path) {
        Ok(keypair) => {
            info!("✅ Payer keypair OK: {}", keypair.pubkey());
            keypair
        }
        Err(err) => {
            return Err(anyhow!(format!(
                "❌ Failed to read payer keypair file: {}",
                err
            )))
        }
    };

    let payer_balance = match rpc_client.get_balance(&payer_keypair.pubkey()).await {
        Ok(payer_balance) => {
            info!("✅ Payer balance OK {}", lamports_to_sol(payer_balance));
            payer_balance
        }
        Err(err) => return Err(anyhow!(format!("❌ Payer balance check failed: {}", err))),
    };

    if payer_balance < minimum_balance_lamports {
        warn!(
            "⚠️ Minimum balance is not currently met {}/{}",
            lamports_to_sol(payer_balance),
            lamports_to_sol(minimum_balance_lamports)
        )
    } else {
        info!("✅ Minimum balance OK: {}", lamports_to_sol(payer_balance));
    }

    // ---------------- VOTE AUTHORITY CHECK ---------------------
    let vote_authority_keypair = match read_keypair_file(vote_authority_keypair_path) {
        Ok(keypair) => {
            info!("✅ Vote authority keypair OK");
            keypair
        }
        Err(err) => {
            return Err(anyhow!(format!(
                "❌ Failed to read vote authority keypair file: {}",
                err
            )))
        }
    };

    let validator_identity =
        match get_validator_identity(&rpc_client, &validator_vote_account).await {
            Ok(identity) => {
                info!("✅ Validator identity OK");
                identity
            }
            Err(err) => {
                return Err(anyhow!(format!(
                    "❌ Failed to get validator identity: {}",
                    err
                )))
            }
        };

    if validator_identity.ne(&vote_authority_keypair.pubkey()) {
        warn!("⚠️ Vote authority keypair does not match validator identity");
    }

    // ---------------- COMMISSION CHECK ---------------------
    if commission_bps > MAX_BPS {
        return Err(anyhow!(
            "❌ Invalid commission percentage: {} cannot be larger than {}",
            commission_bps,
            MAX_BPS
        ));
    } else if commission_bps > 5000 {
        warn!(
            "⚠️ Commission is more than 50% ({}%) - this may result in loss of JitoSOL stake",
            commission_bps
        );
    } else {
        info!("✅ Commission OK");
    }

    // ---------------- MERKLE CHECK ---------------------
    let default_merkle_root_upload_authority =
        pubkey!("2AxPPApUQWvo2JsB52iQC4gbEipAWjRvmnNyDHJgd6Pe");
    if merkle_root_upload_authority.ne(&default_merkle_root_upload_authority) {
        warn!(
            "⚠️ Merkle root upload authority is not default {} != {}",
            merkle_root_upload_authority, default_merkle_root_upload_authority
        );
    } else {
        info!("✅ Merkle root OK");
    }

    // -------------- PRIORITY FEE DISTRIBUTION CHECK --------------
    let default_priority_fee_distribution_program =
        pubkey!("Priority6weCZ5HwDn29NxLFpb7TDp2iLZ6XKc5e8d3");
    if priority_fee_distribution_program.ne(&default_priority_fee_distribution_program) {
        warn!(
            "⚠️ Priority fee distribution program is not default {} != {}",
            priority_fee_distribution_program, default_priority_fee_distribution_program
        );
    } else {
        info!("✅ Priority fee distribution program OK");
    }

    // -------------- TRANSACTIONS PER EPOCH CHECK --------------
    if transactions_per_epoch == 0 {
        return Err(anyhow!("❌ Transactions per epoch cannot be zero"));
    } else {
        info!("✅ Transactions per epoch OK: {}", transactions_per_epoch);
    }

    info!("✅ Priority fees OK: {}", priority_fee_lamports);
    info!("✅ Loop sleep MS OK: {}", loop_timeout_ms);

    if should_send_metrics() {
        info!("✅ Metrics OK");
    }

    info!("");
    info!("✅ All parameters OK");

    Ok(())
}

async fn get_rewards_safe(rpc_client: &RpcClient, slot: u64) -> Result<(bool, u64)> {
    match rpc_client.get_block_with_config(slot, RpcBlockConfig {
        max_supported_transaction_version: Some(0),
        rewards: Some(true),
        ..RpcBlockConfig::default()
    }).await {
        Ok(block) => {
            if let Some(rewards) = block.rewards {
                let priority_fee_lamports: i64 =
                    rewards
                    .iter()
                    .filter(|r| r.reward_type == Some(RewardType::Fee))
                    .map(|r| r.lamports)
                    .sum();
                return Ok((false, priority_fee_lamports as u64));
            } else {
                return Err(anyhow!("No rewards found"));
            }
        }
        Err(e) => {
            // - `Could not get block, RPC response error -32009: Slot 336212841 was skipped, or missing in long-term storage;` - This is OK
            // - `Could not get block, RPC response error -32011: Transaction history is not available from this node;` - This is not okay, and will need a new RPC
            // - `Could not get block, RPC response error -32015: Transaction version (0) is not supported by the requesting client. Please try the request again with the following configuration parameter: "maxSupportedTransactionVersion": 0;` - Not sure yet
            // - `Could not get block, RPC response error -32007: Slot 336638156 was skipped, or missing due to ledger jump to recent snapshot;` - Not sure yet
            // - `Could not get block, invalid type: null, expected struct UiConfirmedBlock` - Not sure yet
            if e.to_string().contains("RPC response error -32009")
                || e.to_string().contains("RPC response error -32007")
            {
                return Ok((true, 0));
            } else {
                return Err(anyhow!(format!("Failed to get block: {}", e)));
            }
        }
    }
}

// ------------------------- HELPER FUNCTIONS -----------------------------

fn should_send_metrics() -> bool {
    env::var("SOLANA_METRICS_CONFIG")
        .map(|v| !v.is_empty())
        .unwrap_or(false)
}

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

async fn sleep_ms(ms: u64) {
    sleep(Duration::from_millis(ms)).await;
}

async fn delay_past_leader_slot(rpc_client: &RpcClient, fee_records: &FeeRecords) -> Result<()> {
    loop {
        let epoch_info = rpc_client
            .get_epoch_info_with_commitment(CommitmentConfig::finalized())
            .await?;
        if fee_records.does_record_exist(epoch_info.absolute_slot, epoch_info.epoch) {
            info!("Currently leader, sleeping...: ( {} )", epoch_info.absolute_slot);
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

async fn get_priority_fee_distribution_account(
    rpc_client: &RpcClient,
    validator_vote_account: &Pubkey,
    priority_fee_distribution_program: &Pubkey,
    running_epoch: u64,
) -> (Option<Account>, Pubkey) {
    let (priority_fee_distribution_account, _) = get_priority_fee_distribution_account_address(
        validator_vote_account,
        priority_fee_distribution_program,
        running_epoch,
    );

    let result = rpc_client.get_account(&priority_fee_distribution_account).await;

    let account = match result {
        Ok(account) => Some(account),
        _ => None,
    };

    (account, priority_fee_distribution_account)
}

async fn check_priority_fee_distribution_account_exsists(
    rpc_client: &RpcClient,
    validator_vote_account: &Pubkey,
    priority_fee_distribution_program: &Pubkey,
    running_epoch: u64,
) -> bool {
    let (account, _) = get_priority_fee_distribution_account(
        rpc_client,
        validator_vote_account,
        priority_fee_distribution_program,
        running_epoch,
    ).await;

    account.is_some()
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
        .get_leader_schedule_with_config(
            Some(epoch_info.absolute_slot),
            RpcLeaderScheduleConfig {
                identity: Some(validator_identity.to_string()),
                commitment: Some(CommitmentConfig::finalized()),
            }
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
        info!("Processing slot {}", slot);
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
            let block_result = get_rewards_safe(rpc_client, record.slot).await;

            match block_result {
                Ok((should_skip, rewards)) => {
                    if should_skip {
                        let result = fee_records.skip_record(record.slot, record.epoch);
                        if let Err(err) = result {
                            error!(
                                "Error skipping priority fee record for slot {}: {}",
                                record.slot, err
                            );
                        }
                    } else {
                        info!(
                            "Recorded Priority Fees for {}: {}",
                            record.slot,
                            lamports_to_sol(rewards as u64)
                        );
                        let result =
                            fee_records.process_record(record.slot, record.epoch, rewards as u64);
                        if let Err(err) = result {
                            error!(
                                "Error processing priority fee record for slot {}: {}",
                                record.slot, err
                            );
                        }
                    }
                }
                Err(e) => {
                    error!(
                        "Could not get block for slot {} - You may have to switch your RPC provider: {}",
                        record.slot, e
                    );
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

    info!(
        "Should Transfer: {:.1}% > {:.1}% ({})",
        percentage_of_epoch,
        transfer_count as f64 * percentage_per_transaction,
        percentage_of_epoch > transfer_count as f64 * percentage_per_transaction
    );

    if transfer_count == 0 {
        return true;
    }

    percentage_of_epoch > transfer_count as f64 * percentage_per_transaction
}

async fn handle_pending_blocks(
    cluster: Cluster,
    rpc_client: &RpcClient,
    fee_records: &FeeRecords,
    payer_keypair: &Keypair,
    vote_authority_keypair: &Keypair,
    validator_vote_account: &Pubkey,
    validator_identity: &Pubkey,
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
    let mut total_priority_fees: u64 = 0;
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

        total_priority_fees = total_priority_fees.saturating_add(record.priority_fee_lamports);
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


            let (priority_fee_distribution_account, _) = get_priority_fee_distribution_account_address(
                validator_vote_account,
                priority_fee_distribution_program,
                running_epoch_info.epoch,
            );
            let internal_balance = match get_priority_fee_distribution_account_internal_balance(
                rpc_client,
                validator_vote_account,
                priority_fee_distribution_program,
                running_epoch_info.epoch,
            ).await {
                Ok(balance) => balance,
                Err(err) => {
                    error!("Error getting internal balance: {:?}", err);
                    0
                }
            };

            info!(
                "Share Transaction sent: {} ({}: {} | {})",
                sig,
                slot_landed,
                lamports_to_sol(amount_to_transfer),
                lamports_to_sol(amount_to_transfer + internal_balance)
            );

            emit_transfer(
                cluster.clone(),
                validator_vote_account,
                validator_identity,
                &priority_fee_distribution_account,
                priority_fee_distribution_program,
                running_epoch_info,
                &sig.to_string(),
                records_to_transfer.len() as u64,
                total_priority_fees,
                amount_to_transfer,
                internal_balance,
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
// ------------------------- METRICS -----------------------------------
pub fn emit_heartbeat(
    cluster: Cluster,
    validator_vote_account: &Pubkey,
    validator_identity: &Pubkey,
    priority_fee_distribution_program: &Pubkey,
    running_epoch_info: &PFEpochInfo,
) {
    if !should_send_metrics() {
        return;
    }

    let (priority_fee_distribution_account, _) = get_priority_fee_distribution_account_address(validator_vote_account, priority_fee_distribution_program, running_epoch_info.epoch);

    datapoint_info!(
        "pfs-heatbeat-0.0.5",
        ("vote-account", validator_vote_account.to_string(), String),
        ("identity", validator_identity.to_string(), String),
        ("priority-fee-distribution-account", priority_fee_distribution_account.to_string(), String),
        ("priority-fee-distribution-program", priority_fee_distribution_program.to_string(), String),
        ("epoch", running_epoch_info.epoch, i64),
        ("slot", running_epoch_info.slot, i64),
        "cluster" => cluster.to_string(),
    );
}

pub async fn emit_state(
    cluster: Cluster,
    rpc_client: &RpcClient,
    fee_records: &FeeRecords,
    validator_vote_account: &Pubkey,
    validator_identity: &Pubkey,
    priority_fee_distribution_program: &Pubkey,
    running_epoch_info: &PFEpochInfo,
) -> Result<()> {
    if !should_send_metrics() {
        return Ok(());
    }

    delay_past_leader_slot(rpc_client, fee_records).await?;

    let (priority_fee_distribution_account, _) = get_priority_fee_distribution_account_address(validator_vote_account, priority_fee_distribution_program, running_epoch_info.epoch);
    let external_balance = get_priority_fee_distribution_account_balance(
        rpc_client,
        validator_vote_account,
        priority_fee_distribution_program,
        running_epoch_info.epoch,
    ).await?;

    let internal_balance = get_priority_fee_distribution_account_internal_balance(
        rpc_client,
        validator_vote_account,
        priority_fee_distribution_program,
        running_epoch_info.epoch,
    ).await?;


    let unprocessed_records = fee_records.get_records_by_state(FeeRecordState::Unprocessed)?;
    let pending_records = fee_records.get_records_by_state(FeeRecordState::ProcessedAndPending)?;

    let unprocessed_record_count = unprocessed_records.len();
    let pending_record_count = pending_records.len();
    let pending_lamports: i64 = pending_records.iter().map(|record| record.priority_fee_lamports as i64).sum();

    datapoint_info!(
        "pfs-state-0.0.5",
        ("vote-account", validator_vote_account.to_string(), String),
        ("identity", validator_identity.to_string(), String),
        ("priority-fee-distribution-account", priority_fee_distribution_account.to_string(), String),
        ("priority-fee-distribution-program", priority_fee_distribution_program.to_string(), String),
        ("priority-fee-distribution-account-external-balance", external_balance, i64),
        ("priority-fee-distribution-account-internal-balance", internal_balance, i64),
        ("unprocessed-record-count", unprocessed_record_count, i64),
        ("pending-record-count", pending_record_count, i64),
        ("pending-lamports", pending_lamports, i64),
        ("epoch", running_epoch_info.epoch, i64),
        ("slot", running_epoch_info.slot, i64),
        "cluster" => cluster.to_string(),
    );

    Ok(())
}

pub fn emit_transfer(
    cluster: Cluster,
    validator_vote_account: &Pubkey,
    validator_identity: &Pubkey,
    priority_fee_distribution_account: &Pubkey,
    priority_fee_distribution_program: &Pubkey,
    running_epoch_info: &PFEpochInfo,
    signature: &String,
    slots_covered: u64,
    total_priority_fees: u64,
    transfer_amount_lamports: u64,
    priority_fee_distribution_account_balance: u64,
){
    if !should_send_metrics() {
        return;
    }

    datapoint_info!(
        "pfs-transfer-0.0.5",
        ("vote-account", validator_vote_account.to_string(), String),
        ("identity", validator_identity.to_string(), String),
        ("epoch", running_epoch_info.epoch, i64),
        ("slot", running_epoch_info.slot, i64),
        ("signature", signature.to_string(), String),
        ("slots-covered", slots_covered, i64),
        ("total-priority-fees", total_priority_fees, i64),
        ("transfer-amount-lamports", transfer_amount_lamports, i64),
        ("priority-fee-distribution-account-balance", priority_fee_distribution_account_balance, i64),
        ("priority-fee-distribution-account", priority_fee_distribution_account.to_string(), String),
        ("priority-fee-distribution-program", priority_fee_distribution_program.to_string(), String),
        "cluster" => cluster.to_string(),
    );
}

pub fn emit_error(
    cluster: Cluster,
    validator_vote_account: &Pubkey,
    validator_identity: &Pubkey,
    priority_fee_distribution_program: &Pubkey,
    running_epoch_info: &PFEpochInfo,
    error_string: String,
){
    if !should_send_metrics() {
        return;
    }

    let (priority_fee_distribution_account, _) = get_priority_fee_distribution_account_address(validator_vote_account, priority_fee_distribution_program, running_epoch_info.epoch);

    datapoint_error!(
        "pfs-error-0.0.5",
        ("vote-account", validator_vote_account.to_string(), String),
        ("identity", validator_identity.to_string(), String),
        ("priority-fee-distribution-account", priority_fee_distribution_account.to_string(), String),
        ("priority-fee-distribution-program", priority_fee_distribution_program.to_string(), String),
        ("epoch", running_epoch_info.slot, i64),
        ("slot", running_epoch_info.epoch, i64),
        ("error", error_string, String),
        "cluster" => cluster.to_string(),
    );
}

// ------------------------- MAIN FUNCTIONS -----------------------------
pub async fn share_priority_fees_loop(
    cluster: Cluster,
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
    loop_sleep_ms: u64,
    verify: bool,
) -> Result<()> {
    // ------------------ VERIFY SETUP -----------------------------
    verify_setup(
        rpc_url.clone(),
        fee_records_db_path.clone(),
        priority_fee_payer_keypair_path.clone(),
        vote_authority_keypair_path.clone(),
        validator_vote_account,
        merkle_root_upload_authority,
        priority_fee_distribution_program,
        minimum_balance_lamports,
        commission_bps,
        priority_fee_lamports,
        transactions_per_epoch,
        loop_sleep_ms,
    )
    .await?;

    if verify {
        return Ok(());
    }

    // ------------------ LOCAL SETUP -----------------------------
    let rpc_client = RpcClient::new(rpc_url);
    let fee_records = FeeRecords::new(fee_records_db_path)?;

    let payer_keypair = read_keypair_file(priority_fee_payer_keypair_path)
        .expect("Failed to read payer keypair file");
    let vote_authority_keypair = read_keypair_file(vote_authority_keypair_path)
        .expect("Failed to read vote authority keypair file");
    let validator_identity = get_validator_identity(&rpc_client, &validator_vote_account).await?;

    let mut running_epoch_info = PFEpochInfo::null();
    let mut transfer_count = 0;

    // ------------------ LOOP -----------------------------
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
            Err(err) => {
                error!("Error handling epoch and leader slots: {}", err);
                emit_error(
                    cluster.clone(),
                    &validator_vote_account,
                    &validator_identity,
                    &priority_fee_distribution_program,
                    &running_epoch_info,
                    err.to_string()
                );
            },
        }

        // 2. Handle unprocessed blocks
        info!(" -------- 2. HANDLE UNPROCESSED BLOCKS -----------");
        let result =
            handle_unprocessed_blocks(&rpc_client, &fee_records, &running_epoch_info).await;
        if let Err(err) = result {
            error!("Error handling unprocessed records: {}", err);
            emit_error(
                cluster.clone(),
                &validator_vote_account,
                &validator_identity,
                &priority_fee_distribution_program,
                &running_epoch_info,
                err.to_string()
            );
        }

        // 3. Handle pending blocks
        info!(" -------- 3. HANDLE PENDING BLOCKS -----------");
        let result = handle_pending_blocks(
            cluster.clone(),
            &rpc_client,
            &fee_records,
            &payer_keypair,
            &vote_authority_keypair,
            &validator_vote_account,
            &validator_identity,
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
            Err(err) => {
                error!("Error handling pending blocks: {}", err);
                emit_error(
                    cluster.clone(),
                    &validator_vote_account,
                    &validator_identity,
                    &priority_fee_distribution_program,
                    &running_epoch_info,
                    err.to_string()
                );
            },
        }

        // 4. Emit heartbeat
        info!(" -------- 4. EMIT HEARTBEAT -----------");
        emit_heartbeat(cluster.clone(), &validator_vote_account, &validator_identity, &priority_fee_distribution_program, &running_epoch_info);
        let result = emit_state(cluster.clone(), &rpc_client, &fee_records, &validator_vote_account, &validator_identity, &priority_fee_distribution_program, &running_epoch_info).await;
        if let Err(err) = result {
            error!("Error emitting state: {}", err);
            emit_error(
                cluster.clone(),
                &validator_vote_account,
                &validator_identity,
                &priority_fee_distribution_program,
                &running_epoch_info,
                err.to_string()
            );
        }

        // 5. Sleep
        info!(" -------- 5. SLEEP {} SECONDS -----------", loop_sleep_ms / 1000);
        sleep_ms(loop_sleep_ms).await;
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
        _ => {
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
