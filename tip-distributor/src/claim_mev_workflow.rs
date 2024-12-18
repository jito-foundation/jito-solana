use {
    crate::{send_until_blockhash_expires, GeneratedMerkleTreeCollection},
    anchor_lang::{AccountDeserialize, InstructionData, ToAccountMetas},
    itertools::Itertools,
    jito_tip_distribution::state::{ClaimStatus, Config, TipDistributionAccount},
    log::{error, info, warn},
    rand::{prelude::SliceRandom, thread_rng},
    solana_client::nonblocking::rpc_client::RpcClient,
    solana_metrics::datapoint_info,
    solana_program::{
        fee_calculator::DEFAULT_TARGET_LAMPORTS_PER_SIGNATURE, native_token::LAMPORTS_PER_SOL,
        system_program,
    },
    solana_rpc_client_api::config::RpcSimulateTransactionConfig,
    solana_sdk::{
        account::Account,
        commitment_config::CommitmentConfig,
        compute_budget::ComputeBudgetInstruction,
        instruction::Instruction,
        pubkey::Pubkey,
        signature::{Keypair, Signer},
        transaction::Transaction,
    },
    std::{
        collections::HashMap,
        sync::Arc,
        time::{Duration, Instant},
    },
    thiserror::Error,
};

#[derive(Error, Debug)]
pub enum ClaimMevError {
    #[error(transparent)]
    IoError(#[from] std::io::Error),

    #[error(transparent)]
    JsonError(#[from] serde_json::Error),

    #[error(transparent)]
    AnchorError(anchor_lang::error::Error),

    #[error(transparent)]
    RpcError(#[from] solana_rpc_client_api::client_error::Error),

    #[error("Expected to have at least {desired_balance} lamports in {payer:?}. Current balance is {start_balance} lamports. Deposit {sol_to_deposit} SOL to continue.")]
    InsufficientBalance {
        desired_balance: u64,
        payer: Pubkey,
        start_balance: u64,
        sol_to_deposit: u64,
    },

    #[error("Not finished with job, transactions left {transactions_left}")]
    NotFinished { transactions_left: usize },

    #[error("UncaughtError {e:?}")]
    UncaughtError { e: String },
}

pub async fn get_claim_transactions_for_valid_unclaimed(
    rpc_client: &RpcClient,
    merkle_trees: &GeneratedMerkleTreeCollection,
    tip_distribution_program_id: Pubkey,
    micro_lamports: u64,
    payer_pubkey: Pubkey,
) -> Result<Vec<Transaction>, ClaimMevError> {
    let tree_nodes = merkle_trees
        .generated_merkle_trees
        .iter()
        .flat_map(|tree| &tree.tree_nodes)
        .collect_vec();

    info!(
        "reading tip distribution related accounts for epoch {}",
        merkle_trees.epoch
    );

    let start = Instant::now();

    let tda_pubkeys = merkle_trees
        .generated_merkle_trees
        .iter()
        .map(|tree| tree.tip_distribution_account)
        .collect_vec();
    let tdas: HashMap<Pubkey, Account> = crate::get_batched_accounts(rpc_client, &tda_pubkeys)
        .await?
        .into_iter()
        .filter_map(|(pubkey, a)| Some((pubkey, a?)))
        .collect();

    let claimant_pubkeys = tree_nodes
        .iter()
        .map(|tree_node| tree_node.claimant)
        .collect_vec();
    let claimants: HashMap<Pubkey, Account> =
        crate::get_batched_accounts(rpc_client, &claimant_pubkeys)
            .await?
            .into_iter()
            .filter_map(|(pubkey, a)| Some((pubkey, a?)))
            .collect();

    let claim_status_pubkeys = tree_nodes
        .iter()
        .map(|tree_node| tree_node.claim_status_pubkey)
        .collect_vec();
    let claim_statuses: HashMap<Pubkey, Account> =
        crate::get_batched_accounts(rpc_client, &claim_status_pubkeys)
            .await?
            .into_iter()
            .filter_map(|(pubkey, a)| Some((pubkey, a?)))
            .collect();

    let elapsed_us = start.elapsed().as_micros();

    // can be helpful for determining mismatch in state between requested and read
    datapoint_info!(
        "claim_mev-get_claim_transactions_account_data",
        ("elapsed_us", elapsed_us, i64),
        ("tdas", tda_pubkeys.len(), i64),
        ("tdas_onchain", tdas.len(), i64),
        ("claimants", claimant_pubkeys.len(), i64),
        ("claimants_onchain", claimants.len(), i64),
        ("claim_statuses", claim_status_pubkeys.len(), i64),
        ("claim_statuses_onchain", claim_statuses.len(), i64),
    );

    let transactions = build_mev_claim_transactions(
        tip_distribution_program_id,
        merkle_trees,
        tdas,
        claimants,
        claim_statuses,
        micro_lamports,
        payer_pubkey,
    );

    Ok(transactions)
}

pub async fn claim_mev_tips(
    merkle_trees: &GeneratedMerkleTreeCollection,
    rpc_url: String,
    tip_distribution_program_id: Pubkey,
    keypair: Arc<Keypair>,
    max_loop_duration: Duration,
    micro_lamports: u64,
) -> Result<(), ClaimMevError> {
    let rpc_client = RpcClient::new_with_timeout_and_commitment(
        rpc_url,
        Duration::from_secs(300),
        CommitmentConfig::confirmed(),
    );

    let start = Instant::now();
    while start.elapsed() <= max_loop_duration {
        let mut all_claim_transactions = get_claim_transactions_for_valid_unclaimed(
            &rpc_client,
            merkle_trees,
            tip_distribution_program_id,
            micro_lamports,
            keypair.pubkey(),
        )
        .await?;

        datapoint_info!(
            "claim_mev_tips-send_summary",
            ("claim_transactions_left", all_claim_transactions.len(), i64),
        );

        if all_claim_transactions.is_empty() {
            return Ok(());
        }

        all_claim_transactions.shuffle(&mut thread_rng());
        let transactions: Vec<_> = all_claim_transactions.into_iter().take(10_000).collect();

        // only check balance for the ones we need to currently send since reclaim rent running in parallel
        if let Some((start_balance, desired_balance, sol_to_deposit)) =
            is_sufficient_balance(&keypair.pubkey(), &rpc_client, transactions.len() as u64).await
        {
            return Err(ClaimMevError::InsufficientBalance {
                desired_balance,
                payer: keypair.pubkey(),
                start_balance,
                sol_to_deposit,
            });
        }

        let blockhash = rpc_client.get_latest_blockhash().await?;
        let _ = send_until_blockhash_expires(&rpc_client, transactions, blockhash, &keypair).await;
    }

    let transactions = get_claim_transactions_for_valid_unclaimed(
        &rpc_client,
        merkle_trees,
        tip_distribution_program_id,
        micro_lamports,
        keypair.pubkey(),
    )
    .await?;
    if transactions.is_empty() {
        return Ok(());
    }

    // if more transactions left, we'll simulate them all to make sure its not an uncaught error
    let mut is_error = false;
    let mut error_str = String::new();
    for tx in &transactions {
        match rpc_client
            .simulate_transaction_with_config(
                tx,
                RpcSimulateTransactionConfig {
                    sig_verify: false,
                    replace_recent_blockhash: true,
                    commitment: Some(CommitmentConfig::processed()),
                    ..RpcSimulateTransactionConfig::default()
                },
            )
            .await
        {
            Ok(_) => {}
            Err(e) => {
                error_str = e.to_string();
                is_error = true;

                match e.get_transaction_error() {
                    None => {
                        break;
                    }
                    Some(e) => {
                        warn!("transaction error. tx: {:?} error: {:?}", tx, e);
                        break;
                    }
                }
            }
        }
    }

    if is_error {
        Err(ClaimMevError::UncaughtError { e: error_str })
    } else {
        Err(ClaimMevError::NotFinished {
            transactions_left: transactions.len(),
        })
    }
}

/// Returns a list of claim transactions for valid, unclaimed MEV tips
/// A valid, unclaimed transaction consists of the following:
/// - there must be lamports to claim for the tip distribution account.
/// - there must be a merkle root.
/// - the claimant (typically a stake account) must exist.
/// - the claimant (typically a stake account) must have a non-zero amount of tips to claim
/// - the claimant must have enough lamports post-claim to be rent-exempt.
///   - note: there aren't any rent exempt accounts on solana mainnet anymore.
/// - it must not have already been claimed.
fn build_mev_claim_transactions(
    tip_distribution_program_id: Pubkey,
    merkle_trees: &GeneratedMerkleTreeCollection,
    tdas: HashMap<Pubkey, Account>,
    claimants: HashMap<Pubkey, Account>,
    claim_status: HashMap<Pubkey, Account>,
    micro_lamports: u64,
    payer_pubkey: Pubkey,
) -> Vec<Transaction> {
    let tip_distribution_accounts: HashMap<Pubkey, TipDistributionAccount> = tdas
        .iter()
        .filter_map(|(pubkey, account)| {
            Some((
                *pubkey,
                TipDistributionAccount::try_deserialize(&mut account.data.as_slice()).ok()?,
            ))
        })
        .collect();

    let claim_statuses: HashMap<Pubkey, ClaimStatus> = claim_status
        .iter()
        .filter_map(|(pubkey, account)| {
            Some((
                *pubkey,
                ClaimStatus::try_deserialize(&mut account.data.as_slice()).ok()?,
            ))
        })
        .collect();

    datapoint_info!(
        "build_mev_claim_transactions",
        (
            "tip_distribution_accounts",
            tip_distribution_accounts.len(),
            i64
        ),
        ("claim_statuses", claim_statuses.len(), i64),
    );

    let tip_distribution_config =
        Pubkey::find_program_address(&[Config::SEED], &tip_distribution_program_id).0;

    let mut instructions = Vec::with_capacity(claimants.len());
    for tree in &merkle_trees.generated_merkle_trees {
        if tree.max_total_claim == 0 {
            continue;
        }

        // if unwrap panics, there's a bug in the merkle tree code because the merkle tree code relies on the state
        // of the chain to claim.
        let tip_distribution_account = tip_distribution_accounts
            .get(&tree.tip_distribution_account)
            .unwrap();

        // can continue here, as there might be tip distribution accounts this account doesn't upload for
        if tip_distribution_account.merkle_root.is_none() {
            continue;
        }

        for node in &tree.tree_nodes {
            // doesn't make sense to claim for claimants that don't exist anymore
            // can't claim for something already claimed
            // don't need to claim for claimants that get 0 MEV
            if !claimants.contains_key(&node.claimant)
                || claim_statuses.contains_key(&node.claim_status_pubkey)
                || node.amount == 0
            {
                continue;
            }

            instructions.push(Instruction {
                program_id: tip_distribution_program_id,
                data: jito_tip_distribution::instruction::Claim {
                    proof: node.proof.clone().unwrap(),
                    amount: node.amount,
                    bump: node.claim_status_bump,
                }
                .data(),
                accounts: jito_tip_distribution::accounts::Claim {
                    config: tip_distribution_config,
                    tip_distribution_account: tree.tip_distribution_account,
                    claimant: node.claimant,
                    claim_status: node.claim_status_pubkey,
                    payer: payer_pubkey,
                    system_program: system_program::id(),
                }
                .to_account_metas(None),
            });
        }
    }

    // TODO (LB): see if we can do >1 claim here
    let transactions: Vec<Transaction> = instructions
        .into_iter()
        .map(|claim_ix| {
            let priority_fee_ix = ComputeBudgetInstruction::set_compute_unit_price(micro_lamports);
            Transaction::new_with_payer(&[priority_fee_ix, claim_ix], Some(&payer_pubkey))
        })
        .collect();

    transactions
}

/// heuristic to make sure we have enough funds to cover the rent costs if epoch has many validators
/// If insufficient funds, returns start balance, desired balance, and amount of sol to deposit
async fn is_sufficient_balance(
    payer: &Pubkey,
    rpc_client: &RpcClient,
    instruction_count: u64,
) -> Option<(u64, u64, u64)> {
    let start_balance = rpc_client
        .get_balance(payer)
        .await
        .expect("Failed to get starting balance");
    // most amounts are for 0 lamports. had 1736 non-zero claims out of 164742
    let min_rent_per_claim = rpc_client
        .get_minimum_balance_for_rent_exemption(ClaimStatus::SIZE)
        .await
        .expect("Failed to calculate min rent");
    let desired_balance = instruction_count
        .checked_mul(
            min_rent_per_claim
                .checked_add(DEFAULT_TARGET_LAMPORTS_PER_SIGNATURE)
                .unwrap(),
        )
        .unwrap();
    if start_balance < desired_balance {
        let sol_to_deposit = desired_balance
            .checked_sub(start_balance)
            .unwrap()
            .checked_add(LAMPORTS_PER_SOL)
            .unwrap()
            .checked_sub(1)
            .unwrap()
            .checked_div(LAMPORTS_PER_SOL)
            .unwrap(); // rounds up to nearest sol
        Some((start_balance, desired_balance, sol_to_deposit))
    } else {
        None
    }
}
