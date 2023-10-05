use crate::{MAX_CONCURRENT_RPC_CALLS, MAX_FETCH_RETRIES, MAX_RETRY_DURATION, MAX_SEND_RETRIES};
use solana_metrics::datapoint_error;
use std::sync::Arc;
use tokio::sync::Semaphore;
use {
    crate::{
        read_json_from_file, sign_and_send_transactions_with_retries, GeneratedMerkleTreeCollection,
    },
    anchor_lang::{AccountDeserialize, InstructionData, ToAccountMetas},
    itertools::Itertools,
    log::{debug, info, warn},
    solana_client::nonblocking::rpc_client::RpcClient,
    solana_metrics::{datapoint_info, datapoint_warn},
    solana_program::{
        fee_calculator::DEFAULT_TARGET_LAMPORTS_PER_SIGNATURE, native_token::LAMPORTS_PER_SOL,
        stake::state::StakeState, system_program,
    },
    solana_rpc_client_api::request::MAX_MULTIPLE_ACCOUNTS,
    solana_sdk::{
        account::Account,
        commitment_config::CommitmentConfig,
        instruction::Instruction,
        pubkey::Pubkey,
        signature::{read_keypair_file, Signer},
        transaction::Transaction,
    },
    std::{
        collections::HashMap,
        path::PathBuf,
        time::{Duration, Instant},
    },
    thiserror::Error,
    tip_distribution::state::*,
};

#[derive(Error, Debug)]
pub enum ClaimMevError {
    #[error(transparent)]
    IoError(#[from] std::io::Error),

    #[error(transparent)]
    JsonError(#[from] serde_json::Error),
}

pub async fn claim_mev_tips(
    merkle_root_path: &PathBuf,
    rpc_url: String,
    tip_distribution_program_id: &Pubkey,
    keypair_path: &PathBuf,
) -> Result<(), ClaimMevError> {
    /// Number of instructions in a transaction
    const TRANSACTION_IX_BATCH_SIZE: usize = 1;

    let transaction_prepare_start = Instant::now();
    let merkle_trees: GeneratedMerkleTreeCollection =
        read_json_from_file(merkle_root_path).expect("read GeneratedMerkleTreeCollection");
    let keypair = read_keypair_file(keypair_path).expect("read keypair file");

    let tip_distribution_config =
        Pubkey::find_program_address(&[Config::SEED], tip_distribution_program_id).0;
    let rpc_client = RpcClient::new_with_commitment(rpc_url, CommitmentConfig::finalized());

    let tree_nodes = merkle_trees
        .generated_merkle_trees
        .iter()
        .flat_map(|tree| &tree.tree_nodes)
        .collect_vec();
    let tree_node_count = tree_nodes.len();
    let stake_acct_min_rent = rpc_client
        .get_minimum_balance_for_rent_exemption(StakeState::size_of())
        .await
        .expect("Failed to calculate min rent");
    let mut skipped_merkle_root_count: usize = 0;
    let mut zero_lamports_count: usize = 0;
    let mut already_claimed_count: usize = 0;
    let mut below_min_rent_count: usize = 0;
    let mut instructions =
        Vec::with_capacity(tree_nodes.iter().filter(|node| node.amount > 0).count());

    // fetch all accounts up front
    info!("Starting to fetch accounts");
    let account_fetch_start = Instant::now();
    let tdas = get_batched_accounts(
        &rpc_client,
        merkle_trees
            .generated_merkle_trees
            .iter()
            .map(|tree| tree.tip_distribution_account)
            .collect_vec(),
    )
    .await
    .unwrap()
    .into_iter()
    .filter_map(|(pubkey, maybe_account)| {
        let account = match maybe_account {
            Some(account) => account,
            None => {
                datapoint_warn!(
                    "claim_mev_workflow-account_error",
                    ("pubkey", pubkey.to_string(), String),
                    ("account_type", "tip_distribution_account", String),
                    ("error", 1, i64),
                    ("err_type", "fetch", String),
                    ("err_str", "Failed to fetch Account", String)
                );
                return None;
            }
        };

        let account = match TipDistributionAccount::try_deserialize(&mut account.data.as_slice()) {
            Ok(a) => a,
            Err(e) => {
                datapoint_warn!(
                    "claim_mev_workflow-account_error",
                    ("pubkey", pubkey.to_string(), String),
                    ("account_type", "tip_distribution_account", String),
                    ("error", 1, i64),
                    ("err_type", "deserialize_tip_distribution_account", String),
                    ("err_str", e.to_string(), String)
                );
                return None;
            }
        };
        Some((pubkey, account))
    })
    .collect::<HashMap<Pubkey, TipDistributionAccount>>();

    // track balances only
    let claimants = get_batched_accounts(
        &rpc_client,
        tree_nodes
            .iter()
            .map(|tree_node| tree_node.claimant)
            .collect_vec(),
    )
    .await
    .unwrap()
    .into_iter()
    .map(|(pubkey, maybe_account)| {
        (
            pubkey,
            maybe_account
                .map(|account| account.lamports)
                .unwrap_or_default(),
        )
    })
    .collect::<HashMap<Pubkey, u64>>();

    let claim_statuses = get_batched_accounts(
        &rpc_client,
        tree_nodes
            .iter()
            .map(|tree_node| tree_node.claim_status_pubkey)
            .collect_vec(),
    )
    .await
    .unwrap();
    let account_fetch_elapsed = account_fetch_start.elapsed();

    // prepare instructions to transfer to all claimants
    for tree in merkle_trees.generated_merkle_trees {
        let fetched_tip_distribution_account = match tdas.get(&tree.tip_distribution_account) {
            Some(account) => account,
            None => panic!(
                "TDA not found in cache for account: {:?}",
                tree.tip_distribution_account
            ),
        };
        // only claim for ones that have merkle root on-chain
        if fetched_tip_distribution_account.merkle_root.is_none() {
            info!(
                "Merkle root has not uploaded yet. Skipped {} claimants for TDA: {:?}",
                tree.tree_nodes.len(),
                tree.tip_distribution_account
            );
            skipped_merkle_root_count = skipped_merkle_root_count.checked_add(1).unwrap();
            continue;
        }
        for node in tree.tree_nodes {
            if node.amount == 0 {
                zero_lamports_count = zero_lamports_count.checked_add(1).unwrap();
                continue;
            }

            // make sure not previously claimed
            match claim_statuses.get(&node.claim_status_pubkey) {
                Some(Some(_account)) => {
                    debug!(
                        "Claim status account already exists (already paid out). Skipping pubkey: {:?}.", node.claim_status_pubkey,
                    );
                    already_claimed_count = already_claimed_count.checked_add(1).unwrap();
                    continue;
                }
                None => panic!(
                    "Account not found in cache for {:?}",
                    node.claim_status_pubkey
                ),
                Some(None) => {} // expected to not find ClaimStatus account, don't skip
            };
            let current_balance = match claimants.get(&node.claimant) {
                Some(balance) => balance,
                None => panic!(
                    "Claimant not found in cache for pubkey: {:?}",
                    node.claimant
                ),
            };

            // some older accounts can be rent-paying
            // any new transfers will need to make the account rent-exempt (runtime enforced)
            let balance_with_tip = current_balance.checked_add(node.amount).unwrap();
            if balance_with_tip < stake_acct_min_rent {
                warn!("Current balance + tip claim amount of {balance_with_tip} is less than required rent-exempt of {stake_acct_min_rent} for pubkey: {}. Skipping.", node.claimant);
                below_min_rent_count = below_min_rent_count.checked_add(1).unwrap();
                continue;
            }
            instructions.push(Instruction {
                program_id: *tip_distribution_program_id,
                data: tip_distribution::instruction::Claim {
                    proof: node.proof.unwrap(),
                    amount: node.amount,
                    bump: node.claim_status_bump,
                }
                .data(),
                accounts: tip_distribution::accounts::Claim {
                    config: tip_distribution_config,
                    tip_distribution_account: tree.tip_distribution_account,
                    claimant: node.claimant,
                    claim_status: node.claim_status_pubkey,
                    payer: keypair.pubkey(),
                    system_program: system_program::id(),
                }
                .to_account_metas(None),
            });
        }
    }
    let instructions_len = instructions.len();

    // balance check heuristic to make sure we have enough funds to cover the rent costs if epoch has many validators
    {
        let start_balance = rpc_client
            .get_balance(&keypair.pubkey())
            .await
            .expect("Failed to get starting balance");
        // most amounts are for 0 lamports. had 1736 non-zero claims out of 164742
        let min_rent_per_claim = rpc_client
            .get_minimum_balance_for_rent_exemption(ClaimStatus::SIZE)
            .await
            .expect("Failed to calculate min rent");
        let desired_balance = (instructions_len as u64)
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
            panic!("Expected to have at least {desired_balance} lamports in {}, current balance is {start_balance} lamports. Deposit {sol_to_deposit} SOL to continue.", &keypair.pubkey());
        }
    }

    let mut transactions = instructions
        .into_iter()
        .chunks(TRANSACTION_IX_BATCH_SIZE)
        .into_iter() // 1.4MM compute vs 0.2MM
        .map(|ix| {
            let mut ixs = Vec::with_capacity(TRANSACTION_IX_BATCH_SIZE + 1);
            // ixs.push(ComputeBudgetInstruction::set_compute_unit_limit(
            //     solana_program_runtime::compute_budget::MAX_COMPUTE_UNIT_LIMIT,
            // ));
            ixs.extend(ix);
            Transaction::new_with_payer(&ixs, Some(&keypair.pubkey()))
        })
        .collect::<Vec<_>>();
    let transaction_prepare_elapsed = transaction_prepare_start.elapsed();

    datapoint_info!(
        "claim_mev_workflow-prepare_transactions",
        ("tree_node_count", tree_node_count, i64),
        ("tda_count", tdas.len(), i64),
        ("claimant_count", claimants.len(), i64),
        ("claim_status_count", claim_statuses.len(), i64),
        ("skipped_merkle_root_count", skipped_merkle_root_count, i64),
        ("zero_lamports_count", zero_lamports_count, i64),
        ("already_claimed_count", already_claimed_count, i64),
        ("below_min_rent_count", below_min_rent_count, i64),
        ("instruction_count", instructions_len, i64),
        ("transaction_count", transactions.len(), i64),
        (
            "account_fetch_latency_us",
            account_fetch_elapsed.as_micros(),
            i64
        ),
        ("latency_us", transaction_prepare_elapsed.as_micros(), i64),
    );

    info!("Sending {} tip claim transactions. {zero_lamports_count} tried sending zero lamports, {below_min_rent_count} would be below minimum rent",
            transactions.len());
    let mut retries = 0;
    let mut failed_transactions = HashMap::new();
    while !transactions.is_empty() && retries < MAX_SEND_RETRIES {
        let transactions_len = transactions.len();
        let send_start = Instant::now();
        let (to_process_transactions, new_failed_transactions) =
            sign_and_send_transactions_with_retries(
                &keypair,
                &rpc_client,
                transactions,
                MAX_RETRY_DURATION,
            )
            .await;

        datapoint_info!(
            "claim_mev_workflow-send_transactions",
            ("transaction_count", transactions_len, i64),
            (
                "successful_transaction_count",
                transactions_len - to_process_transactions.len(),
                i64
            ),
            (
                "remaining_transaction_count",
                to_process_transactions.len(),
                i64
            ),
            (
                "failed_transaction_count",
                new_failed_transactions.len(),
                i64
            ),
            ("send_latency_us", send_start.elapsed().as_micros(), i64),
        );
        transactions = to_process_transactions;
        failed_transactions.extend(new_failed_transactions);
        retries += 1;
    }
    if !failed_transactions.is_empty() {
        panic!(
            "Failed after {MAX_SEND_RETRIES} retries. {} remaining mev claim transactions, {} failed requests.",
            transactions.len(),
            failed_transactions.len()
        );
    }

    Ok(())
}

/// Fetch accounts in parallel batches with retries.
async fn get_batched_accounts(
    rpc_client: &RpcClient,
    pubkeys: Vec<Pubkey>,
) -> solana_rpc_client_api::client_error::Result<HashMap<Pubkey, Option<Account>>> {
    const FAIL_DELAY: Duration = Duration::from_millis(100);

    let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_RPC_CALLS));
    // let mut claimant_accounts = Vec::with_capacity(pubkeys.len());
    let futs = pubkeys.chunks(MAX_MULTIPLE_ACCOUNTS).map(|pubkeys| {
        let semaphore = semaphore.clone();

        async move {
            let _permit = semaphore.acquire_owned().await.unwrap(); // wait until our turn
            let mut retries = 0;
            loop {
                match rpc_client.get_multiple_accounts(pubkeys).await {
                    Ok(accts) => return Ok(accts),
                    Err(e) => {
                        retries += 1;
                        if retries == MAX_FETCH_RETRIES {
                            datapoint_error!(
                                "claim_mev_workflow-get_batched_accounts_error",
                                ("pubkeys", format!("{pubkeys:?}"), String),
                                ("error", 1, i64),
                                ("err_type", "fetch_account", String),
                                ("err_str", e.to_string(), String)
                            );
                            return Err(e);
                        }
                        tokio::time::sleep(FAIL_DELAY).await;
                    }
                }
            }
        }
    });

    let claimant_accounts = futures::future::join_all(futs)
        .await
        .into_iter()
        .collect::<solana_rpc_client_api::client_error::Result<Vec<Vec<Option<Account>>>>>()? // fail on single error
        .into_iter()
        .flatten()
        .collect_vec();

    Ok(pubkeys.into_iter().zip(claimant_accounts).collect())
}
