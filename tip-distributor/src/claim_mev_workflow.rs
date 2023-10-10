use {
    crate::{
        read_json_from_file, sign_and_send_transactions_with_retries_multi_rpc,
        GeneratedMerkleTreeCollection, TreeNode, FAIL_DELAY, MAX_RETRIES,
    },
    anchor_lang::{AccountDeserialize, InstructionData, ToAccountMetas},
    itertools::Itertools,
    jito_tip_distribution::state::{ClaimStatus, Config, TipDistributionAccount},
    log::{debug, info},
    solana_client::nonblocking::rpc_client::RpcClient,
    solana_metrics::{datapoint_error, datapoint_info, datapoint_warn},
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
        sync::Arc,
        time::{Duration, Instant},
    },
    thiserror::Error,
    tokio::sync::Semaphore,
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
    rpc_connection_count: u64,
    max_concurrent_rpc_get_reqs: usize,
    tip_distribution_program_id: &Pubkey,
    keypair_path: &PathBuf,
    max_loop_retries: u64,
    max_loop_duration: Duration,
) -> Result<(), ClaimMevError> {
    let merkle_trees: GeneratedMerkleTreeCollection =
        read_json_from_file(merkle_root_path).expect("read GeneratedMerkleTreeCollection");
    let keypair = Arc::new(read_keypair_file(keypair_path).expect("read keypair file"));
    let payer_pubkey = keypair.pubkey();
    let blockhash_rpc_client = Arc::new(RpcClient::new_with_commitment(
        rpc_url.clone(),
        CommitmentConfig::finalized(),
    ));
    let rpc_clients = Arc::new(
        (0..rpc_connection_count)
            .map(|_| {
                Arc::new(RpcClient::new_with_commitment(
                    rpc_url.clone(),
                    CommitmentConfig::confirmed(),
                ))
            })
            .collect_vec(),
    );

    let tree_nodes = merkle_trees
        .generated_merkle_trees
        .iter()
        .flat_map(|tree| &tree.tree_nodes)
        .collect_vec();
    let stake_acct_min_rent = blockhash_rpc_client
        .get_minimum_balance_for_rent_exemption(StakeState::size_of())
        .await
        .expect("Failed to calculate min rent");

    // fetch all accounts up front
    info!("Starting to fetch accounts");
    let account_fetch_start = Instant::now();
    let tdas = get_batched_accounts(
        &blockhash_rpc_client,
        max_concurrent_rpc_get_reqs,
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
        &blockhash_rpc_client,
        max_concurrent_rpc_get_reqs,
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
        &blockhash_rpc_client,
        max_concurrent_rpc_get_reqs,
        tree_nodes
            .iter()
            .map(|tree_node| tree_node.claim_status_pubkey)
            .collect_vec(),
    )
    .await
    .unwrap();
    let account_fetch_elapsed = account_fetch_start.elapsed();

    // Try sending txns to RPC
    let mut retries = 0;
    let mut failed_transaction_count = 0;
    loop {
        let transaction_prepare_start = Instant::now();
        let (
            skipped_merkle_root_count,
            zero_lamports_count,
            already_claimed_count,
            below_min_rent_count,
            transactions,
        ) = build_transactions(
            tip_distribution_program_id,
            &merkle_trees,
            &payer_pubkey,
            &tree_nodes,
            stake_acct_min_rent,
            &tdas,
            &claimants,
            &claim_statuses,
        );
        datapoint_info!(
            "claim_mev_workflow-prepare_transactions",
            ("tree_node_count", tree_nodes.len(), i64),
            ("tda_count", tdas.len(), i64),
            ("claimant_count", claimants.len(), i64),
            ("claim_status_count", claim_statuses.len(), i64),
            ("skipped_merkle_root_count", skipped_merkle_root_count, i64),
            ("zero_lamports_count", zero_lamports_count, i64),
            ("already_claimed_count", already_claimed_count, i64),
            ("below_min_rent_count", below_min_rent_count, i64),
            ("transaction_count", transactions.len(), i64),
            (
                "account_fetch_latency_us",
                account_fetch_elapsed.as_micros(),
                i64
            ),
            (
                "transaction_prepare_latency_us",
                transaction_prepare_start.elapsed().as_micros(),
                i64
            ),
        );

        if transactions.is_empty() {
            return Ok(());
        }

        if let Some((start_balance, desired_balance, sol_to_deposit)) = is_sufficient_balance(
            &payer_pubkey,
            &blockhash_rpc_client,
            transactions.len() as u64,
        )
        .await
        {
            panic!("Expected to have at least {desired_balance} lamports in {payer_pubkey}. Current balance is {start_balance} lamports. Deposit {sol_to_deposit} SOL to continue.");
        }
        let transactions_len = transactions.len();

        info!("Sending {} tip claim transactions. {zero_lamports_count} would transfer zero lamports, {below_min_rent_count} would be below minimum rent", transactions.len());
        let send_start = Instant::now();
        let (remaining_transactions, new_failed_transaction_count) =
            sign_and_send_transactions_with_retries_multi_rpc(
                &keypair,
                &blockhash_rpc_client,
                &rpc_clients,
                transactions,
                max_loop_duration,
            )
            .await;

        datapoint_info!(
            "claim_mev_workflow-send_transactions",
            ("transaction_count", transactions_len, i64),
            (
                "successful_transaction_count",
                transactions_len - remaining_transactions.len(),
                i64
            ),
            (
                "remaining_transaction_count",
                remaining_transactions.len(),
                i64
            ),
            (
                "failed_transaction_count",
                new_failed_transaction_count,
                i64
            ),
            ("send_latency_us", send_start.elapsed().as_micros(), i64),
        );

        if remaining_transactions.is_empty() {
            info!("Finished claiming tips. {max_loop_retries} retries. {} remaining mev claim transactions, {failed_transaction_count} failed requests.", remaining_transactions.len());
            return Ok(());
        }

        failed_transaction_count += new_failed_transaction_count;
        retries += 1;
        if retries >= max_loop_retries {
            panic!(
                "Failed after {max_loop_retries} retries. {} remaining mev claim transactions, {failed_transaction_count} failed requests.",
                remaining_transactions.len(),
            );
        }
    }
}

fn build_transactions(
    tip_distribution_program_id: &Pubkey,
    merkle_trees: &GeneratedMerkleTreeCollection,
    payer_pubkey: &Pubkey,
    tree_nodes: &[&TreeNode],
    stake_acct_min_rent: u64,
    tdas: &HashMap<Pubkey, TipDistributionAccount>,
    claimants: &HashMap<Pubkey, u64>,
    claim_statuses: &HashMap<Pubkey, Option<Account>>,
) -> (usize, usize, usize, usize, Vec<Transaction>) {
    let tip_distribution_config =
        Pubkey::find_program_address(&[Config::SEED], tip_distribution_program_id).0;
    let mut skipped_merkle_root_count: usize = 0;
    let mut zero_lamports_count: usize = 0;
    let mut already_claimed_count: usize = 0;
    let mut below_min_rent_count: usize = 0;
    let mut instructions =
        Vec::with_capacity(tree_nodes.iter().filter(|node| node.amount > 0).count());

    // prepare instructions to transfer to all claimants
    for tree in &merkle_trees.generated_merkle_trees {
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
        for node in &tree.tree_nodes {
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
                debug!("Current balance + tip claim amount of {balance_with_tip} is less than required rent-exempt of {stake_acct_min_rent} for pubkey: {}. Skipping.", node.claimant);
                below_min_rent_count = below_min_rent_count.checked_add(1).unwrap();
                continue;
            }
            instructions.push(Instruction {
                program_id: *tip_distribution_program_id,
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
                    payer: *payer_pubkey,
                    system_program: system_program::id(),
                }
                .to_account_metas(None),
            });
        }
    }

    let transactions = instructions
        .into_iter()
        .map(|ix| Transaction::new_with_payer(&[ix], Some(payer_pubkey)))
        .collect::<Vec<_>>();
    (
        skipped_merkle_root_count,
        zero_lamports_count,
        already_claimed_count,
        below_min_rent_count,
        transactions,
    )
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

/// Fetch accounts in parallel batches with retries.
async fn get_batched_accounts(
    rpc_client: &RpcClient,
    max_concurrent_rpc_get_reqs: usize,
    pubkeys: Vec<Pubkey>,
) -> solana_rpc_client_api::client_error::Result<HashMap<Pubkey, Option<Account>>> {
    let semaphore = Arc::new(Semaphore::new(max_concurrent_rpc_get_reqs));
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
                        if retries == MAX_RETRIES {
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
