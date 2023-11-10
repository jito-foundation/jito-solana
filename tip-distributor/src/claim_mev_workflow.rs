use {
    crate::{
        claim_mev_workflow::ClaimMevError::{ClaimantNotFound, InsufficientBalance, TDANotFound},
        minimum_balance, sign_and_send_transactions_with_retries_multi_rpc,
        GeneratedMerkleTreeCollection, TreeNode,
    },
    anchor_lang::{AccountDeserialize, InstructionData, ToAccountMetas},
    itertools::Itertools,
    jito_tip_distribution::state::{ClaimStatus, Config, TipDistributionAccount},
    log::{debug, error, info},
    solana_client::nonblocking::rpc_client::RpcClient,
    solana_metrics::{datapoint_info, datapoint_warn},
    solana_program::{
        fee_calculator::DEFAULT_TARGET_LAMPORTS_PER_SIGNATURE, native_token::LAMPORTS_PER_SOL,
        system_program,
    },
    solana_sdk::{
        account::Account,
        commitment_config::CommitmentConfig,
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

    #[error("TDA not found for pubkey: {0:?}")]
    TDANotFound(Pubkey),

    #[error("Claim Status not found for pubkey: {0:?}")]
    ClaimStatusNotFound(Pubkey),

    #[error("Claimant not found for pubkey: {0:?}")]
    ClaimantNotFound(Pubkey),

    #[error(transparent)]
    MaxFetchRetriesExceeded(#[from] solana_rpc_client_api::client_error::Error),

    #[error("Failed after {attempts} retries. {remaining_transaction_count} remaining mev claim transactions, {failed_transaction_count} failed requests.",)]
    MaxSendTransactionRetriesExceeded {
        attempts: u64,
        remaining_transaction_count: usize,
        failed_transaction_count: usize,
    },

    #[error("Expected to have at least {desired_balance} lamports in {payer:?}. Current balance is {start_balance} lamports. Deposit {sol_to_deposit} SOL to continue.")]
    InsufficientBalance {
        desired_balance: u64,
        payer: Pubkey,
        start_balance: u64,
        sol_to_deposit: u64,
    },
}

pub async fn claim_mev_tips(
    merkle_trees: GeneratedMerkleTreeCollection,
    rpc_url: String,
    rpc_send_connection_count: u64,
    max_concurrent_rpc_get_reqs: usize,
    tip_distribution_program_id: &Pubkey,
    keypair: Arc<Keypair>,
    max_loop_retries: u64,
    max_loop_duration: Duration,
) -> Result<(), ClaimMevError> {
    let payer_pubkey = keypair.pubkey();
    let blockhash_rpc_client = Arc::new(RpcClient::new_with_commitment(
        rpc_url.clone(),
        CommitmentConfig::finalized(),
    ));
    let rpc_clients = Arc::new(
        (0..rpc_send_connection_count)
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

    // fetch all accounts up front
    info!(
        "Starting to fetch accounts for epoch {}",
        merkle_trees.epoch
    );
    let tdas = crate::get_batched_accounts(
        &blockhash_rpc_client,
        max_concurrent_rpc_get_reqs,
        merkle_trees
            .generated_merkle_trees
            .iter()
            .map(|tree| tree.tip_distribution_account)
            .collect_vec(),
    )
    .await
    .map_err(ClaimMevError::MaxFetchRetriesExceeded)?
    .into_iter()
    .filter_map(|(pubkey, maybe_account)| {
        let Some(account) = maybe_account else {
            datapoint_warn!(
                "claim_mev_workflow-account_error",
                ("epoch", merkle_trees.epoch, i64),
                ("pubkey", pubkey.to_string(), String),
                ("account_type", "tip_distribution_account", String),
                ("error", 1, i64),
                ("err_type", "fetch", String),
                ("err_str", "Failed to fetch TipDistributionAccount", String)
            );
            return None;
        };

        let account = match TipDistributionAccount::try_deserialize(&mut account.data.as_slice()) {
            Ok(a) => a,
            Err(e) => {
                datapoint_warn!(
                    "claim_mev_workflow-account_error",
                    ("epoch", merkle_trees.epoch, i64),
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

    // track balances and account len to make sure account is rent-exempt after transfer
    let claimants = crate::get_batched_accounts(
        &blockhash_rpc_client,
        max_concurrent_rpc_get_reqs,
        tree_nodes
            .iter()
            .map(|tree_node| tree_node.claimant)
            .collect_vec(),
    )
    .await
    .map_err(ClaimMevError::MaxFetchRetriesExceeded)?
    .into_iter()
    .map(|(pubkey, maybe_account)| {
        (
            pubkey,
            maybe_account
                .map(|account| (account.lamports, account.data.len()))
                .unwrap_or_default(),
        )
    })
    .collect::<HashMap<Pubkey, (u64, usize)>>();

    // Refresh claimants + Try sending txns to RPC
    let mut retries = 0;
    let mut failed_transaction_count = 0usize;
    loop {
        let start = Instant::now();
        let claim_statuses = crate::get_batched_accounts(
            &blockhash_rpc_client,
            max_concurrent_rpc_get_reqs,
            tree_nodes
                .iter()
                .map(|tree_node| tree_node.claim_status_pubkey)
                .collect_vec(),
        )
        .await
        .map_err(ClaimMevError::MaxFetchRetriesExceeded)?;
        let account_fetch_elapsed = start.elapsed();

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
            &tdas,
            &claimants,
            &claim_statuses,
        )?;
        datapoint_info!(
            "claim_mev_workflow-prepare_transactions",
            ("epoch", merkle_trees.epoch, i64),
            ("attempt", retries, i64),
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
                start.elapsed().as_micros(),
                i64
            ),
        );

        if transactions.is_empty() {
            info!("Finished claiming tips after {retries} retries, {failed_transaction_count} failed requests.");
            return Ok(());
        }

        if let Some((start_balance, desired_balance, sol_to_deposit)) = is_sufficient_balance(
            &payer_pubkey,
            &blockhash_rpc_client,
            transactions.len() as u64,
        )
        .await
        {
            return Err(InsufficientBalance {
                desired_balance,
                payer: payer_pubkey,
                start_balance,
                sol_to_deposit,
            });
        }
        let transactions_len = transactions.len();

        info!("Sending {} tip claim transactions. {zero_lamports_count} would transfer zero lamports, {below_min_rent_count} would be below minimum rent", transactions.len());
        let send_start = Instant::now();
        let (remaining_transaction_count, new_failed_transaction_count) =
            sign_and_send_transactions_with_retries_multi_rpc(
                &keypair,
                &blockhash_rpc_client,
                &rpc_clients,
                transactions,
                max_loop_duration,
            )
            .await;
        failed_transaction_count =
            failed_transaction_count.saturating_add(new_failed_transaction_count);

        datapoint_info!(
            "claim_mev_workflow-send_transactions",
            ("epoch", merkle_trees.epoch, i64),
            ("attempt", retries, i64),
            ("transaction_count", transactions_len, i64),
            (
                "successful_transaction_count",
                transactions_len.saturating_sub(remaining_transaction_count),
                i64
            ),
            (
                "remaining_transaction_count",
                remaining_transaction_count,
                i64
            ),
            (
                "failed_transaction_count",
                new_failed_transaction_count,
                i64
            ),
            ("send_latency_us", send_start.elapsed().as_micros(), i64),
        );

        if retries >= max_loop_retries {
            return Err(ClaimMevError::MaxSendTransactionRetriesExceeded {
                attempts: max_loop_retries,
                remaining_transaction_count,
                failed_transaction_count,
            });
        }
        retries = retries.saturating_add(1);
    }
}

#[allow(clippy::result_large_err)]
fn build_transactions(
    tip_distribution_program_id: &Pubkey,
    merkle_trees: &GeneratedMerkleTreeCollection,
    payer_pubkey: &Pubkey,
    tree_nodes: &[&TreeNode],
    tdas: &HashMap<Pubkey, TipDistributionAccount>,
    claimants: &HashMap<Pubkey, (u64 /* lamports */, usize /* allocated bytes */)>,
    claim_statuses: &HashMap<Pubkey, Option<Account>>,
) -> Result<
    (
        usize, /* skipped_merkle_root_count */
        usize, /* zero_lamports_count */
        usize, /* already_claimed_count */
        usize, /* below_min_rent_count */
        Vec<Transaction>,
    ),
    ClaimMevError,
> {
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
        let Some(fetched_tip_distribution_account) = tdas.get(&tree.tip_distribution_account)
        else {
            return Err(TDANotFound(tree.tip_distribution_account));
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
                Some(None) => {} // expected to not find ClaimStatus account, don't skip
                Some(Some(_account)) => {
                    debug!(
                        "Claim status account already exists (already paid out). Skipping pubkey: {:?}.", node.claim_status_pubkey,
                    );
                    already_claimed_count = already_claimed_count.checked_add(1).unwrap();
                    continue;
                }
                None => return Err(ClaimantNotFound(node.claim_status_pubkey)),
            };
            let Some((current_balance, allocated_bytes)) = claimants.get(&node.claimant) else {
                return Err(ClaimantNotFound(node.claimant));
            };

            // some older accounts can be rent-paying
            // any new transfers will need to make the account rent-exempt (runtime enforced)
            let new_balance = current_balance.checked_add(node.amount).unwrap();
            let minimum_rent = minimum_balance(*allocated_bytes);
            if new_balance < minimum_rent {
                debug!("Current balance + claim amount of {new_balance} is less than required rent-exempt of {minimum_rent} for pubkey: {}. Skipping.", node.claimant);
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
    Ok((
        skipped_merkle_root_count,
        zero_lamports_count,
        already_claimed_count,
        below_min_rent_count,
        transactions,
    ))
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
