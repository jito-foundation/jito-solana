use {
    crate::{read_json_from_file, GeneratedMerkleTree, GeneratedMerkleTreeCollection},
    anchor_lang::AccountDeserialize,
    im::HashMap,
    log::{error, info},
    solana_client::nonblocking::rpc_client::RpcClient,
    solana_sdk::{
        commitment_config::CommitmentConfig,
        pubkey::Pubkey,
        signature::{read_keypair_file, Signature, Signer},
        transaction::Transaction,
    },
    std::{path::PathBuf, time::Duration},
    thiserror::Error,
    tip_distribution::{
        sdk::instruction::{upload_merkle_root_ix, UploadMerkleRootAccounts, UploadMerkleRootArgs},
        state::{Config, TipDistributionAccount},
    },
    tokio::{runtime::Builder, time::sleep},
};

#[derive(Error, Debug)]
pub enum MerkleRootUploadError {
    #[error(transparent)]
    IoError(#[from] std::io::Error),

    #[error(transparent)]
    JsonError(#[from] serde_json::Error),
}

pub fn upload_merkle_root(
    merkle_root_path: &PathBuf,
    keypair_path: &PathBuf,
    rpc_url: &str,
    tip_distribution_program_id: &Pubkey,
) -> Result<(), MerkleRootUploadError> {
    let merkle_tree: GeneratedMerkleTreeCollection =
        read_json_from_file(merkle_root_path).expect("read GeneratedMerkleTreeCollection");
    let keypair = read_keypair_file(&keypair_path).expect("read keypair file");

    let tip_distribution_config =
        Pubkey::find_program_address(&[Config::SEED], tip_distribution_program_id).0;

    let runtime = Builder::new_multi_thread()
        .worker_threads(16)
        .enable_all()
        .build()
        .expect("build runtime");

    runtime.block_on(async move {
        let rpc_client =
            RpcClient::new_with_commitment(rpc_url.to_string(), CommitmentConfig::confirmed());
        let recent_blockhash = rpc_client
            .get_latest_blockhash()
            .await
            .expect("get blockhash");

        let trees: Vec<GeneratedMerkleTree> = merkle_tree
            .generated_merkle_trees
            .into_iter()
            .filter(|tree| tree.merkle_root_upload_authority == keypair.pubkey())
            .collect();

        let mut trees_needing_update: Vec<GeneratedMerkleTree> = vec![];
        for tree in trees {
            let account = rpc_client
                .get_account(&tree.tip_distribution_account)
                .await
                .expect("fetch expect");

            let mut data = account.data.as_slice();
            let fetched_tip_distribution_account =
                TipDistributionAccount::try_deserialize(&mut data)
                    .expect("failed to deserialize tip_distribution_account state");

            let needs_upload = match fetched_tip_distribution_account.merkle_root {
                Some(merkle_root) => {
                    merkle_root.total_funds_claimed == 0
                        && merkle_root.root != tree.merkle_tree.get_root().unwrap().to_bytes()
                }
                None => true,
            };

            if needs_upload {
                trees_needing_update.push(tree);
            }
        }

        let transactions: Vec<Transaction> = trees_needing_update
            .iter()
            .map(|tree| {
                let ix = upload_merkle_root_ix(
                    *tip_distribution_program_id,
                    UploadMerkleRootArgs {
                        root: tree.merkle_tree.get_root().unwrap().to_bytes(),
                        max_total_claim: tree.max_total_claim,
                        max_num_nodes: tree.max_num_nodes,
                    },
                    UploadMerkleRootAccounts {
                        config: tip_distribution_config,
                        merkle_root_upload_authority: keypair.pubkey(),
                        tip_distribution_account: tree.tip_distribution_account,
                    },
                );
                Transaction::new_signed_with_payer(
                    &[ix],
                    Some(&keypair.pubkey()),
                    &[&keypair],
                    recent_blockhash,
                )
            })
            .collect();
        send_transactions_with_retry(&rpc_client, &transactions).await;
    });

    Ok(())
}

async fn send_transactions_with_retry(rpc_client: &RpcClient, transactions: &Vec<Transaction>) {
    let mut transactions_to_send: HashMap<Signature, Transaction> = transactions
        .iter()
        .map(|tx| (tx.signatures[0], tx.clone()))
        .collect();
    while !transactions_to_send.is_empty() {
        info!("sending {} transactions", transactions_to_send.len());

        for (signature, tx) in &transactions_to_send {
            match rpc_client.send_transaction(tx).await {
                Ok(send_tx_sig) => {
                    info!("send transaction: {:?}", send_tx_sig);
                }
                Err(e) => {
                    error!("error sending transaction {} error: {}", signature, e);
                }
            }
        }

        sleep(Duration::from_secs(5)).await;

        let mut signatures_confirmed: Vec<Signature> = Vec::new();
        for (signature, _) in &transactions_to_send {
            match rpc_client.confirm_transaction(signature).await {
                Ok(true) => {
                    info!("confirmed signature: {:?}", signature);
                    signatures_confirmed.push(*signature);
                }
                Ok(false) => {
                    info!("didn't confirmed signature: {:?}", signature);
                }
                Err(e) => {
                    error!(
                        "error confirming signature: {:?}, signature: {:?}",
                        signature, e
                    );
                }
            }
        }

        info!("confirmed {} signatures", signatures_confirmed.len());
        for sig in signatures_confirmed {
            transactions_to_send.remove(&sig);
        }
    }
}
