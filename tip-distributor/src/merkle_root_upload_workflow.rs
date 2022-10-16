use {
    crate::{read_json_from_file, GeneratedMerkleTree, GeneratedMerkleTreeCollection},
    anchor_lang::AccountDeserialize,
    solana_client::nonblocking::rpc_client::RpcClient,
    solana_sdk::{
        pubkey::Pubkey,
        signature::{read_keypair_file, Signer},
        transaction::Transaction,
    },
    std::path::PathBuf,
    thiserror::Error,
    tip_distribution::{
        sdk::instruction::{upload_merkle_root_ix, UploadMerkleRootAccounts, UploadMerkleRootArgs},
        state::{Config, TipDistributionAccount},
    },
    tokio::runtime::Builder,
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
        let rpc_client = RpcClient::new(rpc_url.to_string());
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
    });

    Ok(())
}

//     if !txs.is_empty() {
//         execute_transactions(rpc_client, txs);
//     } else {
//         error!("No transactions to execute, use --force-upload-root to reupload roots");
//     }
// }
//
// fn execute_transactions(rpc_client: Arc<RpcClient>, txs: Vec<Transaction>) {
//     const DELAY: Duration = Duration::from_millis(500);
//     const MAX_RETRIES: usize = 5;
//
//     let rt = Builder::new_multi_thread()
//         .worker_threads(txs.len().min(20))
//         .thread_name("execute_transactions")
//         .enable_time()
//         .enable_io()
//         .build()
//         .unwrap();
//
//     let hdls = txs
//         .into_iter()
//         .map(|tx| {
//             let rpc_client = rpc_client.clone();
//             rt.spawn(async move {
//                 if let Err(e) =
//                     send_transaction_with_retry(rpc_client, &tx, DELAY, MAX_RETRIES).await
//                 {
//                     error!(
//                         "error sending transaction [signature={}, error={}]",
//                         tx.signatures[0], e
//                     );
//                 } else {
//                     info!(
//                         "successfully sent transaction: [signature={}]",
//                         tx.signatures[0]
//                     );
//                 }
//             })
//         })
//         .collect::<Vec<JoinHandle<()>>>();
//
//     rt.block_on(async { futures::future::join_all(hdls).await });
// }
//
// async fn send_transaction_with_retry(
//     rpc_client: Arc<RpcClient>,
//     tx: &Transaction,
//     delay: Duration,
//     max_retries: usize,
// ) -> solana_client::client_error::Result<Signature> {
//     let mut retry_count: usize = 0;
//     loop {
//         match rpc_client.send_and_confirm_transaction(tx).await {
//             Ok(sig) => {
//                 return Ok(sig);
//             }
//             Err(e) => {
//                 retry_count = retry_count.checked_add(1).unwrap();
//                 if retry_count == max_retries {
//                     return Err(e);
//                 }
//                 sleep(delay);
//             }
//         }
//     }
// }
