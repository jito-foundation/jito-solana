// Change the alias to `Box<error::Error>`.
// type Result<T> = std::result::Result<T, Box<dyn error::Error>>;
use {
    anchor_client::{
        solana_sdk::signature::{Keypair, Signer},
        Client, Cluster,
    },
    anchor_lang::{system_program::System, Id},
    serde_json,
    solana_client::rpc_client::RpcClient,
    solana_program::{hash::Hash, instruction::Instruction, message::Message, pubkey::Pubkey},
    solana_sdk::{
        commitment_config::CommitmentConfig,
        signature::{read_keypair_file, KeypairInsecureClone},
        transaction::Transaction,
    },
    solana_tip_distributor::GeneratedMerkleTreeCollection,
    std::{rc::Rc, str::FromStr},
    tip_distribution::{
        sdk::instruction::{claim_ix, ClaimAccounts, ClaimArgs},
        state::{ClaimStatus, Config, TipDistributionAccount},
    },
};

const TIP_DISTRIBUTION_PID: &str = "7QnFbRZajkym8mUh9rXuM5nKrPAPRfEU6W31izSWJDVh";
type Error = Box<dyn std::error::Error>;

pub struct RpcConfig {
    pub rpc_client: RpcClient,
    pub fee_payer: Keypair,
}

fn main() -> Result<(), Error> {
    // TODO: clap this and add solana config helpers
    let url = Cluster::Custom(
        "https://api.testnet.solana.com".to_string(),
        "wss://api.testnet.solana.com".to_string(),
    );

    let rpc_client = RpcClient::new_with_commitment(url.clone(), CommitmentConfig::confirmed());

    let payer = read_keypair_file("/Users/edgarxi/.config/solana/id.json")
        .expect("cli command requires a keypair file");

    let client =
        Client::new_with_options(url, Rc::new(payer.clone()), CommitmentConfig::processed());

    let config = RpcConfig {
        rpc_client,
        fee_payer: payer.clone(),
    };
    let merkle_tree = generate_merkle_tree()?;
    command_claim_all(&config, &payer, &client, &merkle_tree);
    Ok(())
}

fn get_latest_blockhash(client: &RpcClient) -> Result<Hash, Error> {
    Ok(client
        .get_latest_blockhash_with_commitment(CommitmentConfig::confirmed())?
        .0)
}

fn checked_transaction_with_signers(
    config: &RpcConfig,
    instructions: &[Instruction],
) -> Result<Transaction, Error> {
    let recent_blockhash = get_latest_blockhash(&config.rpc_client)?;
    let transaction = Transaction::new_signed_with_payer(
        instructions,
        Some(&config.fee_payer.pubkey()),
        &[&config.fee_payer],
        recent_blockhash,
    );
    //
    let signature = config
        .rpc_client
        .send_and_confirm_transaction_with_spinner(&transaction)?;
    println!("Signature: {}", signature);

    Ok(transaction)
}

fn command_claim_all(
    rpc_config: &RpcConfig,
    payer: &Keypair,
    client: &Client,
    merkle_tree: &GeneratedMerkleTreeCollection,
) {
    let pid = Pubkey::from_str(TIP_DISTRIBUTION_PID).unwrap();
    let program = client.program(pid);
    let program_tip_accounts: Vec<(Pubkey, Config)> = program.accounts(vec![]).unwrap();

    for tree in &merkle_tree.generated_merkle_trees {
        let tip_distribution_account = &tree.tip_distribution_account;
        let pid = Pubkey::from_str(TIP_DISTRIBUTION_PID).unwrap();
        for node in &tree.tree_nodes {
            let claimant = node.claimant;

            let claim_seeds = [
                ClaimStatus::SEED,
                claimant.as_ref(), // ordering matters here lmao
                tip_distribution_account.as_ref(),
            ];

            let (claim_status, claim_bump) = Pubkey::find_program_address(&claim_seeds, &pid);
            let claim_args = ClaimArgs {
                proof: node.clone().proof.unwrap(),
                amount: node.clone().amount + 100,
                bump: claim_bump,
            };

            let claim_accounts = ClaimAccounts {
                config: program_tip_accounts[0].0,
                tip_distribution_account: tip_distribution_account.clone(),
                claim_status,
                claimant,
                payer: payer.pubkey(),
                system_program: System::id(),
            };

            let ix = claim_ix(pid, claim_args, claim_accounts);
            println!("{:#?}", ix.clone());
            let _tx = checked_transaction_with_signers(rpc_config, &[ix.clone()]).unwrap();
        }
    }
}

fn command_list_all(client: &Client) -> Vec<(Pubkey, TipDistributionAccount)> {
    let pid = Pubkey::from_str(TIP_DISTRIBUTION_PID).unwrap();
    let program = client.program(pid);
    let program_tip_accounts: Vec<(Pubkey, TipDistributionAccount)> =
        program.accounts(vec![]).unwrap();

    for (pubkey, tip_account) in program_tip_accounts.clone() {
        println!(
            "tip distribution account for validator {:#?}: {:#?}",
            pubkey, tip_account.validator_vote_account
        );
    }
    return program_tip_accounts;
}

fn generate_merkle_tree() -> Result<GeneratedMerkleTreeCollection, Error> {
    // let data_raw = r#"{"generated_merkle_trees":[{"tip_distribution_account":[143,192,59,179,86,98,167,189,144,129,123,117,183,32,126,42,244,175,243,66,255,246,97,141,208,152,49,41,63,193,39,133],"tree_nodes":[{"claimant":[169,185,8,85,235,103,185,234,194,132,87,120,215,154,203,187,163,124,75,117,203,143,101,49,5,23,12,66,221,117,29,219],"amount":12062006,"proof":[[41,135,215,49,127,108,98,111,12,47,52,21,219,206,166,47,234,152,127,173,54,199,112,143,205,147,84,51,8,21,114,245],[162,221,196,42,239,120,47,168,135,138,88,56,131,11,52,160,47,26,222,226,134,83,225,65,237,125,149,6,229,231,224,0]]},{"claimant":[250,138,35,189,156,189,84,27,215,75,199,211,20,82,53,64,148,114,163,26,207,85,198,209,118,126,185,204,44,119,134,123],"amount":1373396,"proof":[[242,135,132,224,52,148,189,149,14,12,210,8,197,192,211,4,52,36,0,80,112,255,106,20,107,71,137,139,220,110,153,98],[162,221,196,42,239,120,47,168,135,138,88,56,131,11,52,160,47,26,222,226,134,83,225,65,237,125,149,6,229,231,224,0]]},{"claimant":[153,144,184,154,157,23,192,129,73,219,182,58,61,72,147,75,78,168,60,221,168,154,22,69,5,133,229,39,113,80,195,124],"amount":137339671,"proof":[[61,81,9,219,229,124,16,143,93,209,10,168,250,93,153,50,10,41,225,33,153,114,134,124,3,92,38,203,242,35,93,206],[103,75,60,24,184,178,77,93,217,58,63,179,229,22,123,43,98,74,226,28,49,89,66,135,225,132,4,246,255,187,241,182]]}],"max_total_claim":150775074,"max_num_nodes":3}],"bank_hash":"yhhqhjU6bWtvyNcQiJGwBffyWK1Newh28KVVdDFjSL3","epoch":352,"slot":146972255}"#;
    let data_raw = r#"{"generated_merkle_trees":[{"tip_distribution_account":[143,192,59,179,86,98,167,189,144,129,123,117,183,32,126,42,244,175,243,66,255,246,97,141,208,152,49,41,63,193,39,133],"tree_nodes":[{"claimant":[169,185,8,85,235,103,185,234,194,132,87,120,215,154,203,187,163,124,75,117,203,143,101,49,5,23,12,66,221,117,29,219],"amount":12062006,"proof":[[41,135,215,49,127,108,98,111,12,47,52,21,219,206,166,47,234,152,127,173,54,199,112,143,205,147,84,51,8,21,114,245],[162,221,196,42,239,120,47,168,135,138,88,56,131,11,52,160,47,26,222,226,134,83,225,65,237,125,149,6,229,231,224,0]]},{"claimant":[250,138,35,189,156,189,84,27,215,75,199,211,20,82,53,64,148,114,163,26,207,85,198,209,118,126,185,204,44,119,134,123],"amount":1373396,"proof":[[242,135,132,224,52,148,189,149,14,12,210,8,197,192,211,4,52,36,0,80,112,255,106,20,107,71,137,139,220,110,153,98],[162,221,196,42,239,120,47,168,135,138,88,56,131,11,52,160,47,26,222,226,134,83,225,65,237,125,149,6,229,231,224,0]]},{"claimant":[153,144,184,154,157,23,192,129,73,219,182,58,61,72,147,75,78,168,60,221,168,154,22,69,5,133,229,39,113,80,195,124],"amount":137339671,"proof":[[61,81,9,219,229,124,16,143,93,209,10,168,250,93,153,50,10,41,225,33,153,114,134,124,3,92,38,203,242,35,93,206],[103,75,60,24,184,178,77,93,217,58,63,179,229,22,123,43,98,74,226,28,49,89,66,135,225,132,4,246,255,187,241,182]]}],"max_total_claim":150775074,"max_num_nodes":3}],"bank_hash":"yhhqhjU6bWtvyNcQiJGwBffyWK1Newh28KVVdDFjSL3","epoch":352,"slot":146972255}"#;
    let merkle_tree: GeneratedMerkleTreeCollection = serde_json::from_str(data_raw)?;
    // println!("{:#?}", merkle_tree);
    Ok(merkle_tree)
}
