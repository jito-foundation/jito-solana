use {
    anchor_client::{
        solana_sdk::signature::{Keypair, Signer},
        Client, Cluster,
    },
    anchor_lang::{system_program::System, Id},
    clap::{crate_name, value_t, App, Arg},
    serde_json,
    solana_client::rpc_client::RpcClient,
    solana_program::{hash::Hash, instruction::Instruction, pubkey::Pubkey},
    solana_sdk::{
        commitment_config::CommitmentConfig,
        signature::{read_keypair_file, KeypairInsecureClone},
        transaction::Transaction,
    },
    solana_tip_distributor::GeneratedMerkleTreeCollection,
    std::{fs::File, io::BufReader, path::Path, rc::Rc, str::FromStr},
    tip_distribution::{
        sdk::instruction::{claim_ix, ClaimAccounts, ClaimArgs},
        state::{ClaimStatus, Config},
    },
};

const TIP_DISTRIBUTION_PID: &str = "7QnFbRZajkym8mUh9rXuM5nKrPAPRfEU6W31izSWJDVh";
type Error = Box<dyn std::error::Error>;

pub struct RpcConfig {
    pub rpc_client: RpcClient,
    pub fee_payer: Keypair,
    pub dry_run: bool,
}

fn main() -> Result<(), Error> {
    let matches = App::new(crate_name!())
        .arg({
            let arg = Arg::with_name("config_file")
                .short('C')
                .long("config")
                .value_name("PATH")
                .takes_value(true)
                .global(true)
                .help("Configuration file to use");
            if let Some(ref config_file) = *solana_cli_config::CONFIG_FILE {
                arg.default_value(config_file)
            } else {
                arg
            }
        })
        .arg(
            Arg::with_name("json_rpc_url")
                .long("url")
                .value_name("URL")
                .takes_value(true)
                .help("JSON RPC URL for the cluster.  Default from the configuration file."),
        )
        .arg(
            Arg::with_name("fee_payer")
                .long("fee-payer")
                .value_name("KEYPAIR")
                .takes_value(true)
                .help("Transaction fee payer account [default: cli config keypair]"),
        )
        .arg(
            Arg::with_name("dry_run")
                .long("dry-run")
                .takes_value(false)
                .global(true)
                .help("Simulate transaction instead of executing"),
        )
        .arg(
            Arg::with_name("merkle_tree")
                .long("merkle-tree")
                .takes_value(true)
                .global(true)
                .help("Filepath of merkle tree json"),
        )
        .get_matches();

    let cli_config = if let Some(config_file) = matches.value_of("config_file") {
        solana_cli_config::Config::load(config_file).unwrap_or_default()
    } else {
        solana_cli_config::Config::default()
    };
    let json_rpc_url = value_t!(matches, "json_rpc_url", String)
        .unwrap_or_else(|_| cli_config.json_rpc_url.clone());
    let url = Cluster::from_str(json_rpc_url.as_str()).unwrap();

    let rpc_client = RpcClient::new_with_commitment(url.clone(), CommitmentConfig::confirmed());

    let fee_payer_path = if let Some(fee_payer) = matches.value_of("fee_payer") {
        fee_payer
    } else {
        cli_config.keypair_path.as_str()
    };

    let fee_payer = read_keypair_file(fee_payer_path).unwrap();

    let client = Client::new_with_options(
        url,
        Rc::new(fee_payer.clone()),
        CommitmentConfig::processed(),
    );
    let dry_run = matches.is_present("dry_run");

    let rpc_config = RpcConfig {
        rpc_client,
        fee_payer: fee_payer.clone(),
        dry_run,
    };

    let merkle_tree_path =
        value_t!(matches, "merkle_tree", String).expect("merkle tree path not found!");

    let merkle_tree = load_merkle_tree(merkle_tree_path)?;
    command_claim_all(&rpc_config, &fee_payer, &client, &merkle_tree);
    Ok(())
}

fn get_latest_blockhash(client: &RpcClient) -> Result<Hash, Error> {
    Ok(client
        .get_latest_blockhash_with_commitment(CommitmentConfig::confirmed())?
        .0)
}

/// Sends transaction payload, optionally simulating only
fn send_transaction(
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

    if config.dry_run {
        let result = config.rpc_client.simulate_transaction(&transaction)?;
        println!("Simulate result: {:?}", result);
    } else {
        let signature = config
            .rpc_client
            .send_and_confirm_transaction_with_spinner(&transaction)?;
        println!("Signature: {}", signature);
    }
    Ok(transaction)
}

/// runs workflow to claim all MEV rewards given a Generated merkle tree collection
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
        for node in &tree.tree_nodes {
            let claimant = node.claimant;

            let claim_seeds = [
                ClaimStatus::SEED,
                claimant.as_ref(), // ordering matters here
                tip_distribution_account.as_ref(),
            ];

            let (claim_status, claim_bump) = Pubkey::find_program_address(&claim_seeds, &pid);
            let claim_args = ClaimArgs {
                proof: node.clone().proof.unwrap(),
                amount: node.clone().amount,
                bump: claim_bump,
            };
            println!("claiming tips for {:#?}", claimant);

            let claim_accounts = ClaimAccounts {
                config: program_tip_accounts[0].0,
                tip_distribution_account: tip_distribution_account.clone(),
                claim_status,
                claimant,
                payer: payer.pubkey(),
                system_program: System::id(),
            };

            let ix = claim_ix(pid, claim_args, claim_accounts);
            send_transaction(rpc_config, &[ix.clone()]).unwrap();
        }
    }
}

fn load_merkle_tree<P: AsRef<Path>>(path: P) -> Result<GeneratedMerkleTreeCollection, Error> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);

    let merkle_tree = serde_json::from_reader(reader)?;
    Ok(merkle_tree)
}
