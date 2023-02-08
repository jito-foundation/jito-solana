use {
    clap::Parser,
    log::*,
    solana_sdk::pubkey::Pubkey,
    solana_tip_distributor::{
        read_json_from_file, GeneratedMerkleTree, GeneratedMerkleTreeCollection,
        StakeMetaCollection,
    },
    std::{collections::HashMap, path::PathBuf},
};

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(long, env)]
    merkle_trees_path: PathBuf,
    #[clap(long, env)]
    stake_meta_path: PathBuf,
}

fn main() {
    env_logger::init();
    let args: Args = Args::parse();

    let merkle_tree_collection: GeneratedMerkleTreeCollection =
        read_json_from_file(&args.merkle_trees_path).unwrap();
    let stake_meta_collection: StakeMetaCollection =
        read_json_from_file(&args.stake_meta_path).unwrap();

    let tda_to_merkle_tree = merkle_tree_collection
        .generated_merkle_trees
        .into_iter()
        .map(|t| (t.tip_distribution_account, t))
        .collect::<HashMap<Pubkey, GeneratedMerkleTree>>();

    let tda_metas = stake_meta_collection
        .stake_metas
        .into_iter()
        .flat_map(|m| m.maybe_tip_distribution_meta)
        .collect::<Vec<_>>();

    for tda in tda_metas {
        match tda_to_merkle_tree.get(&tda.tip_distribution_pubkey) {
            Some(tree) => {
                let calculated_claim: u64 = tree.tree_nodes.iter().map(|t| t.amount).sum();

                if calculated_claim != tree.max_total_claim {
                    let diff = tree.max_total_claim - calculated_claim;
                    error!("Calculation error: calculated total claim={calculated_claim}, expected total claim={}, diff={diff}, tda={}", tree.max_total_claim, tda.tip_distribution_pubkey);
                    continue;
                }

                if calculated_claim != tda.total_tips {
                    let diff = tda.total_tips - calculated_claim;
                    error!("Calculation error: calculated total claim={calculated_claim}, calculated total tips={}, diff={diff}, tda={}", tda.total_tips, tda.tip_distribution_pubkey);
                    continue;
                }

                info!("{} tda is gucci", tda.tip_distribution_pubkey);
            }
            None => {
                warn!(
                    "{} TDA not found in merkle trees",
                    tda.tip_distribution_pubkey
                );
            }
        }
    }
}
