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
        if let Some(tree) = tda_to_merkle_tree.get(&tda.tip_distribution_pubkey) {
            let actual_claim: u64 = tree.tree_nodes.iter().map(|t| t.amount).sum();

            if actual_claim != tree.max_total_claim {
                error!("Calculation error: calculated total claim={actual_claim}, expected total claim={}, tda={}", tree.max_total_claim, tda.tip_distribution_pubkey);
                continue;
            }

            if tda.total_tips != actual_claim {
                error!("Calculation error: calculated total tips={}, calculated total claim = {actual_claim}, tda={}", tda.total_tips, tda.tip_distribution_pubkey);
                continue;
            }

            info!("{} tda is gucci", tda.tip_distribution_pubkey);
        } else {
            warn!(
                "{} TDA not found in merkle trees",
                tda.tip_distribution_pubkey
            );
        }
    }
}
