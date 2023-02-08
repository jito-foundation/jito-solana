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

    info!(
        "Found {} merkle trees, {} TipDistributionMeta",
        tda_to_merkle_tree.len(),
        tda_metas.len()
    );

    for tda in tda_metas {
        match tda_to_merkle_tree.get(&tda.tip_distribution_pubkey) {
            Some(tree) => {
                let calculated_total_claim: i64 = tree.tree_nodes.iter().map(|t| t.amount).sum();
                let max_total_claim = tree.max_total_claim as i64;
                let total_tips = tda.total_tips as i64;

                if calculated_total_claim != max_total_claim {
                    let diff = max_total_claim - calculated_total_claim;
                    let diff_pct = diff as f64 / tree.max_total_claim as f64;
                    error!("Calculation error: calculated total claim={calculated_total_claim}, expected total claim={max_total_claim}, diff={diff}, diff %={diff_pct}, tda={}", tda.tip_distribution_pubkey);
                }

                if calculated_total_claim != total_tips {
                    let diff = total_tips - calculated_total_claim;
                    let diff_pct = diff as f64 / total_tips as f64;
                    error!("Calculation error: calculated total claim={calculated_total_claim}, calculated total tips={total_tips}, diff={diff}, diff %={diff_pct}, tda={}", tda.tip_distribution_pubkey);
                }

                info!("TDA {} is gucci", tda.tip_distribution_pubkey);
            }
            None => {
                warn!(
                    "TDA {} not found in merkle trees",
                    tda.tip_distribution_pubkey
                );
            }
        }
    }
}
