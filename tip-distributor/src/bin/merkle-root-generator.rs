//! This binary generates a merkle tree for each [TipDistributionAccount]; they are derived
//! using a user provided [StakeMetaCollection] JSON file.

use {
    clap::Parser, log::*,
    solana_tip_distributor::merkle_root_generator_workflow::generate_merkle_root,
    std::path::PathBuf,
};

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Path to JSON file containing the [StakeMetaCollection] object.
    #[clap(long, env)]
    stake_meta_coll_path: PathBuf,

    /// Path to JSON file to get populated with tree node data.
    #[clap(long, env)]
    out_path: PathBuf,
}

fn main() {
    env_logger::init();
    info!("Starting merkle-root-generator workflow...");

    let args: Args = Args::parse();
    generate_merkle_root(&args.stake_meta_coll_path, &args.out_path).expect("merkle tree produced");
    info!("saved merkle roots to {:?}", args.stake_meta_coll_path);
}
