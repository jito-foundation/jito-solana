//! Given two stake-meta JSON files, this binary prints out differences any differences disregarding sort order.

use {
    clap::Parser,
    log::*,
    solana_tip_distributor::{read_json_from_file, StakeMetaCollection},
    std::path::PathBuf,
};

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Path to JSON file containing the [StakeMetaCollection] object.
    #[clap(long, env)]
    stake_meta_coll_path_left: PathBuf,

    /// Path to JSON file containing the [StakeMetaCollection] object.
    #[clap(long, env)]
    stake_meta_coll_path_right: PathBuf,
}

fn main() {
    env_logger::init();
    info!("starting");

    let args: Args = Args::parse();

    let stake_meta_coll_left: StakeMetaCollection =
        read_json_from_file(&args.stake_meta_coll_path_left).unwrap();
    let stake_meta_coll_right: StakeMetaCollection =
        read_json_from_file(&args.stake_meta_coll_path_right).unwrap();

    stake_meta_coll_left.assert_eq(&stake_meta_coll_right);
    info!("0 diffs");
}
