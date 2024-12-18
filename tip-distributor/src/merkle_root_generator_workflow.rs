use {
    crate::{read_json_from_file, GeneratedMerkleTreeCollection, StakeMetaCollection},
    log::*,
    solana_client::rpc_client::RpcClient,
    std::{
        fmt::Debug,
        fs::File,
        io::{BufWriter, Write},
        path::PathBuf,
    },
    thiserror::Error,
};

#[derive(Error, Debug)]
pub enum MerkleRootGeneratorError {
    #[error(transparent)]
    IoError(#[from] std::io::Error),

    #[error(transparent)]
    RpcError(#[from] Box<solana_client::client_error::ClientError>),

    #[error(transparent)]
    SerdeJsonError(#[from] serde_json::Error),
}

pub fn generate_merkle_root(
    stake_meta_coll_path: &PathBuf,
    out_path: &PathBuf,
    rpc_url: &str,
) -> Result<(), MerkleRootGeneratorError> {
    let stake_meta_coll: StakeMetaCollection = read_json_from_file(stake_meta_coll_path)?;

    let rpc_client = RpcClient::new(rpc_url);
    let merkle_tree_coll = GeneratedMerkleTreeCollection::new_from_stake_meta_collection(
        stake_meta_coll,
        Some(rpc_client),
    )?;

    write_to_json_file(&merkle_tree_coll, out_path)?;
    Ok(())
}

fn write_to_json_file(
    merkle_tree_coll: &GeneratedMerkleTreeCollection,
    file_path: &PathBuf,
) -> Result<(), MerkleRootGeneratorError> {
    let file = File::create(file_path)?;
    let mut writer = BufWriter::new(file);
    let json = serde_json::to_string_pretty(&merkle_tree_coll).unwrap();
    writer.write_all(json.as_bytes())?;
    writer.flush()?;

    Ok(())
}
