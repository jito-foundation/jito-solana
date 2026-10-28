use {
    serde::Deserialize,
    serde_with::{DisplayFromStr, serde_as},
    solana_pubkey::Pubkey,
    std::{
        net::{IpAddr, SocketAddr},
        path::PathBuf,
    },
};

#[serde_as]
#[derive(Debug, Deserialize, Clone)]
pub struct LocalClusterConfig {
    /// Passed through as `--bam-url`, which also takes a bare host or a BAM Registry node list.
    pub bam_url: String,
    #[serde_as(as = "DisplayFromStr")]
    pub tip_payment_program_id: Pubkey,
    #[serde_as(as = "DisplayFromStr")]
    pub tip_distribution_program_id: Pubkey,
    pub faucet_address: SocketAddr,
    pub ledger_base_directory: PathBuf,
    pub validator_build_path: PathBuf,
    pub ledger_tool_build_path: PathBuf,
    pub validators: Vec<CustomValidatorConfig>,
    pub dynamic_port_range_start: u16,
    /// Faucet mint balance in SOL.
    pub mint_sol: Option<u64>,
    pub bind_address: Option<IpAddr>,
    /// A host name or IP address, passed through as `--gossip-host`.
    pub gossip_host: Option<String>,
    pub limit_blockstore_size: Option<u64>,
}

#[serde_as]
#[derive(Debug, Deserialize, Clone)]
pub struct CustomValidatorConfig {
    pub geyser_config: Option<PathBuf>,
    pub node_keypair: PathBuf,
    #[serde_as(as = "DisplayFromStr")]
    pub node_pubkey: Pubkey,
    pub vote_keypair: PathBuf,
    pub ledger_path: PathBuf,
}

impl LocalClusterConfig {
    pub fn from_file(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let config_str = std::fs::read_to_string(path)?;
        let config: LocalClusterConfig = toml::from_str(&config_str)?;
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn example_config_parses() {
        let example = include_str!("../examples/example_config.toml");
        toml::from_str::<LocalClusterConfig>(example).unwrap();
        // '0' is not base58.
        let bad_pubkey = example.replace(
            "4R3gSG8BpU4t19KYj8CfnbtRpnT8gtk4dvTHxVRwc2r7",
            "4R3gSG8BpU4t19KYj8CfnbtRpnT8gtk4dvTHxVRwc2r0",
        );
        assert!(toml::from_str::<LocalClusterConfig>(&bad_pubkey).is_err());
    }
}
