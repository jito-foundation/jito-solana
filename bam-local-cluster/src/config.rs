use {
    serde::{Deserialize, Serialize},
    std::path::PathBuf,
};

#[derive(Debug, Deserialize, Clone)]
pub struct LocalClusterConfig {
    pub bam_url: String,
    pub tip_payment_program_id: String,
    pub tip_distribution_program_id: String,
    pub faucet_address: String,
    pub ledger_base_directory: String,
    pub validator_build_path: String,
    pub ledger_tool_build_path: String,
    pub validators: Vec<CustomValidatorConfig>,
    pub dynamic_port_range_start: u16,
    pub hashes_per_tick: Option<u64>,
    /// Faucet mint balance in SOL.
    #[serde(default)]
    pub mint_sol: Option<u64>,
    /// Activate the transaction v1 feature in genesis for BAM conformance tests.
    #[serde(default)]
    pub enable_tx_v1: bool,
    pub bind_address: Option<String>,
    pub gossip_host: Option<String>,
    pub limit_ledger_size: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CustomValidatorConfig {
    pub geyser_config: Option<PathBuf>,
    pub node_keypair: PathBuf,
    pub node_pubkey: String,
    pub vote_keypair: PathBuf,
    pub vote_pubkey: String,
    pub ledger_path: PathBuf,
}

#[derive(Debug, Serialize, Clone)]
pub struct ClusterInfo {
    pub rpc_endpoint: String,
    pub bootstrap_gossip: String,
}

impl LocalClusterConfig {
    pub fn from_file(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let config_str = std::fs::read_to_string(path)?;
        let config: LocalClusterConfig = toml::from_str(&config_str)?;
        Ok(config)
    }

    pub fn get_bootstrap_node(&self) -> Option<&CustomValidatorConfig> {
        self.validators.first()
    }

    pub fn get_validator_nodes(&self) -> Vec<&CustomValidatorConfig> {
        self.validators.iter().skip(1).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config_toml(extra: &str) -> String {
        format!(
            r#"
bam_url = "http://127.0.0.1:50055"
tip_payment_program_id = "T1pyyaTNZsKv2WcRAB8oVnk93mLJw2XzjtVYqCsaHqt"
tip_distribution_program_id = "4R3gSG8BpU4t19KYj8CfnbtRpnT8gtk4dvTHxVRwc2r7"
faucet_address = "127.0.0.1:12345"
ledger_base_directory = "/tmp/ledger"
validator_build_path = "/tmp/jito-solana/target/release"
ledger_tool_build_path = "/tmp/jito-solana/dev-bins/target/release"
dynamic_port_range_start = 20000
hashes_per_tick = 62500
{extra}

[[validators]]
node_keypair = "/tmp/ledger/validator-1/node-keypair.json"
node_pubkey = "11111111111111111111111111111111"
vote_keypair = "/tmp/ledger/validator-1/vote-keypair.json"
vote_pubkey = "11111111111111111111111111111111"
ledger_path = "/tmp/ledger/validator-1"
"#
        )
    }

    #[test]
    fn enable_tx_v1_defaults_to_false() {
        let config: LocalClusterConfig = toml::from_str(&config_toml("")).unwrap();

        assert!(!config.enable_tx_v1);
    }

    #[test]
    fn enable_tx_v1_can_be_enabled_from_config() {
        let config: LocalClusterConfig =
            toml::from_str(&config_toml("enable_tx_v1 = true")).unwrap();

        assert!(config.enable_tx_v1);
    }
}
