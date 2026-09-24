//! Explicit feature baselines for local-cluster experiments.
use {
    agave_feature_set::FEATURE_NAMES,
    anyhow::{Context, Result, ensure},
    serde::{Deserialize, Serialize},
    solana_account::Account,
    solana_commitment_config::CommitmentConfig,
    solana_feature_gate_interface::{self as feature, Feature},
    solana_genesis_config::GenesisConfig,
    solana_pubkey::Pubkey,
    solana_rpc_client::rpc_client::RpcClient,
    solana_runtime::genesis_utils::{activate_alpenglow_at_genesis, activate_feature},
    std::{collections::BTreeMap, fs, path::Path, str::FromStr},
};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FeatureFile {
    features: FeatureConfig,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FeatureConfig {
    baseline: String,
    #[serde(default)]
    enable: Vec<String>,
    #[serde(default)]
    disable: Vec<String>,
    #[serde(default)]
    activate_next_epoch: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum FeatureState {
    Active,
    Pending,
    Inactive,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FeatureSnapshot {
    source: String,
    // RPC batches can observe different finalized slots; preserve the range.
    first_observed_slot: u64,
    last_observed_slot: u64,
    features: BTreeMap<String, FeatureState>,
}

#[derive(Debug)]
pub struct ResolvedFeatures {
    states: BTreeMap<Pubkey, FeatureState>,
}

fn known_id(value: &str) -> Result<Pubkey> {
    let id = Pubkey::from_str(value).with_context(|| format!("invalid feature ID: {value}"))?;
    ensure!(
        FEATURE_NAMES.contains_key(&id),
        "unknown feature ID in this build: {id}"
    );
    Ok(id)
}

impl FeatureSnapshot {
    fn from_rpc(source: &str) -> Result<Self> {
        let client = RpcClient::new(source.to_owned());
        let mut ids = FEATURE_NAMES.keys().copied().collect::<Vec<_>>();
        ids.sort();
        let mut snapshot = Self {
            source: source.to_owned(),
            first_observed_slot: u64::MAX,
            last_observed_slot: 0,
            features: BTreeMap::new(),
        };
        for chunk in ids.chunks(100) {
            let response = client
                .get_multiple_accounts_with_commitment(chunk, CommitmentConfig::finalized())?;
            ensure!(
                response.value.len() == chunk.len(),
                "incomplete feature RPC response"
            );
            snapshot.first_observed_slot = snapshot.first_observed_slot.min(response.context.slot);
            snapshot.last_observed_slot = snapshot.last_observed_slot.max(response.context.slot);
            for (id, account) in chunk.iter().zip(response.value) {
                let state = match account {
                    None => FeatureState::Inactive,
                    Some(account) => {
                        let state = feature::from_account(&account)
                            .with_context(|| format!("invalid feature account: {id}"))?;
                        if state.activated_at.is_some() {
                            FeatureState::Active
                        } else {
                            FeatureState::Pending
                        }
                    }
                };
                snapshot.features.insert(id.to_string(), state);
            }
        }
        Ok(snapshot)
    }
}

impl ResolvedFeatures {
    pub fn from_file(path: &Path, snapshot_path: &Path) -> Result<Self> {
        let file: FeatureFile = toml::from_str(&fs::read_to_string(path)?)?;
        let config = file.features;
        let baseline = match config.baseline.as_str() {
            "mainnet-beta" => FeatureSnapshot::from_rpc("https://api.mainnet-beta.solana.com")?,
            "testnet" => FeatureSnapshot::from_rpc("https://api.testnet.solana.com")?,
            _ => {
                let baseline_path = path
                    .parent()
                    .unwrap_or(Path::new("."))
                    .join(&config.baseline);
                toml::from_str(
                    &fs::read_to_string(&baseline_path)
                        .with_context(|| format!("reading baseline {}", baseline_path.display()))?,
                )?
            }
        };
        let resolved = Self::resolve(&baseline, &config)?;
        fs::write(snapshot_path, toml::to_string_pretty(&baseline)?)?;
        Ok(resolved)
    }

    fn resolve(baseline: &FeatureSnapshot, config: &FeatureConfig) -> Result<Self> {
        let mut states = BTreeMap::new();
        for (ids, state) in [
            (&config.enable, FeatureState::Active),
            (&config.disable, FeatureState::Inactive),
            (&config.activate_next_epoch, FeatureState::Pending),
        ] {
            for value in ids {
                let id = known_id(value)?;
                ensure!(
                    states.insert(id, state).is_none(),
                    "duplicate or conflicting feature override: {id}"
                );
            }
        }
        for (id, state) in &baseline.features {
            // Pending features stay inactive unless explicitly requested.
            let state = match state {
                FeatureState::Active => FeatureState::Active,
                _ => FeatureState::Inactive,
            };
            states.entry(known_id(id)?).or_insert(state);
        }
        // These are already required by the local-cluster vote/program fixtures.
        for id in [
            agave_feature_set::vote_state_v4::id(),
            agave_feature_set::remaining_compute_units_syscall_enabled::id(),
        ] {
            ensure!(
                states.get(&id) == Some(&FeatureState::Active),
                "local-cluster prerequisite must be active at genesis: {id} ({})",
                FEATURE_NAMES[&id]
            );
        }
        Ok(Self { states })
    }

    pub fn apply(&self, genesis: &mut GenesisConfig) {
        for id in FEATURE_NAMES.keys() {
            genesis.accounts.remove(id);
        }
        for (id, state) in &self.states {
            log::info!("Local feature {id}: {state:?} ({})", FEATURE_NAMES[id]);
            match state {
                FeatureState::Active => activate_feature(genesis, *id),
                FeatureState::Pending => {
                    genesis.accounts.insert(
                        *id,
                        Account::from(feature::create_account(
                            &Feature::default(),
                            genesis.rent.minimum_balance(Feature::size_of()).max(1),
                        )),
                    );
                }
                FeatureState::Inactive => (),
            }
        }
        if self.states.get(&agave_feature_set::alpenglow::id()) == Some(&FeatureState::Active) {
            activate_alpenglow_at_genesis(genesis);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn baseline() -> FeatureSnapshot {
        FeatureSnapshot {
            source: "fixture".into(),
            first_observed_slot: 42,
            last_observed_slot: 43,
            features: FEATURE_NAMES
                .keys()
                .map(|id| {
                    (
                        id.to_string(),
                        if *id == agave_feature_set::alpenglow::id() {
                            FeatureState::Inactive
                        } else {
                            FeatureState::Active
                        },
                    )
                })
                .collect(),
        }
    }

    fn config() -> FeatureConfig {
        FeatureConfig {
            baseline: "fixture.toml".into(),
            enable: vec![],
            disable: vec![],
            activate_next_epoch: vec![],
        }
    }

    #[test]
    fn baseline_pending_stays_inactive_and_overrides_replace_defaults() {
        let id = agave_feature_set::alpenglow_fast_leader_handover::id();
        let mut baseline = baseline();
        baseline
            .features
            .insert(id.to_string(), FeatureState::Pending);
        let mut config = config();
        let resolved = ResolvedFeatures::resolve(&baseline, &config).unwrap();
        let mut genesis = GenesisConfig::default();
        activate_feature(&mut genesis, id);
        resolved.apply(&mut genesis);
        assert!(!genesis.accounts.contains_key(&id));
        config.enable.push(id.to_string());
        ResolvedFeatures::resolve(&baseline, &config)
            .unwrap()
            .apply(&mut genesis);
        assert_eq!(
            feature::from_account(&genesis.accounts[&id])
                .unwrap()
                .activated_at,
            Some(0)
        );
        config.enable.clear();
        config.disable.push(id.to_string());
        ResolvedFeatures::resolve(&self::baseline(), &config)
            .unwrap()
            .apply(&mut genesis);
        assert!(!genesis.accounts.contains_key(&id));
    }

    #[test]
    fn rejects_unknown_conflicting_and_missing_prerequisite_features() {
        let mut config = config();
        config.enable.push(Pubkey::default().to_string());
        assert!(
            ResolvedFeatures::resolve(&baseline(), &config)
                .unwrap_err()
                .to_string()
                .contains("unknown feature")
        );
        config.enable = vec![agave_feature_set::alpenglow::id().to_string()];
        config.activate_next_epoch = config.enable.clone();
        assert!(
            ResolvedFeatures::resolve(&baseline(), &config)
                .unwrap_err()
                .to_string()
                .contains("conflicting")
        );
        config.activate_next_epoch.clear();
        config
            .disable
            .push(agave_feature_set::vote_state_v4::id().to_string());
        assert!(
            ResolvedFeatures::resolve(&baseline(), &config)
                .unwrap_err()
                .to_string()
                .contains("prerequisite")
        );
    }

    #[test]
    fn snapshot_round_trip_preserves_source_and_pending_state() {
        let mut baseline = baseline();
        let id = agave_feature_set::alpenglow::id();
        baseline
            .features
            .insert(id.to_string(), FeatureState::Pending);
        let directory =
            std::env::temp_dir().join(format!("feature-snapshot-{}", Pubkey::new_unique()));
        fs::create_dir_all(&directory).unwrap();
        fs::write(
            directory.join("baseline.toml"),
            toml::to_string_pretty(&baseline).unwrap(),
        )
        .unwrap();
        fs::write(
            directory.join("features.toml"),
            "[features]\nbaseline = \"baseline.toml\"\n",
        )
        .unwrap();
        let saved = directory.join("saved.toml");
        let replay = ResolvedFeatures::from_file(&directory.join("features.toml"), &saved).unwrap();
        let saved: FeatureSnapshot = toml::from_str(&fs::read_to_string(saved).unwrap()).unwrap();
        assert_eq!(saved.features, baseline.features);
        assert_eq!(saved.first_observed_slot, 42);
        assert_eq!(saved.last_observed_slot, 43);
        assert_eq!(saved.source, "fixture");
        fs::remove_dir_all(directory).unwrap();
        assert_eq!(replay.states[&id], FeatureState::Inactive);
    }

    #[test]
    fn alpenglow_genesis_uses_initializer_but_pending_does_not() {
        let id = agave_feature_set::alpenglow::id();
        let mut config = config();
        config.enable.push(id.to_string());
        let mut genesis = GenesisConfig::default();
        ResolvedFeatures::resolve(&baseline(), &config)
            .unwrap()
            .apply(&mut genesis);
        let mut expected = GenesisConfig::default();
        ResolvedFeatures::resolve(&baseline(), &self::config())
            .unwrap()
            .apply(&mut expected);
        activate_alpenglow_at_genesis(&mut expected);
        assert_eq!(genesis.accounts, expected.accounts);
        assert_eq!(genesis.poh_config.hashes_per_tick, None);

        config.enable.clear();
        config.activate_next_epoch.push(id.to_string());
        let mut pending = GenesisConfig::default();
        ResolvedFeatures::resolve(&baseline(), &config)
            .unwrap()
            .apply(&mut pending);
        assert_eq!(
            feature::from_account(&pending.accounts[&id])
                .unwrap()
                .activated_at,
            None
        );
        assert!(pending.accounts.len() < genesis.accounts.len());
    }

    #[test]
    fn pending_feature_activates_at_epoch_boundary() {
        use solana_runtime::{bank::Bank, genesis_utils::create_genesis_config};
        let id = agave_feature_set::alpenglow_fast_leader_handover::id();
        let mut config = config();
        config.activate_next_epoch.push(id.to_string());
        let mut genesis = create_genesis_config(1_000_000_000).genesis_config;
        ResolvedFeatures::resolve(&baseline(), &config)
            .unwrap()
            .apply(&mut genesis);
        let next_epoch = genesis.epoch_schedule.get_first_slot_in_epoch(1);
        let (bank, _bank_forks) = Bank::new_with_bank_forks_for_tests(&genesis);
        assert!(!bank.feature_set.is_active(&id));
        assert_eq!(bank.compute_pending_activation_slot(&id), Some(next_epoch));
        let bank = Bank::new_from_parent(bank, Default::default(), next_epoch);
        assert!(bank.feature_set.is_active(&id));
    }
}
