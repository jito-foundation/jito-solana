pub mod cli;

use {
    solana_accounts_db::accounts_index::{AccountIndex, AccountSecondaryIndexes},
    solana_pubkey::Pubkey,
    solana_stake_interface as stake,
    std::{fmt, path::PathBuf},
};

const STAKE_PROGRAM_INDEX_REMEDIATION: &str = "--account-index program-id \
    --account-index-include-key Stake11111111111111111111111111111111111111";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TipRouterSnapshotConfig {
    pub output_dir: PathBuf,
    pub ncn: Option<Pubkey>,
    pub tip_router_program_id: Option<Pubkey>,
    pub tip_distribution_program_id: Option<Pubkey>,
    pub priority_fee_distribution_program_id: Option<Pubkey>,
    pub tip_payment_program_id: Option<Pubkey>,
}

/// A tip-router snapshot service account-index configuration error.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StakeProgramAccountIndexError {
    ProgramIdIndexMissing,
    StakeProgramNotIncluded,
    StakeProgramExcluded,
}

impl fmt::Display for StakeProgramAccountIndexError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let reason = match self {
            Self::ProgramIdIndexMissing => "the ProgramId account index is not enabled",
            Self::StakeProgramNotIncluded => {
                "the account-index include filter does not contain the stake program"
            }
            Self::StakeProgramExcluded => {
                "the account-index exclude filter contains the stake program"
            }
        };

        write!(
            formatter,
            "tip-router snapshot service requires an indexed stake-account lookup: {reason}; \
             restart the validator with `{STAKE_PROGRAM_INDEX_REMEDIATION}`"
        )
    }
}

impl std::error::Error for StakeProgramAccountIndexError {}

/// Validate that the ProgramId secondary index covers stake-program accounts.
///
/// An unfiltered ProgramId index is accepted, as is an exclusion filter that
/// does not exclude the stake program. A stake-only include filter is the
/// recommended, lower-memory configuration.
pub fn validate_stake_program_account_index(
    account_indexes: &AccountSecondaryIndexes,
) -> Result<(), StakeProgramAccountIndexError> {
    if !account_indexes.contains(&AccountIndex::ProgramId) {
        return Err(StakeProgramAccountIndexError::ProgramIdIndexMissing);
    }

    let stake_program_id = stake::program::id();
    if account_indexes.include_key(&stake_program_id) {
        return Ok(());
    }

    match &account_indexes.keys {
        Some(keys) if keys.exclude => Err(StakeProgramAccountIndexError::StakeProgramExcluded),
        _ => Err(StakeProgramAccountIndexError::StakeProgramNotIncluded),
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*, solana_accounts_db::accounts_index::AccountSecondaryIndexesIncludeExclude,
        std::collections::HashSet,
    };

    fn program_id_index() -> AccountSecondaryIndexes {
        AccountSecondaryIndexes {
            indexes: HashSet::from([AccountIndex::ProgramId]),
            ..AccountSecondaryIndexes::default()
        }
    }

    #[test]
    fn test_validate_stake_program_account_index_with_stake_only_include_filter() {
        let account_indexes = AccountSecondaryIndexes {
            keys: Some(AccountSecondaryIndexesIncludeExclude {
                exclude: false,
                keys: HashSet::from([stake::program::id()]),
            }),
            ..program_id_index()
        };

        assert_eq!(
            validate_stake_program_account_index(&account_indexes),
            Ok(())
        );
    }

    #[test]
    fn test_validate_stake_program_account_index_with_unfiltered_program_id_index() {
        assert_eq!(
            validate_stake_program_account_index(&program_id_index()),
            Ok(())
        );
    }

    #[test]
    fn test_validate_stake_program_account_index_with_unrelated_exclusion() {
        let account_indexes = AccountSecondaryIndexes {
            keys: Some(AccountSecondaryIndexesIncludeExclude {
                exclude: true,
                keys: HashSet::from([Pubkey::new_unique()]),
            }),
            ..program_id_index()
        };

        assert_eq!(
            validate_stake_program_account_index(&account_indexes),
            Ok(())
        );
    }

    #[test]
    fn test_validate_stake_program_account_index_without_program_id_index() {
        assert_eq!(
            validate_stake_program_account_index(&AccountSecondaryIndexes::default()),
            Err(StakeProgramAccountIndexError::ProgramIdIndexMissing)
        );
    }

    #[test]
    fn test_validate_stake_program_account_index_with_include_filter_missing_stake() {
        let account_indexes = AccountSecondaryIndexes {
            keys: Some(AccountSecondaryIndexesIncludeExclude {
                exclude: false,
                keys: HashSet::from([Pubkey::new_unique()]),
            }),
            ..program_id_index()
        };

        assert_eq!(
            validate_stake_program_account_index(&account_indexes),
            Err(StakeProgramAccountIndexError::StakeProgramNotIncluded)
        );
    }

    #[test]
    fn test_validate_stake_program_account_index_with_explicit_stake_exclusion() {
        let account_indexes = AccountSecondaryIndexes {
            keys: Some(AccountSecondaryIndexesIncludeExclude {
                exclude: true,
                keys: HashSet::from([stake::program::id()]),
            }),
            ..program_id_index()
        };

        assert_eq!(
            validate_stake_program_account_index(&account_indexes),
            Err(StakeProgramAccountIndexError::StakeProgramExcluded)
        );
    }

    #[test]
    fn test_stake_program_account_index_error_has_remediation_flags() {
        let message = StakeProgramAccountIndexError::ProgramIdIndexMissing.to_string();
        assert!(message.contains(STAKE_PROGRAM_INDEX_REMEDIATION));
    }
}
