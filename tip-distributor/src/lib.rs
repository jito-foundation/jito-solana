pub mod merkle_root_generator_workflow;
pub mod stake_meta_generator_workflow;

use {
    crate::{
        merkle_root_generator_workflow::Error,
        stake_meta_generator_workflow::Error::CheckedMathError,
    },
    bigdecimal::{num_bigint::BigUint, BigDecimal},
    num_traits::{CheckedDiv, CheckedMul, ToPrimitive},
    serde::{Deserialize, Serialize},
    solana_merkle_tree::MerkleTree,
    solana_sdk::{
        account::{AccountSharedData, ReadableAccount},
        clock::Slot,
        hash::{Hash, Hasher},
        pubkey::Pubkey,
        stake_history::Epoch,
    },
    std::ops::{Div, Mul},
    tip_distribution::state::TipDistributionAccount,
};

#[derive(Deserialize, Serialize, Debug)]
pub struct GeneratedMerkleTreeCollection {
    pub generated_merkle_trees: Vec<GeneratedMerkleTree>,
    pub bank_hash: String,
    pub epoch: Epoch,
    pub slot: Slot,
}

#[derive(Eq, Debug, Hash, PartialEq, Deserialize, Serialize)]
pub struct GeneratedMerkleTree {
    pub tip_distribution_account: Pubkey,
    #[serde(skip_serializing, skip_deserializing)]
    pub merkle_tree: MerkleTree,
    pub tree_nodes: Vec<TreeNode>,
    pub max_total_claim: u64,
    pub max_num_nodes: u64,
}

impl GeneratedMerkleTreeCollection {
    pub fn new_from_stake_meta_collection(
        stake_meta_coll: StakeMetaCollection,
        merkle_root_upload_authority: Pubkey,
    ) -> Result<GeneratedMerkleTreeCollection, Error> {
        let generated_merkle_trees = stake_meta_coll
            .stake_metas
            .into_iter()
            .filter(|stake_meta| {
                if let Some(tip_distribution_meta) = stake_meta.maybe_tip_distribution_meta.as_ref()
                {
                    tip_distribution_meta.merkle_root_upload_authority
                        == merkle_root_upload_authority
                } else {
                    false
                }
            })
            .filter_map(|stake_meta| {
                // Build the tree
                let mut tree_nodes = match TreeNode::vec_from_stake_meta(&stake_meta) {
                    Err(e) => return Some(Err(e)),
                    Ok(maybe_tree_nodes) => maybe_tree_nodes,
                }?;

                // Hash the nodes
                let hashed_nodes: Vec<[u8; 32]> =
                    tree_nodes.iter().map(|n| n.hash().to_bytes()).collect();

                let tip_distribution_meta = stake_meta.maybe_tip_distribution_meta.unwrap();

                let merkle_tree = MerkleTree::new(&hashed_nodes[..], true);
                let max_num_nodes = tree_nodes.len() as u64;

                for (i, tree_node) in tree_nodes.iter_mut().enumerate() {
                    tree_node.proof = Some(get_proof(&merkle_tree, i));
                }

                Some(Ok(GeneratedMerkleTree {
                    max_num_nodes,
                    tip_distribution_account: tip_distribution_meta.tip_distribution_pubkey,
                    merkle_tree,
                    tree_nodes,
                    max_total_claim: tip_distribution_meta.total_tips,
                }))
            })
            .collect::<Result<Vec<GeneratedMerkleTree>, Error>>()?;

        Ok(GeneratedMerkleTreeCollection {
            generated_merkle_trees,
            bank_hash: stake_meta_coll.bank_hash,
            epoch: stake_meta_coll.epoch,
            slot: stake_meta_coll.slot,
        })
    }
}

pub fn get_proof(merkle_tree: &MerkleTree, i: usize) -> Vec<[u8; 32]> {
    let mut proof = Vec::new();
    let path = merkle_tree.find_path(i).expect("path to index");
    for branch in path.get_proof_entries() {
        if let Some(hash) = branch.get_left_sibling() {
            proof.push(hash.to_bytes());
        } else if let Some(hash) = branch.get_right_sibling() {
            proof.push(hash.to_bytes());
        } else {
            panic!("expected some hash at each level of the tree");
        }
    }
    proof
}

#[derive(Clone, Eq, Debug, Hash, PartialEq, Deserialize, Serialize)]
pub struct TreeNode {
    /// The account entitled to redeem.
    pub claimant: Pubkey,

    /// The amount this account is entitled to.
    pub amount: u64,

    /// The proof associated with this TreeNode
    pub proof: Option<Vec<[u8; 32]>>,
}

impl TreeNode {
    fn vec_from_stake_meta(stake_meta: &StakeMeta) -> Result<Option<Vec<TreeNode>>, Error> {
        if let Some(tip_distribution_meta) = stake_meta.maybe_tip_distribution_meta.as_ref() {
            let validator_fee = calc_validator_fee(
                tip_distribution_meta.total_tips,
                tip_distribution_meta.validator_fee_bps,
            );
            let mut tree_nodes = vec![TreeNode {
                claimant: stake_meta.validator_vote_account,
                amount: validator_fee,
                proof: None,
            }];

            let remaining_tips = tip_distribution_meta
                .total_tips
                .checked_sub(validator_fee)
                .unwrap();

            // The theoretically smallest weight an account can have is  (1 / SOL_TOTAL_SUPPLY_IN_LAMPORTS)
            // where we round SOL_TOTAL_SUPPLY is rounded to 500_000_000. We use u64::MAX. This gives a reasonable
            // guarantee that everyone gets paid out regardless of weight, as long as some non-zero amount of
            // lamports were delegated.
            let uint_precision_multiplier = BigUint::from(u64::MAX);
            let f64_precision_multiplier = BigDecimal::try_from(u64::MAX as f64).unwrap();

            let total_delegated = BigDecimal::try_from(stake_meta.total_delegated as f64)
                .expect("failed to convert total_delegated to BigDecimal");
            tree_nodes.extend(stake_meta
                .delegations
                .iter()
                .map(|delegation| {
                    // TODO(seg): Check this math!
                    let amount_delegated = BigDecimal::try_from(delegation.lamports_delegated as f64)
                        .expect(&*format!(
                            "failed to convert amount_delegated to BigDecimal [stake_account={}, amount_delegated={}]",
                            delegation.stake_account_pubkey,
                            delegation.lamports_delegated,
                        ));
                    let mut weight = amount_delegated.div(&total_delegated);

                    let use_multiplier = weight < f64_precision_multiplier;

                    if use_multiplier {
                        weight = weight.mul(&f64_precision_multiplier);
                    }

                    let truncated_weight = weight.to_u128()
                        .expect(&*format!("failed to convert weight to u128 [stake_account={}, weight={}]", delegation.stake_account_pubkey, weight));
                    let truncated_weight = BigUint::from(truncated_weight);

                    let mut amount = truncated_weight
                        .checked_mul(&BigUint::from(remaining_tips))
                        .unwrap();

                    if use_multiplier {
                        amount = amount.checked_div(&uint_precision_multiplier).unwrap();
                    }

                    Ok(TreeNode {
                        claimant: delegation.stake_account_pubkey,
                        amount: amount.to_u64().unwrap(),
                        proof: None
                    })
                })
                .collect::<Result<Vec<TreeNode>, Error>>()?);

            let total_claim_amount = tree_nodes.iter().fold(0u64, |sum, tree_node| {
                sum.checked_add(tree_node.amount).unwrap()
            });
            assert!(total_claim_amount < stake_meta.total_delegated);

            Ok(Some(tree_nodes))
        } else {
            Ok(None)
        }
    }

    fn hash(&self) -> Hash {
        let mut hasher = Hasher::default();
        hasher.hash(self.claimant.as_ref());
        hasher.hash(self.amount.to_le_bytes().as_ref());
        hasher.result()
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct StakeMetaCollection {
    /// List of [StakeMeta].
    pub stake_metas: Vec<StakeMeta>,

    /// base58 encoded tip-distribution program id.
    #[serde(with = "pubkey_string_conversion")]
    pub tip_distribution_program_id: Pubkey,

    /// Base58 encoded bank hash this object was generated at.
    pub bank_hash: String,

    /// Epoch for which this object was generated for.
    pub epoch: Epoch,

    /// Slot at which this object was generated.
    pub slot: Slot,
}

#[derive(Clone, Deserialize, Serialize, Debug, PartialEq, Eq)]
pub struct StakeMeta {
    #[serde(with = "pubkey_string_conversion")]
    pub validator_vote_account: Pubkey,

    /// The validator's tip-distribution meta if it exists.
    pub maybe_tip_distribution_meta: Option<TipDistributionMeta>,

    /// Delegations to this validator.
    pub delegations: Vec<Delegation>,

    /// The total amount of delegations to the validator.
    pub total_delegated: u64,

    /// The validator's delegation commission rate as a percentage between 0-100.
    pub commission: u8,
}

#[derive(Clone, Deserialize, Serialize, Debug, PartialEq, Eq)]
pub struct TipDistributionMeta {
    #[serde(with = "pubkey_string_conversion")]
    pub merkle_root_upload_authority: Pubkey,

    #[serde(with = "pubkey_string_conversion")]
    pub tip_distribution_pubkey: Pubkey,

    /// The validator's total tips in the [TipDistributionAccount].
    pub total_tips: u64,

    /// The validator's cut of tips from [TipDistributionAccount], calculated from the on-chain
    /// commission fee bps.
    pub validator_fee_bps: u16,
}

impl TipDistributionMeta {
    fn from_tda_wrapper(
        tda_wrapper: TipDistributionAccountWrapper,
        // The amount that will be left remaining in the tda to maintain rent exemption status.
        rent_exempt_amount: u64,
    ) -> Result<Self, stake_meta_generator_workflow::Error> {
        Ok(TipDistributionMeta {
            tip_distribution_pubkey: tda_wrapper.tip_distribution_pubkey,
            total_tips: tda_wrapper
                .account_data
                .lamports()
                .checked_sub(rent_exempt_amount)
                .ok_or(CheckedMathError)?,
            validator_fee_bps: tda_wrapper
                .tip_distribution_account
                .validator_commission_bps,
            merkle_root_upload_authority: tda_wrapper
                .tip_distribution_account
                .merkle_root_upload_authority,
        })
    }
}

#[derive(Clone, Deserialize, Serialize, Debug, PartialEq, Eq)]
pub struct Delegation {
    #[serde(with = "pubkey_string_conversion")]
    pub stake_account_pubkey: Pubkey,

    #[serde(with = "pubkey_string_conversion")]
    pub staker_pubkey: Pubkey,

    #[serde(with = "pubkey_string_conversion")]
    pub withdrawer_pubkey: Pubkey,

    /// Lamports delegated by the stake account
    pub lamports_delegated: u64,
}

/// Convenience wrapper around [TipDistributionAccount]
pub struct TipDistributionAccountWrapper {
    pub tip_distribution_account: TipDistributionAccount,
    pub account_data: AccountSharedData,
    pub tip_distribution_pubkey: Pubkey,
}

// TODO: move to program's sdk
pub fn derive_tip_distribution_account_address(
    tip_distribution_program_id: &Pubkey,
    vote_pubkey: &Pubkey,
    epoch: Epoch,
) -> (Pubkey, u8) {
    Pubkey::find_program_address(
        &[
            TipDistributionAccount::SEED,
            vote_pubkey.to_bytes().as_ref(),
            epoch.to_le_bytes().as_ref(),
        ],
        tip_distribution_program_id,
    )
}

/// Calculate validator fee denominated in lamports
pub fn calc_validator_fee(total_tips: u64, validator_commission_bps: u16) -> u64 {
    let validator_commission_rate =
        math::fee_tenth_of_bps(((validator_commission_bps as u64).checked_mul(10).unwrap()) as u64);
    let validator_fee: math::U64F64 = validator_commission_rate.mul_u64(total_tips);

    validator_fee
        .floor()
        .checked_add((validator_fee.frac_part() != 0) as u64)
        .unwrap()
}

mod math {
    /// copy-pasta from [here](https://github.com/project-serum/serum-dex/blob/e00bb9e6dac0a1fff295acb034722be9afc1eba3/dex/src/fees.rs#L43)
    #[repr(transparent)]
    #[derive(Copy, Clone)]
    pub(crate) struct U64F64(u128);

    #[allow(dead_code)]
    impl U64F64 {
        const ONE: Self = U64F64(1 << 64);

        pub(crate) fn add(self, other: U64F64) -> U64F64 {
            U64F64(self.0.checked_add(other.0).unwrap())
        }

        pub(crate) fn div(self, other: U64F64) -> u128 {
            self.0.checked_div(other.0).unwrap()
        }

        pub(crate) fn mul_u64(self, other: u64) -> U64F64 {
            U64F64(self.0.checked_mul(other as u128).unwrap())
        }

        /// right shift 64
        pub(crate) fn floor(self) -> u64 {
            (self.0.checked_div(2u128.checked_pow(64).unwrap()).unwrap()) as u64
        }

        pub(crate) fn frac_part(self) -> u64 {
            self.0 as u64
        }

        /// left shift 64
        pub(crate) fn from_int(n: u64) -> Self {
            U64F64(
                (n as u128)
                    .checked_mul(2u128.checked_pow(64).unwrap())
                    .unwrap(),
            )
        }
    }

    pub(crate) fn fee_tenth_of_bps(tenth_of_bps: u64) -> U64F64 {
        U64F64(
            ((tenth_of_bps as u128)
                .checked_mul(2u128.checked_pow(64).unwrap())
                .unwrap())
            .checked_div(100_000)
            .unwrap(),
        )
    }
}

mod pubkey_string_conversion {
    use {
        serde::{self, Deserialize, Deserializer, Serializer},
        solana_sdk::pubkey::Pubkey,
        std::str::FromStr,
    };

    pub fn serialize<S>(pubkey: &Pubkey, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = format!("{}", pubkey.to_string());
        serializer.serialize_str(&s)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Pubkey, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Pubkey::from_str(&s).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use {super::*, solana_sdk::bs58, tip_distribution::merkle_proof};

    #[test]
    fn test_merkle_tree_verify() {
        // Create the merkle tree and proofs
        let acct_0 = bs58::encode(Pubkey::new_unique().as_ref()).into_string();
        let acct_1 = bs58::encode(Pubkey::new_unique().as_ref()).into_string();

        let tree_nodes = vec![
            TreeNode {
                claimant: acct_0.parse().unwrap(),
                amount: 151_507,
                proof: None,
            },
            TreeNode {
                claimant: acct_1.parse().unwrap(),
                amount: 176_624,
                proof: None,
            },
        ];

        // First the nodes are hashed and merkle tree constructed
        let hashed_nodes: Vec<[u8; 32]> = tree_nodes.iter().map(|n| n.hash().to_bytes()).collect();
        let mk = MerkleTree::new(&hashed_nodes[..], true);
        let root = mk.get_root().expect("to have valid root").to_bytes();

        // verify first node
        let node = solana_program::hash::hashv(&[&[0u8], &hashed_nodes[0]]);
        let proof = get_proof(&mk, 0);
        assert!(merkle_proof::verify(proof, root, node.to_bytes()));

        // verify second node
        let node = solana_program::hash::hashv(&[&[0u8], &hashed_nodes[1]]);
        let proof = get_proof(&mk, 1);
        assert!(merkle_proof::verify(proof, root, node.to_bytes()));
    }

    #[test]
    fn test_new_from_stake_meta_collection_happy_path() {
        let b58_merkle_root_upload_authority =
            bs58::encode(Pubkey::new_unique().as_ref()).into_string();

        let (tda_0, tda_1) = (
            bs58::encode(Pubkey::new_unique().as_ref()).into_string(),
            bs58::encode(Pubkey::new_unique().as_ref()).into_string(),
        );

        let stake_account_0 = bs58::encode(Pubkey::new_unique().as_ref()).into_string();
        let stake_account_1 = bs58::encode(Pubkey::new_unique().as_ref()).into_string();
        let stake_account_2 = bs58::encode(Pubkey::new_unique().as_ref()).into_string();
        let stake_account_3 = bs58::encode(Pubkey::new_unique().as_ref()).into_string();

        let validator_vote_account_0 = bs58::encode(Pubkey::new_unique().as_ref()).into_string();
        let validator_vote_account_1 = bs58::encode(Pubkey::new_unique().as_ref()).into_string();

        println!("test stake_account {}", stake_account_0);
        println!("test stake_account {}", stake_account_1);
        println!("test stake_account {}", stake_account_2);
        println!("test stake_account {}", stake_account_3);

        let stake_meta_collection = StakeMetaCollection {
            stake_metas: vec![
                StakeMeta {
                    validator_vote_account: validator_vote_account_0.clone(),
                    maybe_tip_distribution_meta: Some(TipDistributionMeta {
                        merkle_root_upload_authority: b58_merkle_root_upload_authority.clone(),
                        tip_distribution_pubkey: tda_0.clone(),
                        total_tips: 1_900_122_111_000,
                        validator_fee_bps: 100,
                    }),
                    delegations: vec![
                        Delegation {
                            stake_account_pubkey: stake_account_0.clone(),
                            lamports_delegated: 123_999_123_555,
                        },
                        Delegation {
                            stake_account_pubkey: stake_account_1.clone(),
                            lamports_delegated: 144_555_444_556,
                        },
                    ],
                    total_delegated: 1_555_123_000_333_454_000,
                    commission: 100,
                },
                StakeMeta {
                    validator_vote_account: validator_vote_account_1.clone(),
                    maybe_tip_distribution_meta: Some(TipDistributionMeta {
                        merkle_root_upload_authority: b58_merkle_root_upload_authority.clone(),
                        tip_distribution_pubkey: tda_1.clone(),
                        total_tips: 1_900_122_111_333,
                        validator_fee_bps: 200,
                    }),
                    delegations: vec![
                        Delegation {
                            stake_account_pubkey: stake_account_2.clone(),
                            lamports_delegated: 224_555_444,
                        },
                        Delegation {
                            stake_account_pubkey: stake_account_3.clone(),
                            lamports_delegated: 700_888_944_555,
                        },
                    ],
                    total_delegated: 2_565_318_909_444_123,
                    commission: 10,
                },
            ],
            tip_distribution_program_id: bs58::encode(Pubkey::new_unique().as_ref()).into_string(),
            bank_hash: solana_sdk::hash::Hash::new_unique().to_string(),
            epoch: 100,
            slot: 2_000_000,
        };

        let merkle_tree_collection = GeneratedMerkleTreeCollection::new_from_stake_meta_collection(
            stake_meta_collection.clone(),
            b58_merkle_root_upload_authority.parse().unwrap(),
        )
        .unwrap();

        assert_eq!(stake_meta_collection.epoch, merkle_tree_collection.epoch);
        assert_eq!(
            stake_meta_collection.bank_hash,
            merkle_tree_collection.bank_hash
        );
        assert_eq!(stake_meta_collection.slot, merkle_tree_collection.slot);
        assert_eq!(
            stake_meta_collection.stake_metas.len(),
            merkle_tree_collection.generated_merkle_trees.len()
        );

        let tree_nodes = vec![
            TreeNode {
                claimant: validator_vote_account_0.parse().unwrap(),
                amount: 19_001_221_110,
                proof: None,
            },
            TreeNode {
                claimant: stake_account_0.parse().unwrap(),
                amount: 149_992,
                proof: None,
            },
            TreeNode {
                claimant: stake_account_1.parse().unwrap(),
                amount: 174_858,
                proof: None,
            },
        ];
        let hashed_nodes: Vec<[u8; 32]> = tree_nodes.iter().map(|n| n.hash().to_bytes()).collect();
        let gmt_0 = GeneratedMerkleTree {
            tip_distribution_account: tda_0.parse().unwrap(),
            merkle_tree: MerkleTree::new(&hashed_nodes[..], true),
            tree_nodes,
            max_total_claim: stake_meta_collection.stake_metas[0]
                .clone()
                .maybe_tip_distribution_meta
                .unwrap()
                .total_tips,
            max_num_nodes: 3,
        };

        let tree_nodes = vec![
            TreeNode {
                claimant: validator_vote_account_1.parse().unwrap(),
                amount: 38_002_442_227,
                proof: None,
            },
            TreeNode {
                claimant: stake_account_2.parse().unwrap(),
                amount: 163_000,
                proof: None,
            },
            TreeNode {
                claimant: stake_account_3.parse().unwrap(),
                amount: 508_762_900,
                proof: None,
            },
        ];
        let hashed_nodes: Vec<[u8; 32]> = tree_nodes.iter().map(|n| n.hash().to_bytes()).collect();
        let gmt_1 = GeneratedMerkleTree {
            tip_distribution_account: tda_1.parse().unwrap(),
            merkle_tree: MerkleTree::new(&hashed_nodes[..], true),
            tree_nodes,
            max_total_claim: stake_meta_collection.stake_metas[1]
                .clone()
                .maybe_tip_distribution_meta
                .unwrap()
                .total_tips,
            max_num_nodes: 3,
        };

        let expected_generated_merkle_trees = vec![gmt_0, gmt_1];
        let actual_generated_merkle_trees = merkle_tree_collection.generated_merkle_trees;

        expected_generated_merkle_trees
            .iter()
            .for_each(|expected_gmt| {
                let actual_gmt = actual_generated_merkle_trees
                    .iter()
                    .find(|gmt| {
                        gmt.tip_distribution_account == expected_gmt.tip_distribution_account
                    })
                    .unwrap();

                assert_eq!(expected_gmt.max_num_nodes, actual_gmt.max_num_nodes);
                assert_eq!(expected_gmt.max_total_claim, actual_gmt.max_total_claim);
                assert_eq!(
                    expected_gmt.tip_distribution_account,
                    actual_gmt.tip_distribution_account
                );
                assert_eq!(expected_gmt.tree_nodes.len(), actual_gmt.tree_nodes.len());
                expected_gmt
                    .tree_nodes
                    .iter()
                    .for_each(|expected_tree_node| {
                        let actual_tree_node = actual_gmt
                            .tree_nodes
                            .iter()
                            .find(|tree_node| tree_node.claimant == expected_tree_node.claimant)
                            .unwrap();
                        assert_eq!(expected_tree_node.amount, actual_tree_node.amount);
                    });
                assert_eq!(
                    expected_gmt.merkle_tree.get_root().unwrap(),
                    actual_gmt.merkle_tree.get_root().unwrap()
                );
            });
    }
}
