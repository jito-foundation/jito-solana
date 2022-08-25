pub mod claim_mev_workflow;
pub mod merkle_root_generator_workflow;
pub mod merkle_root_upload_workflow;
pub mod reclaim_rent_workflow;
pub mod stake_meta_generator_workflow;

use {
    crate::{
        merkle_root_generator_workflow::MerkleRootGeneratorError,
        stake_meta_generator_workflow::StakeMetaGeneratorError::CheckedMathError,
    },
    anchor_lang::Id,
    log::{error, info},
    serde::{de::DeserializeOwned, Deserialize, Serialize},
    solana_client::nonblocking::rpc_client::RpcClient,
    solana_merkle_tree::MerkleTree,
    solana_sdk::{
        account::{AccountSharedData, ReadableAccount},
        clock::Slot,
        hash::{Hash, Hasher},
        pubkey::Pubkey,
        signature::Signature,
        stake_history::Epoch,
        transaction::Transaction,
    },
    std::{
        collections::HashMap,
        fs::File,
        io::BufReader,
        path::PathBuf,
        time::{Duration, Instant},
    },
    tip_distribution::{
        program::TipDistribution,
        state::{ClaimStatus, TipDistributionAccount},
    },
    tip_payment::{
        Config, CONFIG_ACCOUNT_SEED, TIP_ACCOUNT_SEED_0, TIP_ACCOUNT_SEED_1, TIP_ACCOUNT_SEED_2,
        TIP_ACCOUNT_SEED_3, TIP_ACCOUNT_SEED_4, TIP_ACCOUNT_SEED_5, TIP_ACCOUNT_SEED_6,
        TIP_ACCOUNT_SEED_7,
    },
    tokio::time::sleep,
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
    #[serde(with = "pubkey_string_conversion")]
    pub tip_distribution_account: Pubkey,
    #[serde(with = "pubkey_string_conversion")]
    pub merkle_root_upload_authority: Pubkey,
    pub merkle_root: Hash,
    pub tree_nodes: Vec<TreeNode>,
    pub max_total_claim: u64,
    pub max_num_nodes: u64,
}

pub struct TipPaymentPubkeys {
    config_pda: Pubkey,
    tip_pdas: Vec<Pubkey>,
}

impl GeneratedMerkleTreeCollection {
    pub fn new_from_stake_meta_collection(
        stake_meta_coll: StakeMetaCollection,
    ) -> Result<GeneratedMerkleTreeCollection, MerkleRootGeneratorError> {
        let generated_merkle_trees = stake_meta_coll
            .stake_metas
            .into_iter()
            .filter(|stake_meta| stake_meta.maybe_tip_distribution_meta.is_some())
            .filter_map(|stake_meta| {
                let mut tree_nodes = match TreeNode::vec_from_stake_meta(&stake_meta) {
                    Err(e) => return Some(Err(e)),
                    Ok(maybe_tree_nodes) => maybe_tree_nodes,
                }?;

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
                    merkle_root_upload_authority: tip_distribution_meta
                        .merkle_root_upload_authority,
                    merkle_root: *merkle_tree.get_root().unwrap(),
                    tree_nodes,
                    max_total_claim: tip_distribution_meta.total_tips,
                }))
            })
            .collect::<Result<Vec<GeneratedMerkleTree>, MerkleRootGeneratorError>>()?;

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

fn derive_tip_payment_pubkeys(program_id: &Pubkey) -> TipPaymentPubkeys {
    let config_pda = Pubkey::find_program_address(&[CONFIG_ACCOUNT_SEED], program_id).0;
    let tip_pda_0 = Pubkey::find_program_address(&[TIP_ACCOUNT_SEED_0], program_id).0;
    let tip_pda_1 = Pubkey::find_program_address(&[TIP_ACCOUNT_SEED_1], program_id).0;
    let tip_pda_2 = Pubkey::find_program_address(&[TIP_ACCOUNT_SEED_2], program_id).0;
    let tip_pda_3 = Pubkey::find_program_address(&[TIP_ACCOUNT_SEED_3], program_id).0;
    let tip_pda_4 = Pubkey::find_program_address(&[TIP_ACCOUNT_SEED_4], program_id).0;
    let tip_pda_5 = Pubkey::find_program_address(&[TIP_ACCOUNT_SEED_5], program_id).0;
    let tip_pda_6 = Pubkey::find_program_address(&[TIP_ACCOUNT_SEED_6], program_id).0;
    let tip_pda_7 = Pubkey::find_program_address(&[TIP_ACCOUNT_SEED_7], program_id).0;

    TipPaymentPubkeys {
        config_pda,
        tip_pdas: vec![
            tip_pda_0, tip_pda_1, tip_pda_2, tip_pda_3, tip_pda_4, tip_pda_5, tip_pda_6, tip_pda_7,
        ],
    }
}

#[derive(Clone, Eq, Debug, Hash, PartialEq, Deserialize, Serialize)]
pub struct TreeNode {
    /// The stake account entitled to redeem.
    #[serde(with = "pubkey_string_conversion")]
    pub claimant: Pubkey,

    /// Pubkey of the ClaimStatus PDA account, this account should be closed to reclaim rent.
    #[serde(with = "pubkey_string_conversion")]
    pub claim_status_pubkey: Pubkey,

    /// Bump of the ClaimStatus PDA account
    pub claim_status_bump: u8,

    #[serde(with = "pubkey_string_conversion")]
    pub staker_pubkey: Pubkey,

    #[serde(with = "pubkey_string_conversion")]
    pub withdrawer_pubkey: Pubkey,

    /// The amount this account is entitled to.
    pub amount: u64,

    /// The proof associated with this TreeNode
    pub proof: Option<Vec<[u8; 32]>>,
}

impl TreeNode {
    fn vec_from_stake_meta(
        stake_meta: &StakeMeta,
    ) -> Result<Option<Vec<TreeNode>>, MerkleRootGeneratorError> {
        if let Some(tip_distribution_meta) = stake_meta.maybe_tip_distribution_meta.as_ref() {
            let validator_amount = (tip_distribution_meta.total_tips as u128)
                .checked_mul(tip_distribution_meta.validator_fee_bps as u128)
                .unwrap()
                .checked_div(10_000)
                .unwrap() as u64;
            let (claim_status_pubkey, claim_status_bump) = Pubkey::find_program_address(
                &[
                    ClaimStatus::SEED,
                    &stake_meta.validator_vote_account.to_bytes(),
                    &tip_distribution_meta.tip_distribution_pubkey.to_bytes(),
                ],
                &TipDistribution::id(),
            );
            let mut tree_nodes = vec![TreeNode {
                claimant: stake_meta.validator_vote_account,
                claim_status_pubkey,
                claim_status_bump,
                staker_pubkey: Pubkey::default(),
                withdrawer_pubkey: Pubkey::default(),
                amount: validator_amount,
                proof: None,
            }];

            let remaining_total_rewards = tip_distribution_meta
                .total_tips
                .checked_sub(validator_amount)
                .unwrap() as u128;

            let total_delegated = stake_meta.total_delegated as u128;
            tree_nodes.extend(
                stake_meta
                    .delegations
                    .iter()
                    .map(|delegation| {
                        let amount_delegated = delegation.lamports_delegated as u128;
                        let reward_amount = (amount_delegated.checked_mul(remaining_total_rewards))
                            .unwrap()
                            .checked_div(total_delegated)
                            .unwrap();
                        let (claim_status_pubkey, claim_status_bump) = Pubkey::find_program_address(
                            &[
                                ClaimStatus::SEED,
                                &delegation.stake_account_pubkey.to_bytes(),
                                &tip_distribution_meta.tip_distribution_pubkey.to_bytes(),
                            ],
                            &TipDistribution::id(),
                        );
                        Ok(TreeNode {
                            claimant: delegation.stake_account_pubkey,
                            claim_status_pubkey,
                            claim_status_bump,
                            staker_pubkey: delegation.staker_pubkey,
                            withdrawer_pubkey: delegation.withdrawer_pubkey,
                            amount: reward_amount as u64,
                            proof: None,
                        })
                    })
                    .collect::<Result<Vec<TreeNode>, MerkleRootGeneratorError>>()?,
            );

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

impl Ord for StakeMeta {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.validator_vote_account
            .cmp(&other.validator_vote_account)
    }
}

impl PartialOrd<Self> for StakeMeta {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
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
    ) -> Result<Self, stake_meta_generator_workflow::StakeMetaGeneratorError> {
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

impl Ord for Delegation {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (
            self.stake_account_pubkey,
            self.withdrawer_pubkey,
            self.staker_pubkey,
            self.lamports_delegated,
        )
            .cmp(&(
                other.stake_account_pubkey,
                other.withdrawer_pubkey,
                other.staker_pubkey,
                other.lamports_delegated,
            ))
    }
}

impl PartialOrd<Self> for Delegation {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
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

pub async fn send_transactions_with_retry(
    rpc_client: &RpcClient,
    transactions: &[Transaction],
    max_send_duration: Duration,
) -> usize {
    let mut transactions_to_send: HashMap<Signature, Transaction> = transactions
        .iter()
        .map(|tx| (tx.signatures[0], tx.clone()))
        .collect();

    let start = Instant::now();
    while !transactions_to_send.is_empty() && start.elapsed() < max_send_duration {
        info!("sending {} transactions", transactions_to_send.len());

        for (signature, tx) in &transactions_to_send {
            match rpc_client.send_transaction(tx).await {
                Ok(send_tx_sig) => {
                    info!("send transaction: {:?}", send_tx_sig);
                }
                Err(e) => {
                    error!("error sending transaction {:?} error: {:?}", signature, e);
                }
            }
        }

        sleep(Duration::from_secs(10)).await;

        let mut signatures_confirmed: Vec<Signature> = Vec::new();
        for signature in transactions_to_send.keys() {
            match rpc_client.confirm_transaction(signature).await {
                Ok(true) => {
                    info!("confirmed signature: {:?}", signature);
                    signatures_confirmed.push(*signature);
                }
                Ok(false) => {
                    info!("didn't confirmed signature: {:?}", signature);
                }
                Err(e) => {
                    error!(
                        "error confirming signature: {:?}, signature: {:?}",
                        signature, e
                    );
                }
            }
        }

        info!("confirmed #{} signatures", signatures_confirmed.len());
        for sig in signatures_confirmed {
            transactions_to_send.remove(&sig);
        }
    }

    transactions_to_send.len()
}

mod pubkey_string_conversion {
    use {
        serde::{self, Deserialize, Deserializer, Serializer},
        solana_sdk::pubkey::Pubkey,
        std::str::FromStr,
    };

    pub(crate) fn serialize<S>(pubkey: &Pubkey, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&pubkey.to_string())
    }

    pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<Pubkey, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Pubkey::from_str(&s).map_err(serde::de::Error::custom)
    }
}

pub(crate) fn read_json_from_file<T>(path: &PathBuf) -> serde_json::Result<T>
where
    T: DeserializeOwned,
{
    let file = File::open(path).unwrap();
    let reader = BufReader::new(file);
    serde_json::from_reader(reader)
}

#[cfg(test)]
mod tests {
    use {super::*, tip_distribution::merkle_proof};

    #[test]
    fn test_merkle_tree_verify() {
        // Create the merkle tree and proofs
        let tda = Pubkey::new_unique();
        let (acct_0, acct_1) = (Pubkey::new_unique(), Pubkey::new_unique());
        let claim_statuses = &[(acct_0, tda), (acct_1, tda)]
            .iter()
            .map(|(claimant, tda)| {
                Pubkey::find_program_address(
                    &[ClaimStatus::SEED, &claimant.to_bytes(), &tda.to_bytes()],
                    &TipDistribution::id(),
                )
            })
            .collect::<Vec<(Pubkey, u8)>>();
        let tree_nodes = vec![
            TreeNode {
                claimant: acct_0,
                claim_status_pubkey: claim_statuses[0].0,
                claim_status_bump: claim_statuses[0].1,
                staker_pubkey: Pubkey::default(),
                withdrawer_pubkey: Pubkey::default(),
                amount: 151_507,
                proof: None,
            },
            TreeNode {
                claimant: acct_1,
                claim_status_pubkey: claim_statuses[1].0,
                claim_status_bump: claim_statuses[1].1,
                staker_pubkey: Pubkey::default(),
                withdrawer_pubkey: Pubkey::default(),
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
        let merkle_root_upload_authority = Pubkey::new_unique();

        let (tda_0, tda_1) = (Pubkey::new_unique(), Pubkey::new_unique());

        let stake_account_0 = Pubkey::new_unique();
        let stake_account_1 = Pubkey::new_unique();
        let stake_account_2 = Pubkey::new_unique();
        let stake_account_3 = Pubkey::new_unique();

        let staker_account_0 = Pubkey::new_unique();
        let staker_account_1 = Pubkey::new_unique();
        let staker_account_2 = Pubkey::new_unique();
        let staker_account_3 = Pubkey::new_unique();

        let validator_vote_account_0 = Pubkey::new_unique();
        let validator_vote_account_1 = Pubkey::new_unique();

        let stake_meta_collection = StakeMetaCollection {
            stake_metas: vec![
                StakeMeta {
                    validator_vote_account: validator_vote_account_0,
                    maybe_tip_distribution_meta: Some(TipDistributionMeta {
                        merkle_root_upload_authority,
                        tip_distribution_pubkey: tda_0,
                        total_tips: 1_900_122_111_000,
                        validator_fee_bps: 100,
                    }),
                    delegations: vec![
                        Delegation {
                            stake_account_pubkey: stake_account_0,
                            staker_pubkey: staker_account_0,
                            withdrawer_pubkey: staker_account_0,
                            lamports_delegated: 123_999_123_555,
                        },
                        Delegation {
                            stake_account_pubkey: stake_account_1,
                            staker_pubkey: staker_account_1,
                            withdrawer_pubkey: staker_account_1,
                            lamports_delegated: 144_555_444_556,
                        },
                    ],
                    total_delegated: 1_555_123_000_333_454_000,
                    commission: 100,
                },
                StakeMeta {
                    validator_vote_account: validator_vote_account_1,
                    maybe_tip_distribution_meta: Some(TipDistributionMeta {
                        merkle_root_upload_authority,
                        tip_distribution_pubkey: tda_1,
                        total_tips: 1_900_122_111_333,
                        validator_fee_bps: 200,
                    }),
                    delegations: vec![
                        Delegation {
                            stake_account_pubkey: stake_account_2,
                            staker_pubkey: staker_account_2,
                            withdrawer_pubkey: staker_account_2,
                            lamports_delegated: 224_555_444,
                        },
                        Delegation {
                            stake_account_pubkey: stake_account_3,
                            staker_pubkey: staker_account_3,
                            withdrawer_pubkey: staker_account_3,
                            lamports_delegated: 700_888_944_555,
                        },
                    ],
                    total_delegated: 2_565_318_909_444_123,
                    commission: 10,
                },
            ],
            tip_distribution_program_id: Pubkey::new_unique(),
            bank_hash: solana_sdk::hash::Hash::new_unique().to_string(),
            epoch: 100,
            slot: 2_000_000,
        };

        let merkle_tree_collection = GeneratedMerkleTreeCollection::new_from_stake_meta_collection(
            stake_meta_collection.clone(),
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
        let claim_statuses = &[
            (validator_vote_account_0, tda_0),
            (stake_account_0, tda_0),
            (stake_account_1, tda_0),
            (validator_vote_account_1, tda_1),
            (stake_account_2, tda_1),
            (stake_account_3, tda_1),
        ]
        .iter()
        .map(|(claimant, tda)| {
            Pubkey::find_program_address(
                &[ClaimStatus::SEED, &claimant.to_bytes(), &tda.to_bytes()],
                &TipDistribution::id(),
            )
        })
        .collect::<Vec<(Pubkey, u8)>>();
        let tree_nodes = vec![
            TreeNode {
                claimant: validator_vote_account_0,
                claim_status_pubkey: claim_statuses[0].0,
                claim_status_bump: claim_statuses[0].1,
                staker_pubkey: Pubkey::default(),
                withdrawer_pubkey: Pubkey::default(),
                amount: 19_001_221_110,
                proof: None,
            },
            TreeNode {
                claimant: stake_account_0,
                claim_status_pubkey: claim_statuses[1].0,
                claim_status_bump: claim_statuses[1].1,
                staker_pubkey: Pubkey::default(),
                withdrawer_pubkey: Pubkey::default(),
                amount: 149_992,
                proof: None,
            },
            TreeNode {
                claimant: stake_account_1,
                claim_status_pubkey: claim_statuses[2].0,
                claim_status_bump: claim_statuses[2].1,
                staker_pubkey: Pubkey::default(),
                withdrawer_pubkey: Pubkey::default(),
                amount: 174_858,
                proof: None,
            },
        ];
        let hashed_nodes: Vec<[u8; 32]> = tree_nodes.iter().map(|n| n.hash().to_bytes()).collect();
        let merkle_tree = MerkleTree::new(&hashed_nodes[..], true);
        let gmt_0 = GeneratedMerkleTree {
            tip_distribution_account: tda_0,
            merkle_root_upload_authority,
            merkle_root: *merkle_tree.get_root().unwrap(),
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
                claimant: validator_vote_account_1,
                claim_status_pubkey: claim_statuses[3].0,
                claim_status_bump: claim_statuses[3].1,
                staker_pubkey: Pubkey::default(),
                withdrawer_pubkey: Pubkey::default(),
                amount: 38_002_442_226,
                proof: None,
            },
            TreeNode {
                claimant: stake_account_2,
                claim_status_pubkey: claim_statuses[4].0,
                claim_status_bump: claim_statuses[4].1,
                staker_pubkey: Pubkey::default(),
                withdrawer_pubkey: Pubkey::default(),
                amount: 163_000,
                proof: None,
            },
            TreeNode {
                claimant: stake_account_3,
                claim_status_pubkey: claim_statuses[5].0,
                claim_status_bump: claim_statuses[5].1,
                staker_pubkey: Pubkey::default(),
                withdrawer_pubkey: Pubkey::default(),
                amount: 508_762_900,
                proof: None,
            },
        ];
        let hashed_nodes: Vec<[u8; 32]> = tree_nodes.iter().map(|n| n.hash().to_bytes()).collect();
        let merkle_tree = MerkleTree::new(&hashed_nodes[..], true);
        let gmt_1 = GeneratedMerkleTree {
            tip_distribution_account: tda_1,
            merkle_root_upload_authority,
            merkle_root: *merkle_tree.get_root().unwrap(),
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
                assert_eq!(expected_gmt.merkle_root, actual_gmt.merkle_root);
            });
    }
}
