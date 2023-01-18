use {
    anchor_lang::{
        solana_program::hash::Hash, AccountDeserialize, InstructionData, ToAccountMetas,
    },
    log::warn,
    solana_gossip::cluster_info::ClusterInfo,
    solana_runtime::bank::Bank,
    solana_sdk::{
        account::ReadableAccount,
        bundle::error::TipPaymentError,
        instruction::Instruction,
        pubkey::Pubkey,
        signature::Keypair,
        signer::Signer,
        stake_history::Epoch,
        system_program,
        transaction::{SanitizedTransaction, Transaction},
    },
    std::{
        collections::HashSet,
        sync::{Arc, Mutex, MutexGuard},
    },
    tip_distribution::sdk::{
        derive_config_account_address, derive_tip_distribution_account_address,
        instruction::{
            initialize_ix, initialize_tip_distribution_account_ix, InitializeAccounts,
            InitializeArgs, InitializeTipDistributionAccountAccounts,
            InitializeTipDistributionAccountArgs,
        },
    },
    tip_payment::{
        Config, InitBumps, TipPaymentAccount, CONFIG_ACCOUNT_SEED, TIP_ACCOUNT_SEED_0,
        TIP_ACCOUNT_SEED_1, TIP_ACCOUNT_SEED_2, TIP_ACCOUNT_SEED_3, TIP_ACCOUNT_SEED_4,
        TIP_ACCOUNT_SEED_5, TIP_ACCOUNT_SEED_6, TIP_ACCOUNT_SEED_7,
    },
};

pub type Result<T> = std::result::Result<T, TipPaymentError>;

#[derive(Debug, Clone)]
struct TipPaymentProgramInfo {
    program_id: Pubkey,

    config_pda_bump: (Pubkey, u8),
    tip_pda_0: (Pubkey, u8),
    tip_pda_1: (Pubkey, u8),
    tip_pda_2: (Pubkey, u8),
    tip_pda_3: (Pubkey, u8),
    tip_pda_4: (Pubkey, u8),
    tip_pda_5: (Pubkey, u8),
    tip_pda_6: (Pubkey, u8),
    tip_pda_7: (Pubkey, u8),
}

/// Contains metadata regarding the tip-distribution account.
/// The PDAs contained in this struct are presumed to be owned by the program.
#[derive(Debug, Clone)]
struct TipDistributionProgramInfo {
    /// The tip-distribution program_id.
    program_id: Pubkey,

    /// Singleton [Config] PDA and bump tuple.
    config_pda_and_bump: (Pubkey, u8),
}

/// This config is used on each invocation to the `initialize_tip_distribution_account` instruction.
#[derive(Debug, Clone)]
pub struct TipDistributionAccountConfig {
    /// The account with authority to upload merkle-roots to this validator's [TipDistributionAccount].
    pub merkle_root_upload_authority: Pubkey,

    /// This validator's vote account.
    pub vote_account: Pubkey,

    /// This validator's commission rate BPS for tips in the [TipDistributionAccount].
    pub commission_bps: u16,
}

impl Default for TipDistributionAccountConfig {
    fn default() -> Self {
        Self {
            merkle_root_upload_authority: Pubkey::new_unique(),
            vote_account: Pubkey::new_unique(),
            commission_bps: 0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TipManager {
    tip_payment_program_info: TipPaymentProgramInfo,
    tip_distribution_program_info: TipDistributionProgramInfo,
    tip_distribution_account_config: TipDistributionAccountConfig,
    lock: Arc<Mutex<()>>,
}

#[derive(Clone)]
pub struct TipManagerConfig {
    pub tip_payment_program_id: Pubkey,
    pub tip_distribution_program_id: Pubkey,
    pub tip_distribution_account_config: TipDistributionAccountConfig,
}

impl Default for TipManagerConfig {
    fn default() -> Self {
        TipManagerConfig {
            tip_payment_program_id: Pubkey::new_unique(),
            tip_distribution_program_id: Pubkey::new_unique(),
            tip_distribution_account_config: TipDistributionAccountConfig::default(),
        }
    }
}

impl TipManager {
    pub fn new(config: TipManagerConfig) -> TipManager {
        let TipManagerConfig {
            tip_payment_program_id,
            tip_distribution_program_id,
            tip_distribution_account_config,
        } = config;

        let config_pda_bump =
            Pubkey::find_program_address(&[CONFIG_ACCOUNT_SEED], &tip_payment_program_id);

        let tip_pda_0 =
            Pubkey::find_program_address(&[TIP_ACCOUNT_SEED_0], &tip_payment_program_id);
        let tip_pda_1 =
            Pubkey::find_program_address(&[TIP_ACCOUNT_SEED_1], &tip_payment_program_id);
        let tip_pda_2 =
            Pubkey::find_program_address(&[TIP_ACCOUNT_SEED_2], &tip_payment_program_id);
        let tip_pda_3 =
            Pubkey::find_program_address(&[TIP_ACCOUNT_SEED_3], &tip_payment_program_id);
        let tip_pda_4 =
            Pubkey::find_program_address(&[TIP_ACCOUNT_SEED_4], &tip_payment_program_id);
        let tip_pda_5 =
            Pubkey::find_program_address(&[TIP_ACCOUNT_SEED_5], &tip_payment_program_id);
        let tip_pda_6 =
            Pubkey::find_program_address(&[TIP_ACCOUNT_SEED_6], &tip_payment_program_id);
        let tip_pda_7 =
            Pubkey::find_program_address(&[TIP_ACCOUNT_SEED_7], &tip_payment_program_id);

        let config_pda_and_bump = derive_config_account_address(&tip_distribution_program_id);

        TipManager {
            tip_payment_program_info: TipPaymentProgramInfo {
                program_id: tip_payment_program_id,
                config_pda_bump,
                tip_pda_0,
                tip_pda_1,
                tip_pda_2,
                tip_pda_3,
                tip_pda_4,
                tip_pda_5,
                tip_pda_6,
                tip_pda_7,
            },
            tip_distribution_program_info: TipDistributionProgramInfo {
                program_id: tip_distribution_program_id,
                config_pda_and_bump,
            },
            tip_distribution_account_config,
            lock: Arc::new(Mutex::new(())),
        }
    }

    pub fn tip_payment_program_id(&self) -> Pubkey {
        self.tip_payment_program_info.program_id
    }

    /// Returns the [Config] account owned by the tip-payment program.
    pub fn tip_payment_config_pubkey(&self) -> Pubkey {
        self.tip_payment_program_info.config_pda_bump.0
    }

    /// Returns the [Config] account owned by the tip-distribution program.
    pub fn tip_distribution_config_pubkey(&self) -> Pubkey {
        self.tip_distribution_program_info.config_pda_and_bump.0
    }

    /// Given a bank, returns the current `tip_receiver` configured with the tip-payment program.
    pub fn get_configured_tip_receiver(&self, bank: &Bank) -> Result<Pubkey> {
        Ok(self.get_tip_payment_config_account(bank)?.tip_receiver)
    }

    pub fn get_tip_accounts(&self) -> HashSet<Pubkey> {
        HashSet::from([
            self.tip_payment_program_info.tip_pda_0.0,
            self.tip_payment_program_info.tip_pda_1.0,
            self.tip_payment_program_info.tip_pda_2.0,
            self.tip_payment_program_info.tip_pda_3.0,
            self.tip_payment_program_info.tip_pda_4.0,
            self.tip_payment_program_info.tip_pda_5.0,
            self.tip_payment_program_info.tip_pda_6.0,
            self.tip_payment_program_info.tip_pda_7.0,
        ])
    }

    pub fn get_tip_payment_config_account(&self, bank: &Bank) -> Result<Config> {
        let config_data = bank
            .get_account(&self.tip_payment_program_info.config_pda_bump.0)
            .ok_or(TipPaymentError::AccountMissing(
                self.tip_payment_program_info.config_pda_bump.0,
            ))?;

        Ok(Config::try_deserialize(&mut config_data.data())?)
    }

    /// Only called once during contract creation.
    pub fn initialize_tip_payment_program_tx(
        &self,
        recent_blockhash: Hash,
        keypair: &Keypair,
    ) -> SanitizedTransaction {
        let init_ix = Instruction {
            program_id: self.tip_payment_program_info.program_id,
            data: tip_payment::instruction::Initialize {
                _bumps: InitBumps {
                    config: self.tip_payment_program_info.config_pda_bump.1,
                    tip_payment_account_0: self.tip_payment_program_info.tip_pda_0.1,
                    tip_payment_account_1: self.tip_payment_program_info.tip_pda_1.1,
                    tip_payment_account_2: self.tip_payment_program_info.tip_pda_2.1,
                    tip_payment_account_3: self.tip_payment_program_info.tip_pda_3.1,
                    tip_payment_account_4: self.tip_payment_program_info.tip_pda_4.1,
                    tip_payment_account_5: self.tip_payment_program_info.tip_pda_5.1,
                    tip_payment_account_6: self.tip_payment_program_info.tip_pda_6.1,
                    tip_payment_account_7: self.tip_payment_program_info.tip_pda_7.1,
                },
            }
            .data(),
            accounts: tip_payment::accounts::Initialize {
                config: self.tip_payment_program_info.config_pda_bump.0,
                tip_payment_account_0: self.tip_payment_program_info.tip_pda_0.0,
                tip_payment_account_1: self.tip_payment_program_info.tip_pda_1.0,
                tip_payment_account_2: self.tip_payment_program_info.tip_pda_2.0,
                tip_payment_account_3: self.tip_payment_program_info.tip_pda_3.0,
                tip_payment_account_4: self.tip_payment_program_info.tip_pda_4.0,
                tip_payment_account_5: self.tip_payment_program_info.tip_pda_5.0,
                tip_payment_account_6: self.tip_payment_program_info.tip_pda_6.0,
                tip_payment_account_7: self.tip_payment_program_info.tip_pda_7.0,
                system_program: system_program::id(),
                payer: keypair.pubkey(),
            }
            .to_account_metas(None),
        };
        SanitizedTransaction::try_from_legacy_transaction(Transaction::new_signed_with_payer(
            &[init_ix],
            Some(&keypair.pubkey()),
            &[keypair],
            recent_blockhash,
        ))
        .unwrap()
    }

    pub fn lock(&self) -> MutexGuard<()> {
        self.lock.lock().unwrap()
    }

    /// Returns this validator's [TipDistributionAccount] PDA derived from the provided epoch.
    pub fn get_my_tip_distribution_pda(&self, epoch: Epoch) -> Pubkey {
        derive_tip_distribution_account_address(
            &self.tip_distribution_program_info.program_id,
            &self.tip_distribution_account_config.vote_account,
            epoch,
        )
        .0
    }

    /// Returns whether or not the tip-payment program should be initialized.
    pub fn should_initialize_tip_payment_program(&self, bank: &Bank) -> bool {
        match bank.get_account(&self.tip_payment_config_pubkey()) {
            None => true,
            Some(account) => account.owner() != &self.tip_payment_program_info.program_id,
        }
    }

    /// Returns whether or not the tip-distribution program's [Config] PDA should be initialized.
    pub fn should_initialize_tip_distribution_config(&self, bank: &Bank) -> bool {
        match bank.get_account(&self.tip_distribution_config_pubkey()) {
            None => true,
            Some(account) => account.owner() != &self.tip_distribution_program_info.program_id,
        }
    }

    /// Returns whether or not the current [TipDistributionAccount] PDA should be initialized for this epoch.
    pub fn should_init_tip_distribution_account(&self, bank: &Bank) -> bool {
        let pda = derive_tip_distribution_account_address(
            &self.tip_distribution_program_info.program_id,
            &self.tip_distribution_account_config.vote_account,
            bank.epoch(),
        )
        .0;
        match bank.get_account(&pda) {
            None => true,
            // Since anyone can derive the PDA and send it lamports we must also check the owner is the program.
            Some(account) => account.owner() != &self.tip_distribution_program_info.program_id,
        }
    }

    /// Creates an [Initialize] transaction object.
    pub fn initialize_tip_distribution_config_tx(
        &self,
        recent_blockhash: Hash,
        cluster_info: &Arc<ClusterInfo>,
    ) -> SanitizedTransaction {
        let ix = initialize_ix(
            self.tip_distribution_program_info.program_id,
            InitializeArgs {
                authority: cluster_info.id(),
                expired_funds_account: cluster_info.id(),
                num_epochs_valid: 10,
                max_validator_commission_bps: 10_000,
                bump: self.tip_distribution_program_info.config_pda_and_bump.1,
            },
            InitializeAccounts {
                config: self.tip_distribution_program_info.config_pda_and_bump.0,
                system_program: system_program::id(),
                initializer: cluster_info.id(),
            },
        );

        SanitizedTransaction::try_from_legacy_transaction(Transaction::new_signed_with_payer(
            &[ix],
            Some(&cluster_info.id()),
            &[cluster_info.keypair().as_ref()],
            recent_blockhash,
        ))
        .unwrap()
    }

    /// Creates an [InitializeTipDistributionAccount] transaction object using the provided Epoch.
    pub fn initialize_tip_distribution_account_tx(
        &self,
        recent_blockhash: Hash,
        epoch: Epoch,
        cluster_info: &Arc<ClusterInfo>,
    ) -> SanitizedTransaction {
        let (tip_distribution_account, bump) = derive_tip_distribution_account_address(
            &self.tip_distribution_program_info.program_id,
            &self.tip_distribution_account_config.vote_account,
            epoch,
        );

        let ix = initialize_tip_distribution_account_ix(
            self.tip_distribution_program_info.program_id,
            InitializeTipDistributionAccountArgs {
                merkle_root_upload_authority: self
                    .tip_distribution_account_config
                    .merkle_root_upload_authority,
                validator_commission_bps: self.tip_distribution_account_config.commission_bps,
                bump,
            },
            InitializeTipDistributionAccountAccounts {
                config: self.tip_distribution_program_info.config_pda_and_bump.0,
                tip_distribution_account,
                system_program: system_program::id(),
                signer: cluster_info.id(),
                validator_vote_account: self.tip_distribution_account_config.vote_account,
            },
        );

        SanitizedTransaction::try_from_legacy_transaction(Transaction::new_signed_with_payer(
            &[ix],
            Some(&cluster_info.id()),
            &[cluster_info.keypair().as_ref()],
            recent_blockhash,
        ))
        .unwrap()
    }

    /// Builds a transaction that changes the current tip receiver to new_tip_receiver.
    /// The on-chain program will transfer tips sitting in the tip accounts to the tip receiver
    /// before changing ownership.
    pub fn change_tip_receiver_and_block_builder_tx(
        &self,
        new_tip_receiver: &Pubkey,
        bank: &Bank,
        keypair: &Keypair,
        block_builder: &Pubkey,
        block_builder_commission: u64,
    ) -> Result<SanitizedTransaction> {
        let config = self.get_tip_payment_config_account(bank)?;

        let change_tip_ix = Instruction {
            program_id: self.tip_payment_program_info.program_id,
            data: tip_payment::instruction::ChangeTipReceiver {}.data(),
            accounts: tip_payment::accounts::ChangeTipReceiver {
                config: self.tip_payment_program_info.config_pda_bump.0,
                old_tip_receiver: config.tip_receiver,
                new_tip_receiver: *new_tip_receiver,
                block_builder: config.block_builder,
                tip_payment_account_0: self.tip_payment_program_info.tip_pda_0.0,
                tip_payment_account_1: self.tip_payment_program_info.tip_pda_1.0,
                tip_payment_account_2: self.tip_payment_program_info.tip_pda_2.0,
                tip_payment_account_3: self.tip_payment_program_info.tip_pda_3.0,
                tip_payment_account_4: self.tip_payment_program_info.tip_pda_4.0,
                tip_payment_account_5: self.tip_payment_program_info.tip_pda_5.0,
                tip_payment_account_6: self.tip_payment_program_info.tip_pda_6.0,
                tip_payment_account_7: self.tip_payment_program_info.tip_pda_7.0,
                signer: keypair.pubkey(),
            }
            .to_account_metas(None),
        };
        let change_block_builder_ix = Instruction {
            program_id: self.tip_payment_program_info.program_id,
            data: tip_payment::instruction::ChangeBlockBuilder {
                block_builder_commission,
            }
            .data(),
            accounts: tip_payment::accounts::ChangeBlockBuilder {
                config: self.tip_payment_program_info.config_pda_bump.0,
                tip_receiver: *new_tip_receiver, // tip receiver will have just changed in previous ix
                old_block_builder: config.block_builder,
                new_block_builder: *block_builder,
                tip_payment_account_0: self.tip_payment_program_info.tip_pda_0.0,
                tip_payment_account_1: self.tip_payment_program_info.tip_pda_1.0,
                tip_payment_account_2: self.tip_payment_program_info.tip_pda_2.0,
                tip_payment_account_3: self.tip_payment_program_info.tip_pda_3.0,
                tip_payment_account_4: self.tip_payment_program_info.tip_pda_4.0,
                tip_payment_account_5: self.tip_payment_program_info.tip_pda_5.0,
                tip_payment_account_6: self.tip_payment_program_info.tip_pda_6.0,
                tip_payment_account_7: self.tip_payment_program_info.tip_pda_7.0,
                signer: keypair.pubkey(),
            }
            .to_account_metas(None),
        };
        Ok(
            SanitizedTransaction::try_from_legacy_transaction(Transaction::new_signed_with_payer(
                &[change_tip_ix, change_block_builder_ix],
                Some(&keypair.pubkey()),
                &[keypair],
                bank.last_blockhash(),
            ))
            .unwrap(),
        )
    }

    /// Returns the balance of all the MEV tip accounts
    pub fn get_tip_account_balances(&self, bank: &Arc<Bank>) -> Vec<(Pubkey, u64)> {
        let accounts = self.get_tip_accounts();
        accounts
            .into_iter()
            .map(|account| {
                let balance = bank.get_balance(&account);
                (account, balance)
            })
            .collect()
    }

    /// Returns the balance of all the MEV tip accounts above the rent-exempt amount.
    /// NOTE: the on-chain program has rent_exempt = force
    pub fn get_tip_account_balances_above_rent_exempt(
        &self,
        bank: &Arc<Bank>,
    ) -> Vec<(Pubkey, u64)> {
        let accounts = self.get_tip_accounts();
        accounts
            .into_iter()
            .map(|account| {
                let account_data = bank.get_account(&account).unwrap_or_default();
                let balance = bank.get_balance(&account);
                let rent_exempt =
                    bank.get_minimum_balance_for_rent_exemption(account_data.data().len());
                // NOTE: don't unwrap here in case bug in on-chain program, don't want all validators to crash
                // if program gets stuck in bad state
                (account, balance.checked_sub(rent_exempt).unwrap_or_else(|| {
                    warn!("balance is below rent exempt amount. balance: {} rent_exempt: {} acc size: {}", balance, rent_exempt, TipPaymentAccount::SIZE);
                    0
                }))
            })
            .collect()
    }
}
