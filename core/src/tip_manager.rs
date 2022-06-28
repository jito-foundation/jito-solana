use {
    anchor_lang::{
        solana_program::hash::Hash, AccountDeserialize, InstructionData, ToAccountMetas,
    },
    log::warn,
    solana_runtime::bank::Bank,
    solana_sdk::{
        account::ReadableAccount,
        bundle::error::TipPaymentError,
        instruction::Instruction,
        pubkey::Pubkey,
        signature::Keypair,
        signer::Signer,
        system_program,
        transaction::{SanitizedTransaction, Transaction},
    },
    std::{
        collections::HashSet,
        sync::{Arc, Mutex, MutexGuard},
    },
    tip_payment::{
        Config, InitBumps, TipPaymentAccount, CONFIG_ACCOUNT_SEED, TIP_ACCOUNT_SEED_0,
        TIP_ACCOUNT_SEED_1, TIP_ACCOUNT_SEED_2, TIP_ACCOUNT_SEED_3, TIP_ACCOUNT_SEED_4,
        TIP_ACCOUNT_SEED_5, TIP_ACCOUNT_SEED_6, TIP_ACCOUNT_SEED_7,
    },
};

pub type Result<T> = std::result::Result<T, TipPaymentError>;

#[derive(Debug, Clone)]
struct ProgramInfo {
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

#[derive(Debug, Clone)]
pub struct TipManager {
    program_info: ProgramInfo,
    lock: Arc<Mutex<()>>,
}

impl TipManager {
    pub fn new(program_id: Pubkey) -> TipManager {
        let config_pda_bump = Pubkey::find_program_address(&[CONFIG_ACCOUNT_SEED], &program_id);

        let tip_pda_0 = Pubkey::find_program_address(&[TIP_ACCOUNT_SEED_0], &program_id);
        let tip_pda_1 = Pubkey::find_program_address(&[TIP_ACCOUNT_SEED_1], &program_id);
        let tip_pda_2 = Pubkey::find_program_address(&[TIP_ACCOUNT_SEED_2], &program_id);
        let tip_pda_3 = Pubkey::find_program_address(&[TIP_ACCOUNT_SEED_3], &program_id);
        let tip_pda_4 = Pubkey::find_program_address(&[TIP_ACCOUNT_SEED_4], &program_id);
        let tip_pda_5 = Pubkey::find_program_address(&[TIP_ACCOUNT_SEED_5], &program_id);
        let tip_pda_6 = Pubkey::find_program_address(&[TIP_ACCOUNT_SEED_6], &program_id);
        let tip_pda_7 = Pubkey::find_program_address(&[TIP_ACCOUNT_SEED_7], &program_id);

        TipManager {
            program_info: ProgramInfo {
                program_id,
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
            lock: Arc::new(Mutex::new(())),
        }
    }

    pub fn lock(&self) -> MutexGuard<()> {
        self.lock.lock().unwrap()
    }

    pub fn program_id(&self) -> Pubkey {
        self.program_info.program_id
    }

    pub fn config_pubkey(&self) -> Pubkey {
        self.program_info.config_pda_bump.0
    }

    /// Given a bank, returns the current tip receiver
    pub fn get_current_tip_receiver(&self, bank: &Arc<Bank>) -> Result<Pubkey> {
        Ok(self.get_config_account(bank)?.tip_receiver)
    }

    pub fn get_tip_accounts(&self) -> HashSet<Pubkey> {
        HashSet::from([
            self.program_info.tip_pda_0.0,
            self.program_info.tip_pda_1.0,
            self.program_info.tip_pda_2.0,
            self.program_info.tip_pda_3.0,
            self.program_info.tip_pda_4.0,
            self.program_info.tip_pda_5.0,
            self.program_info.tip_pda_6.0,
            self.program_info.tip_pda_7.0,
        ])
    }

    pub fn get_config_account(&self, bank: &Arc<Bank>) -> Result<Config> {
        let config_data = bank
            .get_account(&self.program_info.config_pda_bump.0)
            .ok_or(TipPaymentError::AccountMissing(
                self.program_info.config_pda_bump.0,
            ))?;

        Ok(Config::try_deserialize(&mut config_data.data())?)
    }

    /// Only called once during contract creation
    pub fn initialize_config_tx(
        &self,
        blockhash: &Hash,
        keypair: &Keypair,
    ) -> SanitizedTransaction {
        let init_ix = Instruction {
            program_id: self.program_info.program_id,
            data: tip_payment::instruction::Initialize {
                _bumps: InitBumps {
                    config: self.program_info.config_pda_bump.1,
                    tip_payment_account_0: self.program_info.tip_pda_0.1,
                    tip_payment_account_1: self.program_info.tip_pda_1.1,
                    tip_payment_account_2: self.program_info.tip_pda_2.1,
                    tip_payment_account_3: self.program_info.tip_pda_3.1,
                    tip_payment_account_4: self.program_info.tip_pda_4.1,
                    tip_payment_account_5: self.program_info.tip_pda_5.1,
                    tip_payment_account_6: self.program_info.tip_pda_6.1,
                    tip_payment_account_7: self.program_info.tip_pda_7.1,
                },
            }
            .data(),
            accounts: tip_payment::accounts::Initialize {
                config: self.program_info.config_pda_bump.0,
                tip_payment_account_0: self.program_info.tip_pda_0.0,
                tip_payment_account_1: self.program_info.tip_pda_1.0,
                tip_payment_account_2: self.program_info.tip_pda_2.0,
                tip_payment_account_3: self.program_info.tip_pda_3.0,
                tip_payment_account_4: self.program_info.tip_pda_4.0,
                tip_payment_account_5: self.program_info.tip_pda_5.0,
                tip_payment_account_6: self.program_info.tip_pda_6.0,
                tip_payment_account_7: self.program_info.tip_pda_7.0,
                system_program: system_program::id(),
                payer: keypair.pubkey(),
            }
            .to_account_metas(None),
        };
        SanitizedTransaction::try_from_legacy_transaction(Transaction::new_signed_with_payer(
            &[init_ix],
            Some(&keypair.pubkey()),
            &[keypair],
            *blockhash,
        ))
        .unwrap()
    }

    /// Builds a transaction that changes the current tip receiver to new_tip_receiver.
    /// The on-chain program will transfer tips sitting in the tip accounts to the tip receiver
    /// before changing ownership.
    pub fn change_tip_receiver_tx(
        &self,
        new_tip_receiver: &Pubkey,
        bank: &Arc<Bank>,
        keypair: &Keypair,
    ) -> Result<SanitizedTransaction> {
        let config = self.get_config_account(bank)?;

        let change_tip_ix = Instruction {
            program_id: self.program_info.program_id,
            data: tip_payment::instruction::ChangeTipReceiver {}.data(),
            accounts: tip_payment::accounts::ChangeTipReceiver {
                config: self.program_info.config_pda_bump.0,
                old_tip_receiver: config.tip_receiver,
                new_tip_receiver: *new_tip_receiver,
                tip_payment_account_0: self.program_info.tip_pda_0.0,
                tip_payment_account_1: self.program_info.tip_pda_1.0,
                tip_payment_account_2: self.program_info.tip_pda_2.0,
                tip_payment_account_3: self.program_info.tip_pda_3.0,
                tip_payment_account_4: self.program_info.tip_pda_4.0,
                tip_payment_account_5: self.program_info.tip_pda_5.0,
                tip_payment_account_6: self.program_info.tip_pda_6.0,
                tip_payment_account_7: self.program_info.tip_pda_7.0,
                signer: keypair.pubkey(),
            }
            .to_account_metas(None),
        };
        Ok(
            SanitizedTransaction::try_from_legacy_transaction(Transaction::new_signed_with_payer(
                &[change_tip_ix],
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
