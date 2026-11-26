use {
    crate::banking_stage::{
        committer::CommitTransactionDetails,
        consumer::ProcessTransactionBatchOutput,
        scheduler_messages::{NotCommittedReason, TransactionResult},
    },
    jito_protos::proto::bam_types::{TransactionCommittedResult, TransactionErrorReason},
    solana_account::ReadableAccount,
    solana_nonce::state::{DurableNonce, State},
    solana_nonce_account::verify_nonce_account,
    solana_runtime::bank::Bank,
    solana_runtime_transaction::transaction_with_meta::{TransactionWithMeta, writable_accounts},
    solana_transaction_error::TransactionError,
};

/// A prior atomic-bundle write may change the nonce authority, including via CPI.
pub(super) fn recover_dependent_nonce_error<'a, Tx: TransactionWithMeta + 'a>(
    bank: &Bank,
    transaction: &Tx,
    mut prior_transactions: impl Iterator<Item = &'a Tx>,
    result: Result<(), TransactionError>,
) -> Result<(), TransactionError> {
    if result != Err(TransactionError::BlockhashNotFound) {
        return result;
    }
    let Some(nonce_account) = transaction.get_durable_nonce(true) else {
        return result;
    };
    if !prior_transactions.any(|prior| writable_accounts(prior).any(|key| key == nonce_account)) {
        return result;
    }

    // Execution still needs the committed nonce to be well-sized, match and be advanceable.
    let committed = bank
        .get_account(nonce_account)
        .filter(|account| account.data().len() == State::size())
        .and_then(|account| verify_nonce_account(&account, transaction.recent_blockhash()));
    if committed.is_none_or(|nonce| {
        nonce.durable_nonce == DurableNonce::from_blockhash(&bank.last_blockhash())
    }) {
        return result;
    }

    // Durable nonce values are separate from recent blockhashes; nonce-path messages aren't cached.
    Ok(())
}

pub fn convert_txn_error_to_proto(err: TransactionError) -> TransactionErrorReason {
    match err {
        TransactionError::AccountInUse => TransactionErrorReason::AccountInUse,
        TransactionError::AccountLoadedTwice => TransactionErrorReason::AccountLoadedTwice,
        TransactionError::AccountNotFound => TransactionErrorReason::AccountNotFound,
        TransactionError::ProgramAccountNotFound => TransactionErrorReason::ProgramAccountNotFound,
        TransactionError::InsufficientFundsForFee => {
            TransactionErrorReason::InsufficientFundsForFee
        }
        TransactionError::InvalidAccountForFee => TransactionErrorReason::InvalidAccountForFee,
        TransactionError::AlreadyProcessed => TransactionErrorReason::AlreadyProcessed,
        TransactionError::BlockhashNotFound => TransactionErrorReason::BlockhashNotFound,
        TransactionError::InstructionError(_, _) => TransactionErrorReason::InstructionError,
        TransactionError::CallChainTooDeep => TransactionErrorReason::CallChainTooDeep,
        TransactionError::MissingSignatureForFee => TransactionErrorReason::MissingSignatureForFee,
        TransactionError::InvalidAccountIndex => TransactionErrorReason::InvalidAccountIndex,
        TransactionError::SignatureFailure => TransactionErrorReason::SignatureFailure,
        TransactionError::InvalidProgramForExecution => {
            TransactionErrorReason::InvalidProgramForExecution
        }
        TransactionError::SanitizeFailure => TransactionErrorReason::SanitizeFailure,
        TransactionError::ClusterMaintenance => TransactionErrorReason::ClusterMaintenance,
        TransactionError::AccountBorrowOutstanding => {
            TransactionErrorReason::AccountBorrowOutstanding
        }
        TransactionError::WouldExceedMaxBlockCostLimit => {
            TransactionErrorReason::WouldExceedMaxBlockCostLimit
        }
        TransactionError::UnsupportedVersion => TransactionErrorReason::UnsupportedVersion,
        TransactionError::InvalidWritableAccount => TransactionErrorReason::InvalidWritableAccount,
        TransactionError::WouldExceedMaxAccountCostLimit => {
            TransactionErrorReason::WouldExceedMaxAccountCostLimit
        }
        TransactionError::WouldExceedAccountDataBlockLimit => {
            TransactionErrorReason::WouldExceedAccountDataBlockLimit
        }
        TransactionError::TooManyAccountLocks => TransactionErrorReason::TooManyAccountLocks,
        TransactionError::AddressLookupTableNotFound => {
            TransactionErrorReason::AddressLookupTableNotFound
        }
        TransactionError::InvalidAddressLookupTableOwner => {
            TransactionErrorReason::InvalidAddressLookupTableOwner
        }
        TransactionError::InvalidAddressLookupTableData => {
            TransactionErrorReason::InvalidAddressLookupTableData
        }
        TransactionError::InvalidAddressLookupTableIndex => {
            TransactionErrorReason::InvalidAddressLookupTableIndex
        }
        TransactionError::InvalidRentPayingAccount => {
            TransactionErrorReason::InvalidRentPayingAccount
        }
        TransactionError::WouldExceedMaxVoteCostLimit => {
            TransactionErrorReason::WouldExceedMaxVoteCostLimit
        }
        TransactionError::WouldExceedAccountDataTotalLimit => {
            TransactionErrorReason::WouldExceedAccountDataTotalLimit
        }
        TransactionError::DuplicateInstruction(_) => TransactionErrorReason::DuplicateInstruction,
        TransactionError::InsufficientFundsForRent { .. } => {
            TransactionErrorReason::InsufficientFundsForRent
        }
        TransactionError::MaxLoadedAccountsDataSizeExceeded => {
            TransactionErrorReason::MaxLoadedAccountsDataSizeExceeded
        }
        TransactionError::InvalidLoadedAccountsDataSizeLimit => {
            TransactionErrorReason::InvalidLoadedAccountsDataSizeLimit
        }
        TransactionError::ResanitizationNeeded => TransactionErrorReason::ResanitizationNeeded,
        TransactionError::ProgramExecutionTemporarilyRestricted { .. } => {
            TransactionErrorReason::ProgramExecutionTemporarilyRestricted
        }
        TransactionError::UnbalancedTransaction => TransactionErrorReason::UnbalancedTransaction,
        TransactionError::ProgramCacheHitMaxLimit => {
            TransactionErrorReason::ProgramCacheHitMaxLimit
        }
        TransactionError::CommitCancelled => TransactionErrorReason::CommitCancelled,
        // ponytail: TransactionError is #[non_exhaustive] since solana-transaction-error 4.0.
        // `BailOut` (program-runtime bail-out) and any future variant map to the generic
        // execution failure until bam-protos grows a matching reason.
        _ => TransactionErrorReason::InstructionError,
    }
}

/// Builds per-transaction results from consume output for BAM responses.
///
/// If commit details are available, each `CommitTransactionDetails` is mapped into a
/// `TransactionResult` with commit metadata or a not-committed error. If commit details are
/// unavailable (e.g., a PoH recorder failure), it falls back to one `NotCommitted(PohTimeout)`
/// result per input transaction.
pub(in crate::banking_stage) fn build_finished_consume_work_extra_info(
    output: &ProcessTransactionBatchOutput,
    transaction_count: usize,
) -> Vec<TransactionResult> {
    let Ok(commit_transactions_result) = output
        .execute_and_commit_transactions_output
        .commit_transactions_result
        .as_ref()
    else {
        return vec![
            TransactionResult::NotCommitted(
                NotCommittedReason::PohTimeout, // Note: ChannelFull, ChannelDisconnected, MaxHeightReached are misreported as PohTimeout
            );
            transaction_count
        ];
    };

    commit_transactions_result
        .iter()
        .map(|commit_info| match commit_info {
            CommitTransactionDetails::Committed {
                compute_units,
                loaded_accounts_data_size,
                fee_payer_post_balance,
                result,
            } => TransactionResult::Committed(TransactionCommittedResult {
                cus_consumed: *compute_units as u32,
                feepayer_balance_lamports: fee_payer_post_balance.unwrap_or(0),
                loaded_accounts_data_size: *loaded_accounts_data_size,
                execution_success: result.is_ok(),
            }),
            CommitTransactionDetails::NotCommitted(err) => {
                TransactionResult::NotCommitted(NotCommittedReason::Error(err.clone()))
            }
        })
        .collect()
}

#[cfg(test)]
pub(in crate::banking_stage) mod tests {
    use {
        solana_account::{
            AccountSharedData, ReadableAccount, WritableAccount, state_traits::StateMutWincode as _,
        },
        solana_clock::MAX_PROCESSING_AGE,
        solana_compute_budget_interface::ComputeBudgetInstruction,
        solana_hash::Hash,
        solana_keypair::Keypair,
        solana_message::Message,
        solana_nonce::{
            state::{Data, DurableNonce, State},
            versions::Versions,
        },
        solana_pubkey::Pubkey,
        solana_runtime::bank::Bank,
        solana_signer::Signer,
        solana_system_interface::{instruction, program},
        solana_transaction::Transaction,
    };

    #[derive(Clone, Copy, Debug)]
    pub enum NoncePrecheckCase {
        Rotation,
        NoPrior,
        Readonly,
        Unrelated,
        Reversed,
        AgedRecentHashReplay,
        StaleNonce,
        UnadvanceableNonce,
        MissingNonce,
        InvalidNonceSize,
        MalformedNonce,
        WrongNonceOwner,
        MalformedBudget,
    }

    pub struct NonceRotationFixture {
        pub payer: Keypair,
        pub old_authority: Keypair,
        pub new_authority: Keypair,
        pub nonce_account: Pubkey,
        pub nonce: DurableNonce,
        pub recipient: Pubkey,
    }

    impl NonceRotationFixture {
        pub fn new(bank: &Bank) -> Self {
            let fixture = Self {
                payer: Keypair::new(),
                old_authority: Keypair::new(),
                new_authority: Keypair::new(),
                nonce_account: Pubkey::new_unique(),
                nonce: DurableNonce::from_blockhash(&Hash::new_unique()),
                recipient: Pubkey::new_unique(),
            };
            bank.store_account(
                &fixture.payer.pubkey(),
                &AccountSharedData::empty(1_000_000_000_000, &program::id()),
            );
            let account = AccountSharedData::new_data(
                bank.get_minimum_balance_for_rent_exemption(State::size()),
                &Versions::new(State::Initialized(Data::new(
                    fixture.old_authority.pubkey(),
                    fixture.nonce,
                    5000,
                ))),
                &program::id(),
            )
            .unwrap();
            bank.store_account(&fixture.nonce_account, &account);
            fixture
        }

        pub fn rotation(
            &self,
            bank: &Bank,
            account: Pubkey,
            authority: &Keypair,
            writable: bool,
        ) -> Transaction {
            let mut ix = instruction::authorize_nonce_account(
                &account,
                &authority.pubkey(),
                &self.new_authority.pubkey(),
            );
            ix.accounts[0].is_writable = writable;
            Transaction::new_signed_with_payer(
                &[ix],
                Some(&self.payer.pubkey()),
                &[&self.payer, authority],
                bank.last_blockhash(),
            )
        }

        pub fn transfer(&self, authority: &Keypair, hash: Hash) -> Transaction {
            solana_system_transaction::nonced_transfer(
                &self.payer,
                &self.recipient,
                1,
                &self.nonce_account,
                authority,
                hash,
            )
        }

        pub fn transactions(&self, bank: &Bank, case: NoncePrecheckCase) -> Vec<Transaction> {
            let mut account = bank.get_account(&self.nonce_account).unwrap();
            let mut hash = *self.nonce.as_hash();
            match case {
                NoncePrecheckCase::StaleNonce => hash = Hash::new_unique(),
                NoncePrecheckCase::UnadvanceableNonce => {
                    let nonce = DurableNonce::from_blockhash(&bank.last_blockhash());
                    account
                        .set_state(&Versions::new(State::Initialized(Data::new(
                            self.old_authority.pubkey(),
                            nonce,
                            5000,
                        ))))
                        .unwrap();
                    hash = *nonce.as_hash();
                }
                NoncePrecheckCase::MissingNonce => account = AccountSharedData::default(),
                NoncePrecheckCase::InvalidNonceSize => {
                    let mut data = account.data().to_vec();
                    data.push(0);
                    account.set_data_from_slice(&data);
                }
                NoncePrecheckCase::MalformedNonce => {
                    account.set_data_from_slice(&[255; State::size()])
                }
                NoncePrecheckCase::WrongNonceOwner => account.set_owner(Pubkey::new_unique()),
                _ => {}
            }
            bank.store_account(&self.nonce_account, &account);
            if matches!(case, NoncePrecheckCase::MissingNonce) {
                assert!(bank.get_account(&self.nonce_account).is_none());
            }
            let followup =
                match case {
                    NoncePrecheckCase::MalformedBudget => Transaction::new(
                        &[&self.payer, &self.new_authority],
                        Message::new_with_nonce(
                            vec![ComputeBudgetInstruction::set_loaded_accounts_data_size_limit(0)],
                            Some(&self.payer.pubkey()),
                            &self.nonce_account,
                            &self.new_authority.pubkey(),
                        ),
                        hash,
                    ),
                    NoncePrecheckCase::AgedRecentHashReplay => {
                        // Commit on the recent-blockhash path, then replay while its status remains cached.
                        let tx = self.transfer(&self.old_authority, bank.last_blockhash());
                        bank.process_transaction(&tx).unwrap();
                        for _ in 0..=MAX_PROCESSING_AGE {
                            bank.register_unique_recent_blockhash_for_test();
                        }
                        assert!(!bank.is_hash_valid_for_age(
                            &tx.message.recent_blockhash,
                            MAX_PROCESSING_AGE
                        ));
                        assert!(
                            bank.get_transaction_status_and_slot_from_status_cache(
                                &tx.message.hash(),
                                &tx.message.recent_blockhash,
                            )
                            .is_some()
                        );
                        tx
                    }
                    _ => self.transfer(&self.new_authority, hash),
                };
            let prior = self.rotation(
                bank,
                if matches!(case, NoncePrecheckCase::Unrelated) {
                    Pubkey::new_unique()
                } else {
                    self.nonce_account
                },
                &self.old_authority,
                !matches!(case, NoncePrecheckCase::Readonly),
            );
            let mut transactions = vec![prior, followup];
            match case {
                NoncePrecheckCase::NoPrior => {
                    transactions.remove(0);
                }
                NoncePrecheckCase::Reversed => transactions.reverse(),
                _ => {}
            }
            transactions
        }
    }
}
