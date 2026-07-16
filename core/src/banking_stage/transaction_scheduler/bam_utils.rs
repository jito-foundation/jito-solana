use {
    jito_protos::proto::bam_types::TransactionErrorReason,
    solana_account::{ReadableAccount, state_traits::StateMut},
    solana_instruction::error::InstructionError,
    solana_nonce::{
        NONCED_TX_MARKER_IX_INDEX,
        state::{DurableNonce, State as NonceState},
        versions::Versions as NonceVersions,
    },
    solana_runtime::bank::Bank,
    solana_sdk_ids::system_program,
    solana_svm_transaction::svm_message::SVMMessage,
    solana_transaction_error::TransactionError,
};

pub(crate) fn get_nonce_authority_error(
    bank: &Bank,
    transaction: &impl SVMMessage,
    err: TransactionError,
) -> TransactionError {
    transaction
        .get_durable_nonce()
        .filter(|_| err == TransactionError::BlockhashNotFound)
        .and_then(|address| bank.get_account(address))
        .filter(|account| {
            account.owner() == &system_program::id() && account.data().len() == NonceState::size()
        })
        .and_then(|account| {
            let versions: NonceVersions = account.state().ok()?;
            let data = versions.verify_recent_blockhash(transaction.recent_blockhash())?;
            (data.durable_nonce != DurableNonce::from_blockhash(&bank.last_blockhash())
                && transaction
                    .get_ix_signers(NONCED_TX_MARKER_IX_INDEX as usize)
                    .all(|signer| signer != &data.authority))
            .then_some(TransactionError::InstructionError(
                NONCED_TX_MARKER_IX_INDEX,
                InstructionError::MissingRequiredSignature,
            ))
        })
        .unwrap_or(err)
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
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        solana_account::AccountSharedData,
        solana_hash::Hash,
        solana_keypair::Keypair,
        solana_nonce::{state::Data as NonceData, versions::Versions as NonceVersions},
        solana_signer::Signer,
        solana_system_interface::instruction::advance_nonce_account,
        solana_transaction::{Transaction, sanitized::SanitizedTransaction},
    };

    #[test]
    fn test_get_nonce_authority_error() {
        let bank = Bank::new_for_tests(&solana_genesis_config::create_genesis_config(10).0);
        let nonce_address = solana_pubkey::Pubkey::new_unique();
        let nonce_authority = solana_pubkey::Pubkey::new_unique();
        let signer = Keypair::new();
        let durable_nonce = DurableNonce::from_blockhash(&Hash::new_unique());
        let nonce_state =
            NonceState::Initialized(NonceData::new(nonce_authority, durable_nonce, 5_000));
        let nonce_account = AccountSharedData::new_data(
            1,
            &NonceVersions::new(nonce_state),
            &solana_sdk_ids::system_program::id(),
        )
        .unwrap();
        bank.store_account(&nonce_address, &nonce_account);

        let transaction =
            SanitizedTransaction::from_transaction_for_tests(Transaction::new_signed_with_payer(
                &[advance_nonce_account(&nonce_address, &signer.pubkey())],
                Some(&signer.pubkey()),
                &[&signer],
                *durable_nonce.as_hash(),
            ));

        assert_eq!(
            get_nonce_authority_error(&bank, &transaction, TransactionError::BlockhashNotFound,),
            TransactionError::InstructionError(
                NONCED_TX_MARKER_IX_INDEX,
                InstructionError::MissingRequiredSignature,
            ),
        );

        let non_nonce_transaction =
            SanitizedTransaction::from_transaction_for_tests(Transaction::new_signed_with_payer(
                &[],
                Some(&signer.pubkey()),
                &[&signer],
                Hash::new_unique(),
            ));
        assert_eq!(
            get_nonce_authority_error(
                &bank,
                &non_nonce_transaction,
                TransactionError::BlockhashNotFound,
            ),
            TransactionError::BlockhashNotFound,
        );
    }
}
