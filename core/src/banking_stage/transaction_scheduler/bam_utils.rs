use {
    crate::banking_stage::{
        committer::CommitTransactionDetails,
        consumer::ProcessTransactionBatchOutput,
        scheduler_messages::{NotCommittedReason, TransactionResult},
    },
    jito_protos::proto::bam_types::{TransactionCommittedResult, TransactionErrorReason},
    solana_transaction_error::TransactionError,
};

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
                feepayer_balance_lamports: *fee_payer_post_balance,
                loaded_accounts_data_size: *loaded_accounts_data_size,
                execution_success: result.is_ok(),
            }),
            CommitTransactionDetails::NotCommitted(err) => {
                TransactionResult::NotCommitted(NotCommittedReason::Error(err.clone()))
            }
        })
        .collect()
}
