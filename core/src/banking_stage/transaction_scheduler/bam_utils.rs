use solana_transaction_error::TransactionError;
use {
    crate::banking_stage::immutable_deserialized_packet::DeserializedPacketError,
    jito_protos::proto::bam_types::{DeserializationErrorReason, TransactionErrorReason},
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

pub fn convert_deserialize_error_to_proto(
    err: &DeserializedPacketError,
) -> DeserializationErrorReason {
    match err {
        DeserializedPacketError::ShortVecError(_) => DeserializationErrorReason::BincodeError,
        DeserializedPacketError::DeserializationError(_) => {
            DeserializationErrorReason::BincodeError
        }
        DeserializedPacketError::SignatureOverflowed(_) => {
            DeserializationErrorReason::SignatureOverflowed
        }
        DeserializedPacketError::SanitizeError(_) => DeserializationErrorReason::SanitizeError,
        DeserializedPacketError::PrioritizationFailure => {
            DeserializationErrorReason::PrioritizationFailure
        }
        DeserializedPacketError::VoteTransactionError => {
            DeserializationErrorReason::VoteTransactionFailure
        }
    }
}
