#![cfg(feature = "full")]

use {
    anchor_lang::error::Error, serde::Deserialize, solana_program::pubkey::Pubkey,
    solana_sdk::transaction::TransactionError, std::time::Duration, thiserror::Error,
};

#[derive(Error, Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum BundleExecutionError {
    #[error("PoH max height reached in the middle of a bundle.")]
    PohMaxHeightError,

    #[error("A transaction in the bundle failed with - {0}")]
    TransactionFailure(#[from] TransactionError),

    #[error("The bundle exceeds the cost model")]
    ExceedsCostModel,

    #[error("Tip error {0}")]
    TipError(#[from] TipPaymentError),

    #[error("Shutdown triggered")]
    Shutdown,

    #[error("The time spent retrying bundles exceeded the allowed time {0:?}")]
    MaxRetriesExceeded(Duration),

    #[error("Error locking bundle because the transaction is malformed")]
    LockError,
}

#[derive(Error, Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TipPaymentError {
    #[error("account is missing from bank: {0}")]
    AccountMissing(Pubkey),

    #[error("MEV program is non-existent")]
    ProgramNonExistent(Pubkey),

    #[error("Anchor error: {0}")]
    AnchorError(String),
}

impl From<anchor_lang::error::Error> for TipPaymentError {
    fn from(anchor_err: Error) -> Self {
        match anchor_err {
            Error::AnchorError(e) => Self::AnchorError(e.error_msg),
            Error::ProgramError(e) => Self::AnchorError(e.to_string()),
        }
    }
}
