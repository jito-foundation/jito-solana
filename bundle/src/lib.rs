use {
    crate::bundle_execution::LoadAndExecuteBundleError,
    anchor_lang::error::Error,
    itertools::Itertools,
    serde::{Deserialize, Serialize},
    sha2::{Digest, Sha256},
    solana_poh::poh_recorder::PohRecorderError,
    solana_runtime_transaction::runtime_transaction::RuntimeTransaction,
    solana_sdk::{pubkey::Pubkey, transaction::SanitizedTransaction},
    thiserror::Error,
};

pub mod bundle_execution;

#[derive(Error, Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum TipError {
    #[error("account is missing from bank: {0}")]
    AccountMissing(Pubkey),

    #[error("Anchor error: {0}")]
    AnchorError(String),

    #[error("Lock error")]
    LockError,

    #[error("Error executing initialize programs")]
    InitializeProgramsError,

    #[error("Error cranking tip programs")]
    CrankTipError,
}

impl From<anchor_lang::error::Error> for TipError {
    fn from(anchor_err: Error) -> Self {
        match anchor_err {
            Error::AnchorError(e) => Self::AnchorError(e.error_msg),
            Error::ProgramError(e) => Self::AnchorError(e.to_string()),
        }
    }
}

pub type BundleExecutionResult<T> = Result<T, BundleExecutionError>;

#[derive(Error, Debug, Clone)]
pub enum BundleExecutionError {
    #[error("The bank has hit the max allotted time for processing transactions")]
    BankProcessingTimeLimitReached,

    #[error("The bundle exceeds the cost model")]
    ExceedsCostModel,

    #[error("Runtime error while executing the bundle: {0}")]
    TransactionFailure(#[from] LoadAndExecuteBundleError),

    #[error("Error locking bundle because a transaction is malformed")]
    LockError,

    #[error("PoH record error: {0}")]
    PohRecordError(#[from] PohRecorderError),

    #[error("Tip payment error {0}")]
    TipError(#[from] TipError),
}

#[derive(Debug)]
pub struct SanitizedBundle {
    pub transactions: Vec<RuntimeTransaction<SanitizedTransaction>>,
    pub bundle_id: String,
}

pub fn derive_bundle_id_from_sanitized_transactions(
    transactions: &[RuntimeTransaction<SanitizedTransaction>],
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(transactions.iter().map(|tx| tx.signature()).join(","));
    format!("{:x}", hasher.finalize())
}
