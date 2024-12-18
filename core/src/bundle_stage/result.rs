use {
    crate::{
        bundle_stage::bundle_account_locker::BundleAccountLockerError, tip_manager::TipPaymentError,
    },
    anchor_lang::error::Error,
    solana_bundle::bundle_execution::LoadAndExecuteBundleError,
    solana_poh::poh_recorder::PohRecorderError,
    thiserror::Error,
};

pub type BundleExecutionResult<T> = Result<T, BundleExecutionError>;

#[derive(Error, Debug, Clone)]
pub enum BundleExecutionError {
    #[error("PoH record error: {0}")]
    PohRecordError(#[from] PohRecorderError),

    #[error("Bank is done processing")]
    BankProcessingDone,

    #[error("Execution error: {0}")]
    ExecutionError(#[from] LoadAndExecuteBundleError),

    #[error("The bundle exceeds the cost model")]
    ExceedsCostModel,

    #[error("Tip error {0}")]
    TipError(#[from] TipPaymentError),

    #[error("Error locking bundle")]
    LockError(#[from] BundleAccountLockerError),
}

impl From<anchor_lang::error::Error> for TipPaymentError {
    fn from(anchor_err: Error) -> Self {
        match anchor_err {
            Error::AnchorError(e) => Self::AnchorError(e.error_msg),
            Error::ProgramError(e) => Self::AnchorError(e.to_string()),
        }
    }
}
