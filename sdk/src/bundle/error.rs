use {
    anchor_lang::error::Error, serde::Deserialize, solana_program::pubkey::Pubkey,
    solana_sdk::transaction::TransactionError, thiserror::Error,
};

#[derive(Error, Debug, Clone, Serialize, Deserialize)]
pub enum BundleExecutionError {
    #[error("Bank is not processing transactions.")]
    BankNotProcessingTransactions,

    #[error("Bundle is invalid")]
    InvalidBundle,

    #[error("PoH max height reached in the middle of a bundle.")]
    PohMaxHeightError,

    #[error("No records to record to PoH")]
    NoRecordsToRecord,

    #[error("A transaction in the bundle failed")]
    TransactionFailure(#[from] TransactionError),

    #[error("The bundle exceeds the cost model")]
    ExceedsCostModel,

    #[error("The validator is not a leader yet, dropping")]
    NotLeaderYet,

    #[error("Tip error {0}")]
    TipError(#[from] TipPaymentError),
}

#[derive(Error, Debug, Clone, Serialize, Deserialize)]
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
