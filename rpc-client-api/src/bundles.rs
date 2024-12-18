//! Use a separate file for Jito related code to minimize upstream merge conflicts.

use {
    crate::config::RpcSimulateTransactionAccountsConfig,
    solana_account_decoder_client_types::UiAccount,
    solana_bundle::{bundle_execution::LoadAndExecuteBundleError, BundleExecutionError},
    solana_sdk::{
        clock::Slot,
        commitment_config::{CommitmentConfig, CommitmentLevel},
        signature::Signature,
        transaction::TransactionError,
    },
    solana_svm::transaction_processing_result::ProcessedTransaction,
    solana_transaction_status_client_types::{UiTransactionEncoding, UiTransactionReturnData},
    thiserror::Error,
};

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub enum RpcBundleSimulationSummary {
    /// error and offending transaction signature if applicable
    Failed {
        error: RpcBundleExecutionError,
        tx_signature: Option<String>,
    },
    Succeeded,
}

#[derive(Error, Debug, Clone, Serialize, Deserialize)]
pub enum RpcBundleExecutionError {
    #[error("The bank has hit the max allotted time for processing transactions")]
    BankProcessingTimeLimitReached,

    #[error("Error locking bundle because a transaction is malformed")]
    BundleLockError,

    #[error("Bundle execution timed out")]
    BundleExecutionTimeout,

    #[error("The bundle exceeds the cost model")]
    ExceedsCostModel,

    #[error("Invalid pre or post accounts")]
    InvalidPreOrPostAccounts,

    #[error("PoH record error: {0}")]
    PohRecordError(String),

    #[error("Tip payment error: {0}")]
    TipError(String),

    #[error("A transaction in the bundle failed to execute: [signature={0}, error={1}]")]
    TransactionFailure(Signature, String),
}

impl From<BundleExecutionError> for RpcBundleExecutionError {
    fn from(bundle_execution_error: BundleExecutionError) -> Self {
        match bundle_execution_error {
            BundleExecutionError::BankProcessingTimeLimitReached => {
                Self::BankProcessingTimeLimitReached
            }
            BundleExecutionError::ExceedsCostModel => Self::ExceedsCostModel,
            BundleExecutionError::TransactionFailure(load_and_execute_bundle_error) => {
                match load_and_execute_bundle_error {
                    LoadAndExecuteBundleError::ProcessingTimeExceeded(_) => {
                        Self::BundleExecutionTimeout
                    }
                    LoadAndExecuteBundleError::LockError {
                        signature,
                        transaction_error,
                    } => Self::TransactionFailure(signature, transaction_error.to_string()),
                    LoadAndExecuteBundleError::TransactionError {
                        signature,
                        execution_result,
                    } => match *execution_result {
                        Ok(ProcessedTransaction::Executed(executed_transaction)) => {
                            let err_msg =
                                if let Err(e) = executed_transaction.execution_details.status {
                                    e.to_string()
                                } else {
                                    "Unknown error".to_string()
                                };
                            Self::TransactionFailure(signature, err_msg)
                        }
                        Ok(ProcessedTransaction::FeesOnly(fees_only)) => {
                            Self::TransactionFailure(signature, fees_only.load_error.to_string())
                        }
                        Err(e) => Self::TransactionFailure(signature, e.to_string()),
                    },
                    LoadAndExecuteBundleError::InvalidPreOrPostAccounts => {
                        Self::InvalidPreOrPostAccounts
                    }
                }
            }
            BundleExecutionError::LockError => Self::BundleLockError,
            BundleExecutionError::PohRecordError(e) => Self::PohRecordError(e.to_string()),
            BundleExecutionError::TipError(e) => Self::TipError(e.to_string()),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct RpcSimulateBundleResult {
    pub summary: RpcBundleSimulationSummary,
    pub transaction_results: Vec<RpcSimulateBundleTransactionResult>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct RpcSimulateBundleTransactionResult {
    pub err: Option<TransactionError>,
    pub logs: Option<Vec<String>>,
    pub pre_execution_accounts: Option<Vec<UiAccount>>,
    pub post_execution_accounts: Option<Vec<UiAccount>>,
    pub units_consumed: Option<u64>,
    pub return_data: Option<UiTransactionReturnData>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RpcSimulateBundleConfig {
    /// Gives the state of accounts pre/post transaction execution.
    /// The length of each of these must be equal to the number transactions.   
    pub pre_execution_accounts_configs: Vec<Option<RpcSimulateTransactionAccountsConfig>>,
    pub post_execution_accounts_configs: Vec<Option<RpcSimulateTransactionAccountsConfig>>,

    /// Specifies the encoding scheme of the contained transactions.
    pub transaction_encoding: Option<UiTransactionEncoding>,

    /// Specifies the bank to run simulation against.
    pub simulation_bank: Option<SimulationSlotConfig>,

    /// Opt to skip sig-verify for faster performance.
    #[serde(default)]
    pub skip_sig_verify: bool,

    /// Replace recent blockhash to simulate old transactions without resigning.
    #[serde(default)]
    pub replace_recent_blockhash: bool,
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug)]
#[serde(rename_all = "camelCase")]
pub enum SimulationSlotConfig {
    /// Simulate on top of bank with the provided commitment.
    Commitment(CommitmentConfig),

    /// Simulate on the provided slot's bank.
    Slot(Slot),

    /// Simulates on top of the RPC's highest slot's bank i.e. the working bank.
    Tip,
}

impl Default for SimulationSlotConfig {
    fn default() -> Self {
        Self::Commitment(CommitmentConfig {
            commitment: CommitmentLevel::Confirmed,
        })
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RpcBundleRequest {
    pub encoded_transactions: Vec<String>,
}
