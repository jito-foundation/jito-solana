/// Arbitrage integration for the banking stage
///
/// This module integrates the shred-streamer arbitrage detection system
/// directly into the validator for PRE-EXECUTION MEV detection:
/// 
/// 1. RAW TRANSACTION INTERCEPTION (TPU/QUIC) - 200-400ms edge
/// 2. TRANSACTION SIMULATION against current bank state
/// 3. SIMULATED ACCOUNT UPDATES extraction
/// 4. MEV OPPORTUNITY DETECTION and execution
///
/// The key innovation: simulate transactions BEFORE they execute to predict
/// pool state changes and capture arbitrage opportunities.

use solana_runtime::bank::Bank;

#[cfg(feature = "arbitrage-integration")]
use {
    arbitrage::{ValidatorArbitrageBridge, AccountUpdate, RawTransaction},
    crossbeam_channel::Receiver,
    log::{debug, info, warn},
    solana_pubkey::Pubkey,
    solana_runtime::bank_forks::BankForks,
    solana_sdk::{signature::Signature, transaction::VersionedTransaction},
    std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc, RwLock,
    },
    tokio::runtime::Handle,
};

#[cfg(not(feature = "arbitrage-integration"))]
use {
    solana_runtime::bank_forks::BankForks,
    std::marker::PhantomData,
    std::sync::{Arc, RwLock},
};

/// Integration point for arbitrage detection in the banking stage
#[derive(Clone)]
pub struct ArbitrageIntegration {
    #[cfg(feature = "arbitrage-integration")]
    bridge: Option<Arc<ValidatorArbitrageBridge>>,
    #[cfg(feature = "arbitrage-integration")]
    runtime_handle: Option<Handle>,
    #[cfg(feature = "arbitrage-integration")]
    enabled: bool,
    #[cfg(feature = "arbitrage-integration")]
    raw_tx_enabled: bool,
    #[cfg(not(feature = "arbitrage-integration"))]
    _phantom: PhantomData<()>,
}

/// Handle for the raw transaction processing thread
#[cfg(feature = "arbitrage-integration")]
pub struct RawTransactionProcessor {
    thread: Option<std::thread::JoinHandle<()>>,
}

#[cfg(feature = "arbitrage-integration")]
impl Drop for RawTransactionProcessor {
    fn drop(&mut self) {
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl ArbitrageIntegration {
    /// Create a new arbitrage integration
    /// 
    /// If config_path is None, arbitrage integration is disabled.
    /// If the arbitrage-integration feature is not enabled, this is a no-op.
    pub fn new(config_path: Option<&str>) -> Self {
        Self::new_with_raw_tx(config_path, true)
    }

    /// Create a new arbitrage integration with optional raw transaction processing
    /// 
    /// If enable_raw_tx is true, the integration will also process raw transactions
    /// from the TPU for pre-execution MEV detection.
    pub fn new_with_raw_tx(config_path: Option<&str>, enable_raw_tx: bool) -> Self {
        Self::new_with_bank_forks(config_path, enable_raw_tx, None)
    }

    /// Create integration with bank_forks for transaction simulation
    #[cfg(feature = "arbitrage-integration")]
    pub fn new_with_bank_forks(
        config_path: Option<&str>,
        enable_raw_tx: bool,
        bank_forks: Option<Arc<RwLock<BankForks>>>,
    ) -> Self {
        let mut config_allows_raw_tx = false;
        let (bridge, runtime_handle) = if let Some(path) = config_path {
            info!("Initializing arbitrage integration from config: {}", path);
            info!("Raw transaction processing: {}", if enable_raw_tx { "ENABLED" } else { "DISABLED" });
            
            // Create a Tokio runtime for async operations
            let runtime = tokio::runtime::Runtime::new()
                .expect("Failed to create tokio runtime for arbitrage");
            let handle = runtime.handle().clone();
            
            // Initialize the bridge on the runtime
            let bridge_result = handle.block_on(async {
                let mut bridge = ValidatorArbitrageBridge::new(path).await?;
                
                // Set bank_forks if provided (enables transaction simulation)
                if let Some(forks) = bank_forks {
                    bridge.set_bank_forks(forks);
                    info!("Bank forks configured - transaction simulation ENABLED");
                } else {
                    warn!("Bank forks not provided - transaction simulation DISABLED");
                }
                config_allows_raw_tx = bridge.raw_tx_processing_enabled();

                Ok::<_, anyhow::Error>(bridge)
            });
            
            match bridge_result {
                Ok(bridge) => {
                    info!("Arbitrage integration initialized successfully");
                    // Leak the runtime to keep it alive for the duration of the validator
                    std::mem::forget(runtime);
                    (Some(Arc::new(bridge)), Some(handle))
                }
                Err(e) => {
                    warn!("Failed to initialize arbitrage integration: {}", e);
                    (None, None)
                }
            }
        } else {
            (None, None)
        };
        
        let enabled = bridge.is_some();
        let raw_tx_enabled = enabled && enable_raw_tx && config_allows_raw_tx;

        if enabled && enable_raw_tx && !config_allows_raw_tx {
            info!(
                "Raw transaction processing disabled by config (enable_raw_tx_processing = false)"
            );
        }
        
        Self {
            bridge,
            runtime_handle,
            enabled,
            raw_tx_enabled,
        }
    }

    #[cfg(not(feature = "arbitrage-integration"))]
    pub fn new_with_bank_forks(
        config_path: Option<&str>,
        enable_raw_tx: bool,
        _bank_forks: Option<Arc<RwLock<BankForks>>>,
    ) -> Self {
        Self::new_with_raw_tx(config_path, enable_raw_tx);
        
        #[cfg(not(feature = "arbitrage-integration"))]
        {
            let _ = (config_path, enable_raw_tx); // Suppress unused variable warnings
            Self {
                _phantom: PhantomData,
            }
        }
    }
    
    /// Start processing raw transactions from the interceptor
    /// 
    /// This spawns a background thread that receives raw transactions from TPU/QUIC
    /// and processes them for MEV opportunities.
    /// 
    /// Returns a processor handle that will shut down when dropped.
    #[cfg(feature = "arbitrage-integration")]
    pub fn start_raw_transaction_processor(
        &self,
        raw_tx_receiver: Receiver<crate::arbitrage_interceptor::InterceptedTransaction>,
        exit: Arc<AtomicBool>,
    ) -> RawTransactionProcessor {
        if !self.raw_tx_enabled {
            info!("Raw transaction processing is disabled");
            return RawTransactionProcessor { thread: None };
        }

        let Some(bridge) = self.bridge.clone() else {
            warn!("Cannot start raw transaction processor: bridge not initialized");
            return RawTransactionProcessor { thread: None };
        };

        let Some(runtime_handle) = self.runtime_handle.clone() else {
            warn!("Cannot start raw transaction processor: runtime handle not available");
            return RawTransactionProcessor { thread: None };
        };

        info!("Starting raw transaction processor for MEV detection");

        let thread = std::thread::Builder::new()
            .name("arb-raw-tx-processor".to_string())
            .spawn(move || {
                Self::run_raw_transaction_processor(
                    bridge,
                    runtime_handle,
                    raw_tx_receiver,
                    exit,
                );
            })
            .expect("Failed to spawn raw transaction processor");

        RawTransactionProcessor {
            thread: Some(thread),
        }
    }

    /// Main loop for raw transaction processing
    #[cfg(feature = "arbitrage-integration")]
    fn run_raw_transaction_processor(
        bridge: Arc<ValidatorArbitrageBridge>,
        runtime_handle: Handle,
        raw_tx_receiver: Receiver<crate::arbitrage_interceptor::InterceptedTransaction>,
        exit: Arc<AtomicBool>,
    ) {
        use std::time::Duration;

        const BATCH_SIZE: usize = 100;
        const BATCH_TIMEOUT: Duration = Duration::from_micros(500); // Process in 0.5ms batches

        let mut batch = Vec::with_capacity(BATCH_SIZE);
        let mut last_process = std::time::Instant::now();

        while !exit.load(Ordering::Relaxed) {
            // Receive with timeout
            match raw_tx_receiver.recv_timeout(Duration::from_millis(10)) {
                Ok(intercepted) => {
                    // Only process if we successfully decoded the transaction
                    if let Some(transaction) = intercepted.transaction {
                        if let Some(signature) = intercepted.signature {
                            batch.push(RawTransaction {
                                transaction,
                                signature,
                                received_at_us: intercepted.received_at_us,
                                forwarded: intercepted.forwarded,
                            });
                        }
                    }

                    // Process batch if full or timeout reached
                    if batch.len() >= BATCH_SIZE || last_process.elapsed() >= BATCH_TIMEOUT {
                        if !batch.is_empty() {
                            let txs = std::mem::replace(&mut batch, Vec::with_capacity(BATCH_SIZE));
                            let bridge_clone = bridge.clone();
                            runtime_handle.spawn(async move {
                                if let Err(e) = bridge_clone.process_raw_transactions(txs).await {
                                    warn!("Failed to process raw transactions: {}", e);
                                }
                            });
                            last_process = std::time::Instant::now();
                        }
                    }
                }
                Err(_) => {
                    // Timeout or disconnect
                    if !batch.is_empty() {
                        let txs = std::mem::replace(&mut batch, Vec::with_capacity(BATCH_SIZE));
                        let bridge_clone = bridge.clone();
                        runtime_handle.spawn(async move {
                            if let Err(e) = bridge_clone.process_raw_transactions(txs).await {
                                warn!("Failed to process raw transactions: {}", e);
                            }
                        });
                        last_process = std::time::Instant::now();
                    }
                    
                    if exit.load(Ordering::Relaxed) {
                        break;
                    }
                }
            }
        }

        // Process any remaining transactions
        if !batch.is_empty() {
            runtime_handle.block_on(async move {
                if let Err(e) = bridge.process_raw_transactions(batch).await {
                    warn!("Failed to process final raw transactions: {}", e);
                }
            });
        }

        info!("Raw transaction processor shutting down");
        
        // Report statistics
        bridge.get_raw_tx_stats().report();
    }

    /// Check if raw transaction processing is enabled
    #[inline]
    pub fn raw_tx_enabled(&self) -> bool {
        #[cfg(feature = "arbitrage-integration")]
        {
            self.raw_tx_enabled
        }
        #[cfg(not(feature = "arbitrage-integration"))]
        {
            false
        }
    }
    
    /// Check if arbitrage integration is enabled
    #[inline]
    pub fn is_enabled(&self) -> bool {
        #[cfg(feature = "arbitrage-integration")]
        {
            self.enabled
        }
        #[cfg(not(feature = "arbitrage-integration"))]
        {
            false
        }
    }
    
    /// Process a transaction and extract relevant account updates
    /// 
    /// This is called after a transaction is executed. It extracts account
    /// updates from the transaction and feeds them to the arbitrage system.
    /// 
    /// This method returns immediately and processes updates asynchronously.
    #[inline]
    pub fn process_transaction<T>(
        &self,
        bank: &Bank,
        tx: &T,
        slot: u64,
    ) where
        T: solana_runtime_transaction::transaction_with_meta::TransactionWithMeta,
    {
        #[cfg(feature = "arbitrage-integration")]
        {
            if !self.enabled {
                return;
            }
            
            let Some(bridge) = &self.bridge else {
                return;
            };
            
            // Quick filter: check if transaction touches tracked programs
            if !self.transaction_is_relevant(tx, bridge) {
                return;
            }
            
            // Extract account updates from the transaction
            let updates = self.extract_account_updates(bank, tx, slot, bridge);
            
            if updates.is_empty() {
                return;
            }
            
            debug!("Found {} relevant account updates for slot {}", updates.len(), slot);
            
            // Process updates asynchronously
            let bridge = bridge.clone();
            if let Some(handle) = &self.runtime_handle {
                handle.spawn(async move {
                    if let Err(e) = bridge.process_account_updates(slot, updates).await {
                        warn!("Failed to process account updates: {}", e);
                    }
                });
            }
        }
        
        #[cfg(not(feature = "arbitrage-integration"))]
        {
            let _ = (bank, tx, slot); // Suppress unused variable warnings
        }
    }
    
    #[cfg(feature = "arbitrage-integration")]
    fn transaction_is_relevant<T>(
        &self,
        tx: &T,
        bridge: &ValidatorArbitrageBridge,
    ) -> bool 
    where
        T: solana_runtime_transaction::transaction_with_meta::TransactionWithMeta,
    {
        // Check if any instruction calls tracked programs (Meteora, Pumpfun, etc.)
        use solana_svm_transaction::{svm_message::SVMMessage, svm_transaction::SVMTransaction};
        
        let account_keys = tx.account_keys();
        let program_indices = tx.program_instructions_iter()
            .map(|(_, ix)| ix.program_id_index as usize)
            .collect::<std::collections::HashSet<_>>();
        
        for prog_idx in program_indices {
            if prog_idx < account_keys.len() {
                if bridge.is_tracked_program(&account_keys[prog_idx]) {
                    return true;
                }
            }
        }
        false
    }
    
    #[cfg(feature = "arbitrage-integration")]
    fn extract_account_updates<T>(
        &self,
        bank: &Bank,
        tx: &T,
        slot: u64,
        bridge: &ValidatorArbitrageBridge,
    ) -> Vec<AccountUpdate> 
    where
        T: solana_runtime_transaction::transaction_with_meta::TransactionWithMeta,
    {
        use solana_account::ReadableAccount;
        use solana_svm_transaction::svm_message::SVMMessage;
        
        let mut updates = Vec::new();
        
        // Get all account keys from the transaction
        let account_keys = tx.account_keys();
        
        // For each writable account, check if it's tracked
        for index in 0..account_keys.len() {
            // Only process writable accounts
            if !tx.is_writable(index) {
                continue;
            }
            
            let account_key = &account_keys[index];
            
            // Check if this account is tracked
            if !bridge.is_tracked_account(account_key) {
                continue;
            }
            
            // Get the account from the bank
            if let Some(account) = bank.get_account(account_key) {
                updates.push(AccountUpdate {
                    pubkey: *account_key,
                    data: account.data().to_vec(),
                    slot,
                    lamports: account.lamports(),
                    owner: *account.owner(),
                });
            }
        }
        
        updates
    }
}

impl Default for ArbitrageIntegration {
    fn default() -> Self {
        Self::new(None)
    }
}

// Ensure ArbitrageIntegration is Send + Sync
unsafe impl Send for ArbitrageIntegration {}
unsafe impl Sync for ArbitrageIntegration {}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_arbitrage_integration_disabled() {
        let integration = ArbitrageIntegration::new(None);
        assert!(!integration.is_enabled());
    }
    
    #[test]
    fn test_arbitrage_integration_default() {
        let integration = ArbitrageIntegration::default();
        assert!(!integration.is_enabled());
    }
}

