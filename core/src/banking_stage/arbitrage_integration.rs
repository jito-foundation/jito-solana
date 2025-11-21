/// Arbitrage integration for the banking stage
///
/// This module integrates the shred-streamer arbitrage detection system
/// directly into the validator's banking stage. It simulates/extracts account
/// updates from transaction execution and feeds them to the arbitrage system.

use solana_runtime::bank::Bank;

#[cfg(feature = "arbitrage-integration")]
use {
    arbitrage::{ValidatorArbitrageBridge, AccountUpdate},
    log::{debug, info, warn},
    solana_pubkey::Pubkey,
    std::sync::Arc,
    tokio::runtime::Handle,
};

#[cfg(not(feature = "arbitrage-integration"))]
use std::marker::PhantomData;

/// Integration point for arbitrage detection in the banking stage
#[derive(Clone)]
pub struct ArbitrageIntegration {
    #[cfg(feature = "arbitrage-integration")]
    bridge: Option<Arc<ValidatorArbitrageBridge>>,
    #[cfg(feature = "arbitrage-integration")]
    runtime_handle: Option<Handle>,
    #[cfg(feature = "arbitrage-integration")]
    enabled: bool,
    #[cfg(not(feature = "arbitrage-integration"))]
    _phantom: PhantomData<()>,
}

impl ArbitrageIntegration {
    /// Create a new arbitrage integration
    /// 
    /// If config_path is None, arbitrage integration is disabled.
    /// If the arbitrage-integration feature is not enabled, this is a no-op.
    pub fn new(config_path: Option<&str>) -> Self {
        #[cfg(feature = "arbitrage-integration")]
        {
            let (bridge, runtime_handle) = if let Some(path) = config_path {
                info!("Initializing arbitrage integration from config: {}", path);
                
                // Create a Tokio runtime for async operations
                let runtime = tokio::runtime::Runtime::new()
                    .expect("Failed to create tokio runtime for arbitrage");
                let handle = runtime.handle().clone();
                
                // Initialize the bridge on the runtime
                let bridge_result = handle.block_on(async {
                    ValidatorArbitrageBridge::new(path).await
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
            
            Self {
                bridge,
                runtime_handle,
                enabled,
            }
        }
        
        #[cfg(not(feature = "arbitrage-integration"))]
        {
            let _ = config_path; // Suppress unused variable warning
            Self {
                _phantom: PhantomData,
            }
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

