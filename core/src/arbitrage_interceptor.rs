//! Transaction Interceptor for MEV Detection
//!
//! This module intercepts raw transactions as they arrive via TPU/QUIC,
//! providing a 200-400ms head start for MEV extraction before transactions
//! are serialized into shreds or included in blocks.
//!
//! The interceptor operates in parallel with normal transaction processing,
//! cloning incoming packets and forwarding them to the arbitrage bridge
//! for simulation and opportunity detection.

use {
    crossbeam_channel::{unbounded, Receiver, Sender},
    log::{debug, error, info, warn},
    solana_perf::packet::PacketBatch,
    solana_pubkey::Pubkey,
    solana_signature::Signature,
    solana_transaction::versioned::VersionedTransaction,
    solana_message::VersionedMessage,
    std::{
        sync::{
            atomic::{AtomicBool, AtomicU64, Ordering},
            Arc,
        },
        thread::{Builder, JoinHandle},
        time::{Duration, Instant, SystemTime, UNIX_EPOCH},
    },
};

/// Statistics for transaction interception
#[derive(Debug, Default)]
pub struct InterceptorStats {
    /// Total packets received for analysis
    pub total_packets: AtomicU64,
    /// Transactions successfully decoded
    pub decoded_transactions: AtomicU64,
    /// Transactions containing relevant programs
    pub relevant_transactions: AtomicU64,
    /// Decode errors
    pub decode_errors: AtomicU64,
    /// Transactions dropped (channel full)
    pub dropped_transactions: AtomicU64,
}

impl InterceptorStats {
    pub fn report(&self) {
        let total = self.total_packets.load(Ordering::Relaxed);
        let decoded = self.decoded_transactions.load(Ordering::Relaxed);
        let relevant = self.relevant_transactions.load(Ordering::Relaxed);
        let errors = self.decode_errors.load(Ordering::Relaxed);
        let dropped = self.dropped_transactions.load(Ordering::Relaxed);

        info!(
            "Interceptor Stats: total={}, decoded={}, relevant={}, errors={}, dropped={}",
            total, decoded, relevant, errors, dropped
        );
    }
}

/// Raw transaction data captured from TPU
#[derive(Debug, Clone)]
pub struct InterceptedTransaction {
    /// Raw transaction bytes
    pub transaction_bytes: Vec<u8>,
    /// Decoded transaction (if successful)
    pub transaction: Option<VersionedTransaction>,
    /// Transaction signature
    pub signature: Option<Signature>,
    /// Timestamp when received (microseconds since epoch)
    pub received_at_us: u64,
    /// Source address
    pub source_addr: String,
    /// Whether this was forwarded
    pub forwarded: bool,
}

/// Configuration for the transaction interceptor
#[derive(Debug, Clone)]
pub struct InterceptorConfig {
    /// Program IDs to filter for (only intercept txs involving these programs)
    pub tracked_programs: Vec<Pubkey>,
    /// Whether to intercept all transactions (ignore program filter)
    pub intercept_all: bool,
    /// Maximum queue size before dropping transactions
    pub max_queue_size: usize,
    /// Enable detailed logging
    pub verbose: bool,
}

impl Default for InterceptorConfig {
    fn default() -> Self {
        Self {
            tracked_programs: Vec::new(),
            intercept_all: false,
            max_queue_size: 10000,
            verbose: false,
        }
    }
}

/// Transaction interceptor that monitors TPU packet flow
pub struct TransactionInterceptor {
    /// Configuration
    config: InterceptorConfig,
    /// Channel to send intercepted transactions
    tx_sender: Sender<InterceptedTransaction>,
    /// Statistics
    stats: Arc<InterceptorStats>,
    /// Worker thread handle
    thread: Option<JoinHandle<()>>,
}

impl TransactionInterceptor {
    /// Create a new transaction interceptor
    ///
    /// Returns (interceptor, receiver) where receiver is used by the arbitrage bridge
    pub fn new(
        config: InterceptorConfig,
    ) -> (Self, Receiver<InterceptedTransaction>) {
        let (tx_sender, tx_receiver) = unbounded();
        let stats = Arc::new(InterceptorStats::default());

        (
            Self {
                config,
                tx_sender,
                stats,
                thread: None,
            },
            tx_receiver,
        )
    }

    /// Start intercepting packets from the TPU pipeline
    ///
    /// This is called with a receiver that gets packets from the QUIC/fetch stage
    pub fn start(
        &mut self,
        packet_receiver: Receiver<PacketBatch>,
        exit: Arc<AtomicBool>,
    ) {
        let config = self.config.clone();
        let tx_sender = self.tx_sender.clone();
        let stats = self.stats.clone();

        let thread = Builder::new()
            .name("arb-interceptor".to_string())
            .spawn(move || {
                Self::run_interceptor(config, packet_receiver, tx_sender, stats, exit);
            })
            .expect("Failed to spawn interceptor thread");

        self.thread = Some(thread);
        info!("Transaction interceptor started");
    }

    /// Main interceptor loop
    fn run_interceptor(
        config: InterceptorConfig,
        packet_receiver: Receiver<PacketBatch>,
        tx_sender: Sender<InterceptedTransaction>,
        stats: Arc<InterceptorStats>,
        exit: Arc<AtomicBool>,
    ) {
        let mut last_stats_report = Instant::now();
        const STATS_INTERVAL: Duration = Duration::from_secs(60);

        while !exit.load(Ordering::Relaxed) {
            // Receive packet batch with timeout
            let packet_batch = match packet_receiver.recv_timeout(Duration::from_millis(100)) {
                Ok(batch) => batch,
                Err(_) => {
                    // Timeout or disconnect
                    if exit.load(Ordering::Relaxed) {
                        break;
                    }
                    continue;
                }
            };

            // Process each packet in the batch
            for packet in packet_batch.iter() {
                if packet.meta().discard() {
                    continue;
                }

                stats.total_packets.fetch_add(1, Ordering::Relaxed);

                // Extract raw transaction data
                let packet_data = match packet.data(..) {
                    Some(data) => data,
                    None => continue,
                };

                let received_at_us = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_micros() as u64;

                let forwarded = packet.meta().forwarded();
                let source_addr = format!("{}:{}", packet.meta().addr, packet.meta().port);

                // Try to decode transaction
                let (transaction, signature) = match Self::decode_transaction(packet_data) {
                    Ok((tx, sig)) => {
                        stats.decoded_transactions.fetch_add(1, Ordering::Relaxed);
                        (Some(tx), Some(sig))
                    }
                    Err(e) => {
                        stats.decode_errors.fetch_add(1, Ordering::Relaxed);
                        if config.verbose {
                            debug!("Failed to decode transaction: {}", e);
                        }
                        (None, None)
                    }
                };

                // Filter by program if configured
                let should_forward = if config.intercept_all {
                    true
                } else if let Some(tx) = &transaction {
                    Self::contains_tracked_program(tx, &config.tracked_programs)
                } else {
                    false
                };

                if !should_forward {
                    continue;
                }

                stats.relevant_transactions.fetch_add(1, Ordering::Relaxed);

                // Create intercepted transaction
                let intercepted = InterceptedTransaction {
                    transaction_bytes: packet_data.to_vec(),
                    transaction,
                    signature,
                    received_at_us,
                    source_addr,
                    forwarded,
                };

                // Send to arbitrage bridge
                // Use try_send to avoid blocking if bridge is slow
                if tx_sender.try_send(intercepted).is_err() {
                    stats.dropped_transactions.fetch_add(1, Ordering::Relaxed);
                    if config.verbose {
                        warn!("Dropped transaction - channel full");
                    }
                }
            }

            // Periodic stats reporting
            if last_stats_report.elapsed() >= STATS_INTERVAL {
                stats.report();
                last_stats_report = Instant::now();
            }
        }

        info!("Transaction interceptor shutting down");
        stats.report();
    }

    /// Decode a transaction from raw bytes
    fn decode_transaction(data: &[u8]) -> Result<(VersionedTransaction, Signature), String> {
        let transaction: VersionedTransaction = bincode::deserialize(data)
            .map_err(|e| format!("Failed to deserialize transaction: {}", e))?;

        let signature = transaction.signatures.get(0)
            .copied()
            .ok_or_else(|| "Transaction has no signatures".to_string())?;

        Ok((transaction, signature))
    }

    /// Check if transaction involves any of the tracked programs
    fn contains_tracked_program(tx: &VersionedTransaction, tracked_programs: &[Pubkey]) -> bool {
        if tracked_programs.is_empty() {
            return true;
        }

        // Get all program IDs involved in the transaction
        let account_keys = match &tx.message {
            VersionedMessage::Legacy(msg) => &msg.account_keys,
            VersionedMessage::V0(msg) => &msg.account_keys,
        };

        // Check if any instruction involves a tracked program
        let instructions = match &tx.message {
            VersionedMessage::Legacy(msg) => &msg.instructions,
            VersionedMessage::V0(msg) => &msg.instructions,
        };

        for instruction in instructions {
            if let Some(program_id) = account_keys.get(instruction.program_id_index as usize) {
                if tracked_programs.contains(program_id) {
                    return true;
                }
            }
        }

        false
    }

    /// Get statistics
    pub fn stats(&self) -> &Arc<InterceptorStats> {
        &self.stats
    }

    /// Join the worker thread
    pub fn join(self) -> std::thread::Result<()> {
        if let Some(thread) = self.thread {
            thread.join()
        } else {
            Ok(())
        }
    }
}

/// Helper to create a tee channel that duplicates packets
///
/// This is used to intercept packets without disrupting the normal flow
pub fn create_packet_tee(
    input: Receiver<PacketBatch>,
    output: Sender<PacketBatch>,
    intercept: Sender<PacketBatch>,
    exit: Arc<AtomicBool>,
) -> JoinHandle<()> {
    Builder::new()
        .name("packet-tee".to_string())
        .spawn(move || {
            while !exit.load(Ordering::Relaxed) {
                match input.recv_timeout(Duration::from_millis(100)) {
                    Ok(batch) => {
                        // Clone for interceptor (best effort, don't block)
                        let _ = intercept.try_send(batch.clone());
                        
                        // Forward to normal pipeline (must succeed)
                        if output.send(batch).is_err() {
                            error!("Output channel closed, shutting down packet tee");
                            break;
                        }
                    }
                    Err(_) => {
                        if exit.load(Ordering::Relaxed) {
                            break;
                        }
                        continue;
                    }
                }
            }
            info!("Packet tee shutting down");
        })
        .expect("Failed to spawn packet tee thread")
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_keypair::Keypair;
    use solana_system_transaction as system_transaction;

    #[test]
    fn test_decode_transaction() {
        let from = Keypair::new();
        let to = Pubkey::new_unique();
        let tx = system_transaction::transfer(&from, &to, 1000000, Hash::default());
        let versioned: VersionedTransaction = tx.into();
        
        let bytes = bincode::serialize(&versioned).unwrap();
        let (decoded, sig) = TransactionInterceptor::decode_transaction(&bytes).unwrap();
        
        assert_eq!(decoded.signatures[0], sig);
    }

    #[test]
    fn test_program_filtering() {
        let from = Keypair::new();
        let to = Pubkey::new_unique();
        let tx = system_transaction::transfer(&from, &to, 1000000, Hash::default());
        let versioned: VersionedTransaction = tx.into();

        let system_program = solana_program::system_program::id();
        let tracked = vec![system_program];

        assert!(TransactionInterceptor::contains_tracked_program(&versioned, &tracked));

        let other_program = Pubkey::new_unique();
        let tracked = vec![other_program];
        assert!(!TransactionInterceptor::contains_tracked_program(&versioned, &tracked));
    }
}

