use {
    log::*,
    solana_commitment_config::CommitmentConfig,
    solana_tps_client::TpsClient,
    std::{
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc, RwLock,
        },
        thread::sleep,
        time::{Duration, Instant},
    },
};

#[derive(Default, Clone, Copy, Debug)]
pub struct SampleStats {
    /// Maximum TPS reported by this node
    pub tps: f32,
    /// Total transactions reported by this node
    pub txs: u64,
}

pub fn sample_txs<T>(
    exit_signal: Arc<AtomicBool>,
    sample_stats: &Arc<RwLock<Vec<(String, SampleStats)>>>,
    sample_period: u64,
    client: &Arc<T>,
) where
    T: TpsClient + ?Sized,
{
    let mut max_tps = 0.0;
    let mut total_txs = 0;
    
    // OPTIMIZATION: Robust Initialization
    // Instead of crashing with .expect(), we retry until we get an initial value.
    let initial_txs = loop {
        if exit_signal.load(Ordering::Relaxed) {
            return;
        }
        
        match client.get_transaction_count_with_commitment(CommitmentConfig::processed()) {
            Ok(count) => break count,
            Err(e) => {
                warn!("Failed to get initial transaction count: {e:?}. Retrying in 1s...");
                sleep(Duration::from_secs(1));
            }
        }
    };

    let mut last_txs = initial_txs;
    let start_time = Instant::now();
    let mut last_sample_time = start_time;

    loop {
        // Check exit signal *before* sleeping to respond faster
        if exit_signal.load(Ordering::Relaxed) {
            // Record final stats before exiting
            let stats = SampleStats {
                tps: max_tps,
                txs: total_txs,
            };
            
            // Gracefully handle lock poisoning if it happens
            if let Ok(mut guard) = sample_stats.write() {
                guard.push((client.addr(), stats));
            } else {
                error!("Failed to acquire lock for sample_stats update");
            }
            return;
        }

        sleep(Duration::from_secs(sample_period));

        // Capture time *immediately* after sleep
        let now = Instant::now();
        let elapsed = now.duration_since(last_sample_time);
        let total_elapsed = start_time.elapsed();

        // Fetch current transaction count
        let txs = match client.get_transaction_count_with_commitment(CommitmentConfig::processed()) {
            Ok(tx_count) => tx_count,
            Err(e) => {
                // Don't update 'last_sample_time' here so the next successful 
                // iteration calculates TPS over the total extended period, 
                // smoothing out the network failure gap.
                info!("Couldn't get transaction count: {e:?}");
                continue;
            }
        };

        // Update sampling timestamp only on success
        last_sample_time = now;

        // Handle potential node resets or load balancer inconsistencies
        let current_txs = if txs < last_txs {
            info!("Transaction count regression detected ({} < {}). Assuming 0 progress.", txs, last_txs);
            last_txs
        } else {
            txs
        };

        // Calculate Deltas
        let sample_tx_delta = current_txs - last_txs;
        total_txs = current_txs - initial_txs;
        last_txs = current_txs;

        // Calculate TPS (Using f64 for better precision during calc)
        let elapsed_secs = elapsed.as_secs_f64();
        let tps = if elapsed_secs > 0.0 {
            sample_tx_delta as f64 / elapsed_secs
        } else {
            0.0
        };

        if tps as f32 > max_tps {
            max_tps = tps as f32;
        }

        info!(
            "Sampler {:9.2} TPS, Transactions: {:6}, Total transactions: {} over {} s",
            tps,
            sample_tx_delta,
            total_txs,
            total_elapsed.as_secs(),
        );
    }
}
