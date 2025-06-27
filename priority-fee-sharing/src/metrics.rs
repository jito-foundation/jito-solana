use std::{env, collections::HashMap, sync::OnceLock};
use anyhow::{anyhow, Result};
use solana_metrics::{datapoint_error, datapoint_info, set_host_id};
use solana_pubkey::Pubkey;
use prometheus::{Counter, Gauge, push_metrics, Registry};

// Metrics struct to hold all Prometheus metrics
struct PrometheusMetrics {
    heartbeat_counter: Counter,
    error_counter: Counter,
    transfer_counter: Counter,

    // State metrics as individual gauges
    external_balance_gauge: Gauge,
    internal_balance_gauge: Gauge,
    unprocessed_record_count_gauge: Gauge,
    pending_record_count_gauge: Gauge,
    pending_lamports_gauge: Gauge,

    // Track current epoch as a gauge
    current_epoch_gauge: Gauge,
    current_slot_gauge: Gauge,

    // Registry to track all metrics
    registry: Registry,
}

// Global metrics instance using OnceLock for thread-safe one-time initialization
static METRICS: OnceLock<PrometheusMetrics> = OnceLock::new();

// Re-use from main module
use crate::{Cluster, PFEpochInfo};

// Version constant for consistent metric naming
const METRICS_VERSION: &str = "0.0.9";

// ============================================================================
// METRICS CONFIGURATION FUNCTIONS
// ============================================================================

/// Determines if any metrics should be sent (InfluxDB or Prometheus)
pub fn should_send_metrics() -> bool {
    should_send_influx_metrics() || should_send_prometheus_metrics()
}

/// Checks if InfluxDB metrics should be sent based on environment variables
pub fn should_send_influx_metrics() -> bool {
    env::var("SOLANA_METRICS_CONFIG")
        .map(|v| !v.is_empty())
        .unwrap_or(false)
}

/// Checks if Prometheus metrics should be sent based on environment variables
pub fn should_send_prometheus_metrics() -> bool {
    env::var("PROMETHEUS_PUSH_GATEWAY")
        .map(|v| !v.is_empty())
        .unwrap_or(false)
        && env::var("PROMETHEUS_JOB_NAME")
            .map(|v| !v.is_empty())
            .unwrap_or(false)
        && env::var("PROMETHEUS_INSTANCE")
            .map(|v| !v.is_empty())
            .unwrap_or(false)
}

// ============================================================================
// PROMETHEUS HELPER FUNCTIONS
// ============================================================================

fn push_prometheus_metrics() -> Result<()> {
    let (gateway, job, instance) = (
        env::var("PROMETHEUS_PUSH_GATEWAY")?,
        env::var("PROMETHEUS_JOB_NAME")?,
        env::var("PROMETHEUS_INSTANCE")?
    );

    // Get metrics from our registry
    let metric_families = match METRICS.get() {
        Some(m) => m.registry.gather(),
        None => return Err(anyhow!("Metrics not initialized")),
    };

    let mut labels = HashMap::new();
    labels.insert("instance".to_string(), instance);

    push_metrics(
        &job,
        labels,
        &gateway,
        metric_families,
        None
    ).map_err(|e| anyhow!("Failed to push metrics: {}", e))
}

// ============================================================================
// METRICS INITIALIZATION
// ============================================================================

/// Initialize Prometheus metrics
fn initialize_prometheus_metrics() -> Result<PrometheusMetrics> {
    let registry = Registry::new();

    // Create counters with no variable labels
    let heartbeat_counter = Counter::new(
        "pfs_heartbeat_total",
        format!("Total heartbeats (version {})", METRICS_VERSION)
    )?;
    registry.register(Box::new(heartbeat_counter.clone()))?;

    let error_counter = Counter::new(
        "pfs_error_total",
        format!("Total errors (version {})", METRICS_VERSION)
    )?;
    registry.register(Box::new(error_counter.clone()))?;

    let transfer_counter = Counter::new(
        "pfs_transfer_total",
        format!("Total transfers (version {})", METRICS_VERSION)
    )?;
    registry.register(Box::new(transfer_counter.clone()))?;

    // Create gauges for state metrics
    let external_balance_gauge = Gauge::new(
        "pfs_external_balance",
        format!("External balance (version {})", METRICS_VERSION)
    )?;
    registry.register(Box::new(external_balance_gauge.clone()))?;

    let internal_balance_gauge = Gauge::new(
        "pfs_internal_balance",
        format!("Internal balance (version {})", METRICS_VERSION)
    )?;
    registry.register(Box::new(internal_balance_gauge.clone()))?;

    let unprocessed_record_count_gauge = Gauge::new(
        "pfs_unprocessed_record_count",
        format!("Unprocessed record count (version {})", METRICS_VERSION)
    )?;
    registry.register(Box::new(unprocessed_record_count_gauge.clone()))?;

    let pending_record_count_gauge = Gauge::new(
        "pfs_pending_record_count",
        format!("Pending record count (version {})", METRICS_VERSION)
    )?;
    registry.register(Box::new(pending_record_count_gauge.clone()))?;

    let pending_lamports_gauge = Gauge::new(
        "pfs_pending_lamports",
        format!("Pending lamports (version {})", METRICS_VERSION)
    )?;
    registry.register(Box::new(pending_lamports_gauge.clone()))?;

    // Gauges to track current epoch and slot
    let current_epoch_gauge = Gauge::new(
        "pfs_current_epoch",
        format!("Current epoch (version {})", METRICS_VERSION)
    )?;
    registry.register(Box::new(current_epoch_gauge.clone()))?;

    let current_slot_gauge = Gauge::new(
        "pfs_current_slot",
        format!("Current slot (version {})", METRICS_VERSION)
    )?;
    registry.register(Box::new(current_slot_gauge.clone()))?;

    Ok(PrometheusMetrics {
        heartbeat_counter,
        error_counter,
        transfer_counter,
        external_balance_gauge,
        internal_balance_gauge,
        unprocessed_record_count_gauge,
        pending_record_count_gauge,
        pending_lamports_gauge,
        current_epoch_gauge,
        current_slot_gauge,
        registry,
    })
}

/// Sets up metrics configuration for the priority fee sharing service
pub fn setup_metrics(
    cluster: &Cluster,
    validator_vote_account: &Pubkey,
    validator_identity: &Pubkey,
    priority_fee_distribution_program: &Pubkey,
) -> Result<()> {
    if should_send_influx_metrics() {
        let service_name = "priority_fee_sharing";
        let vote = validator_vote_account.to_string();
        let identity = validator_identity.to_string();
        set_host_id(format!(
            "{}-{}-{},service_name={},vote={},identity={},priority_fee_distribution_program={},cluster={}",
            service_name,
            vote,
            cluster,
            service_name,
            vote,
            identity,
            priority_fee_distribution_program.to_string(),
            cluster,
        ));
    }

    // Initialize Prometheus metrics
    if should_send_prometheus_metrics() {
        let metrics = initialize_prometheus_metrics()?;
        METRICS.set(metrics)
            .map_err(|_| anyhow!("Metrics already initialized"))?;
    }

    Ok(())
}

// ============================================================================
// HEARTBEAT METRICS
// ============================================================================

/// Emits heartbeat metrics to indicate the service is running
/// For Prometheus, this is the only function that pushes metrics to the gateway
pub fn emit_heartbeat_metrics(
    priority_fee_distribution_account: &Pubkey,
    running_epoch_info: &PFEpochInfo,
) {
    if should_send_influx_metrics() {
        datapoint_info!(
            "pfs-heartbeat-0.0.9",
            ("epoch", running_epoch_info.epoch, i64),
            ("slot", running_epoch_info.slot, i64),
            "epoch" => format!("{}", running_epoch_info.epoch),
            "priority-fee-distribution-account" => priority_fee_distribution_account.to_string(),
        );
    }

    if should_send_prometheus_metrics() {
        if let Some(metrics) = METRICS.get() {
            // Increment heartbeat counter
            metrics.heartbeat_counter.inc();

            // Update current epoch and slot
            metrics.current_epoch_gauge.set(running_epoch_info.epoch as f64);
            metrics.current_slot_gauge.set(running_epoch_info.slot as f64);

            // Push all accumulated metrics to Prometheus
            if let Err(e) = push_prometheus_metrics() {
                eprintln!("Failed to push metrics to Prometheus: {}", e);
            }
        }
    }
}

// ============================================================================
// STATE METRICS
// ============================================================================

/// Emits current state metrics including balances and record counts
pub fn emit_state_metrics(
    priority_fee_distribution_account: &Pubkey,
    running_epoch_info: &PFEpochInfo,
    external_balance: u64,
    internal_balance: u64,
    unprocessed_record_count: usize,
    pending_record_count: usize,
    pending_lamports: i64,
) {
    if should_send_influx_metrics() {
        datapoint_info!(
            "pfs-state-0.0.9",
            ("priority-fee-distribution-account-external-balance", external_balance, i64),
            ("priority-fee-distribution-account-internal-balance", internal_balance, i64),
            ("unprocessed-record-count", unprocessed_record_count, i64),
            ("pending-record-count", pending_record_count, i64),
            ("pending-lamports", pending_lamports, i64),
            ("epoch", running_epoch_info.epoch, i64),
            ("slot", running_epoch_info.slot, i64),
            "epoch" => format!("{}", running_epoch_info.epoch),
            "priority-fee-distribution-account" => priority_fee_distribution_account.to_string(),
        );
    }

    if should_send_prometheus_metrics() {
        if let Some(metrics) = METRICS.get() {
            // Update all gauge values (but don't push - wait for heartbeat)
            metrics.external_balance_gauge.set(external_balance as f64);
            metrics.internal_balance_gauge.set(internal_balance as f64);
            metrics.unprocessed_record_count_gauge.set(unprocessed_record_count as f64);
            metrics.pending_record_count_gauge.set(pending_record_count as f64);
            metrics.pending_lamports_gauge.set(pending_lamports as f64);

            // Update current epoch and slot
            metrics.current_epoch_gauge.set(running_epoch_info.epoch as f64);
            metrics.current_slot_gauge.set(running_epoch_info.slot as f64);
        }
    }
}

// ============================================================================
// TRANSFER METRICS
// ============================================================================

/// Emits metrics when priority fee transfers are completed
pub fn emit_transfer_metrics(
    priority_fee_distribution_account: &Pubkey,
    running_epoch_info: &PFEpochInfo,
    signature: &str,
    slots_covered: u64,
    total_priority_fees: u64,
    transfer_amount_lamports: u64,
    priority_fee_distribution_account_balance: u64,
) {
    if should_send_influx_metrics() {
        datapoint_info!(
            "pfs-transfer-0.0.9",
            ("epoch", running_epoch_info.epoch, i64),
            ("slot", running_epoch_info.slot, i64),
            ("signature", signature.to_string(), String),
            ("slots-covered", slots_covered, i64),
            ("total-priority-fees", total_priority_fees, i64),
            ("transfer-amount-lamports", transfer_amount_lamports, i64),
            ("priority-fee-distribution-account-balance", priority_fee_distribution_account_balance, i64),
            "epoch" => format!("{}", running_epoch_info.epoch),
            "priority-fee-distribution-account" => priority_fee_distribution_account.to_string(),
        );
    }

    if should_send_prometheus_metrics() {
        if let Some(metrics) = METRICS.get() {
            // Increment transfer counter (but don't push - wait for heartbeat)
            metrics.transfer_counter.inc();

            // Update current epoch and slot
            metrics.current_epoch_gauge.set(running_epoch_info.epoch as f64);
            metrics.current_slot_gauge.set(running_epoch_info.slot as f64);
        }
    }
}

// ============================================================================
// ERROR METRICS
// ============================================================================

/// Emits error metrics when issues occur in the priority fee sharing process
pub fn emit_error_metrics(
    priority_fee_distribution_account: &Pubkey,
    running_epoch_info: &PFEpochInfo,
    error_string: String,
) {
    if should_send_influx_metrics() {
        datapoint_error!(
            "pfs-error-0.0.9",
            ("epoch", running_epoch_info.epoch, i64),
            ("slot", running_epoch_info.slot, i64),
            ("error", error_string, String),
            "epoch" => format!("{}", running_epoch_info.epoch),
            "priority-fee-distribution-account" => priority_fee_distribution_account.to_string(),
        );
    }

    if should_send_prometheus_metrics() {
        if let Some(metrics) = METRICS.get() {
            // Increment error counter (but don't push - wait for heartbeat)
            metrics.error_counter.inc();

            // Update current epoch and slot
            metrics.current_epoch_gauge.set(running_epoch_info.epoch as f64);
            metrics.current_slot_gauge.set(running_epoch_info.slot as f64);
        }
    }
}
