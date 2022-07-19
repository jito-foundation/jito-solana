use solana_program_runtime::compute_budget::ComputeBudget;

/// Encapsulates flags that can be used to tweak the runtime behavior.
#[derive(Default, Clone)]
pub struct RuntimeConfig {
    pub bpf_jit: bool,
    pub compute_budget: Option<ComputeBudget>,
    pub log_messages_bytes_limit: Option<usize>,
}
