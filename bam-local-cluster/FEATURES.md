# Feature configuration

```bash
bam-local-cluster --config local-cluster.toml \
  --features-config bam-local-cluster/features.example.toml
```

`[features]` accepts `baseline`, `enable`, `disable`, and
`activate_next_epoch`. Overrides contain feature public keys known to this
validator build. `baseline = "mainnet-beta"` reads finalized feature accounts;
otherwise it is a snapshot file path relative to the feature TOML.

Mainnet-active features are active at local genesis. Mainnet-pending and
absent features stay inactive unless explicitly overridden. Overrides replace
the baseline. Unknown IDs, duplicate overrides, and inactive local-cluster
prerequisites are errors. Prerequisites are vote-state-v4 and the
remaining-compute-units syscall; errors report IDs for explicit overrides.

The source baseline is saved as `features-baseline.toml` beside the generated
ledger directory. It includes all observed states and the first/last finalized
slots across RPC batches, rather than claiming an atomic snapshot. Preserve
it outside the output directory before rerunning, and use its path as the
baseline to reproduce the same starting conditions. Missing IDs default to
inactive; snapshot IDs unknown to the selected build are rejected.

`enable` activates at genesis. For Alpenglow, this also installs the existing
synthetic genesis certificate and skips migration. `activate_next_epoch`
creates a pending feature account, exercising normal runtime activation at
the first epoch boundary. It does not install the Alpenglow certificate.
Activation and completion of Alpenglow migration are separate events.
Existing epoch timing is unchanged. This configuration does not assert
successful migration or transaction progress.

Do not combine feature configuration with `enable_tx_v1 = true` or
`slot_time_ms` in the cluster config. Without `--features-config`, existing
local-cluster behavior is unchanged.
