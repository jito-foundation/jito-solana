# BAM Local Cluster

A tool for spinning up local Solana clusters with BAM (Block Assembly Marketplace) support for testing purposes. This
tool uses subprocess-based execution to spawn `agave-validator` instances.

## Quick Start

The cluster tool belongs to the `dev-bins` workspace so its development
features do not propagate into production validator builds. Run these
commands from the repository root.

1. **Build the binaries**:

   ```bash
   # Build agave-validator
   cargo build --release -p agave-validator

   # Build the development tools separately
   cargo build --release --manifest-path dev-bins/Cargo.toml \
     --bin bam-local-cluster --bin agave-ledger-tool
   ```

2. **Create a configuration file** (see `examples/example_config.toml`)

3. **Run the cluster**:

   ```bash
   RUST_LOG=info ./dev-bins/target/release/bam-local-cluster \
     --config bam-local-cluster/examples/example_config.toml
   ```

## Configuration

The configuration file specifies BAM service URLs, tip program IDs, and validator settings. See
`examples/example_config.toml` for a complete example.

Key configuration options:

- `bam_url`: BAM service endpoint
- `tip_payment_program_id` / `tip_distribution_program_id`: Tip manager programs
- `faucet_address`: Faucet service for airdrops
- `mint_sol`: Optional faucet genesis balance in SOL
- `ledger_base_directory`: Base directory for validator ledgers
- `validator_build_path`: Build output directory (e.g., "target/debug" or "target/release") - required
- `ledger_tool_build_path`: Ledger tool output directory, such as
  "dev-bins/target/debug" or "dev-bins/target/release" - required
- `dynamic_port_range_start`: First port of the validators' dynamic port ranges, 1,000 per
  validator - required
- `bind_address`: Optional validator listen address override passed through as `--bind-address`
- `gossip_host`: Optional validator gossip advertisement override passed through as `--gossip-host`
- `limit_blockstore_size`: Optional data and coding shreds each validator keeps, passed through as
  `--limit-blockstore-size` (at least 100M)
- `validators`: Array of validator configurations (first is bootstrap node)

## Feature configuration

Pass `--features-config bam-local-cluster/features.example.toml` alongside
`--config` (see [example](features.example.toml)). In `[features]`,
`baseline = "mainnet-beta"` or `baseline = "testnet"` starts with that
cluster's active features at genesis; pending and absent features stay
inactive. Override by feature public key:

- `enable`: activate at genesis.
- `disable`: leave inactive.
- `activate_next_epoch`: request runtime activation at the first epoch
  boundary.
- `activate_at_epoch`: map fast leader handover or the 200 ms slot feature to a
  target epoch (1 or later). Other gates are rejected because some have extra
  activation hooks. The feature becomes active at that epoch's first slot. For
  example:

  ```toml
  [features.activate_at_epoch]
  "FLh2kvwNhDNN6kkQrwdDQ4p5FrgdqxnwykgMUryABRwT" = 3  # fast leader handover
  ```

BAM's `local-cluster/FEATURES.md` has fast leader handover examples.
Slot-time reductions take effect in the epoch *after* feature activation.

The source baseline is saved as `features-baseline.toml` beside the ledger
directory. Copy it outside the output directory before rerunning; set
`baseline` to that path (relative to the feature TOML) to reuse it. RPC
reads span multiple finalized slots, recorded in the snapshot. A saved
baseline's features that this build no longer has log a warning, and an active
one still gets its feature account; an unknown override is an error.

Alpenglow must be active at genesis, where it uses a synthetic certificate and
skips migration. Validators no longer start leaders before the migration, so a
config that leaves Alpenglow inactive or schedules it for later is rejected
instead of staying at slot 0. Invalid or conflicting overrides and missing
prerequisites are also rejected. Without `--features-config`, genesis enables
every feature in the build, with Alpenglow at genesis.

## Usage

The cluster runs until you press Ctrl+C or send SIGTERM, which exits 0, or a validator process exits, which exits 1; a
signal during startup stops it once startup finishes. All validator output is streamed to the console for debugging.

Use `--skip-last-validator` to omit starting the final validator (useful when running an alternate validator for testing
purposes); the validator still receives stake/airdrop in genesis and remains in the leader schedule.

## Troubleshooting

Common issues:

- **Port conflicts**: Bootstrap node uses gossip port 8001 and RPC port 8899
- **Binary not found**: Check `validator_build_path` and
  `ledger_tool_build_path` against the separate build output directories
- **Permission errors**: Make sure the ledger base directory is writable
