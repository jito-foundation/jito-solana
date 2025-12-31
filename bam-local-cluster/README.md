# BAM Local Cluster

A tool for spinning up local Solana clusters with BAM (Block Assembly Marketplace) support for testing purposes. This tool uses subprocess-based execution to spawn `agave-validator` instances.

## Overview

BAM Local Cluster is a development tool for spinning up local Solana clusters with BAM (Block Assembly Marketplace) support. It's designed for testing BAM-related functionality in a controlled local environment.

The tool automatically handles:
- Validator process management and monitoring
- Genesis configuration with SPL programs
- Keypair generation and ledger setup
- Bootstrap node coordination
- Process health monitoring and graceful shutdown

## Quick Start

1. **Build the binaries**:
   ```bash
   # Build agave-validator
   cargo build --release --bin agave-validator
   
   # Build bam-local-cluster
   cargo build --release --bin bam-local-cluster
   ```

2. **Create a configuration file** (see `examples/example_config.toml`)

3. **Run the cluster**:
   ```bash
   RUST_LOG=info ./target/release/bam-local-cluster --config bam-local-cluster/examples/example_config.toml
   ```

## Configuration

The configuration file specifies BAM service URLs, tip program IDs, and validator settings. See `examples/example_config.toml` for a complete example.

Key configuration options:
- `bam_url`: BAM service endpoint
- `tip_payment_program_id` / `tip_distribution_program_id`: Tip manager programs
- `faucet_address`: Faucet service for airdrops
- `ledger_base_directory`: Base directory for validator ledgers
- `validator_build_path`: Build output directory (e.g., "target/debug" or "target/release") - required
- `validators`: Array of validator configurations (first is bootstrap node)



## How It Works

The tool spawns `agave-validator` processes as subprocesses, automatically handling:

1. Genesis configuration with SPL programs
2. Keypair generation and ledger setup
3. Bootstrap node startup and coordination
4. Validator process spawning and monitoring
5. Graceful shutdown on Ctrl+C or process failure

## Usage

The cluster runs until you press Ctrl+C or a validator process fails. All validator output is streamed to the console for debugging.



## Troubleshooting

Common issues:
- **Port conflicts**: Bootstrap node uses gossip port 8001 and RPC port 8899
- **Binary not found**: Ensure `agave-validator` and `agave-ledger-tool` are built in your target directory
- **Permission errors**: Make sure the ledger base directory is writable

Validator output is streamed to the console for debugging.

## Development

To modify the cluster behavior:

1. Update `src/cluster_manager.rs` for process spawning logic
2. Modify `src/config.rs` for configuration structure
3. Update `src/main.rs` for command-line interface

## License

This project is part of the Jito Solana JDS repository and follows the same license terms. 