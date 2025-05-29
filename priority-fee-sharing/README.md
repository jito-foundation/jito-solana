# Priority Fee Sharing

This service enables validators to distribute priority fees to their delegators through Jito's priority fee distribution program.

# Service

## Prerequisites

1. Clone the repository:

```bash
git clone --recursive https://github.com/jito-foundation/jito-solana.git
cd jito-solana
git checkout ck/distro-script
```

**NOTE:**
We need to have all submodules initialized and updated - if you've already cloned the repo, please run: `git submodule update --init --recursive`

2. Move to the `priority-fee-sharing` directory:

```bash
cd priority-fee-sharing
```

**NOTE:**
To help, you may want to install the [Solana CLI](https://solana.com/docs/intro/installation) if you have not already

## Setup

The easiest way to set up the Priority Fee Sharing service is to use the automated setup script. First, create and configure your environment file:

### 1. Create Environment Configuration

Copy the example environment file and fill in your values:

```bash
cp .env.example .env
```

Edit the resulting `.env` file with your configuration. Everything needs to be filled out - if you are unsure about any value, keep the deafult if possible.

```bash
vim .env
```

**NOTE:** If you are using your local RPC, you have to run your validator with `--enable-rpc-transaction-history` enabled.

| Variable | Description |
|----------|-------------|
| `USER` | System user to run the service (e.g., 'solana') |
| `RPC_URL` | RPC endpoint URL that supports `get_block` |
| `PRIORITY_FEE_PAYER_KEYPAIR_PATH` | Path to validator identity keypair |
| `VOTE_AUTHORITY_KEYPAIR_PATH` | Path to vote authority keypair |
| `VALIDATOR_VOTE_ACCOUNT` | Your validator's vote account public key |
| `MINIMUM_BALANCE_SOL` | Minimum SOL balance to maintain |
| `COMMISSION_BPS` | Commission in basis points (5000 = 50%) |
| `PRIORITY_FEE_DISTRIBUTION_PROGRAM` | Fee distribution program address |
| `MERKLE_ROOT_UPLOAD_AUTHORITY` | Merkle root upload authority address |
| `FEE_RECORDS_DB_PATH` | Path for fee records database |
| `PRIORITY_FEE_LAMPORTS` | Priority fee for transactions (in lamports) |
| `TRANSACTIONS_PER_EPOCH` | Number of transactions per epoch |

### 2. Run Installation Script

The installation script will:
1. Install/update Rust (minimum version 1.75.0)
2. Build and install the Priority Fee Sharing CLI
3. Generate a systemd `priority-fee-sharing.service` service file from your `.env` configuration
4. Create the fee records database path
5. Provide clear next steps for service setup

Run the installation script:

```bash
./setup_priority_fee_sharing.sh
```

### After Installation

After the script completes, follow the displayed next steps to set up the systemd service:

```bash
# 1. Review the generated service file
cat priority-fee-sharing.service

# 2. Copy to systemd directory
sudo cp priority-fee-sharing.service /etc/systemd/system/

# 3. Reload systemd
sudo systemctl daemon-reload

# 4. Enable service
sudo systemctl enable priority-fee-sharing

# 5. Start service
sudo systemctl start priority-fee-sharing

# 6. Check status
sudo systemctl status priority-fee-sharing
```

## Managing the Service

You can manage the service using the following commands:

```bash
# Start the service
sudo systemctl start priority-fee-share.service

# Stop the service
sudo systemctl stop priority-fee-share.service

# Restart the service
sudo systemctl restart priority-fee-share.service

# Check service status
sudo systemctl status priority-fee-share.service

# View service logs
sudo journalctl -u priority-fee-share.service -f
```

## Troubleshooting

If you encounter issues with the Priority Fee Sharing service:

1. Check the service status and logs
   ```bash
   sudo systemctl status priority-fee-share.service
   sudo journalctl -u priority-fee-share.service -n 50
   ```

2. Verify that the priority fee account has sufficient funds
   ```bash
   solana balance --keypair /path/to/priority-fee-keypair.json
   ```

3. Ensure your validator vote account address is correct
   ```bash
   solana account YOUR_VALIDATOR_ADDRESS
   ```

4. Check that your RPC endpoint is accessible
   ```bash
   curl -X POST -H "Content-Type: application/json" -d '{"jsonrpc":"2.0","id":1,"method":"getHealth"}' YOUR_RPC_URL
   ```

5. If using a local RPC, verify it was started with transaction history enabled
   ```bash
   grep enable-rpc-transaction-history /etc/systemd/system/solana-validator.service
   ```

6. Make sure all required keypaths are valid and accessible
   ```bash
   ls -la /path/to/priority-fee-keypair.json
   ls -la /path/to/vote-authority-keypair.json
   ```

# CLI

## Install

To install the CLI, run the following command:

```bash
git clone --recursive https://github.com/jito-foundation/jito-solana.git
cd jito-solana/priority-fee-sharing
git checkout ck/distro-script
cargo install --path .
```

## Usage

To use the CLI, run the following command:

```bash
priority-fee-sharing --help
```

### Run Service

```bash
priority-fee-sharing run \
  --rpc-url http://localhost:8899 \
  --fee-records-db-path /var/lib/solana/fee_records \
  --priority-fee-payer-keypair-path PATH_TO_PRIORITY_FEE_KEYPAIR.json \
  --vote-authority-keypair-path PATH_TO_VOTE_AUTHORITY_KEYPAIR.json \
  --validator-vote-account YOUR_VALIDATOR_VOTE_ACCOUNT \
  --merkle-root-upload-authority MERKLE_ROOT_UPLOAD_AUTHORITY \
  --minimum-balance-sol 100.0
```

### Export CSV

Export fee records to a CSV file:

```bash
priority-fee-sharing export-csv \
  --fee-records-db-path /var/lib/solana/fee_records \
  --output-path ./output.csv \
  --state any
```

The state parameter can be one of: unprocessed, processed, pending, skipped, antedup, complete, any
