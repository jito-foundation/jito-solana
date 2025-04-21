# Priority Fee Sharing

This service enables validators to distribute priority fees to their delegators through Jito's priority fee distribution program.

# Service

## Variable Descriptions

The following environment variables are used in the service file:

### Required Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `RPC_URL` | URL of the Solana RPC endpoint. This RPC needs to be able to call `get_block`. If using a local RPC, ensure it is running with `--enable-rpc-transaction-history`| `http://localhost:8899` |
| `FEE_RECORDS_DB_PATH` | Directory path for storing fee records Rocks DB | `/var/lib/solana/fee_records` |
| `PRIORITY_FEE_KEYPAIR_PATH` | Path to keypair that will pay the   | None, must be provided |
| `VALIDATOR_ADDRESS` | Your validator's vote account address | None, must be provided |
| `MINIMUM_BALANCE_SOL` | Minimum balance to maintain in the payer account (in SOL) |  None, must be provided |

### Optional Parameters ( with defaults )

| Parameter | Description | Default |
|-----------|-------------|---------|
| `PRIORITY_FEE_DISTRIBUTION_PROGRAM` | Program address for the fee distribution | `BBBATax9kikSHQp8UTcyQL3tfU3BmQD9yid5qhC7QEAA` |
| `COMMISSION_BPS` | Commission in basis points (100 = 1%, 5000 = 50%, 10000 = 100%) | `5000` |
| `CHUNK_SIZE` | Batch size for processing transactions | `1` |
| `CALL_LIMIT` | Maximum number of calls | `1` |
| `GO_LIVE_EPOCH` | Epoch number when the service should go live | `1000` |

## Prerequisites

Clone the repository:

```bash
git clone --recursive https://github.com/jito-foundation/jito-solana.git
git checkout ck/distro-script
```

Move to the `priority-fee-sharing` directory:

```bash
cd jito-solana/priority-fee-sharing
```

Install the CLI

```bash
cargo install --path .
```

## Easy Setup

The easiest way to set up the Priority Fee Sharing service is to use the automated setup script:

```bash
sudo ./setup_priority_fee_sharing.sh
```

*NOTE* If you are using your local RPC, you have to run your validator with `--enable-rpc-transaction-history` enabled.

The setup script will:
1. Install the necessary dependencies
2. Guide you through inputting the required parameters
3. Create and configure the service files
4. Enable and start the service automatically

## Manual Setup

If you prefer a manual setup, follow these steps:

### 1. Install the Jito Priority Fee Sharing Binary
```bash
cargo install --path .
```

### 2. Copy and Edit the `.service` File

Copy the `priority-fee-share.service.service` file to `/etc/systemd/system/`.

```bash
sudo cp priority-fee-sharing/priority-fee-share.service /etc/systemd/system/
```

Fill out the required parameters in the `.service` file:

```bash
sudo vim /etc/systemd/system/priority-fee-share.service
```

*NOTE* If you are using your local RPC, you have to run your validator with `--enable-rpc-transaction-history` enabled.

### 3. Create Fee Records Directory

```bash
sudo mkdir -p /var/lib/solana/fee_records
```

### 4. Enable and Start the Service

```bash
sudo systemctl daemon-reload
sudo systemctl enable priority-fee-share.service
sudo systemctl start priority-fee-share.service
```

### 5. Check Service Status

```bash
sudo systemctl status priority-fee-share.service
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
sudo journalctl -u priority-fee-share.service
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


# CLI

## Install

To install the CLI, run the following command:

```bash
cargo install --path .
```

## Usage

To use the CLI, run the following command:

```bash
priority-fee-share-cli --help
```

### Run Service

```bash
priority-fee-share-cli run \
  --rpc-url http://localhost:8899 \
  --fee-records-db-path /var/lib/solana/fee_records \
  --priority-fee-keypair-path PATH_TO_PRIORITY_FEE_KEYPAIR.json \
  --validator-address YOUR_VALIDATOR_VOTE_ACCOUNT
```

### Export CSV

```bash
priority-fee-share-cli export-csv \
  --fee-records-db-path /var/lib/solana/fee_records \
  --output-path ./output.csv \
  --state any
```

### Compact DB

For performance reasons, it is recommended to compact the database periodically. To do so, run the following command:

```bash
priority-fee-share-cli compact-db \
  --fee-records-db-path /var/lib/solana/fee_records
```
