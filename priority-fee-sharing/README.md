# Priority Fee Sharing

This service enables validators to distribute priority fees to their delegators through Jito's priority fee distribution program.

# Service

## Prerequisites

Clone the repository:

```bash
git clone --recursive https://github.com/jito-foundation/jito-solana.git
git checkout ck/distro-script
```

*NOTE:* We need to have all submodules initialized and updated - if you've already cloned the repo, please run:

```bash
git submodule update --init --recursive
```

Move to the `priority-fee-sharing` directory:

```bash
cd jito-solana/priority-fee-sharing
```

## Easy Setup

The easiest way to set up the Priority Fee Sharing service is to use the automated setup script:

Info you will need before running the setup script:
1. `RPC_URL` - this must be able to call `get_block`.  **Example:** `http://localhost:8899`
2. `FEE_RECORDS_DB_PATH` - Path to store fee records. **Default:** `/var/lib/solana/fee_records`
3. `PAYER_KEYPAIR` - the account which the priority fee shares come out of. This will usually be your validator's identity keypair.
4. `VOTE_AUTHORITY_KEYPAIR` - the keypair of your vote authority - this is needed to sign and create the distribution account ( no funds will be used from this account ). To get the authority run: `'solana vote-account YOUR_VOTE_ACCOUNT'`
5. `VALIDATOR_VOTE_ACCOUNT` - the vote account of your validator ( Not identity )
6. `COMMISSION_BPS` - the commission the validator takes, this should be 50% or lower to receive stake ( 5000 )
7. `MINIMUM_BALANCE_SOL` - a reserve balance of SOL kept in your `PAYER_KEYPAIR` for saftey, this should be kept above an amount needed to profitably run the validator.

**Note**: It's advised to use the defaults on all other parameters unless you have a specific reason to change them.

```bash
sudo ./setup_priority_fee_sharing.sh
```

**NOTE:** If you are using your local RPC, you have to run your validator with `--enable-rpc-transaction-history` enabled.

The setup script will:
1. Guide you through inputting the required parameters
2. Install the necessary dependencies
3. Create and configure the service files
4. Enable and start the service automatically

### Required Parameters

You will need to provide the following information during setup:

| Parameter | Description |
|-----------|-------------|
| `RPC_URL` | URL of the Solana RPC endpoint. This RPC needs to be able to call `get_block`. If using a local RPC, ensure it is running with `--enable-rpc-transaction-history` |
| `PRIORITY_FEE_PAYER_KEYPAIR_PATH` | Path to keypair that will pay the priority fees |
| `VOTE_AUTHORITY_KEYPAIR_PATH` | Path to your vote authority keypair (needed to create the PriorityFeeDistribution Account) |
| `VALIDATOR_VOTE_ACCOUNT` | Your validator's vote account address |
| `MINIMUM_BALANCE_SOL` | Minimum balance to maintain in the payer account (in SOL) |

### Optional Parameters

These parameters have default values but can be customized during setup:

| Parameter | Description | Default |
|-----------|-------------|---------|
| `PRIORITY_FEE_DISTRIBUTION_PROGRAM` | Program address for the fee distribution | `9yw8YAKz16nFmA9EvHzKyVCYErHAJ6ZKtmK6adDBvmuU` |
| `MERKLE_ROOT_UPLOAD_AUTHORITY` | Authority for uploading the merkle root | `2AxPPApUQWvo2JsB52iQC4gbEipAWjRvmnNyDHJgd6Pe` |
| `COMMISSION_BPS` | Commission in basis points (100 = 1%, 5000 = 50%, 10000 = 100%) | `5000` |
| `FEE_RECORDS_DB_PATH` | Directory path for storing fee records Rocks DB | `/var/lib/solana/fee_records` |
| `CHUNK_SIZE` | Batch size for processing transactions | `1` |
| `CALL_LIMIT` | Maximum number of calls | `1` |

## Manual Setup

If you prefer a manual setup, follow these steps:

### 1. Install the Jito Priority Fee Sharing Binary

Clone the repo

```bash
git clone --recursive https://github.com/jito-foundation/jito-solana.git
cd jito-solana/priority-fee-sharing
git checkout ck/distro-script
```

Install the binary

```bash
cargo install --path .
```

### 2. Copy and Edit the `.service` File

Copy the `priority-fee-share.service` file to `/etc/systemd/system/`.

```bash
sudo cp priority-fee-sharing/priority-fee-share.service /etc/systemd/system/
```

Fill out the required parameters in the `.service` file:

```bash
sudo vim /etc/systemd/system/priority-fee-share.service
```

**NOTE:** Make sure to fill out all of the `REQUIRED` parameters in the service file

**NOTE:** If you are using your local RPC, you have to run your validator with `--enable-rpc-transaction-history` enabled.

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

Status

```bash
sudo systemctl status priority-fee-share.service
```

Logs

```bash
sudo journalctl -u priority-fee-share.service -f
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
