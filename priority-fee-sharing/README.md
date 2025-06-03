# Priority Fee Sharing

This service enables validators to distribute priority fees to their delegators through Jito's priority fee distribution program.

# Service

## Prerequisites

1. Clone the repository:

```bash
git clone --recursive https://github.com/jito-foundation/jito-solana.git jito-priority-fee-sharing
```

2. Checkout the priority fee branch:

```bash
cd jito-priority-fee-sharing
git checkout ck/distro-script
```

3. Navigate to the priority fee directory:

```bash
cd priority-fee-sharing
```

**NOTE:**
We need to have all submodules initialized and updated - if you've already cloned the repo, please run: `git submodule update --init --recursive`

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

| Variable                            | Description                                     |
| ----------------------------------- | ----------------------------------------------- |
| `USER`                              | System user to run the service (e.g., 'solana') |
| `CLUSTER`                              | Cluster mainet, devnet, testnet                      |
| `RPC_URL`                           | RPC endpoint URL that supports `get_block`      |
| `PRIORITY_FEE_PAYER_KEYPAIR_PATH`   | Path to validator identity keypair              |
| `VOTE_AUTHORITY_KEYPAIR_PATH`       | Path to vote authority keypair                  |
| `VALIDATOR_VOTE_ACCOUNT`            | Your validator's vote account public key        |
| `MINIMUM_BALANCE_SOL`               | Minimum SOL balance to maintain                 |
| `COMMISSION_BPS`                    | Commission in basis points (5000 = 50%)         |
| `PRIORITY_FEE_DISTRIBUTION_PROGRAM` | Fee distribution program address                |
| `MERKLE_ROOT_UPLOAD_AUTHORITY`      | Merkle root upload authority address            |
| `FEE_RECORDS_DB_PATH`               | Path for fee records database                   |
| `PRIORITY_FEE_LAMPORTS`             | Priority fee for transactions (in lamports)     |
| `TRANSACTIONS_PER_EPOCH`            | Number of transactions per epoch                |

### 2. Run Installation Script

The installation script will:

1. Install/update Rust (minimum version 1.75.0)
2. Build and install the Priority Fee Sharing CLI
3. Generate a systemd `priority-fee-sharing.service` service file from your `.env` configuration
4. Create the fee records database path
5. Run verification and provide next steps

Run the installation script:

```bash
./setup_priority_fee_sharing.sh
```

### After Installation

After the script completes, follow the displayed next steps to set up the systemd service:

    echo -e "Next steps:"
    echo -e "1. Move the generated service file to systemd directory: \033[34msudo mv priority-fee-sharing.service /etc/systemd/system/\033[0m"
    echo -e "2. Reload systemd: \033[34msudo systemctl daemon-reload\033[0m"
    echo -e "3. Enable service: \033[34msudo systemctl enable priority-fee-sharing\033[0m"
    echo -e "4. Review the generated service file:  \033[34msystemctl cat priority-fee-sharing.service\033[0m"
    echo -e "5. Start service: \033[34msudo systemctl start priority-fee-sharing\033[0m"
    echo -e "6. Check status: \033[34msudo systemctl status priority-fee-sharing\033[0m"
    echo -e "7. View logs: \033[34msudo journalctl -u priority-fee-sharing.service -f\033[0m"

```bash
# 0. Verify the .env file
priority-fee-sharing run --verify

# 1. Move the generated service file to systemd directory
sudo mv priority-fee-sharing.service

# 2. Reload systemd
sudo systemctl daemon-reload

# 3. Enable service
sudo systemctl enable priority-fee-sharing

# 4. Review the generated service file
systemctl cat priority-fee-sharing.service

# 5. Start service
sudo systemctl start priority-fee-sharing

# 6. Check status
sudo systemctl status priority-fee-sharing

# 7. View logs
sudo journalctl -u priority-fee-sharing.service -f
```

## Managing the Service

You can manage the service using the following commands:

```bash
# Start the service
sudo systemctl start priority-fee-sharing.service

# Stop the service
sudo systemctl stop priority-fee-sharing.service

# Restart the service
sudo systemctl restart priority-fee-sharing.service

# Check service status
sudo systemctl status priority-fee-sharing.service

# View service logs
sudo journalctl -u priority-fee-sharing.service -f
```

## Troubleshooting

If you encounter issues with the Priority Fee Sharing service:

1. Check the service status and logs

   ```bash
   sudo systemctl status priority-fee-sharing.service
   sudo journalctl -u priority-fee-sharing.service -n 50
   ```

2. Run the verify script to ensure the `.env` file is setup correctly + Check all fields

   ```bash
   priority-fee-sharing run --verify
   ```
