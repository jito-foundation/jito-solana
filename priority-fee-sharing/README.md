# Priority Fee Sharing

This service enables validators to distribute priority fees to their delegators through Jito's priority fee distribution program.

### Notes

1. `git status`
```bash
core@ny-testnet-validator-2:~/jito-priority-fee-sharing/priority-fee-sharing$ git status
On branch ck/pfs
Your branch is up to date with 'origin/ck/pfs'.

Changes not staged for commit:
  (use "git add <file>..." to update what will be committed)
  (use "git restore <file>..." to discard changes in working directory)
	modified:   ../anchor (new commits)
	modified:   ../jito-programs (new commits)

no changes added to commit (use "git add" and/or "git commit -a")
```

# Service

## Prerequisites

1. Clone the repository:

```bash
git clone --recursive https://github.com/jito-foundation/jito-solana.git jito-priority-fee-sharing
```

2. Navigate to the priority fee directory:

```bash
cd jito-priority-fee-sharing/priority-fee-sharing
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

| Variable                            | Required | Description                                     |
| ----------------------------------- | -------- | ----------------------------------------------- |
| `USER`                              | Required | System user to run the service (e.g., 'solana') |
| `CLUSTER`                           | Required | Cluster mainet, devnet, testnet                 |
| `RPC_URL`                           | Required | RPC endpoint URL that supports `get_block`      |
| `PRIORITY_FEE_PAYER_KEYPAIR_PATH`   | Required | Path to validator identity keypair              |
| `VOTE_AUTHORITY_KEYPAIR_PATH`       | Required | Path to vote authority keypair                  |
| `VALIDATOR_VOTE_ACCOUNT`            | Required | Your validator's vote account public key        |
| `MINIMUM_BALANCE_SOL`               | Required | Minimum SOL balance to maintain                 |
| `COMMISSION_BPS`                    | Default  | Commission in basis points (5000 = 50%)         |
| `PRIORITY_FEE_DISTRIBUTION_PROGRAM` | Default  | Fee distribution program address                |
| `MERKLE_ROOT_UPLOAD_AUTHORITY`      | Default  | Merkle root upload authority address            |
| `FEE_RECORDS_DB_PATH`               | Default  | Path for fee records database                   |
| `PRIORITY_FEE_LAMPORTS`             | Default  | Priority fee for transactions (in lamports)     |
| `TRANSACTIONS_PER_EPOCH`            | Default  | Number of transactions per epoch                |
| `SOLANA_METRICS_CONFIG`             | Optional | Solana metrics configuration credentials        |
| `PROMETHEUS_PUSH_GATEWAY`           | Optional | Prometheus push gateway URL                     |
| `PROMETHEUS_JOB_NAME`               | Optional | Prometheus job name                             |
| `PROMETHEUS_INSTANCE`               | Optional | Prometheus instance                             |

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

## Updating the Service

To update the Priority Fee Sharing service, follow these steps ( do in the `priority-fee-sharing` directory ):

1. Stop the service

   ```bash
   sudo systemctl stop priority-fee-sharing.service
   ```

2. Update the repo

   ```bash
   git pull
   ```

3. Re-run startup script

   ```bash
   ./setup_priority_fee_sharing.sh
   ```

## Metrics

### Prometheus

Prometheus is enabled when the corresponding `.env` variables are provided.

### InfluxDB

The Priority Fee Sharing service sends metrics to InfluxDB instances managed by Jito Labs. There are two separate datasources:

1. **Priority Fee Sharing Script (PFS)** - Metrics from your validator's fee sharing service
2. **Priority Fee History (PFH)** - Network-wide on-chain priority fee data

To view your metrics, you'll need to:

1. **Get Access Credentials**
   - Contact Jito Labs to receive your InfluxDB credentials
   - You'll receive connection details for the PFS datasource

2. **Configure SOLANA_METRICS_CONFIG**
   - Add the provided metrics configuration to your `.env` file
   - This enables automatic metric submission from your PFS service

   ```env
   SOLANA_METRICS_CONFIG="host=http://tip-router.metrics.jito.wtf:8086,db=priority-fee-sharing,u=validator,p=<PASSWORD>"
   ```

### Grafana

We provide a pre-configured Grafana dashboard template to monitor your Priority Fee Sharing service.

#### Setting Up the Dashboard

1. **Access Grafana**
   - Log into your Grafana instance
   - Navigate to Dashboards → Import

2. **Import the Dashboard**
   - Click "Upload dashboard JSON file"
   - Select the provided `./grafana.json` file
   - You'll be prompted to configure datasources

3. **Configure Datasources**

   During import, map the following datasources ( Reach out to Jito for datasources ):

   | Template Variable | URL | Database | User | Password |
   |---|---|---|---|---|
   | **Priority Fee Sharing Script** | http://tip-router.metrics.jito.wtf:8086 | priority-fee-sharing | validator | *Reach out to Jito |
   | **Priority Fee History** | http://tip-router.metrics.jito.wtf:8086 | priority-fee-history | validator | *Reach out to Jito |

4. **Configure Dashboard Variables**

   After import, the dashboard will auto-populate these variables:
   - **Cluster**: Select `mainnet` or `testnet`
   - **Vote Account**: Your validator's vote account will appear after datasource connection
   - **Epoch**: Select the epoch you want to monitor

#### Dashboard Panels

Your dashboard includes four main panels:

- 🟨 **PFS Heartbeat**: Shows the current epoch your Priority Fee Sharing service is processing
- 🟨 **PFS Total Shared**: Displays the total SOL distributed to delegators for the selected epoch
- 🟦 **PFH Heartbeat**: Monitors the on-chain history service availability
- 🟦 **PFH Expected Share**: Shows the expected SOL distribution based on network data

**Note**: A small difference between "Total Shared" and "Expected Share" is normal due to transfers near epoch boundaries.

### Alerting

It is reccomeneded to create the following alerting condition in grafana:

**PFS Heartbeat Alert**
  - Alert on PFSHeartbeat on `no data` - this is under `Configure no data and error handling` dropdown. Make sure to select `Alerting`, for both options.
  - Alert based off of your `vote` account

Configure alerts through Grafana's alerting system to notify via your preferred channels (email, Slack, PagerDuty, etc.).

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
