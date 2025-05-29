#!/bin/bash

# Script to install Priority Fee Sharing CLI and generate systemd service file
# Usage: ./install-and-generate.sh

set -euo pipefail

#################################################
# HELPER FUNCTIONS
#################################################

# Function to compare version numbers
version_compare() {
    local version1="$1"
    local version2="$2"
    local IFS=.
    local i ver1=($version1) ver2=($version2)

    # Fill empty fields in ver1 with zeros
    for ((i=${#ver1[@]}; i<${#ver2[@]}; i++)); do
        ver1[i]=0
    done

    for ((i=0; i<${#ver1[@]}; i++)); do
        if [[ -z ${ver2[i]} ]]; then
            # ver2 field is empty (or doesn't exist)
            ver2[i]=0
        fi
        if ((10#${ver1[i]} > 10#${ver2[i]})); then
            return 1  # version1 > version2
        fi
        if ((10#${ver1[i]} < 10#${ver2[i]})); then
            return 2  # version1 < version2
        fi
    done
    return 0  # version1 == version2
}

#################################################
# INSTALL CARGO
#################################################
install_cargo() {
    local min_version="1.75.0"

    # Check if cargo is in PATH
    if command -v cargo &> /dev/null; then
        echo "✅ Cargo is already installed!"
        local current_version=$(cargo --version | grep -oE '[0-9]+\.[0-9]+\.[0-9]+')
        echo "Current version: $current_version"

        # Check if version is sufficient
        version_compare "$current_version" "$min_version"
        case $? in
            0|1)  # Equal or greater
                echo "✅ Rust version $current_version meets minimum requirement ($min_version)"
                return 0
                ;;
            2)  # Less than required
                echo -e "\033[31m❌ Rust version $current_version is below minimum requirement ($min_version)\033[0m"
                echo "Updating Rust..."
                rustup update stable

                # Check version again
                local new_version=$(cargo --version | grep -oE '[0-9]+\.[0-9]+\.[0-9]+')
                version_compare "$new_version" "$min_version"
                case $? in
                    0|1)
                        echo "✅ Rust updated to $new_version"
                        return 0
                        ;;
                    2)
                        echo -e "\033[31m❌ Failed to update Rust to minimum version\033[0m"
                        return 1
                        ;;
                esac
                ;;
        esac
    else
        echo "❌ Cargo is not installed. Installing now..."

        # Check for curl
        if ! command -v curl &> /dev/null; then
            echo "Installing curl first..."
            sudo apt-get update && sudo apt-get install -y curl
        fi

        # Install Rust and Cargo using rustup
        curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y

        # Source the environment to make cargo available in current shell
        source "$HOME/.cargo/env"

        # Verify installation and version
        if command -v cargo &> /dev/null; then
            local installed_version=$(cargo --version | grep -oE '[0-9]+\.[0-9]+\.[0-9]+')
            echo "✅ Cargo installation successful!"
            echo "Installed version: $installed_version"

            version_compare "$installed_version" "$min_version"
            case $? in
                0|1)
                    echo "✅ Rust version $installed_version meets minimum requirement ($min_version)"
                    return 0
                    ;;
                2)
                    echo -e "\033[31m❌ Installed Rust version $installed_version is below minimum requirement ($min_version)\033[0m"
                    return 1
                    ;;
            esac
        else
            echo -e "\033[31m❌ Something went wrong with the Cargo installation.\033[0m"
            echo "Please try installing manually with:"
            echo "curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh"
            echo "Then run: source \$HOME/.cargo/env"
            return 1
        fi
    fi
}

#################################################
# INSTALL CLI
#################################################
install_cli() {
    echo "Installing Priority Fee Sharing CLI..."

    # Update git submodules if needed
    if [ -f ".gitmodules" ]; then
        echo "Updating git submodules..."
        git submodule update --init --recursive
    fi

    # Install the CLI
    echo "Building and installing CLI from source..."
    cargo install --path .

    echo ""
    echo -e "✅ CLI installed successfully! Run: \033[34mpriority-fee-sharing --help\033[0m"

    # Verify CLI installation
    local cli_path=$(which priority-fee-sharing 2>/dev/null || echo "")
    if [[ -n "$cli_path" ]]; then
        echo "CLI installed at: $cli_path"
        return 0
    else
        echo -e "\033[31m❌ CLI installation failed - binary not found in PATH\033[0m"
        return 1
    fi
}

#################################################
# GENERATE SERVICE FILE SCRIPT
#################################################
generate_service_file() {
    local env_file=".env"
    local output_file="priority-fee-sharing.service"

    # Check if .env file exists
    if [[ ! -f "$env_file" ]]; then
        echo -e "\033[31mError: Environment file '$env_file' not found!\033[0m"
        echo -e "\033[34mRun \`cp .env.example .env\` and fill out the resulting .env file\033[0m"
        return 1
    fi

    echo "Reading configuration from: $env_file"
    echo "Generating service file: $output_file"

    # Source the .env file
    set -a  # Automatically export all variables
    source "$env_file"
    set +a  # Turn off automatic export

    # Check for ALL required variables (assuming all are required now)
    local required_vars=(
        "USER"
        "RPC_URL"
        "PRIORITY_FEE_PAYER_KEYPAIR_PATH"
        "VOTE_AUTHORITY_KEYPAIR_PATH"
        "VALIDATOR_VOTE_ACCOUNT"
        "MINIMUM_BALANCE_SOL"
        "COMMISSION_BPS"
        "PRIORITY_FEE_DISTRIBUTION_PROGRAM"
        "MERKLE_ROOT_UPLOAD_AUTHORITY"
        "FEE_RECORDS_DB_PATH"
        "FEE_RECORDS_DB_BACKUP_PATH"
        "PRIORITY_FEE_LAMPORTS"
        "TRANSACTIONS_PER_EPOCH"
        "RUST_LOG"
    )

    echo "Checking required variables..."
    local missing_vars=()
    for var in "${required_vars[@]}"; do
        if [[ -z "${!var:-}" ]]; then
            missing_vars+=("$var")
        fi
    done

    if [[ ${#missing_vars[@]} -gt 0 ]]; then
        echo -e "\033[31mError: The following required variables are missing or empty in $env_file:\033[0m"
        for var in "${missing_vars[@]}"; do
            case "$var" in
                "USER")
                echo -e "\033[31m  - $var: System user to run the service (e.g., 'root, solana') - to find current user run `whoami`\033[0m"
                    ;;
                "RPC_URL")
                    echo -e "\033[31m  - $var: RPC endpoint URL\033[0m"
                    ;;
                "PRIORITY_FEE_PAYER_KEYPAIR_PATH")
                    echo -e "\033[31m  - $var: Path to validator identity keypair (e.g., '/path/to/validator-keypair.json')\033[0m"
                    ;;
                "VOTE_AUTHORITY_KEYPAIR_PATH")
                    echo -e "\033[31m  - $var: Path to vote authority keypair (e.g., '/path/to/vote-authority.json')\033[0m"
                    ;;
                "VALIDATOR_VOTE_ACCOUNT")
                    echo -e "\033[31m  - $var: Your validator's vote account public key\033[0m"
                    ;;
                "MINIMUM_BALANCE_SOL")
                    echo -e "\033[31m  - $var: Minimum SOL balance to maintain (e.g., '1.0')\033[0m"
                    ;;
                "COMMISSION_BPS")
                    echo -e "\033[31m  - $var: Commission in basis points (e.g., '5000' for 50%)\033[0m"
                    ;;
                "PRIORITY_FEE_DISTRIBUTION_PROGRAM")
                    echo -e "\033[31m  - $var: Priority fee distribution program address\033[0m"
                    ;;
                "MERKLE_ROOT_UPLOAD_AUTHORITY")
                    echo -e "\033[31m  - $var: Merkle root upload authority address\033[0m"
                    ;;
                "FEE_RECORDS_DB_PATH")
                    echo -e "\033[31m  - $var: Path for fee records database (e.g., '/var/lib/solana/fee_records')\033[0m"
                    ;;
                "FEE_RECORDS_DB_BACKUP_PATH")
                    echo -e "\033[31m  - $var: Path for fee records database backup (e.g., '/var/lib/solana/fee_records_backup')\033[0m"
                    ;;
                "PRIORITY_FEE_LAMPORTS")
                    echo -e "\033[31m  - $var: Priority fee in lamports (e.g., '0')\033[0m"
                    ;;
                "TRANSACTIONS_PER_EPOCH")
                    echo -e "\033[31m  - $var: Number of transactions per epoch (e.g., '10')\033[0m"
                    ;;
                "RUST_LOG")
                    echo -e "\033[31m  - $var: Log level (e.g., 'info, debug')\033[0m"
                    ;;
            esac
        done
        echo ""
        echo -e "\033[34mPlease fill in all required values in your $env_file file and run this script again.\033[0m"
        return 1
    fi

    # Get the binary path - try priority-fee-sharing first, then priority-fee-share
    local binary_path=""
    if command -v priority-fee-sharing &>/dev/null; then
        binary_path=$(which priority-fee-sharing)
    elif command -v priority-fee-share &>/dev/null; then
        binary_path=$(which priority-fee-share)
    else
        binary_path="/usr/local/bin/priority-fee-sharing"
        echo -e "\033[33mWarning: Binary not found in PATH. Using default path: $binary_path\033[0m"
        echo -e "\033[33mMake sure to update the ExecStart path in the generated service file if needed.\033[0m"
    fi

    echo "Generating systemd service file..."

    # Generate the service file
    cat > "$output_file" << EOF
[Unit]
Description=Priority Fee Sharing Service
After=network.target

[Service]
Type=simple
User=$USER

# --------------- REQUIRED --------------------
# RPC URL - This RPC needs to be able to call \`get_block\`. If using a local RPC, ensure it is running with \`--enable-rpc-transaction-history\`
Environment=RPC_URL=$RPC_URL
# The account that the priority fees are paid out from - this is usually the validator's identity keypair
Environment=PRIORITY_FEE_PAYER_KEYPAIR_PATH=$PRIORITY_FEE_PAYER_KEYPAIR_PATH
# The vote authority needed to create the PriorityFeeDistribution Account
# Can be found by running \`solana vote-account YOUR_VOTE_ACCOUNT\`
Environment=VOTE_AUTHORITY_KEYPAIR_PATH=$VOTE_AUTHORITY_KEYPAIR_PATH
# Your validator vote account address
Environment=VALIDATOR_VOTE_ACCOUNT=$VALIDATOR_VOTE_ACCOUNT
# Minimum balance in your priority fee keypair, no fees will be sent if below this amount
Environment=MINIMUM_BALANCE_SOL=$MINIMUM_BALANCE_SOL

# --------------- DEFAULTS --------------------
# How much priority fees to keep in bps ( Suggested 5000 - 50% )
Environment=COMMISSION_BPS=$COMMISSION_BPS
# The Priority Fee Distribution Program
Environment=PRIORITY_FEE_DISTRIBUTION_PROGRAM=$PRIORITY_FEE_DISTRIBUTION_PROGRAM
# The merkle root upload authority
Environment=MERKLE_ROOT_UPLOAD_AUTHORITY=$MERKLE_ROOT_UPLOAD_AUTHORITY
# Rocks DB that holds all priority fee records - this will be created by the script and can go anywhere
Environment=FEE_RECORDS_DB_PATH=$FEE_RECORDS_DB_PATH
# Rocks DB backup path for fee records database
Environment=FEE_RECORDS_DB_BACKUP_PATH=$FEE_RECORDS_DB_BACKUP_PATH

# --------------- PERFORMANCE --------------------
# Priority fee for sending share transactions (in lamports)
Environment=PRIORITY_FEE_LAMPORTS=$PRIORITY_FEE_LAMPORTS
# How many TXs to send per epoch
Environment=TRANSACTIONS_PER_EPOCH=$TRANSACTIONS_PER_EPOCH

# --------------- LOGGING -------------------------
# Log level
Environment=RUST_LOG=$RUST_LOG

# --------------- PATH REQUIRED --------------------
ExecStart=$binary_path run
Restart=on-failure
RestartSec=5s

[Install]
WantedBy=multi-user.target
EOF

    echo -e "\033[32m✅ Service file generated successfully: $output_file\033[0m"

    return 0
}

#################################################
# CHECK OR CREATE DIRECTORY (REUSABLE FUNCTION)
#################################################
check_or_create_directory() {
    local dir_path="$1"
    local dir_description="$2"

    if [[ -z "$dir_path" ]]; then
        echo -e "\033[31mError: Directory path not provided\033[0m"
        return 1
    fi

    echo "Checking $dir_description directory: $dir_path"

    # Check if directory exists
    if [[ -d "$dir_path" ]]; then
        echo "✅ $dir_description directory already exists: $dir_path"

        # Check permissions
        if [[ -r "$dir_path" && -w "$dir_path" ]]; then
            echo "✅ Directory is read/writable"
        else
            echo -e "\033[33m⚠️  Warning: Directory exists but may not be writable by current user\033[0m"
            echo "You may need to adjust permissions or run the service as a different user"
            echo -e "Run to set permissions: \033[34msudo chmod 777 $dir_path\033[0m"
            return 1
        fi
    else
        echo "📁 Creating $dir_description directory: $dir_path"

        # Try to create the directory
        if mkdir -p "$dir_path" 2>/dev/null; then
            echo "✅ Successfully created directory: $dir_path"
        else
            echo -e "\033[31m❌ Failed to create directory: $dir_path\033[0m"
            echo "Either, use sudo to create and update permissions on the database directory"
            echo -e "Run to create directory and set permissions: \033[34msudo mkdir -p $dir_path && sudo chmod 777 $dir_path\033[0m"
            echo "Or, change the path in your .env file to a different location"
            return 1
        fi

        # Set appropriate permissions if we created it successfully
        if chmod 755 "$dir_path" 2>/dev/null; then
            echo "✅ Set directory permissions to 755"
        else
            echo -e "\033[33m⚠️  Warning: Could not set directory permissions\033[0m"
        fi
    fi

    return 0
}

#################################################
# CHECK OR CREATE FEE RECORDS DIRECTORIES
#################################################
check_or_create_fee_records_directories() {
    local env_file=".env"

    # Check if .env file exists
    if [[ ! -f "$env_file" ]]; then
        echo -e "\033[31mError: Environment file '$env_file' not found!\033[0m"
        echo -e "\033[34mRun \`cp .env.example .env\` and fill out the resulting .env file\033[0m"
        return 1
    fi

    echo "Reading directory paths from: $env_file"

    # Source the .env file to get directory paths
    set -a  # Automatically export all variables
    source "$env_file"
    set +a  # Turn off automatic export

    # Check if FEE_RECORDS_DB_PATH is set
    if [[ -z "${FEE_RECORDS_DB_PATH:-}" ]]; then
        echo -e "\033[31mError: FEE_RECORDS_DB_PATH is not set in $env_file\033[0m"
        echo -e "\033[34mPlease set FEE_RECORDS_DB_PATH to the desired database path (e.g., '/var/lib/solana/fee_records')\033[0m"
        return 1
    fi

    if [[ -z "${FEE_RECORDS_DB_BACKUP_PATH:-}" ]]; then
        echo -e "\033[31mError: FEE_RECORDS_DB_BACKUP_PATH is not set in $env_file\033[0m"
        echo -e "\033[34mPlease set FEE_RECORDS_DB_BACKUP_PATH to the desired backup path (e.g., '/var/lib/solana/fee_records_backup')\033[0m"
        return 1
    fi

    # Check/create main fee records directory
    check_or_create_directory "$FEE_RECORDS_DB_PATH" "fee records" || return 1

    # Check/create backup directory
    check_or_create_directory "$FEE_RECORDS_DB_BACKUP_PATH" "fee records backup" || return 1

    return 0
}

#################################################
# MAIN SCRIPT
#################################################
main() {
    echo "========================================================="
    echo "      Priority Fee Sharing CLI Installation Script      "
    echo "========================================================="
    echo ""
    echo "This script will:"
    echo "1. Install/update Rust (minimum version 1.75.0)"
    echo "2. Build and install the Priority Fee Sharing CLI"
    echo "3. Check or create fee records directory"
    echo "4. Generate systemd service file from .env configuration"
    echo ""

    # Install Cargo/Rust
    echo "========================================================="
    echo "              INSTALLING/CHECKING RUST                  "
    echo "========================================================="
    install_cargo || {
        echo -e "\033[31m❌ Failed to install/update Rust\033[0m"
        exit 1
    }
    echo ""

    # Install CLI
    echo "========================================================="
    echo "              BUILDING AND INSTALLING CLI               "
    echo "========================================================="
    install_cli || {
        echo -e "\033[31m❌ Failed to install CLI\033[0m"
        exit 1
    }
    echo ""

    # Generate service file if .env exists
    echo "========================================================="
    echo "              GENERATING SERVICE FILE                   "
    echo "========================================================="
    if [[ -f ".env" ]]; then
        generate_service_file
    else
        echo -e "\033[33mNo .env file found. Skipping service file generation.\033[0m"
        echo -e "\033[34mTo generate a service file later, create a .env file and run this script again.\033[0m"
    fi
    echo ""

    # Check or create fee records directory if .env exists
    echo "========================================================="
    echo "              CHECK OR CREATE FEE RECORDS DIRECTORY      "
    echo "========================================================="
    if [[ -f ".env" ]]; then
        check_or_create_fee_records_directories
    else
        echo -e "\033[33mNo .env file found. Skipping fee records directory check.\033[0m"
        echo -e "\033[34mTo check/create the fee records directory later, create a .env file and run this script again.\033[0m"
    fi
    echo ""

    echo "========================================================="
    echo "                   INSTALLATION COMPLETE                "
    echo "========================================================="
    echo -e "\033[32m✅ Priority Fee Sharing CLI installation completed successfully!\033[0m"
    echo ""
    echo "Available commands:"
    echo -e "Show CLI help: \033[34mpriority-fee-sharing --help\033[0m"
    echo -e "Run manually: \033[34mpriority-fee-sharing run\033[0m"
    echo -e "Show export command help: \033[34mpriority-fee-sharing export-csv --help\033[0m"
    echo -e "Show info command help: \033[34mpriority-fee-sharing print-info --help\033[0m"
    echo ""
    echo -e "Next steps:"
    echo -e "1. Review the generated service file: \033[34mcat priority-fee-sharing.service\033[0m"
    echo -e "2. Copy to systemd directory: \033[34msudo cp priority-fee-sharing.service /etc/systemd/system/\033[0m"
    echo -e "3. Reload systemd: \033[34msudo systemctl daemon-reload\033[0m"
    echo -e "4. Enable service: \033[34msudo systemctl enable priority-fee-sharing\033[0m"
    echo -e "5. Start service: \033[34msudo systemctl start priority-fee-sharing\033[0m"
    echo -e "6. Check status: \033[34msudo systemctl status priority-fee-sharing\033[0m"
    echo -e "7. View logs: \033[34msudo journalctl -u priority-fee-sharing.service -f\033[0m"
    echo ""
    echo -e "2-7. All together:"
    echo -e "\033[34msudo cp priority-fee-sharing.service /etc/systemd/system/ && \\"
    echo -e "sudo systemctl daemon-reload && \\"
    echo -e "sudo systemctl enable priority-fee-sharing && \\"
    echo -e "sudo systemctl start priority-fee-sharing && \\"
    echo -e "sudo journalctl -u priority-fee-sharing.service -f\033[0m"
    echo ""
}

# Call the main function
main "$@"
