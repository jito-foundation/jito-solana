#!/bin/bash

#################################################
# HELPER FUNCTIONS
#################################################

# Function to ask a yes/no question with a default answer
ask_yes_no() {
    local prompt="$1"
    local default="$2"
    local yn

    # Set default display and value
    case $default in
        [Yy]*)
            prompt="$prompt [Y/n]: "
            default="Y"
            ;;
        [Nn]*)
            prompt="$prompt [y/N]: "
            default="N"
            ;;
        *)
            echo "Error: Default must be Y or N"
            exit 1
            ;;
    esac

    while true; do
        # Print the prompt and read the answer
        read -p "$prompt" yn

        # If answer is empty, use the default
        if [ -z "$yn" ]; then
            yn=$default
        fi

        # Check the response
        case $yn in
            [Yy]*)
                return 0  # Yes
                ;;
            [Nn]*)
                return 1  # No
                ;;
            *)
                echo "Please answer yes (y) or no (n)."
                ;;
        esac
    done
}

# Function to get a string input with a default value
ask_string() {
    local prompt="$1"
    local default="$2"
    local answer

    # Display prompt with default value
    prompt="$prompt [$default]: "

    # Read the input
    read -p "$prompt" answer

    # If empty answer, use the default
    if [ -z "$answer" ]; then
        answer="$default"
    fi

    # Return the answer
    echo "$answer"
}

# Function to get an integer input with a default value
ask_integer() {
    local prompt="$1"
    local default="$2"
    local answer

    # Display prompt with default value
    prompt="$prompt [$default]: "

    while true; do
        # Read the input
        read -p "$prompt" answer

        # If empty answer, use the default
        if [ -z "$answer" ]; then
            echo "$default"
            return
        fi

        # Check if the answer is an integer
        if [[ "$answer" =~ ^[0-9]+$ ]]; then
            echo "$answer"
            return
        else
            echo "Please enter a valid integer."
        fi
    done
}

# Function to get a floating-point input with a default value
ask_float() {
    local prompt="$1"
    local default="$2"
    local answer

    # Display prompt with default value
    prompt="$prompt [$default]: "

    while true; do
        # Read the input
        read -p "$prompt" answer

        # If empty answer, use the default
        if [ -z "$answer" ]; then
            echo "$default"
            return
        fi

        # Check if the answer is a valid number (integer or float)
        if [[ "$answer" =~ ^[0-9]+(\.[0-9]+)?$ ]]; then
            echo "$answer"
            return
        else
            echo "Please enter a valid number."
        fi
    done
}

# Function to get Solana config values
get_solana_config() {
    # Try to use solana directly (whether it's a command or alias)
    echo "Trying to use solana command or alias..."

    # We need to source the profile to get aliases
    if [ -f ~/.bashrc ]; then
        # Source the bashrc if it exists
        source ~/.bashrc 2>/dev/null
    fi
    if [ -f ~/.bash_profile ]; then
        # Source bash_profile if it exists
        source ~/.bash_profile 2>/dev/null
    fi
    if [ -f ~/.profile ]; then
        # Source profile if it exists
        source ~/.profile 2>/dev/null
    fi

    # Try to run solana command (which should work if it's an alias or command)
    CONFIG_OUTPUT=$(solana config get 2>/dev/null)

    if [ $? -eq 0 ] && [ -n "$CONFIG_OUTPUT" ]; then
        echo "Successfully accessed solana configuration."

        # Extract RPC URL
        RPC_URL_DEFAULT=$(echo "$CONFIG_OUTPUT" | grep "RPC URL:" | sed 's/RPC URL: //')

        # Extract Keypair Path
        KEYPAIR_PATH_DEFAULT=$(echo "$CONFIG_OUTPUT" | grep "Keypair Path:" | sed 's/Keypair Path: //')

        echo "Found Solana configuration:"
        [ -n "$RPC_URL_DEFAULT" ] && echo "- RPC URL: $RPC_URL_DEFAULT"
        [ -n "$KEYPAIR_PATH_DEFAULT" ] && echo "- Keypair Path: $KEYPAIR_PATH_DEFAULT"

        # Try to get validator address
        VALIDATOR_ADDRESS_DEFAULT=$(solana address 2>/dev/null)
        [ -n "$VALIDATOR_ADDRESS_DEFAULT" ] && echo "- Validator Address: $VALIDATOR_ADDRESS_DEFAULT"
    else
        echo "Could not access solana configuration. Using default values."

        # Check common locations for Jito-Solana
        JITO_PATHS=(
            "~/jito-solana/docker-output/solana"
            "/home/$(whoami)/jito-solana/docker-output/solana"
            "/opt/jito-solana/docker-output/solana"
        )

        for path in "${JITO_PATHS[@]}"; do
            eval expanded_path="$path"
            if [ -f "$expanded_path" ]; then
                echo "Found Jito Solana at: $expanded_path"
                CONFIG_OUTPUT=$("$expanded_path" config get 2>/dev/null)

                if [ -n "$CONFIG_OUTPUT" ]; then
                    # Extract RPC URL
                    RPC_URL_DEFAULT=$(echo "$CONFIG_OUTPUT" | grep "RPC URL:" | sed 's/RPC URL: //')

                    # Extract Keypair Path
                    KEYPAIR_PATH_DEFAULT=$(echo "$CONFIG_OUTPUT" | grep "Keypair Path:" | sed 's/Keypair Path: //')

                    echo "Found Solana configuration:"
                    [ -n "$RPC_URL_DEFAULT" ] && echo "- RPC URL: $RPC_URL_DEFAULT"
                    [ -n "$KEYPAIR_PATH_DEFAULT" ] && echo "- Keypair Path: $KEYPAIR_PATH_DEFAULT"

                    # Try to get validator address
                    VALIDATOR_ADDRESS_DEFAULT=$("$expanded_path" address 2>/dev/null)
                    [ -n "$VALIDATOR_ADDRESS_DEFAULT" ] && echo "- Validator Address: $VALIDATOR_ADDRESS_DEFAULT"

                    break
                fi
            fi
        done
    fi
}

#################################################
# MAIN SCRIPT
#################################################

echo "Priority Fee Sharing Service Setup"
echo "=================================="

# Check if running as root or with sudo
if [[ $EUID -ne 0 ]]; then
   echo -e "\033[31m\033[1mThis script must be run as root or with sudo\033[0m"
   exit 1
fi

# Initialize default values
RPC_URL_DEFAULT="http://localhost:8899"
KEYPAIR_PATH_DEFAULT=""
VALIDATOR_ADDRESS_DEFAULT=""
MINIMUM_BALANCE_SOL_DEFAULT="100.0"

# Get Solana config values
get_solana_config

# Get installation path parameter
SERVICE_FILE="/etc/systemd/system/priority-fee-share.service"

echo
echo "This script will set up the Priority Fee Sharing Service."
echo "You will need to provide the following information:"
echo "- RPC URL"
echo "- Payer keypair path"
echo "- Validator vote account address"
echo "- Minimum balance of SOL that the service will maintain"
echo

# Get RPC URL with comment
RPC_URL=$(ask_string "Enter your RPC URL" "${RPC_URL_DEFAULT}")
# Check if RPC URL is using port 8899 (Local)
if [[ "$RPC_URL" == *":8899" ]]; then
    echo -e "\033[31m\033[1mIf you are using your local RPC, you have to run your validator with \`--enable-rpc-transaction-history\` enabled.\033[0m"
fi

# Get other required parameters with detected defaults
PRIORITY_FEE_KEYPAIR_PATH=$(ask_string "Enter the path to your payer keypair file" "${KEYPAIR_PATH_DEFAULT}")
VALIDATOR_ADDRESS=$(ask_string "Enter your validator vote account address" "${VALIDATOR_ADDRESS_DEFAULT}")
MINIMUM_BALANCE_SOL=$(ask_float "Enter minimum balance to maintain (in SOL)" "${MINIMUM_BALANCE_SOL_DEFAULT}")

# Set default values for optional parameters
FEE_RECORDS_DB_PATH="/var/lib/solana/fee_records"
PRIORITY_FEE_DISTRIBUTION_PROGRAM="BBBATax9kikSHQp8UTcyQL3tfU3BmQD9yid5qhC7QEAA"
COMMISSION_BPS="5000"
CHUNK_SIZE="1"
CALL_LIMIT="1"
GO_LIVE_EPOCH="1000"

# Create the service file directory if it doesn't exist
mkdir -p "$(dirname "$SERVICE_FILE")"

# Create the service file
cat > "$SERVICE_FILE" << EOF
[Unit]
Description=Priority Fee Sharing Service
After=network.target

[Service]
Type=simple
User=root

# Required parameters
Environment=RPC_URL=$RPC_URL
Environment=FEE_RECORDS_DB_PATH=$FEE_RECORDS_DB_PATH
Environment=PRIORITY_FEE_KEYPAIR_PATH=$PRIORITY_FEE_KEYPAIR_PATH
Environment=VALIDATOR_ADDRESS=$VALIDATOR_ADDRESS
Environment=MINIMUM_BALANCE_SOL=$MINIMUM_BALANCE_SOL

# Optional parameters with defaults
Environment=PRIORITY_FEE_DISTRIBUTION_PROGRAM=$PRIORITY_FEE_DISTRIBUTION_PROGRAM
Environment=COMMISSION_BPS=$COMMISSION_BPS
Environment=CHUNK_SIZE=$CHUNK_SIZE
Environment=CALL_LIMIT=$CALL_LIMIT
Environment=GO_LIVE_EPOCH=$GO_LIVE_EPOCH

ExecStart=priority-fee-sharing run
Restart=on-failure
RestartSec=5s

[Install]
WantedBy=multi-user.target
EOF

# Check if cargo is in PATH
if command -v cargo &> /dev/null; then
    echo "✅ Cargo is already installed!"
    cargo --version
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

    # Verify installation
    if command -v cargo &> /dev/null; then
        echo "✅ Cargo installation successful!"
        cargo --version
    else
        echo "❌ Something went wrong with the Cargo installation."
        echo "Please try installing manually with:"
        echo "curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh"
        echo "Then run: source \$HOME/.cargo/env"
        exit 1
    fi
fi

# Check if we're in the correct directory
if [ ! -f "Cargo.toml" ]; then
    echo "⚠️ Cannot find Cargo.toml in the current directory."
    echo "Please make sure you are in the priority-fee-sharing directory."

    if ask_yes_no "Try to find and navigate to the priority-fee-sharing directory?" "Y"; then
        # Try to find the priority-fee-sharing directory
        if [ -d "../priority-fee-sharing" ]; then
            cd "../priority-fee-sharing"
            echo "✅ Changed to ../priority-fee-sharing directory"
        elif [ -d "priority-fee-sharing" ]; then
            cd "priority-fee-sharing"
            echo "✅ Changed to priority-fee-sharing directory"
        else
            echo "❌ Could not find the priority-fee-sharing directory."
            echo "Please navigate to the priority-fee-sharing directory and run this script again."
            exit 1
        fi
    else
        echo "Please navigate to the priority-fee-sharing directory and run this script again."
        exit 1
    fi
fi

# Updating repo submodules if git exists
if command -v git &> /dev/null; then
    echo "Updating git submodules..."
    git submodule update --init --recursive
else
    echo "Git not found, skipping submodule update."
fi

# Install the CLI
echo "Installing CLI..."
cargo install --path .

echo
echo -e "Installed CLI, run: \033[34mpriority-fee-sharing --help\033[0m"

# Create the fee records directory if it doesn't exist
mkdir -p "$FEE_RECORDS_DB_PATH"
echo
echo -e "Created fee records directory at \033[34m$FEE_RECORDS_DB_PATH\033[0m"

echo
echo -e "Service file created at \033[34m$SERVICE_FILE\033[0m"

# Reload systemd
systemctl daemon-reload

# Extract service name from service file path
SERVICE_NAME=$(basename "$SERVICE_FILE")

# Enable the service to start on boot
systemctl enable "$SERVICE_NAME"
echo "Service enabled to start on boot"

if ask_yes_no "Start the service now?" "Y"; then
    systemctl start "$SERVICE_NAME"
    echo "Service started"

    # Check service status
    echo
    echo "Service status:"
    systemctl status "$SERVICE_NAME" --no-pager
fi

echo
echo "Setup complete! You can manage the service with these commands:"
echo -e "  \033[32msystemctl start $SERVICE_NAME\033[0m    # Start the service"
echo -e "  \033[32msystemctl stop $SERVICE_NAME\033[0m     # Stop the service"
echo -e "  \033[32msystemctl restart $SERVICE_NAME\033[0m  # Restart the service"
echo -e "  \033[32msystemctl status $SERVICE_NAME\033[0m   # Check service status"
echo -e "  \033[32mjournalctl -u $SERVICE_NAME\033[0m      # View service logs"
