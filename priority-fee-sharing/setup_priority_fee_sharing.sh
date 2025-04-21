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

#################################################
# MAIN SCRIPT
#################################################

echo "Priority Fee Sharing Service Setup"
echo "=================================="

# Check if running as root or with sudo
if [[ $EUID -ne 0 ]]; then
   echo "This script must be run as root or with sudo"
   exit 1
fi

# Get installation path parameter
SERVICE_FILE="/etc/systemd/system/priority-fee-share.service"

echo "This script will set up the Priority Fee Sharing Service."
echo "You will need to provide the following information:"
echo "- RPC URL"
echo "- Payer keypair path"
echo "- Validator vote account address"
echo "- Minimum balance of SOL that the service will maintain"
echo

# Get RPC URL with comment
RPC_URL=$(ask_string "Enter your RPC URL" "http://localhost:8899")
# Check if RPC URL is using port 8899 ( Local )
if [[ "$RPC_URL" == *":8899" ]]; then
    echo -e "\033[31m\033[1mIf you are using your local RPC, you have to run your validator with \`--enable-rpc-transaction-history\` enabled.\033[0m"
fi

# Get other required parameters
PRIORITY_FEE_KEYPAIR_PATH=$(ask_string "Enter the path to your payer keypair file" "")
VALIDATOR_ADDRESS=$(ask_string "Enter your validator vote account address" "")
MINIMUM_BALANCE_SOL=$(ask_float "Enter minimum balance to maintain (in SOL)" "")

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

ExecStart=/usr/local/bin/priority-fee-sharing run
Restart=on-failure
RestartSec=5s

[Install]
WantedBy=multi-user.target
EOF


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
