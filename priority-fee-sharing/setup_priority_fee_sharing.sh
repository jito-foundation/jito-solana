#!/bin/bash

#################################################
# ALL VARIABLES
#################################################
# Default values
RPC_URL_DEFAULT=""
KEYPAIR_PATH_DEFAULT=""
VALIDATOR_ADDRESS_DEFAULT=""
MINIMUM_BALANCE_SOL_DEFAULT="100.0"

# Required parameters ( Will be filled out in script )
RPC_URL=""
PRIORITY_FEE_KEYPAIR_PATH=""
VALIDATOR_ADDRESS=""
MINIMUM_BALANCE_SOL=""

# Optional parameters with defaults
FEE_RECORDS_DB_PATH="/var/lib/solana/fee_records"
PRIORITY_FEE_DISTRIBUTION_PROGRAM="F2Zu7QZiTYUhPd7u9ukRVwxh7B71oA3NMJcHuCHc29P2"
COMMISSION_BPS="5000"
CHUNK_SIZE="1"
CALL_LIMIT="1"
GO_LIVE_EPOCH="1000"

# Service configuration
SERVICE_FILE="/etc/systemd/system/priority-fee-share.service"
CLI_PATH=""
SERVICE_NAME=""

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
# INSTALL CARGO
#################################################
install_cargo() {
    # Check if cargo is in PATH
    if command -v cargo &> /dev/null; then
        echo "✅ Cargo is already installed!"
        cargo --version
        return 0
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
            return 0
        else
            echo "❌ Something went wrong with the Cargo installation."
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
    git submodule update --init --recursive

    # Install the CLI
    echo "Installing CLI..."
    cargo install --path .

    echo
    echo -e "Installed CLI, run: \033[34mpriority-fee-sharing --help\033[0m"

    # Set CLI path
    CLI_PATH=$(which priority-fee-sharing)
    echo "CLI_PATH: $CLI_PATH"

    return 0
}

#################################################
# SETUP SERVICE FILE
#################################################
setup_service_file() {
    # Create the fee records directory if it doesn't exist
    sudo mkdir -p "$FEE_RECORDS_DB_PATH"
    sudo chmod 777 "$FEE_RECORDS_DB_PATH"
    echo
    echo -e "Created fee records directory at \033[34m$FEE_RECORDS_DB_PATH\033[0m"

    # Create the service file directory if it doesn't exist
    sudo mkdir -p "$(dirname "$SERVICE_FILE")"

    cp "priority-fee-share.service" ".priority-fee-share.service"

    # Now modify the service file with the correct values
    # Replace RPC_URL
    sed -i "s|RPC_URL=.*|RPC_URL=$RPC_URL|g" .priority-fee-share.service

    # Replace FEE_RECORDS_DB_PATH
    sed -i "s|FEE_RECORDS_DB_PATH=.*|FEE_RECORDS_DB_PATH=$FEE_RECORDS_DB_PATH|g" .priority-fee-share.service

    # Replace PRIORITY_FEE_KEYPAIR_PATH
    sed -i "s|PRIORITY_FEE_KEYPAIR_PATH=.*|PRIORITY_FEE_KEYPAIR_PATH=$PRIORITY_FEE_KEYPAIR_PATH|g" .priority-fee-share.service

    # Replace VALIDATOR_ADDRESS
    sed -i "s|VALIDATOR_ADDRESS=.*|VALIDATOR_ADDRESS=$VALIDATOR_ADDRESS|g" .priority-fee-share.service

    # Replace MINIMUM_BALANCE_SOL
    sed -i "s|MINIMUM_BALANCE_SOL=.*|MINIMUM_BALANCE_SOL=$MINIMUM_BALANCE_SOL|g" .priority-fee-share.service

    # Replace optional parameters if they differ from defaults
    sed -i "s|PRIORITY_FEE_DISTRIBUTION_PROGRAM=.*|PRIORITY_FEE_DISTRIBUTION_PROGRAM=$PRIORITY_FEE_DISTRIBUTION_PROGRAM|g" .priority-fee-share.service
    sed -i "s|COMMISSION_BPS=.*|COMMISSION_BPS=$COMMISSION_BPS|g" .priority-fee-share.service
    sed -i "s|CHUNK_SIZE=.*|CHUNK_SIZE=$CHUNK_SIZE|g" .priority-fee-share.service
    sed -i "s|CALL_LIMIT=.*|CALL_LIMIT=$CALL_LIMIT|g" .priority-fee-share.service
    sed -i "s|GO_LIVE_EPOCH=.*|GO_LIVE_EPOCH=$GO_LIVE_EPOCH|g" .priority-fee-share.service

    # Replace the ExecStart path with the actual CLI path
    sed -i "s|ExecStart=.*|ExecStart=$CLI_PATH run|g" .priority-fee-share.service

    # Copy the modified service file to the system directory
    sudo cp .priority-fee-share.service "$SERVICE_FILE"

    echo
    echo -e "Service file created at \033[34m$SERVICE_FILE\033[0m"

    # Reload systemd
    sudo systemctl daemon-reload

    # Extract service name from service file path
    SERVICE_NAME=$(basename "$SERVICE_FILE")

    # Enable the service to start on boot
    sudo systemctl enable "$SERVICE_NAME"
    echo "Service enabled to start on boot"

    if ask_yes_no "Start the service now?" "Y"; then
        sudo systemctl stop "$SERVICE_NAME" 2>/dev/null
        sudo systemctl start "$SERVICE_NAME"
        echo "Service started"

        # Check service status
        echo
        echo "Service status:"
        sudo systemctl status "$SERVICE_NAME" --no-pager
    fi

    return 0
}

collect_parameters() {
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
}

display_instructions() {
    echo
    echo "Setup complete! You can manage the service with these commands:"
    echo -e "  \033[32msudo systemctl start $SERVICE_NAME\033[0m    # Start the service"
    echo -e "  \033[32msudo systemctl stop $SERVICE_NAME\033[0m     # Stop the service"
    echo -e "  \033[32msudo systemctl restart $SERVICE_NAME\033[0m  # Restart the service"
    echo -e "  \033[32msudo systemctl status $SERVICE_NAME\033[0m   # Check service status"
    echo -e "  \033[32msudo journalctl -u $SERVICE_NAME -f\033[0m   # View service logs"
}

#################################################
# MAIN SCRIPT
#################################################
main() {
    echo "Priority Fee Sharing Service Setup"
    echo "=================================="

    # Get Solana config values
    get_solana_config

    # Collect parameters from user
    collect_parameters

    # Install Cargo
    install_cargo || exit 1

    # Install CLI
    install_cli || exit 1

    # Setup service file
    setup_service_file || exit 1

    # Display instructions
    display_instructions
}

# Call the main function
main
