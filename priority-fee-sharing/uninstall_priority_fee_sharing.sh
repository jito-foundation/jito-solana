#!/bin/bash

# Script to uninstall Priority Fee Sharing CLI
# Usage: ./uninstall.sh [-yf|--yes-force]

set -euo pipefail

#################################################
# GLOBAL VARIABLES
#################################################
declare -a actions_taken=()
FORCE_YES=false

#################################################
# PARSE COMMAND LINE ARGUMENTS
#################################################
parse_args() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            -yf|--yes-force)
                FORCE_YES=true
                shift
                ;;
            -h|--help)
                echo "Usage: $0 [-yf|--yes-force]"
                echo ""
                echo "Options:"
                echo "  -yf, --yes-force    Answer 'yes' to all prompts and skip confirmations"
                echo "  -h, --help          Show this help message"
                echo ""
                echo "WARNING: Using --yes-force will automatically delete all data without confirmation!"
                exit 0
                ;;
            *)
                echo "Unknown option: $1"
                echo "Use -h or --help for usage information"
                exit 1
                ;;
        esac
    done
}

#################################################
# HELPER FUNCTIONS
#################################################

# Function to ask yes/no questions (defaults to no, unless force mode)
ask_yes_no() {
    local question
    question="$1"

    if [[ "$FORCE_YES" == true ]]; then
        echo "$question [y/N]: y (forced)"
        return 0  # Always yes in force mode
    fi

    local response

    while true; do
        echo -n "$question [y/N]: "
        read -r response

        # Default to 'no' if empty response
        if [[ -z "$response" ]]; then
            response="n"
        fi

        case "${response,,}" in
            y|yes)
                return 0  # Yes
                ;;
            n|no)
                return 1  # No
                ;;
            *)
                echo -e "\033[31mPlease answer 'y' or 'n'\033[0m"
                ;;
        esac
    done
}

# Function to ask for confirmation with typed word (skipped in force mode)
ask_type_confirmation() {
    local word
    word="$1"
    local question
    question="$2"

    if [[ "$FORCE_YES" == true ]]; then
        echo "$question"
        echo "Type '$word' to confirm: $word (forced)"
        return 0  # Always confirmed in force mode
    fi

    local response

    echo "$question"
    echo -ne "Type '\033[33m$word\033[0m' to confirm: "
    read -r response

    if [[ "$response" == "$word" ]]; then
        return 0  # Confirmed
    else
        return 1  # Not confirmed
    fi
}

#################################################
# STOP SERVICE
#################################################
stop_service() {
    local service_name
    service_name="priority-fee-sharing"

    echo "========================================================="
    echo "                   STOPPING SERVICE                     "
    echo "========================================================="

    # Check if service exists and is running
    if systemctl is-active --quiet "$service_name" 2>/dev/null; then
        echo "🔍 Service $service_name is currently running"

        if ask_yes_no "🛑 Do you want to stop the $service_name service?"; then
            echo "Stopping $service_name service..."
            if sudo systemctl stop "$service_name"; then
                echo -e "✅ \033[32mService stopped successfully\033[0m"
                actions_taken+=("🛑 Service stopped")
            else
                echo -e "❌ \033[31mFailed to stop service\033[0m"
                return 1
            fi
        else
            echo "⏭️  Skipping service stop"
        fi
    elif systemctl list-unit-files | grep -q "$service_name.service"; then
        echo "🔍 Service $service_name exists but is not running"
        echo "✅ No need to stop service"
    else
        echo "ℹ️  Service $service_name not found - nothing to stop"
    fi

    echo ""
    return 0
}

#################################################
# REMOVE SERVICE FILE
#################################################
remove_service_file() {
    local service_file
    service_file="/etc/systemd/system/priority-fee-sharing.service"

    echo "========================================================="
    echo "                 REMOVING SERVICE FILE                   "
    echo "========================================================="

    if [[ -f "$service_file" ]]; then
        echo "🔍 Found service file: $service_file"

        if ask_yes_no "🗑️  Do you want to remove the service file and disable the service?"; then
            echo "Disabling and removing service..."

            # Disable service first
            if sudo systemctl disable priority-fee-sharing 2>/dev/null; then
                echo "✅ Service disabled"
            else
                echo "ℹ️  Service was not enabled"
            fi

            # Remove service file
            if sudo rm -f "$service_file"; then
                echo "✅ Service file removed"
            else
                echo -e "❌ \033[31mFailed to remove service file\033[0m"
                return 1
            fi

            # Reload systemd
            echo "Reloading systemd daemon..."
            if sudo systemctl daemon-reload; then
                echo -e "✅ \033[32mSystemd daemon reloaded\033[0m"
                actions_taken+=("🗑️ Service file removed and systemd reloaded")
            else
                echo -e "❌ \033[31mFailed to reload systemd daemon\033[0m"
                return 1
            fi
        else
            echo "⏭️  Skipping service file removal"
        fi
    else
        echo "ℹ️  Service file not found - nothing to remove"
    fi

    echo ""
    return 0
}

#################################################
# DELETE DATABASE
#################################################
delete_database() {
    local env_file
    env_file=".env"

    echo "========================================================="
    echo "                 DELETING DATABASE                      "
    echo "========================================================="

    # Check if .env file exists
    if [[ ! -f "$env_file" ]]; then
        echo -e "\033[33m⚠️  Environment file '$env_file' not found!\033[0m"
        echo "Cannot determine database path - skipping database deletion"
        echo ""
        return 0
    fi

    echo "📖 Reading database paths from: $env_file"

    # Source the .env file to get database paths
    set -a  # Automatically export all variables
    source "$env_file"
    set +a  # Turn off automatic export

    # Check if FEE_RECORDS_DB_PATH is set
    if [[ -z "${FEE_RECORDS_DB_PATH:-}" ]]; then
        echo -e "\033[33m⚠️  FEE_RECORDS_DB_PATH is not set in $env_file\033[0m"
        echo "Cannot determine database path - skipping database deletion"
        echo ""
        return 0
    fi

    # Check if backup path is set
    if [[ -z "${FEE_RECORDS_DB_BACKUP_PATH:-}" ]]; then
        echo -e "\033[33m⚠️  FEE_RECORDS_DB_BACKUP_PATH is not set in $env_file\033[0m"
        echo "Will create backup in current directory instead"
        FEE_RECORDS_DB_BACKUP_PATH="."
    fi

    # Check if main database directory exists
    if [[ ! -d "$FEE_RECORDS_DB_PATH" ]]; then
        echo "ℹ️  Database folder does not exist: $FEE_RECORDS_DB_PATH"
        if [[ -d "$FEE_RECORDS_DB_BACKUP_PATH" && "$FEE_RECORDS_DB_BACKUP_PATH" != "." ]]; then
            echo "ℹ️  Backup directory will be preserved: $FEE_RECORDS_DB_BACKUP_PATH"
        fi
        echo ""
        return 0
    fi

    echo "🔍 Found database folder: $FEE_RECORDS_DB_PATH"
    if [[ "$FEE_RECORDS_DB_BACKUP_PATH" != "." ]]; then
        echo "🔍 Backup directory: $FEE_RECORDS_DB_BACKUP_PATH"
    fi
    echo ""

    if [[ "$FORCE_YES" != true ]]; then
        echo -e "\033[31m⚠️  WARNING: This will permanently delete all fee records!\033[0m"
        echo -e "\033[31m⚠️  This action is UNRECOVERABLE and may result in double spending!\033[0m"
        echo ""
    fi

    # First confirmation
    if ask_yes_no "🗑️  Do you want to delete the fee records database ($FEE_RECORDS_DB_PATH)?"; then

        # Create backup first
        local timestamp
        timestamp=$(date +"%Y%m%d_%H%M%S")
        local backup_filename
        backup_filename="fee-records-backup-${timestamp}.tar.gz"

        # Ensure backup directory exists
        if [[ "$FEE_RECORDS_DB_BACKUP_PATH" != "." ]]; then
            echo "📁 Ensuring backup directory exists: $FEE_RECORDS_DB_BACKUP_PATH"
            if ! mkdir -p "$FEE_RECORDS_DB_BACKUP_PATH" 2>/dev/null; then
                echo -e "❌ \033[31mFailed to create backup directory, trying with sudo...\033[0m"
                if ! sudo mkdir -p "$FEE_RECORDS_DB_BACKUP_PATH"; then
                    echo -e "❌ \033[31mFailed to create backup directory\033[0m"
                    echo "Will create backup in current directory instead"
                    FEE_RECORDS_DB_BACKUP_PATH="."
                fi
            fi
        fi

        local backup_path
        backup_path="$FEE_RECORDS_DB_BACKUP_PATH/$backup_filename"

        echo ""
        echo "📦 Creating backup of database folder..."
        echo "Backup path: $backup_path"

        if tar -czf "$backup_path" -C "$(dirname "$FEE_RECORDS_DB_PATH")" "$(basename "$FEE_RECORDS_DB_PATH")" 2>/dev/null; then
            echo -e "✅ \033[32mBackup created successfully: $backup_path\033[0m"
            actions_taken+=("📦 Database backup created: $backup_path")
        else
            echo -e "❌ \033[31mFailed to create backup\033[0m"
            if ! ask_yes_no "Continue with deletion without backup?"; then
                echo -e "⏭️  Database deletion cancelled - \033[32mfee records preserved\033[0m"
                echo ""
                return 0
            fi
        fi

        echo ""
        if [[ "$FORCE_YES" != true ]]; then
            echo -e "\033[31m🚨 FINAL WARNING: This will permanently delete ALL fee records!\033[0m"
            echo -e "\033[31m🚨 This action is UNRECOVERABLE and may result in double spending!\033[0m"
            echo ""
        fi

        # Second confirmation with typed word
        if ask_type_confirmation "delete" "Are you absolutely sure you want to delete the database?"; then
            echo ""
            echo "🗑️  Deleting database folder: $FEE_RECORDS_DB_PATH"

            # Check if folder contains any files
            if [[ -n "$(ls -A "$FEE_RECORDS_DB_PATH" 2>/dev/null)" ]]; then
                if sudo rm -rf "$FEE_RECORDS_DB_PATH"/*; then
                    echo -e "✅ \033[32mDatabase contents deleted successfully\033[0m"
                    actions_taken+=("🗑️ Database contents deleted")

                    # Verify the folder still exists and is empty
                    if [[ -d "$FEE_RECORDS_DB_PATH" ]]; then
                        echo -e "ℹ️  Database folder preserved (empty): $FEE_RECORDS_DB_PATH"
                    fi
                else
                    echo -e "❌ \033[31mFailed to delete database contents\033[0m"
                    return 1
                fi
            else
                echo -e "ℹ️  Database folder is already empty: $FEE_RECORDS_DB_PATH"
                actions_taken+=("ℹ️ Database folder was already empty")
            fi
        else
            echo -e "⏭️  Database deletion cancelled - \033[32mfee records preserved\033[0m"
        fi
    else
        echo -e "⏭️  Skipping database deletion - \033[32mfee records preserved\033[0m"
    fi

    echo ""
    return 0
}

#################################################
# UNINSTALL CLI BINARY
#################################################
uninstall_cli() {
    echo "========================================================="
    echo "                 UNINSTALLING CLI                       "
    echo "========================================================="

    # Check for binary locations
    local binary_paths
    binary_paths=()
    local found_binaries
    found_binaries=()

    # Check common locations
    if command -v priority-fee-sharing &>/dev/null; then
        local pfs_path
        pfs_path=$(which priority-fee-sharing)
        [[ -n "$pfs_path" ]] && binary_paths+=("$pfs_path")
        found_binaries+=("priority-fee-sharing")
    fi

    if command -v priority-fee-share &>/dev/null; then
        local pfs_path
        pfs_path=$(which priority-fee-sharing)
        [[ -n "$pfs_path" ]] && binary_paths+=("$pfs_path")
        found_binaries+=("priority-fee-share")
    fi

    # Check cargo install location
    local cargo_bin
    cargo_bin="$HOME/.cargo/bin/priority-fee-sharing"
    if [[ -f "$cargo_bin" ]]; then
        binary_paths+=("$cargo_bin")
        found_binaries+=("priority-fee-sharing (cargo)")
    fi

    if [[ ${#binary_paths[@]} -eq 0 ]]; then
        echo "ℹ️  No CLI binaries found - nothing to uninstall"
        echo ""
        return 0
    fi

    echo "🔍 Found CLI binaries:"
    for i in "${!binary_paths[@]}"; do
        echo "  - ${found_binaries[$i]}: ${binary_paths[$i]}"
    done
    echo ""

    if ask_yes_no "🗑️  Do you want to remove the CLI binaries?"; then
        echo "Removing CLI binaries..."

        local success
        success=true
        for path in "${binary_paths[@]}"; do
            if [[ -f "$path" ]]; then
                if rm -f "$path" 2>/dev/null || sudo rm -f "$path" 2>/dev/null; then
                    echo "✅ Removed: $path"
                else
                    echo -e "❌ \033[31mFailed to remove: $path\033[0m"
                    success=false
                fi
            fi
        done

        if $success; then
            echo -e "✅ \033[32mAll CLI binaries removed successfully\033[0m"
            actions_taken+=("🗑️ CLI binaries removed")
        else
            echo -e "⚠️  \033[33mSome binaries could not be removed\033[0m"
        fi

        # Try cargo uninstall as well
        if command -v cargo &>/dev/null; then
            echo "Attempting cargo uninstall..."
            if cargo uninstall priority-fee-sharing 2>/dev/null; then
                echo "✅ Cargo uninstall successful"
            else
                echo "ℹ️  Cargo uninstall not needed or failed (this is normal)"
            fi
        fi
    else
        echo "⏭️  Skipping CLI binary removal"
    fi

    echo ""
    return 0
}

#################################################
# MAIN SCRIPT
#################################################
main() {
    # Parse command line arguments first
    parse_args "$@"

    echo "========================================================="
    echo "      Priority Fee Sharing CLI Uninstall Script        "
    echo "========================================================="
    echo ""
    echo "This script will help you uninstall the Priority Fee Sharing CLI"
    echo "and clean up associated files and services."
    echo ""

    if [[ "$FORCE_YES" == true ]]; then
        echo -e "\033[33m🚀 FORCE MODE ENABLED: All prompts will be answered 'yes'\033[0m"
        echo -e "\033[31m⚠️  WARNING: This will automatically delete all data without confirmation!\033[0m"
        echo ""
    else
        echo -e "\033[33m⚠️  WARNING: This process may be irreversible!\033[0m"
        echo ""
    fi

    if ! ask_yes_no "🤔 Do you want to proceed with the uninstallation?"; then
        echo ""
        echo -e "❌ \033[32mUninstallation cancelled\033[0m"
        echo "👋 Goodbye!"
        exit 0
    fi

    echo ""

    # Stop service
    stop_service || {
        echo -e "\033[31m❌ Failed to stop service\033[0m"
        if ! ask_yes_no "Continue with uninstallation anyway?"; then
            exit 1
        fi
    }

    # Remove service file
    remove_service_file || {
        echo -e "\033[31m❌ Failed to remove service file\033[0m"
        if ! ask_yes_no "Continue with uninstallation anyway?"; then
            exit 1
        fi
    }

    # Remove database folder (with double confirmation)
    delete_database || {
        echo -e "\033[31m❌ Failed to delete database\033[0m"
        if ! ask_yes_no "Continue with uninstallation anyway?"; then
            exit 1
        fi
    }

    # Uninstall CLI binary
    uninstall_cli || {
        echo -e "\033[31m❌ Failed to uninstall CLI\033[0m"
        if ! ask_yes_no "Continue anyway?"; then
            exit 1
        fi
    }

    echo "========================================================="
    echo "                UNINSTALLATION COMPLETE                 "
    echo "========================================================="
    echo -e "\033[32m✅ Priority Fee Sharing CLI uninstallation completed!\033[0m"
    echo ""

    # Only show actions that were actually taken
    if [[ ${#actions_taken[@]} -gt 0 ]]; then
        echo "Summary of actions taken:"
        for action in "${actions_taken[@]}"; do
            echo "$action"
        done
        echo ""
    fi

    if [[ "$FORCE_YES" == true ]]; then
        echo -e "\033[33mNote: Force mode was used - all prompts were automatically answered 'yes'\033[0m"
        echo ""
    fi
}

# Call the main function with all arguments
main "$@"
