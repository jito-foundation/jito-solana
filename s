#!/usr/bin/env bash
set -euxo pipefail

REMOTE_DIR="~/jito-solana"
SYNC_MODE="git-tracked" # Default to git tracked sync mode

# Function to display the script's usage
usage() {
  echo "The script syncs the script directory with a remote directory"
  echo "Usage: [--remote_dir </path/to/remote/dir>] [--mode <git-tracked|all>]"
  echo "If no remote directory is specified, we sync with the default remote directory: $REMOTE_DIR"
  echo "If no mode is specified, we use git-tracked files mode"
  echo "Usage: need to run as ./s [--remote_dir </path/to/remote/dir>] [--mode <git|all>] ..."
  echo "  --remote_dir : Remote directory to sync the current directory to. Example --remote_dir /path/to/remote/dir"
  echo "  --mode       : Sync mode - 'git-tracked' for git-tracked files only, 'all' for all files from which the script is run"
}

parse_args() {
  # Loop through the remaining arguments
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --remote_dir)
        if [[ $# -gt 1 ]]; then
          REMOTE_DIR="$2" # Assign the value to IS_DEBUG
          shift         # Consume the value
        else
          echo "Error: Missing value for $1. Example usage: --remote_dir /path/to/remote/dir"
          exit 1
        fi
        ;;
      --mode)
        if [[ $# -gt 1 ]]; then
          if [[ "$2" == "git-tracked" || "$2" == "all" ]]; then
            SYNC_MODE="$2"
            shift
          else
            echo "Error: Invalid mode. Must be either 'git-tracked' or 'all'"
            exit 1
          fi
        else
          echo "Error: Missing value for $1. Must be either 'git-tracked' or 'all'"
          exit 1
        fi
        ;;
      --help)
        # Display usage information
        usage
        exit 0
        ;;
      *)
        # Handle unknown arguments here if needed
        echo "Unknown argument: $1"
        usage
        exit 1
        ;;
    esac
    shift
  done
}

# Parse command-line arguments
parse_args "$@"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

if [ -f .env ]; then
  # Load Environment Variables
  export "$(cat .env | grep -v '#' | awk '/=/ {print $1}')"
else
  echo "Missing .env file"
  exit 0
fi

echo "Using sync mode: $SYNC_MODE"
echo "Syncing $SCRIPT_DIR to host: $HOST:$REMOTE_DIR"

if [[ "$SYNC_MODE" == "git-tracked" ]]; then
  # only sync git tracked files
  rsync -avh --files-from=<(git ls-files "$SCRIPT_DIR" --full-name --recurse-submodules) --delete-after --compress "$SCRIPT_DIR" "$HOST":"$REMOTE_DIR"
  # copy over files to allow running `git rev-parse --short HEAD` in ./f
  rsync -avh --exclude .git/objects/pack --exclude .git/modules --compress "$SCRIPT_DIR"/.git "$HOST":"$REMOTE_DIR"
else
  # Sync all files
  rsync -avh --delete --exclude target --exclude docker-output "$SCRIPT_DIR" --compress "$SCRIPT_DIR" "$HOST":"$REMOTE_DIR"
fi
