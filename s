#!/usr/bin/env bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

if [ -f .env ]; then
  export $(cat .env | grep -v '#' | awk '/=/ {print $1}')
else
  echo "Missing .env file"
  exit 0
fi

echo "Syncing to host: $HOST"

# sync to build server, ignoring local builds and local/remote dev ledger
rsync -avh --delete --exclude target --exclude docker-output "$SCRIPT_DIR" "$HOST":~/
