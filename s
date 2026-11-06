#!/usr/bin/env bash

if [ "${BASH_VERSINFO[0]}" -lt 4 ]; then
  echo "ERROR: Bash 4.0 or higher required" >&2
  echo "Current version: ${BASH_VERSION}" >&2
  if [ "$(uname)" = "Darwin" ]; then
    echo "" >&2
    echo "On MacOS you can install a newer version of bash using:" >&2
    echo "  brew install bash" >&2
  fi
  exit 1
fi

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
cd "$SCRIPT_DIR"

if [[ -f .env ]]; then
  set -a
  # shellcheck source=/dev/null
  source .env
  set +a
else
  echo "Missing .env file" >&2
  exit 1
fi

if [[ -z "${HOST:-}" ]]; then
  echo "Error: HOST is not set in .env" >&2
  exit 1
fi

echo "Syncing to host: $HOST"

# sync to build server, ignoring local builds and local/remote dev ledger
rsync -avh --delete --exclude target --exclude dist --exclude docker-output \
  "$SCRIPT_DIR" "$HOST":~/
