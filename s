#!/usr/bin/env sh

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

if [ -f .env ]; then
  export $(cat .env | grep -v '#' | awk '/=/ {print $1}')
else
  echo "Missing .env file"
  exit 0
fi

echo "Syncing to host: $HOST"

# sync + build
rsync -avh --delete --exclude .git --exclude target "$SCRIPT_DIR" "$HOST":~/
ssh $HOST "source ~/.profile && cd mev-solana-priv && cargo b --release"
