#!/usr/bin/env sh
# Builds jito-solana in a docker container
set -eux

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

DOCKER_BUILDKIT=1 docker build \
  -t jitolabs/build-solana \
  -f dev/Dockerfile . \
  --progress=plain

# Creates a temporary container, copies solana-validator built inside container there and
# removes the temporary container.
docker rm temp
docker container create --name temp jitolabs/build-solana
mkdir -p $SCRIPT_DIR/docker-output
docker container cp temp:/solana/solana-validator $SCRIPT_DIR/docker-output/solana-validator
docker rm temp
