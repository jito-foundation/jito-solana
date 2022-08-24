#!/usr/bin/env sh
# Builds jito-solana in a docker container
set -eux

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

DOCKER_BUILDKIT=1 docker build \
  -t jitolabs/build-solana \
  -f dev/Dockerfile . \
  --progress=plain

DOCKER_BUILDKIT=1 docker run \
#  -v "$SCRIPT_DIR":/solana \
  jitolabs/build-solana --progress=plain
