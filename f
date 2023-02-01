#!/usr/bin/env sh
# Builds jito-solana in a docker container.
# Useful for running on machines that might not have cargo installed but can run docker (Flatcar Linux).
# run `./f true` to compile with debug flags

set -eux

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

GIT_SHA="$(git describe --always --dirty)"

echo $GIT_SHA

DEBUG_FLAGS=${1-false}

DOCKER_BUILDKIT=1 docker build \
  --build-arg debug=$DEBUG_FLAGS \
  --build-arg ci_commit=$GIT_SHA \
  -t jitolabs/build-solana \
  -f dev/Dockerfile . \
  --progress=plain

# Creates a temporary container, copies solana-validator built inside container there and
# removes the temporary container.
docker rm temp || true
docker container create --name temp jitolabs/build-solana
mkdir -p $SCRIPT_DIR/docker-output
# Outputs the solana-validator binary to $SOLANA/docker-output/solana-validator
docker container cp temp:/solana/docker-output $SCRIPT_DIR/
docker rm temp
