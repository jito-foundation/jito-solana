#!/usr/bin/env bash
# Validate the final heartbeat fixture correction in normal and coverage runs.
set -e
cd "$(dirname "$0")"/..
source ci/_

cat >pipeline.yml <<'YAML'
steps:
  - name: heartbeat-core-nextest
    command: >-
      ci/docker-run-default-image.sh
      bash -c 'source ci/stable/common.sh; cargo nextest run --locked --profile ci --cargo-profile ci --package solana-core --tests --features dev-context-only-utils --no-fail-fast --retries 0 --verbose'
    agents:
      queue: default
    timeout_in_minutes: 60
  - name: heartbeat-coverage-2
    command: >-
      ci/docker-run-default-image.sh
      env RUST_BACKTRACE=full NO_INTERCEPT=1
      bash -c 'ci/coverage/part-2.sh 2>heartbeat-final-coverage-stderr.log'
    agents:
      queue: default
    timeout_in_minutes: 60
    env:
      FETCH_CODECOV_ENVS: 'true'
    artifact_paths:
      - heartbeat-final-coverage-stderr.log
YAML
cat pipeline.yml
_ buildkite-agent pipeline upload pipeline.yml
