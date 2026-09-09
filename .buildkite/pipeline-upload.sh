#!/usr/bin/env bash
#
# This script is used to upload the full buildkite pipeline. The steps defined
# in the buildkite UI should simply be:
#
#   steps:
#    - command: ".buildkite/pipeline-upload.sh"
#

set -e
cd "$(dirname "$0")"/..
source ci/_

# Supplement only the incomplete stable shard and checks blocked in build 5652.
# Production code, tests, retry policies, and timeouts remain unchanged.
cat >pipeline.yml <<'YAML'
steps:
  - name: stable-1
    command: ci/docker-run-default-image.sh ci/stable/run-partition.sh 1 3
    agents:
      queue: default
    timeout_in_minutes: 25
    retry:
      automatic: 'true'
  - name: stable-sbf
    command: ci/docker-run-default-image.sh ci/test-stable-sbf.sh
    agents:
      queue: default
    timeout_in_minutes: 35
  - name: shuttle
    command: ci/docker-run-default-image.sh ci/test-shuttle.sh
    agents:
      queue: default
    timeout_in_minutes: 10
  - name: coverage-1
    command: ci/docker-run-default-image.sh ci/coverage/part-1.sh
    agents:
      queue: default
    timeout_in_minutes: 60
    env:
      FETCH_CODECOV_ENVS: 'true'
  - name: coverage-2
    command: >-
      ci/docker-run-default-image.sh
      env RUST_BACKTRACE=full NO_INTERCEPT=1
      bash -c 'ci/coverage/part-2.sh 2>coverage-2-stderr.log'
    artifact_paths:
      - coverage-2-stderr.log
    agents:
      queue: default
    timeout_in_minutes: 60
    env:
      FETCH_CODECOV_ENVS: 'true'
  - name: coverage-3
    command: ci/docker-run-default-image.sh ci/coverage/part-3.sh
    agents:
      queue: default
    timeout_in_minutes: 60
    env:
      FETCH_CODECOV_ENVS: 'true'
  - name: cargo audit
    command: ci/docker-run-default-image.sh ci/do-audit.sh
    agents:
      queue: default
    timeout_in_minutes: 10
YAML
echo +++ pipeline
cat pipeline.yml

_ buildkite-agent pipeline upload pipeline.yml
