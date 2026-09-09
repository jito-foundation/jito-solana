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

# Run unchanged coverage part 2 with stderr retained for heartbeat diagnosis.
cat >pipeline.yml <<'YAML'
steps:
  - name: coverage-2-heartbeat-diagnostic
    command: >-
      ci/docker-run-default-image.sh
      env RUST_BACKTRACE=full NO_INTERCEPT=1
      bash -c 'ci/coverage/part-2.sh 2>heartbeat-core-stderr.log'
    agents:
      queue: default
    timeout_in_minutes: 60
    env:
      FETCH_CODECOV_ENVS: 'true'
    artifact_paths:
      - heartbeat-core-stderr.log
YAML
echo +++ pipeline
cat pipeline.yml

_ buildkite-agent pipeline upload pipeline.yml
