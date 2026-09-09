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

# Coverage-2 in build 5638 stopped after the live Block Engine test failed.
# Run its remaining instrumented packages and preserve any failed assertions.
cat >pipeline.yml <<'YAML'
steps:
  - name: coverage-2
    command: ci/docker-run-default-image.sh ci/coverage/part-2.sh
    agents:
      queue: default
    timeout_in_minutes: 60
    env:
      FETCH_CODECOV_ENVS: 'true'
YAML
echo +++ pipeline
cat pipeline.yml

_ buildkite-agent pipeline upload pipeline.yml
