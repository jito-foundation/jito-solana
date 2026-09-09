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

cat > pipeline.yml <<'YAML'
steps:
  - label: "local-cluster-2 diagnostic (no fail fast)"
    command: >-
      ci/docker-run-default-image.sh
      ci/stable/run-local-cluster-partially.sh 2 10
    agents:
      queue: default
    timeout_in_minutes: 30
YAML
echo +++ pipeline
cat pipeline.yml

_ buildkite-agent pipeline upload pipeline.yml
