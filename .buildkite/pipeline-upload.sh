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

if [[ "${BUILDKITE_BRANCH:-}" == "ex/fix-lc2-duplicate-repair" ]]; then
  if [[ -n "${LC2_EXPERIMENT:-}" ]]; then
    python3 ci/lc2-diagnostic.py pipeline "$LC2_EXPERIMENT" | buildkite-agent pipeline upload
  else
    echo "LC2 experiment branch: set LC2_EXPERIMENT=baseline, matrix, or validation to run diagnostics."
  fi
  exit 0
fi

_ cargo xtask generate-pipeline
echo +++ pipeline
cat pipeline.yml

_ buildkite-agent pipeline upload pipeline.yml
