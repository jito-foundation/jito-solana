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

if [[ "${BUILDKITE_BRANCH:-}" == "ex/lc2-affinity-probe" ]]; then
  if [[ "${LC2_EXPERIMENT:-}" == "affinity-128" ]]; then
    python3 ci/lc2-diagnostic.py pipeline affinity-128 | buildkite-agent pipeline upload
  elif [[ -n "${LC2_EXPERIMENT:-}" ]]; then
    echo "Unsupported LC2 experiment; expected affinity-128."
    exit 2
  else
    echo "Set LC2_EXPERIMENT=affinity-128 for the bounded affinity contrast."
  fi
  exit 0
fi

_ cargo xtask generate-pipeline
echo +++ pipeline
cat pipeline.yml

_ buildkite-agent pipeline upload pipeline.yml
