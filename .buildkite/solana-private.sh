#!/usr/bin/env bash

# This script is used to dynamically generate and upload the full Buildkite pipeline.
# It should be the ONLY command executed by the initial 'Pipeline Upload' step in the Buildkite UI.

# Exit immediately if a command exits with a non-zero status.
set -e 

# Change to the root of the repository, relative to the script's location.
cd "$(dirname "$0")"/..

# Source shared CI environment variables and utility functions (e.g., the '_' function)
source ci/_

# --- 1. Dynamic Pipeline Generation ---

PIPELINE_FILE="pipeline.yml"

# Execute the primary script responsible for generating the pipeline steps (to stdout), 
# and redirect the output into the designated YAML file.
_ ci/buildkite-solana-private.sh > "$PIPELINE_FILE"

# --- 2. Debugging and Verification ---

# Print a header and the generated pipeline content to the Buildkite logs for debugging.
echo "+++ Generated Pipeline Content"
cat "$PIPELINE_FILE"

# --- 3. Pipeline Upload ---

# Use the Buildkite agent to upload the dynamically generated steps.
_ buildkite-agent pipeline upload "$PIPELINE_FILE"
