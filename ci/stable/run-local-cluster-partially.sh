#!/usr/bin/env bash
set -eo pipefail

CURRENT=$1
: "${CURRENT:?}"

TOTAL=$2
: "${TOTAL:?}"

if [[ "${CURRENT}" -gt "${TOTAL}" ]]; then
  echo "Error: The value of CURRENT (\$1) cannot be greater than the value of TOTAL (\$2)."
  exit 1
fi

here="$(dirname "$0")"

#shellcheck source=ci/common/shared-functions.sh
source "${here}"/../common/shared-functions.sh

#shellcheck source=ci/stable/common.sh
source "${here}"/common.sh

# List before selecting a partition so new tests cannot disappear from CI.
python3 -m unittest discover -s "${here}" -p test_partition_local_cluster.py

args=(
  --profile ci
  --cargo-profile ci
  --package solana-local-cluster
  --test local_cluster
)
filter=$(
  cargo nextest list "${args[@]}" --message-format json |
    python3 "${here}/partition_local_cluster.py" "${CURRENT}" "${TOTAL}"
)

_ cargo nextest run "${args[@]}" \
  --filterset "${filter}" \
  --test-threads=1 \
  --no-tests=warn
