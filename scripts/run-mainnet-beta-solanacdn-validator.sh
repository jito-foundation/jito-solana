#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF' 1>&2
Usage:
  run-mainnet-beta-solanacdn-validator.sh [PIPE_API_KEY] [extra agave-validator args...]

Keypair paths (override via env vars; generated if missing):
  IDENTITY_KEYPAIR     (default: $HOME/validator-keypair.json)
  VOTE_ACCOUNT_KEYPAIR (default: $HOME/vote-account-keypair.json)
  LEDGER_DIR           (default: $HOME/ledger if /mnt not writable)

Optional env vars:
  PIPE_API_BASE          (default: https://api.pipedev.network)
  SOLANACDN_TLS_CA_CERT_PATH (where to store/find POP CA cert)
  SOLANACDN_ONLY         (default: "1"; set "0" to disable --solanacdn-only)
  SOLANACDN_HYBRID       (default: "1"; set "0" to disable --solanacdn-hybrid)
  SOLANACDN_HYBRID_STALE_MS (default: 2000; only when SOLANACDN_HYBRID=1)
  SOLANACDN_RACE         (default: "1"; set "0" to disable SolanaCDN vs gossip race metrics)
  SOLANACDN_RACE_SAMPLE_BITS (default: 12; only when SOLANACDN_RACE!=0)
  SOLANACDN_RACE_WINDOW_MS   (default: 5000; only when SOLANACDN_RACE!=0)
  SOLANACDN_METRICS_ADDR (optional; sets --solanacdn-metrics-addr for local /metrics + /solanacdn/status)
  BOOTSTRAP_RPC_ADDRS_URL (optional; sets --bootstrap-rpc-addrs-url to reduce RPC scan noise)
  BOOTSTRAP_RPC_ADDRS    (optional; comma/space-separated; adds --bootstrap-rpc-addr entries)
  SNAPSHOT_MANIFEST_URL  (default: https://data.pipedev.network/snapshot-manifest.json)
  SNAPSHOT_DOWNLOAD_CONCURRENCY       (default: 64)
  SNAPSHOT_DOWNLOAD_CHUNK_SIZE_BYTES  (default: 8388608)
  SNAPSHOT_DOWNLOAD_TIMEOUT_MS        (default: 30000)
  SNAPSHOT_DOWNLOAD_MAX_RETRIES       (default: 3)
  EXPECTED_SHRED_VERSION (adds --expected-shred-version)
  KNOWN_VALIDATORS       (comma/space-separated; adds --known-validator entries)
  KEYGEN_BIN             (path to solana-keygen)
  RPC_PORT               (default: 8899; enables local RPC for /health + debugging)
  RUST_LOG               (validator log filter; default: warn,solana_core::solanacdn=info,agave_validator::bootstrap=info,solana_ledger::blockstore_processor=error)

Notes:
  - This script tries to fully automate SolanaCDN setup for Agave:
      - Reads Pipe API key/base from env, or /etc/solanacdn/agent.env (via sudo) if available.
      - Ensures /etc/solanacdn/tls/ca.crt exists by downloading the SolanaCDN POP CA from the
        Pipe control plane (or solanacdn downloads) when missing.
  - Passing PIPE_API_KEY as argv is supported but NOT recommended (shell history).
EOF
}

have_cmd() { command -v "$1" >/dev/null 2>&1; }
need_cmd() { have_cmd "$1" || { echo "error: missing required command: $1" >&2; exit 1; }; }

need_cmd curl

sudo_maybe() {
  if [[ "$(id -u)" -eq 0 ]]; then
    "$@"
    return
  fi
  if have_cmd sudo; then
    sudo "$@"
    return
  fi
  echo "error: need sudo for: $*" >&2
  exit 1
}

read_env_file_value() {
  local path="$1" key="$2"
  [[ -f "$path" ]] || return 1
  # Safe parse: KEY=VALUE, optional export prefix, optional quotes; no eval.
  # Attempt read without sudo first; then try sudo -n (non-interactive) if needed.
  local raw=""
  raw="$(sed -nE "s/^[[:space:]]*(export[[:space:]]+)?${key}[[:space:]]*=[[:space:]]*(.*)[[:space:]]*$/\\2/p" "$path" 2>/dev/null | head -n 1 | tr -d '\r' || true)"
  if [[ -z "$raw" && "$(id -u)" -ne 0 ]] && have_cmd sudo; then
    raw="$(sudo -n sed -nE "s/^[[:space:]]*(export[[:space:]]+)?${key}[[:space:]]*=[[:space:]]*(.*)[[:space:]]*$/\\2/p" "$path" 2>/dev/null | head -n 1 | tr -d '\r' || true)"
  fi
  raw="${raw#\"}"; raw="${raw%\"}"
  raw="${raw#\'}"; raw="${raw%\'}"
  raw="$(echo "$raw" | xargs || true)"
  [[ -n "$raw" ]] || return 1
  printf '%s' "$raw"
}

args_has() {
  # Check if the remaining argv contains a given flag, in either "--flag" or "--flag=..." form.
  local want="$1"
  shift || true
  local a
  for a in "$@"; do
    if [[ "$a" == "$want" || "$a" == "${want}="* ]]; then
      return 0
    fi
  done
  return 1
}

sha256_file() {
  local path="$1"
  if have_cmd sha256sum; then
    sha256sum "$path" | awk '{print $1}'
  else
    shasum -a 256 "$path" | awk '{print $1}'
  fi
}

PIPE_API_BASE="${PIPE_API_BASE:-${SOLANACDN_AGENT_API_BASE:-}}"
if [[ -z "${PIPE_API_BASE}" && -f /etc/solanacdn/agent.env ]]; then
  PIPE_API_BASE="$(read_env_file_value /etc/solanacdn/agent.env SOLANACDN_AGENT_API_BASE || true)"
fi
PIPE_API_BASE="${PIPE_API_BASE:-https://api.pipedev.network}"
PIPE_API_BASE="${PIPE_API_BASE%/}"

PIPE_API_KEY_ENV="${PIPE_API_KEY:-${SOLANACDN_AGENT_API_TOKEN:-}}"
PIPE_API_KEY_ARG="${1:-}"
if [[ -n "$PIPE_API_KEY_ARG" && "${PIPE_API_KEY_ARG:0:1}" != "-" ]]; then
  PIPE_API_KEY="$PIPE_API_KEY_ARG"
  shift
else
  PIPE_API_KEY="$PIPE_API_KEY_ENV"
fi
if [[ -z "${PIPE_API_KEY}" && -f /etc/solanacdn/agent.env ]]; then
  PIPE_API_KEY="$(read_env_file_value /etc/solanacdn/agent.env SOLANACDN_AGENT_API_TOKEN || true)"
fi
if [[ -z "${PIPE_API_KEY}" ]]; then
  echo "error: missing Pipe API key. Set PIPE_API_KEY (or SOLANACDN_AGENT_API_TOKEN), or configure /etc/solanacdn/agent.env" >&2
  usage
  exit 2
fi

# Ensure POP private-CA trust exists locally (so QUIC to POPs can verify certs).
# This keeps SolanaCDN working even when running without the external solanacdn-agent install.
HOME_DIR="${HOME:-/home/ubuntu}"
CA_DST_SYSTEM="/etc/solanacdn/tls/ca.crt"
CA_DST_USER="${HOME_DIR}/.solanacdn/tls/ca.crt"
CA_DST_TMP="/tmp/solanacdn/tls/ca.crt"
CA_DEFAULTS=("${CA_DST_SYSTEM}" "/opt/solanacdn/tls/ca.crt")

is_default_ca_path() {
  local p="$1"
  for d in "${CA_DEFAULTS[@]}"; do
    if [[ "$p" == "$d" ]]; then
      return 0
    fi
  done
  return 1
}

try_install_ca() {
  local dst="$1"
  local dst_source="$2"
  local tmp_ca="$3"

  # Don't let set -e abort before we can fallback to /tmp.
  (
    set +e
    if [[ "${dst_source}" == "system" ]]; then
      sudo_maybe mkdir -p "$(dirname "${dst}")" || exit 1
      sudo_maybe install -m 0644 "${tmp_ca}" "${dst}" || exit 1
    else
      mkdir -p "$(dirname "${dst}")" || exit 1
      install -m 0644 "${tmp_ca}" "${dst}" || exit 1
    fi
  )
}

CA_DST="${SOLANACDN_TLS_CA_CERT_PATH:-}"
CA_DST_SOURCE="env"
if [[ -z "${CA_DST}" ]]; then
  CA_DST="${CA_DST_SYSTEM}"
  CA_DST_SOURCE="system"
  # If the system CA exists, use it. Otherwise, prefer a user-writable path unless running as root.
  if [[ ! -s "${CA_DST_SYSTEM}" && "$(id -u)" -ne 0 ]]; then
    CA_DST="${CA_DST_USER}"
    CA_DST_SOURCE="user"
  fi
fi

SOLANACDN_TLS_CA_CERT_PATH_ARG=""
if ! is_default_ca_path "${CA_DST}"; then
  SOLANACDN_TLS_CA_CERT_PATH_ARG="${CA_DST}"
fi

if [[ ! -s "${CA_DST}" ]]; then
  echo "==> Installing SolanaCDN POP CA (strict TLS) to ${CA_DST}" >&2
  tls_json="$(curl -fsSL "${PIPE_API_BASE}/solanacdn/tls" 2>/dev/null || true)"
  ca_url=""
  ca_sha=""

  if [[ -n "${tls_json}" ]]; then
    if have_cmd jq; then
      ca_url="$(printf '%s' "${tls_json}" | jq -r '.pop_ca_cert_url // empty' 2>/dev/null || true)"
      ca_sha="$(printf '%s' "${tls_json}" | jq -r '.pop_ca_cert_sha256 // empty' 2>/dev/null || true)"
    elif have_cmd python3; then
      ca_url="$(python3 - <<'PY' "${tls_json}" 2>/dev/null || true
import json,sys
doc=json.loads(sys.argv[1])
print(doc.get("pop_ca_cert_url",""))
PY
)"
      ca_sha="$(python3 - <<'PY' "${tls_json}" 2>/dev/null || true
import json,sys
doc=json.loads(sys.argv[1])
print(doc.get("pop_ca_cert_sha256","") or "")
PY
)"
    else
      ca_url="$(printf '%s' "${tls_json}" | sed -nE 's/.*"pop_ca_cert_url"[[:space:]]*:[[:space:]]*"([^"]+)".*/\1/p' | head -n1)"
      ca_sha="$(printf '%s' "${tls_json}" | sed -nE 's/.*"pop_ca_cert_sha256"[[:space:]]*:[[:space:]]*"([^"]+)".*/\1/p' | head -n1)"
    fi
  fi

  if [[ -z "${ca_url}" ]]; then
    ca_url="https://solanacdn.pipedev.network/downloads/latest/linux-x86_64/solanacdn-pop-ca.crt"
  fi

  tmp_ca="$(mktemp)"
  curl -fsSL "${ca_url}" -o "${tmp_ca}"

  if [[ -n "${ca_sha}" ]]; then
    got="$(sha256_file "${tmp_ca}")"
    if [[ "${got}" != "${ca_sha}" ]]; then
      echo "error: POP CA sha mismatch (expected ${ca_sha}, got ${got})" >&2
      rm -f "${tmp_ca}" || true
      exit 2
    fi
  fi

  if ! try_install_ca "${CA_DST}" "${CA_DST_SOURCE}" "${tmp_ca}"; then
    echo "warn: failed to write CA to ${CA_DST}; falling back to ${CA_DST_TMP}" >&2
    CA_DST="${CA_DST_TMP}"
    CA_DST_SOURCE="tmp"
    SOLANACDN_TLS_CA_CERT_PATH_ARG="${CA_DST}"
    try_install_ca "${CA_DST}" "${CA_DST_SOURCE}" "${tmp_ca}"
  fi
  rm -f "${tmp_ca}" || true
fi

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
agave_root="$(cd "$script_dir/.." && pwd)"

AGAVE_VALIDATOR_BIN="${AGAVE_VALIDATOR_BIN:-$agave_root/target/release/agave-validator}"
if [[ ! -x "$AGAVE_VALIDATOR_BIN" ]]; then
  AGAVE_VALIDATOR_BIN="$agave_root/target/debug/agave-validator"
fi
if [[ ! -x "$AGAVE_VALIDATOR_BIN" ]]; then
  echo "agave-validator binary not found; build it first:" 1>&2
  echo "  (cd \"$agave_root\" && cargo build -p agave-validator --release)" 1>&2
  exit 1
fi

first_existing_file() {
  for p in "$@"; do
    if [[ -n "$p" && -f "$p" ]]; then
      echo "$p"
      return 0
    fi
  done
  return 1
}

IDENTITY_KEYPAIR="${IDENTITY_KEYPAIR:-}"
if [[ -z "${IDENTITY_KEYPAIR}" ]]; then
  IDENTITY_KEYPAIR="$(
    first_existing_file \
      "${HOME_DIR}/agave-rpc-identity.json" \
      "${HOME_DIR}/agave-validator-identity.json" \
      "${HOME_DIR}/validator-keypair.json" \
      "${HOME_DIR}/identity.json" \
      || true
  )"
fi
IDENTITY_KEYPAIR="${IDENTITY_KEYPAIR:-$HOME_DIR/validator-keypair.json}"

VOTE_ACCOUNT_KEYPAIR="${VOTE_ACCOUNT_KEYPAIR:-}"
if [[ -z "${VOTE_ACCOUNT_KEYPAIR}" ]]; then
  VOTE_ACCOUNT_KEYPAIR="$(
    first_existing_file \
      "${HOME_DIR}/vote-account-keypair.json" \
      "${HOME_DIR}/vote-account.json" \
      || true
  )"
fi
VOTE_ACCOUNT_KEYPAIR="${VOTE_ACCOUNT_KEYPAIR:-$HOME_DIR/vote-account-keypair.json}"

if [[ -n "${LEDGER_DIR:-}" ]]; then
  LEDGER_DIR="$LEDGER_DIR"
elif [[ -d "./ledger" ]]; then
  LEDGER_DIR="./ledger"
elif [[ -d "${HOME_DIR}/ledger" ]]; then
  LEDGER_DIR="${HOME_DIR}/ledger"
elif [[ -d /mnt && -w /mnt ]]; then
  LEDGER_DIR="/mnt/ledger"
else
  LEDGER_DIR="${HOME_DIR}/ledger"
fi

KEYGEN_BIN="${KEYGEN_BIN:-}"
find_keygen_bin() {
  if [[ -n "$KEYGEN_BIN" ]]; then
    return 0
  fi
  if command -v solana-keygen >/dev/null 2>&1; then
    KEYGEN_BIN="$(command -v solana-keygen)"
    return 0
  fi
  if [[ -x "$agave_root/target/release/solana-keygen" ]]; then
    KEYGEN_BIN="$agave_root/target/release/solana-keygen"
    return 0
  fi
  if [[ -x "$agave_root/target/debug/solana-keygen" ]]; then
    KEYGEN_BIN="$agave_root/target/debug/solana-keygen"
    return 0
  fi
  return 1
}

need_keygen=false
if [[ ! -f "$IDENTITY_KEYPAIR" || ! -f "$VOTE_ACCOUNT_KEYPAIR" ]]; then
  need_keygen=true
fi
if $need_keygen; then
  if ! find_keygen_bin; then
    echo "solana-keygen not found. Install Solana CLI or build it from this repo:" 1>&2
    echo "  (cd \"$agave_root\" && cargo build -p solana-keygen --release)" 1>&2
    exit 1
  fi

  ensure_keypair() {
    local path="$1"
    local label="$2"
    if [[ -f "$path" ]]; then
      return 0
    fi
    mkdir -p "$(dirname "$path")"
    echo "Generating $label keypair: $path" 1>&2
    "$KEYGEN_BIN" new --no-passphrase -so "$path"
    chmod 600 "$path" || true
  }

  ensure_keypair "$IDENTITY_KEYPAIR" "validator identity"
  ensure_keypair "$VOTE_ACCOUNT_KEYPAIR" "vote account"
fi

# Print pubkeys early so operators know what to fund / delegate.
if find_keygen_bin; then
  identity_pubkey="$("$KEYGEN_BIN" pubkey "$IDENTITY_KEYPAIR" 2>/dev/null || true)"
  vote_pubkey="$("$KEYGEN_BIN" pubkey "$VOTE_ACCOUNT_KEYPAIR" 2>/dev/null || true)"
  if [[ -n "$identity_pubkey" ]]; then
    echo "==> Validator identity pubkey: $identity_pubkey" >&2
    echo "==> Fund this address with SOL for fees: $identity_pubkey" >&2
  fi
  if [[ -n "$vote_pubkey" ]]; then
    echo "==> Vote account pubkey: $vote_pubkey" >&2
  fi
else
  echo "==> Identity keypair: $IDENTITY_KEYPAIR" >&2
  echo "==> Vote keypair:     $VOTE_ACCOUNT_KEYPAIR" >&2
  echo "==> Install/build solana-keygen to print pubkeys:" >&2
  echo "    (cd \"$agave_root\" && cargo build -p solana-keygen --release)" >&2
fi
if ! mkdir -p "$LEDGER_DIR"; then
  echo "Cannot create ledger dir: $LEDGER_DIR" 1>&2
  echo "Set LEDGER_DIR to a writable path, or create/chown it (eg: sudo mkdir -p /mnt/ledger && sudo chown \"$(id -u):$(id -g)\" /mnt/ledger)" 1>&2
  exit 1
fi

EXPECTED_GENESIS_HASH="${EXPECTED_GENESIS_HASH:-5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d}"
EXPECTED_SHRED_VERSION="${EXPECTED_SHRED_VERSION:-}"
# Validator requires a sufficiently wide range for dynamically assigned ports.
DYNAMIC_PORT_RANGE="${DYNAMIC_PORT_RANGE:-8000-8025}"
RPC_PORT="${RPC_PORT:-8899}"
SNAPSHOT_MANIFEST_URL="${SNAPSHOT_MANIFEST_URL:-https://data.pipedev.network/snapshot-manifest.json}"
SNAPSHOT_DOWNLOAD_CONCURRENCY="${SNAPSHOT_DOWNLOAD_CONCURRENCY:-64}"
SNAPSHOT_DOWNLOAD_CHUNK_SIZE_BYTES="${SNAPSHOT_DOWNLOAD_CHUNK_SIZE_BYTES:-8388608}"
SNAPSHOT_DOWNLOAD_TIMEOUT_MS="${SNAPSHOT_DOWNLOAD_TIMEOUT_MS:-30000}"
SNAPSHOT_DOWNLOAD_MAX_RETRIES="${SNAPSHOT_DOWNLOAD_MAX_RETRIES:-3}"

ENTRYPOINT_1="${ENTRYPOINT_1:-entrypoint.mainnet-beta.solana.com:8001}"
ENTRYPOINT_2="${ENTRYPOINT_2:-entrypoint2.mainnet-beta.solana.com:8001}"
ENTRYPOINT_3="${ENTRYPOINT_3:-entrypoint3.mainnet-beta.solana.com:8001}"
ENTRYPOINT_4="${ENTRYPOINT_4:-entrypoint4.mainnet-beta.solana.com:8001}"

args=(
  --identity "$IDENTITY_KEYPAIR"
  --vote-account "$VOTE_ACCOUNT_KEYPAIR"
  --ledger "$LEDGER_DIR"
  --entrypoint "$ENTRYPOINT_1"
  --entrypoint "$ENTRYPOINT_2"
  --entrypoint "$ENTRYPOINT_3"
  --entrypoint "$ENTRYPOINT_4"
  --expected-genesis-hash "$EXPECTED_GENESIS_HASH"
  --dynamic-port-range "$DYNAMIC_PORT_RANGE"
  --snapshot-manifest-url "$SNAPSHOT_MANIFEST_URL"
  --snapshot-download-concurrency "$SNAPSHOT_DOWNLOAD_CONCURRENCY"
  --snapshot-download-chunk-size-bytes "$SNAPSHOT_DOWNLOAD_CHUNK_SIZE_BYTES"
  --snapshot-download-timeout-ms "$SNAPSHOT_DOWNLOAD_TIMEOUT_MS"
  --snapshot-download-max-retries "$SNAPSHOT_DOWNLOAD_MAX_RETRIES"
  --rpc-port "$RPC_PORT"
  --private-rpc
  --log -
)

export PIPE_API_BASE="${PIPE_API_BASE}"
export PIPE_API_KEY="${PIPE_API_KEY}"
export SOLANACDN_AGENT_API_BASE="${PIPE_API_BASE}"
export SOLANACDN_AGENT_API_TOKEN="${PIPE_API_KEY}"

# Keep stdout/stderr readable by default: suppress noisy validator logs but keep SolanaCDN logs at info.
# Users can override by exporting RUST_LOG/_RUST_LOG (env_logger format).
if [[ -z "${RUST_LOG:-}" && -z "${_RUST_LOG:-}" ]]; then
  export RUST_LOG="warn,solana_core::solanacdn=info,agave_validator::bootstrap=info,solana_ledger::blockstore_processor=error"
fi

if [[ -n "${SOLANACDN_TLS_CA_CERT_PATH_ARG}" ]]; then
  args+=(--solanacdn-tls-ca-cert-path "${SOLANACDN_TLS_CA_CERT_PATH_ARG}")
fi

if [[ -n "${SOLANACDN_METRICS_ADDR:-}" ]] && ! args_has --solanacdn-metrics-addr "$@"; then
  args+=(--solanacdn-metrics-addr "${SOLANACDN_METRICS_ADDR}")
fi

if [[ -n "${BOOTSTRAP_RPC_ADDRS_URL:-}" ]] && ! args_has --bootstrap-rpc-addrs-url "$@"; then
  args+=(--bootstrap-rpc-addrs-url "${BOOTSTRAP_RPC_ADDRS_URL}")
fi

if [[ -n "${BOOTSTRAP_RPC_ADDRS:-}" ]] && ! args_has --bootstrap-rpc-addr "$@"; then
  # comma- or whitespace-separated HOST:PORT values
  # shellcheck disable=SC2206
  bootstrap_rpc_addrs=( ${BOOTSTRAP_RPC_ADDRS//,/ } )
  for addr in "${bootstrap_rpc_addrs[@]}"; do
    args+=(--bootstrap-rpc-addr "$addr")
  done
fi

# Enable/disable SolanaCDN vs gossip race metrics (enabled by default).
race_env="${SOLANACDN_RACE:-1}"
race_env="$(echo "${race_env}" | tr '[:upper:]' '[:lower:]' | xargs || true)"
race_disabled=0
if [[ "${race_env}" == "0" || "${race_env}" == "false" || "${race_env}" == "no" || "${race_env}" == "off" ]]; then
  race_disabled=1
fi
if ! args_has --solanacdn-race "$@" && [[ "${race_disabled}" -eq 1 ]]; then
  args+=(--solanacdn-race=false)
fi
if [[ "${race_disabled}" -eq 0 ]]; then
  if [[ -n "${SOLANACDN_RACE_SAMPLE_BITS:-}" ]] && ! args_has --solanacdn-race-sample-bits "$@"; then
    args+=(--solanacdn-race-sample-bits "${SOLANACDN_RACE_SAMPLE_BITS}")
  fi
  if [[ -n "${SOLANACDN_RACE_WINDOW_MS:-}" ]] && ! args_has --solanacdn-race-window-ms "$@"; then
    args+=(--solanacdn-race-window-ms "${SOLANACDN_RACE_WINDOW_MS}")
  fi
fi

# Respect explicit SolanaCDN ingest mode flags passed by the caller.
if ! args_has --solanacdn-only "$@" \
  && ! args_has --solanacdn-hybrid "$@"; then
  if [[ "${SOLANACDN_HYBRID:-1}" == "1" ]]; then
    args+=(--solanacdn-hybrid)
    args+=(--solanacdn-hybrid-stale-ms "${SOLANACDN_HYBRID_STALE_MS:-2000}")
  elif [[ "${SOLANACDN_ONLY:-1}" != "0" ]]; then
    args+=(--solanacdn-only)
  fi
fi

if [[ -n "$EXPECTED_SHRED_VERSION" ]]; then
  args+=(--expected-shred-version "$EXPECTED_SHRED_VERSION")
fi

if [[ -n "${KNOWN_VALIDATORS:-}" ]]; then
  # comma- or whitespace-separated pubkeys
  # shellcheck disable=SC2206
  known_validators=( ${KNOWN_VALIDATORS//,/ } )
  for pubkey in "${known_validators[@]}"; do
    args+=(--known-validator "$pubkey")
  done
fi

exec "$AGAVE_VALIDATOR_BIN" "${args[@]}" "$@"
