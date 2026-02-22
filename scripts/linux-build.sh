#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF' 1>&2
Usage:
  bash agave/scripts/linux-build.sh [--max-perf] [--install-deps]

Builds an optimized Agave binary set for Linux (including solana-perf-libs).

Options:
  --max-perf       Enable fat LTO + codegen-units=1 (slower build, slightly better runtime).
  --install-deps   Install common build deps via apt-get (requires sudo).

Env vars:
  RUSTFLAGS        Extra rustc flags (this script appends -C target-cpu=native by default).
  TARGET_CPU       Override target CPU (default: native). Set TARGET_CPU=generic to disable native tuning.
  BUILD_JOBS       Override cargo jobs (default: nproc).
EOF
}

have_cmd() { command -v "$1" >/dev/null 2>&1; }
need_cmd() { have_cmd "$1" || { echo "error: missing required command: $1" >&2; exit 1; }; }

MAX_PERF=0
INSTALL_DEPS=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --max-perf) MAX_PERF=1; shift ;;
    --install-deps) INSTALL_DEPS=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "error: unknown argument: $1" >&2; usage; exit 2 ;;
  esac
done

if [[ "$(uname)" != "Linux" ]]; then
  echo "error: linux-build.sh is intended for Linux hosts" >&2
  exit 2
fi

if [[ "$INSTALL_DEPS" -eq 1 ]]; then
  if [[ "$(id -u)" -ne 0 ]]; then
    need_cmd sudo
  fi
  need_cmd apt-get
  set -x
  sudo -n true 2>/dev/null || true
  sudo apt-get update
  sudo apt-get install -y --no-install-recommends \
    build-essential \
    clang \
    cmake \
    curl \
    git \
    pkg-config \
    libssl-dev \
    libudev-dev \
    protobuf-compiler
  set +x
fi

need_cmd curl
need_cmd git
need_cmd rustup

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
agave_root="$(cd "$script_dir/.." && pwd)"

jobs="${BUILD_JOBS:-}"
if [[ -z "$jobs" ]] && have_cmd nproc; then
  jobs="$(nproc)"
fi

target_cpu="${TARGET_CPU:-native}"
if [[ "$target_cpu" == "native" ]]; then
  if [[ -n "${RUSTFLAGS:-}" ]]; then
    export RUSTFLAGS="${RUSTFLAGS} -C target-cpu=native"
  else
    export RUSTFLAGS="-C target-cpu=native"
  fi
fi

if [[ "$MAX_PERF" -eq 1 ]]; then
  export CARGO_PROFILE_RELEASE_LTO=fat
  export CARGO_PROFILE_RELEASE_CODEGEN_UNITS=1
fi

cd "$agave_root"

echo "==> Fetching solana-perf-libs (SIMD accelerators)..." >&2
./fetch-perf-libs.sh

echo "==> Building agave-validator (release)..." >&2
set -x
./cargo build --release --locked ${jobs:+-j "$jobs"} -p agave-validator
./cargo build --release --locked ${jobs:+-j "$jobs"} -p solana-keygen
set +x

bin="$agave_root/target/release/agave-validator"
if [[ ! -x "$bin" ]]; then
  echo "error: build succeeded but $bin is missing" >&2
  exit 1
fi

echo "==> Done" >&2
echo "agave-validator: $bin" >&2
echo "perf-libs dir:  $agave_root/target/release/perf-libs" >&2
