#!/usr/bin/env bash
#
# Build jito-solana release artifacts inside a container and export them to
# the host. See dev/BUILD.md for the full workflow.
#
# Bundled binaries (in <basename>-<tag>-<target>.tar.bz2 under bin/):
#   agave-validator          [Jito-patched: BAM, bundles, tip-distribution]
#   agave-ledger-tool        [Jito-patched ledger inspection]
#   agave-watchtower         monitoring
#   solana-genesis           cluster bring-up
#   solana-gossip            cluster diagnostics
#   solana-faucet            testnet faucet
#   solana                   RPC CLI
#   solana-keygen            keypair tooling
#
# Output (default ./dist/):
#   <basename>-<tag>-<target>.tar.bz2   release tarball (bzip2)
#   <basename>-<tag>-<target>.yml       version manifest: channel, commit,
#                                       target
#
# The <target> triple is derived inside the container from the platform the
# build actually runs as, so it always matches the binaries. Use --platform
# to build for a non-native platform (needs emulation or a builder node for
# that platform).
#
# Back-compat output (always at the repo root, regardless of --output):
#   docker-output/<bin>             loose binaries, old pipeline layout
#
# Examples:
#   ./f                                          # release build, tag from git
#   ./f --profile debug
#   ./f --profile release-with-lto --tag v3.0.1
#   ./f --output ./out
#   ./f --platform linux/amd64                   # x86_64 build on a non-x86
#                                                # host (emulated or remote
#                                                # builder node)
#   ./f --checkout v3.0.1                        # build the v3.0.1 ref using
#                                                # the *current* checkout's
#                                                # tooling (worktree-based; no
#                                                # mutation to your HEAD)
#
# Requires: docker with buildx (Docker Engine >=23 or buildx plugin installed).

if [ "${BASH_VERSINFO[0]}" -lt 4 ]; then
  echo "ERROR: Bash 4.0 or higher required" >&2
  echo "Current version: ${BASH_VERSION}" >&2
  if [ "$(uname)" = "Darwin" ]; then
    echo "" >&2
    echo "On MacOS you can install a newer version of bash using:" >&2
    echo "  brew install bash" >&2
  fi
  exit 1
fi

set -o noclobber  # Prevent overwriting existing files with >
set -o nounset    # Treat unset variables as an error
set -o errexit    # Exit immediately if a command exits with non-zero status
set -o pipefail   # Prevent errors in a pipeline from being masked
set -o errtrace   # Ensure that any error traps are inherited by functions

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
cd "$SCRIPT_DIR"

usage() {
  local -r exit_code="${1:-0}"
  cat <<'EOF'
Build jito-solana release artifacts inside a container and export them to ./dist.
Drives Agave's upstream scripts/create-release-tarball.sh through dev/Dockerfile,
with a Jito-curated operator binary set.

Output (default ./dist/):
  <basename>-<tag>-<target>.tar.bz2   release tarball (bzip2)
  <basename>-<tag>-<target>.yml       version manifest: channel, commit, target

The <target> triple is derived inside the container from the actual build
platform, so it always matches the binaries.

Back-compat output (always at the repo root, regardless of --output):
  docker-output/<bin>             loose binaries, old pipeline layout

Usage: ./f [options]

Options:
  --profile PROFILE       release | release-with-debug | release-with-lto | debug
                          (default: release)
  --tag VALUE             channel/tag to embed in version.yml
                          (default: --checkout value, or
                          `git describe --tags --always --dirty`)
  --checkout REF          build a specific git ref (tag/branch/SHA) using a
                          throwaway worktree. The current checkout's f and
                          dev/Dockerfile drive the build, but the source being
                          compiled comes from REF. Your working tree and HEAD
                          are not modified. The ref must already exist locally
                          (run `git fetch --tags origin` first if needed).
  --platform PLATFORM     docker platform to build as, e.g. linux/amd64
                          (default: the builder's native platform). Single
                          platform only. Non-native platforms need QEMU
                          emulation (slow) or a buildx builder node of that
                          platform.
  --basename NAME         tarball/yml base name (default: jito-solana-release);
                          the channel/tag is appended automatically
  --no-val-bins           drop validator/operator binaries from the tarball
                          (defaults to including them, since they are the point
                          of a Jito release artifact)
  --output DIR            host directory to receive artifacts (default: ./dist)
  --no-cache              pass --no-cache to docker buildx build
  --pull-cache REF        --cache-from=type=registry,ref=REF (repeatable)
  --push-cache REF        --cache-to=type=registry,ref=REF,mode=max (repeatable)
  --progress MODE         buildx --progress (default: plain)
  -h, --help              show this help

Environment overrides:
  CI_COMMIT               passed through as a build arg (defaults to git HEAD)

Note: this is a breaking change from the old `./f true` debug invocation.
      Use `./f --profile debug` instead.
EOF
  exit "${exit_code}"
}

require_arg() {
  local -r flag="$1"
  local -r value="${2:-}"
  if [[ -z "$value" || "$value" == -* ]]; then
    echo "Error: ${flag} requires a value" >&2
    usage 1
  fi
}

profile=release
tag=""
checkout=""
platform=""
basename=jito-solana-release
include_val_bins=1
output_dir=./dist
no_cache=0
progress=plain
declare -a pull_cache_refs=()
declare -a push_cache_refs=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --profile)
      require_arg "$1" "${2:-}"
      profile="$2"
      shift 2
      ;;
    --tag)
      require_arg "$1" "${2:-}"
      tag="$2"
      shift 2
      ;;
    --checkout)
      require_arg "$1" "${2:-}"
      checkout="$2"
      shift 2
      ;;
    --platform)
      require_arg "$1" "${2:-}"
      platform="$2"
      shift 2
      ;;
    --basename)
      require_arg "$1" "${2:-}"
      basename="$2"
      shift 2
      ;;
    --no-val-bins)     include_val_bins=0;  shift ;;
    --include-val-bins) include_val_bins=1; shift ;;  # back-compat: keep old flag
    --output)
      require_arg "$1" "${2:-}"
      output_dir="$2"
      shift 2
      ;;
    --no-cache)        no_cache=1;          shift ;;
    --pull-cache)
      require_arg "$1" "${2:-}"
      pull_cache_refs+=("$2")
      shift 2
      ;;
    --push-cache)
      require_arg "$1" "${2:-}"
      push_cache_refs+=("$2")
      shift 2
      ;;
    --progress)
      require_arg "$1" "${2:-}"
      progress="$2"
      shift 2
      ;;
    -h|--help)         usage 0 ;;
    *)
      echo "Unknown argument: $1" >&2
      usage 1
      ;;
  esac
done

case "$profile" in
  release|release-with-debug|release-with-lto|debug) ;;
  *)
    echo "Invalid --profile: $profile" >&2
    echo "Expected one of: release, release-with-debug, release-with-lto, debug" >&2
    exit 1
    ;;
esac

# When --checkout is provided, materialize REF into a throwaway worktree and
# point the docker build context at it. The Dockerfile (and this f script) come
# from the *current* checkout, so we always drive the build with the new
# tooling regardless of what's in REF's source tree.
build_context="."
if [[ -n "$checkout" ]]; then
  if ! resolved_sha="$(git rev-parse --verify "${checkout}^{commit}" 2>/dev/null)"; then
    echo "Error: ref '$checkout' does not resolve to a commit in this repo." >&2
    echo "       If it's a remote ref, fetch it first: git fetch --tags origin" >&2
    exit 1
  fi

  worktree_dir="$(mktemp -d -t jito-build-XXXXXXXX)"
  cleanup_worktree() {
    git worktree remove --force "$worktree_dir" >/dev/null 2>&1 || true
    rm -rf "$worktree_dir"
  }
  trap cleanup_worktree EXIT INT TERM

  echo "+++ creating worktree for $checkout ($resolved_sha) at $worktree_dir"
  git worktree add --detach --quiet "$worktree_dir" "$resolved_sha"

  # Initialize submodules in the worktree separately. (`git worktree add
  # --recurse-submodules` is git >=2.39; doing it explicitly works on older
  # git too.) Submodule git dirs are shared with the main repo, so this is
  # cheap when they're already populated upstream.
  git -C "$worktree_dir" submodule update --init --recursive --quiet

  build_context="$worktree_dir"
  : "${tag:=$checkout}"
  ci_commit="$resolved_sha"
fi

if [[ -z "$tag" ]]; then
  if tag="$(git -C "$build_context" describe --tags --always --dirty 2>/dev/null)"; then
    :
  else
    tag="$(git -C "$build_context" rev-parse --short HEAD)"
  fi
fi

# Embed the channel/tag in the artifact names so releases are
# self-identifying on disk. Tags can be branch names (user/branch); replace
# path separators to keep the result a single filename component. The
# tarball's internal jito-solana-release/ prefix is unaffected (it comes
# from --build-dir, not the basename). The target triple suffix is appended
# inside the container, derived from the actual build platform.
artifact_basename="${basename}-${tag//\//_}"

# Read rust-toolchain.toml from the build context (worktree if --checkout was
# used, else the current checkout) just for the informational banner. The
# Dockerfile resolves the actual toolchain via rustup against the same file
# inside the container.
# shellcheck source=scripts/read-cargo-variable.sh
source "$SCRIPT_DIR/scripts/read-cargo-variable.sh"
rust_version="$(readCargoVariable channel "$build_context/rust-toolchain.toml")"

: "${ci_commit:=${CI_COMMIT:-$(git rev-parse HEAD)}}"

if ! command -v docker >/dev/null 2>&1; then
  echo "Error: docker not found in PATH" >&2
  exit 1
fi

if ! docker buildx version >/dev/null 2>&1; then
  echo "Error: docker buildx is required (install the buildx plugin or upgrade Docker Engine)" >&2
  exit 1
fi

if ! docker buildx inspect >/dev/null 2>&1; then
  echo "+++ no active buildx builder, creating one"
  docker buildx create --use --name jito-solana-builder >/dev/null
fi

mkdir -p "$output_dir"

declare -a buildx_args=(
  --file "$SCRIPT_DIR/dev/Dockerfile"
  --target export
  --output "type=local,dest=$output_dir"
  --build-arg "CHANNEL_OR_TAG=$tag"
  --build-arg "TARBALL_BASENAME=$artifact_basename"
  --build-arg "BUILD_PROFILE=$profile"
  --build-arg "INCLUDE_VAL_BINS=$include_val_bins"
  --build-arg "CI_COMMIT=$ci_commit"
  --progress "$progress"
)

if [[ -n "$platform" ]]; then
  buildx_args+=(--platform "$platform")
fi

if [[ "$no_cache" -eq 1 ]]; then
  buildx_args+=(--no-cache)
fi

for ref in "${pull_cache_refs[@]}"; do
  buildx_args+=(--cache-from "type=registry,ref=$ref")
done

for ref in "${push_cache_refs[@]}"; do
  buildx_args+=(--cache-to "type=registry,ref=$ref,mode=max")
done

echo "+++ building jito-solana release artifacts"
echo "    profile        : $profile"
echo "    platform       : ${platform:-native (builder default)}"
echo "    channel/tag    : $tag"
echo "    commit         : $ci_commit"
echo "    rust toolchain : $rust_version"
echo "    build context  : $build_context"
echo "    output dir     : $output_dir"
echo "    basename       : $artifact_basename"
echo "    include val ops: $include_val_bins"

DOCKER_BUILDKIT=1 docker buildx build "${buildx_args[@]}" "$build_context"

# Back-compat: the image exports loose binaries under
# $output_dir/docker-output; relocate them to <repo>/docker-output so
# consumers of the old pipeline layout keep working. Skip the move when
# --output already points at the repo root.
if [[ -d "$output_dir/docker-output" ]]; then
  output_canon="$(cd "$output_dir" && pwd)"
  if [[ "$output_canon/docker-output" != "$SCRIPT_DIR/docker-output" ]]; then
    rm -rf "$SCRIPT_DIR/docker-output"
    mv "$output_canon/docker-output" "$SCRIPT_DIR/docker-output"
  fi
fi

echo
echo "+++ artifacts in $output_dir:"
ls -lh "$output_dir"
if [[ -d "$SCRIPT_DIR/docker-output" ]]; then
  echo
  echo "+++ loose binaries in ./docker-output/ (old layout):"
  ls -lh "$SCRIPT_DIR/docker-output"
fi
