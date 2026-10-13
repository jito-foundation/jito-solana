# syntax=docker/dockerfile:1.7
#
# Containerized release-artifact build for jito-solana.
#
# Pipeline (multi-stage, BuildKit):
#   1. toolchain  : Debian + Rust + system build deps. Cacheable.
#   2. builder    : Drives Agave's upstream scripts/create-release-tarball.sh
#                   under BuildKit cache mounts for target/ + cargo registry
#                   and git, but with a Jito-curated binary set (no
#                   agave-install bootstrap, no SBF SDK, no spl-token-cli, no
#                   CUDA perf-libs). Stages the tarball + version.yml into
#                   /out so they survive the cache mount, plus loose
#                   binaries under /out/docker-output/ for consumers of the
#                   old layout.
#   3. export     : FROM scratch. The host extracts /out via
#                   `docker buildx build --output type=local,dest=...`.
#
# Driven by the `f` wrapper at the repo root. Artifact names use
# <basename>-<tag>[-<profile-suffix>]_<target>.
# See dev/BUILD.md for tag resolution and naming rules.

# Pin the OS layer; the actual rustc/cargo version is resolved at build time
# from rust-toolchain.toml (see the rustup step in the builder stage). We
# intentionally use the version-less rust image so we never have to keep a
# Dockerfile pin in sync with rust-toolchain.toml.
FROM rust:slim-bookworm AS toolchain

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

ENV DEBIAN_FRONTEND=noninteractive \
    TZ=UTC

RUN apt-get update \
 && apt-get install --no-install-recommends -y \
      build-essential \
      ca-certificates \
      clang \
      cmake \
      curl \
      git \
      jq \
      libclang-dev \
      libprotobuf-dev \
      libssl-dev \
      libudev-dev \
      make \
      pkg-config \
      protobuf-compiler \
      zlib1g-dev \
 && rm -rf /var/lib/apt/lists/*


FROM toolchain AS builder

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

WORKDIR /solana

# Install the rustc/cargo toolchain pinned by rust-toolchain.toml. Doing this
# in its own layer (before COPY . .) means a source-only change doesn't
# re-trigger the toolchain install, while a rust-toolchain.toml bump does.
COPY rust-toolchain.toml ./
RUN rustup show active-toolchain || rustup toolchain install

# Source layer: changing build args below won't invalidate the COPY cache.
COPY . .

ARG CHANNEL_OR_TAG=unknown
ARG TARBALL_BASENAME=jito-solana-release
# release | release-with-debug | release-with-lto | debug
ARG BUILD_PROFILE=release
ARG INCLUDE_VAL_BINS=0
ARG INCLUDE_TIP_ROUTER=0
ARG CI_COMMIT=
# Automatic BuildKit arg (amd64 / arm64); reflects the platform this stage
# actually executes as (native, --platform, or a remote builder node). Used
# only to key the cache mounts; the target triple in artifact names is
# derived at run time from uname inside the container.
ARG TARGETARCH

# Cache mounts are keyed by arch+profile so concurrent/sequential builds
# across different platforms/profiles don't trash each other's incremental
# state.
RUN --mount=type=cache,mode=0777,target=/solana/target,id=jito-target-${TARGETARCH}-${BUILD_PROFILE} \
    --mount=type=cache,mode=0777,target=/solana/dev-bins/target,id=jito-devbins-target-${TARGETARCH}-${BUILD_PROFILE} \
    --mount=type=cache,mode=0777,target=/usr/local/cargo/registry,id=jito-cargo-registry \
    --mount=type=cache,mode=0777,target=/usr/local/cargo/git,id=jito-cargo-git \
<<'BUILD_SCRIPT'
set -euxo pipefail

case "$BUILD_PROFILE" in
  release|release-with-debug|release-with-lto|debug) ;;
  *) echo "Unknown BUILD_PROFILE: $BUILD_PROFILE" >&2; exit 1 ;;
esac

# Derive the target triple from the platform this container is actually
# running as, so artifact names cannot lie about the binaries inside.
# cargo builds natively (no --target), making this exact by construction.
TARGET_TRIPLE="$(uname -m)-unknown-linux-gnu"

if [ "$BUILD_PROFILE" = "release-with-debug" ]; then
  triple_env="${TARGET_TRIPLE^^}"
  export "CARGO_TARGET_${triple_env//-/_}_RUSTFLAGS=-Cforce-frame-pointers=yes"
fi

# The host .git is intentionally excluded from the build context (see
# .dockerignore) to keep the COPY layer cheap. Upstream scripts still call
# `git rev-parse HEAD` to stamp version.yml. Synthesize a minimal .git that
# makes HEAD resolve to the commit the host already passed in via CI_COMMIT.
if [ -z "${CI_COMMIT:-}" ]; then
  echo "CI_COMMIT build-arg is required" >&2
  exit 1
fi
git config --global --add safe.directory /solana
if [ ! -d .git ]; then
  mkdir -p .git/refs/heads .git/objects
  echo "ref: refs/heads/build" > .git/HEAD
  echo "$CI_COMMIT" > .git/refs/heads/build
fi
# Sanity-check the stub: this should print $CI_COMMIT.
test "$(git rev-parse HEAD)" = "$CI_COMMIT"

# --- Container-only customization of the upstream build scripts -------------
#
# All edits below mutate files inside this build container only; the host
# source tree is never touched.
#
# 1. Override scripts/agave-build-lists.sh with a Jito operator-focused binary
#    set. The upstream Anza release ships agave-install / agave-install-init /
#    spl-token-cli / SBF SDK / etc.; for a Jito validator artifact we don't
#    need any of that. Only the AGAVE_BINS_* groups are Jito-curated;
#    DCOU_TAINTED_PACKAGES is captured from upstream's file before the
#    overwrite so it tracks their workspace instead of a hardcoded snapshot.
#    scripts/cargo-install-all.sh (and our patch below) consume it as cargo
#    --exclude args to defeat dev-context-only-utils feature unification.
source scripts/agave-build-lists.sh
upstream_dcou=("${DCOU_TAINTED_PACKAGES[@]}")

cat > scripts/agave-build-lists.sh <<'LISTS'
# Generated by dev/Dockerfile for the Jito release pipeline.
# shellcheck disable=SC2034
AGAVE_BINS_DEV=()
AGAVE_BINS_END_USER=( solana solana-keygen )
AGAVE_BINS_VAL_OP=( agave-validator agave-watchtower
                    solana-gossip solana-genesis solana-faucet )
AGAVE_BINS_DCOU=( agave-ledger-tool )
AGAVE_BINS_DEPRECATED=()
LISTS
{
  echo "DCOU_TAINTED_PACKAGES=("
  printf '  %s\n' "${upstream_dcou[@]}"
  echo ")"
} >> scripts/agave-build-lists.sh

# 2. Patch scripts/cargo-install-all.sh: it sources DCOU_TAINTED_PACKAGES but
#    never feeds it to cargo as --exclude, despite its own inline comment
#    (see :193-196) saying that's required to keep dev-context-only-utils out
#    of the production unit graph. Without the fix, the check_dcou sanity
#    check trips before the real build starts. The same patch adds the
#    workspace-qualified tip-router feature when requested by `./f` so the
#    graph check and production build use identical Cargo arguments.
source scripts/agave-build-lists.sh
dcou_exclude_args=""
for pkg in "${DCOU_TAINTED_PACKAGES[@]}"; do
  dcou_exclude_args+=" --exclude $pkg"
done

tip_router_feature_arg=""
if [ "$INCLUDE_TIP_ROUTER" = "1" ]; then
  tip_router_feature_arg=" --features agave-validator/tip-router"
fi
sed -i "s#\"\${binArgs\[@\]}\" --workspace#\"\${binArgs[@]}\" --workspace${dcou_exclude_args}${tip_router_feature_arg}#g" \
  scripts/cargo-install-all.sh

# 3. Patch scripts/create-release-tarball.sh to select the profile, skip unused
#    components and agave-install, and join basename to target with `_`.
cargo_profile_arg=""
if [ "$BUILD_PROFILE" != "release" ]; then
  cargo_profile_arg="--${BUILD_PROFILE} "
  sed -i "/^target:/a profile: ${BUILD_PROFILE}" \
    scripts/create-release-tarball.sh
fi
sed -i \
  -e "s#scripts/cargo-install-all.sh stable \"\${build_dir}\"#scripts/cargo-install-all.sh ${cargo_profile_arg}--no-spl-token --no-build-platform-tools --no-perf-libs stable \"\${build_dir}\"#" \
  -e '/agave-install-init/d' \
  -e 's#"${tarball_basename}-${target}#"${tarball_basename}_${target}#g' \
  scripts/create-release-tarball.sh
# Fail immediately if an upstream script change leaves a patch incomplete.
test "$(grep -Fxc "scripts/cargo-install-all.sh ${cargo_profile_arg}--no-spl-token --no-build-platform-tools --no-perf-libs stable \"\${build_dir}\"" scripts/create-release-tarball.sh)" = 1
test "$(grep -Fxc 'cp "${build_dir}/version.yml" "${tarball_basename}_${target}.yml"' scripts/create-release-tarball.sh)" = 1
test "$(grep -Fxc 'output_tar="${tarball_basename}_${target}.tar.bz2"' scripts/create-release-tarball.sh)" = 1
test "$(grep -Fc '${tarball_basename}-${target}' scripts/create-release-tarball.sh)" = 0
test "$(grep -Fc 'cp "${build_dir}/bin/agave-install-init"' scripts/create-release-tarball.sh)" = 0
if [ "$BUILD_PROFILE" != "release" ]; then
  test "$(grep -Fxc "profile: ${BUILD_PROFILE}" scripts/create-release-tarball.sh)" = 1
else
  test "$(grep -c '^profile:' scripts/create-release-tarball.sh)" = 0
fi
# ----------------------------------------------------------------------------

extra_args=()
if [ "$INCLUDE_VAL_BINS" = "1" ]; then
  extra_args+=(--include-val-bins)
fi

scripts/create-release-tarball.sh \
  --build-dir jito-solana-release \
  --channel-or-tag "$CHANNEL_OR_TAG" \
  --target "$TARGET_TRIPLE" \
  --tarball-basename "$TARBALL_BASENAME" \
  "${extra_args[@]}"

# Stage outputs to a non-cache-mounted dir so the scratch export stage can
# COPY them out.
mkdir -p /out
cp "${TARBALL_BASENAME}_${TARGET_TRIPLE}.tar.bz2" /out/
cp "${TARBALL_BASENAME}_${TARGET_TRIPLE}.yml"     /out/

# Back-compat: also export loose binaries under docker-output/
mkdir -p /out/docker-output
find jito-solana-release/bin -maxdepth 1 -type f \
  -exec cp {} /out/docker-output/ \;
BUILD_SCRIPT


FROM scratch AS export
COPY --from=builder /out/ /
