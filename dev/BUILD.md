# Release build (`./f`)

Build Jito operator binaries inside Docker and export them to the host.
The wrapper drives Agave's upstream `scripts/create-release-tarball.sh`
through `dev/Dockerfile` (BuildKit multi-stage).

## Requirements

- Docker with **buildx** (Docker Engine >= 23 or the buildx plugin)
- Git submodules initialized (`git submodule update --init --recursive`)

## Quick start

```bash
./f
```

Release build of the current checkout. Artifacts land in `./dist/` and loose
binaries in `./docker-output/` (repo root).

## Outputs

| Path | Contents |
|------|----------|
| `dist/<basename>-<tag>-<target>.tar.bz2` | Release tarball |
| `dist/<basename>-<tag>-<target>.yml` | Version manifest |
| `docker-output/<bin>` | Loose binaries (legacy layout) |

- **tag** -- from `--tag`, the `--checkout` ref, or `git describe`
- **target** -- derived inside the container from the build platform
  (e.g. `x86_64-unknown-linux-gnu` on `linux/amd64`)

Tarball binaries live under `jito-solana-release/bin/`.

Default curated set: `agave-validator`, `agave-ledger-tool`,
`agave-watchtower`, `solana`, `solana-keygen`, `solana-genesis`,
`solana-gossip`, `solana-faucet`.

## Common invocations

```bash
# Named release tag
./f --tag v4.0.3-jito

# Build a tagged ref with current tooling (throwaway worktree)
git fetch --tags origin
./f --checkout v4.0.3-jito

# Debug build
./f --profile debug

# x86_64 artifact from a non-x86 host (emulation or remote builder)
./f --platform linux/amd64 --tag v4.0.3-jito
```

## How it works

1. **`f`** invokes `docker buildx build` against `dev/Dockerfile`,
   exporting the `export` stage to `./dist/` (or `--output`).
2. **toolchain** -- Debian bookworm + Rust from `rust-toolchain.toml`.
3. **builder** -- Patches upstream build scripts in-container only
   (Jito binary set, DCOU excludes, no SBF SDK / spl-token / perf-libs),
   runs `create-release-tarball.sh`, stages tarball + loose binaries.
4. **`f`** relocates `dist/docker-output/` to `./docker-output/` at the
   repo root for back-compat with existing deploy scripts.

Incremental state persists via BuildKit cache mounts (keyed by arch +
profile). Optional registry cache: `--pull-cache` / `--push-cache`.

## Syncing to a remote build host

```bash
./s    # rsyncs repo to $HOST (see .env), excludes target/dist/docker-output
```

Run `./f` on the remote after sync for native x86_64 builds.
