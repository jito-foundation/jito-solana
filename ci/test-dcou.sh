#!/usr/bin/env bash

set -o noclobber  # Prevent overwriting existing files with >
set -o nounset    # Treat unset variables as an error
set -o errexit    # Exit immediately if a command exits with non-zero status
set -o pipefail   # Prevent errors in a pipeline from being masked
set -o errtrace   # Ensure that any error traps are inherited by functions

if [[ "${BASH_VERSINFO[0]}" -lt 4 ]]; then
    echo 'ERROR: Bash 4.0 or higher required' >&2
    exit 1
fi

on_fatal_error() {
    local -r exit_code="$?"
    echo "DCOU regression failed at line $1 (exit ${exit_code})" >&2
    if [[ -f "${fixture_dir:-}/output" ]]; then
        cat "${fixture_dir}/output" >&2
    fi
    exit "${exit_code}"
}
trap 'on_fatal_error ${LINENO}' ERR

declare script_dir
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
declare fixture_dir
fixture_dir="$(mktemp -d)"
trap 'rm -rf "$fixture_dir"' EXIT

make_fixture() {
    mkdir -p "${fixture_dir}/scripts"
    cp "${script_dir}/../scripts/cargo-install-all.sh" "${fixture_dir}/scripts/"
    cp "${script_dir}/../scripts/agave-build-lists.sh" "${fixture_dir}/scripts/"
    cat > "${fixture_dir}/cargo" <<'CARGO'
#!/usr/bin/env bash
set -euo pipefail

# This replaces only Cargo; the installer, binary lists, jq and file copies
# are real. Each invocation is logged as a JSON array to preserve arguments.
declare fixture_dir
declare group=production profile=debug mode=build
declare -a bins=()
fixture_dir="$(cd "$(dirname "$0")" && pwd)"
jq -cn --args '$ARGS.positional' -- "$@" >> "$fixture_dir/calls"
while [[ $# -gt 0 ]]; do
    case "$1" in
        --manifest-path) group=development; shift ;;
        --profile) profile="$2"; shift ;;
        --unit-graph) mode=check ;;
        --bin) bins+=("$2"); shift ;;
    esac
    shift
done

if [[ "$mode" == check ]]; then
    declare graph="${DCOU_TEST_PRODUCTION_GRAPH:-clean}"
    if [[ "$group" == development ]]; then
        graph="${DCOU_TEST_DEVELOPMENT_GRAPH:-dcou}"
    fi
    case "$graph" in
        clean) echo '{"units":[{"features":[]}]}' ;;
        dcou)
            echo '{"units":[{"features":[]},{"features":["dev-context-only-utils"]}]}'
            ;;
        cargo-error)
            # Valid stdout must not hide a failed Cargo invocation.
            echo '{"units":[{"features":[]}]}'
            exit 42
            ;;
        invalid) echo 'not-json' ;;
        empty) ;;
        empty-units) echo '{"units":[]}' ;;
        missing-features) echo '{"units":[{}]}' ;;
        invalid-features) echo '{"units":[{"features":"dev-context-only-utils"}]}' ;;
        multiple) printf '%s\n' '{"units":[{"features":[]}]}' '{"units":[{"features":[]}]}' ;;
        *) exit 1 ;;
    esac
    exit 0
fi

# Let the real installation/copy phase finish without compiling Rust.
declare target_dir="$fixture_dir/target/$profile"
if [[ "$group" == development ]]; then
    target_dir="$fixture_dir/dev-bins/target/$profile"
fi
mkdir -p "$target_dir"
declare bin
for bin in "${bins[@]}"; do
    touch "$target_dir/$bin"
done
CARGO
    chmod +x "${fixture_dir}/cargo"
}

run_installer() {
    rm -f "${fixture_dir}/calls" "${fixture_dir}/output" || return
    "${fixture_dir}/scripts/cargo-install-all.sh" \
        --no-spl-token --no-build-platform-tools "$@" \
        "${fixture_dir}/install" > "${fixture_dir}/output" 2>&1
}

assert_build_arguments() {
    # Compare complete argv, dropping only the three graph-inspection args.
    jq -e -s '
        def compiled: map(select(
            . != "-Z" and . != "unstable-options" and . != "--unit-graph"));
        length == 4
        and (.[0] | index("--unit-graph") != null)
        and (.[1] | index("--unit-graph") != null)
        and (.[0] | compiled) == .[2]
        and (.[1] | compiled) == .[3]
        and (.[2] | index("--workspace") != null)
        and (.[2] | index("--manifest-path") == null)
        and (.[3] | index("--workspace") == null)
        and (.[3] | index("--manifest-path") as $i |
            $i != null and .[$i + 1] == "dev-bins/Cargo.toml")
    ' "${fixture_dir}/calls" > /dev/null
}

assert_no_compilation() {
    jq -e -s 'length > 0 and all(.[];
        index("--unit-graph") != null)' "${fixture_dir}/calls" > /dev/null
}

expect_rejection() {
    # shellcheck disable=SC2310 # Inspect the installer exit status.
    if run_installer "$@"; then
        echo 'Installer accepted an invalid feature graph' >&2
        return 1
    fi
    assert_no_compilation
    grep -Eq '^Failed to obtain DCOU unit graph\.|^Invalid DCOU unit graph' "${fixture_dir}/output"
}

main() {
    make_fixture
    local profile pkg graph
    local -a profile_args=()
    # shellcheck source=scripts/agave-build-lists.sh
    source "${script_dir}/../scripts/agave-build-lists.sh"

    for profile in release release-with-debug release-with-lto; do
        profile_args=()
        if [[ "${profile}" != release ]]; then
            profile_args=("--${profile}")
        fi
        run_installer "${profile_args[@]}"
        assert_build_arguments
        for pkg in "${DCOU_TAINTED_PACKAGES[@]}"; do
            jq -e -s --arg pkg "${pkg}" '.[2] | index($pkg) as $i |
                $i != null and .[$i - 1] == "--exclude"' \
                "${fixture_dir}/calls" > /dev/null
        done
        jq -e -s --arg profile "${profile}" 'all(.[];
            index("--profile") as $i | $i != null and .[$i + 1] == $profile)' \
            "${fixture_dir}/calls" > /dev/null
    done

    run_installer --dcou-check-only
    assert_no_compilation
    run_installer --dcou-check-only --no-build-dcou-bins
    assert_no_compilation

    # Exercise failures in the normal installation path, with and without
    # the development-tool positive control that used to mask some errors.
    for graph in dcou cargo-error invalid empty empty-units \
        missing-features invalid-features multiple; do
        DCOU_TEST_PRODUCTION_GRAPH="${graph}" expect_rejection
        DCOU_TEST_PRODUCTION_GRAPH="${graph}" \
            expect_rejection --no-build-dcou-bins
    done
    for graph in clean cargo-error invalid empty; do
        DCOU_TEST_DEVELOPMENT_GRAPH="${graph}" expect_rejection
    done
    DCOU_TEST_PRODUCTION_GRAPH=dcou expect_rejection --dcou-check-only

    # Reproduce #1620's original bug in the fixture. The argument assertion
    # must detect that the checked exclusions disappeared from compilation.
    # shellcheck disable=SC2016 # Match literal shell source.
    sed 's/cargo_build "${productionBuildArgs\[@\]}"/cargo_build "${binArgs[@]}" --workspace/' \
        "${script_dir}/../scripts/cargo-install-all.sh" \
        > "${fixture_dir}/scripts/mutated-installer"
    mv "${fixture_dir}/scripts/mutated-installer" \
        "${fixture_dir}/scripts/cargo-install-all.sh"
    chmod +x "${fixture_dir}/scripts/cargo-install-all.sh"
    run_installer
    # shellcheck disable=SC2310 # A mismatch is expected for this mutation.
    if assert_build_arguments; then
        echo 'Regression test missed the removed production exclusions' >&2
        return 1
    fi

    echo 'DCOU regression tests passed.'
}

main "$@"
