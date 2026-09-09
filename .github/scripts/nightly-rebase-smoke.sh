#!/usr/bin/env bash
# Smoke test for nightly-rebase.sh against throwaway local repos and a fake gh.
# Needs bash >= 4.2, git, jq, ssh-keygen.
# Covers: draft staging, staging reuse, auto landing, fresh, conflict, stale lease.
set -euo pipefail

script="$(cd "$(dirname "$0")" && pwd)/nightly-rebase.sh"
t="$(mktemp -d)"
trap 'rm -rf "${t}"' EXIT
cd "${t}"

ssh-keygen -q -t ed25519 -N '' -f key
export GIT_CONFIG_GLOBAL="${t}/gitconfig"
git config --global gpg.format ssh
git config --global user.signingkey "${t}/key"
git config --global init.defaultBranch master
git config --global user.email smoke@example.com
git config --global user.name smoke

mkdir bin
cat > bin/gh <<'EOF'
#!/usr/bin/env bash
echo "gh $*" >> "${GH_LOG}"
case "$1 $2" in
    "pr create") echo "https://example/pr/1" ;;
    "issue create") echo "https://example/issue/1" ;;
    "api "*)
        # First status poll simulates a merge landing on the channel mid-CI.
        if [[ -n "${MOVE_ON_API:-}" && ! -e "${MOVE_ON_API}.done" ]]; then
            (cd "${MOVE_ON_API}" && echo m > moved && git add . && \
                git commit -qm "merged during CI" && git push -q origin HEAD:master)
            touch "${MOVE_ON_API}.done"
        fi
        echo success ;;
esac
EOF
chmod +x bin/gh
export PATH="${t}/bin:${PATH}" GH_LOG="${t}/gh.log"

git init -q upstream
(cd upstream && echo a > a && git add . && git commit -qm "agave 1" \
    && echo b > b && git add . && git commit -qm "agave 2")
git init -q --bare origin.git
git clone -q upstream work
(cd work && git remote set-url origin "${t}/origin.git" && git reset -q --hard HEAD~1 \
    && echo j > jito && git add . && git commit -qm "Jito Patch" && git push -q origin master)

export CHANNEL=master UPSTREAM_CHANNEL=master CHANNEL_OWNER=smoke GH_REPO=o/r \
    GITHUB_RUN_ID=1 GITHUB_SERVER_URL=https://example UPSTREAM_REPO="${t}/upstream" \
    CI_POLL_SECONDS=0 CI_TIMEOUT_MINUTES=1

run() {
    local -r name="$1" expected="$2"
    shift 2
    (cd work && git checkout -q master && { git remote remove agave 2>/dev/null || true; } \
        && env "$@" RESULT_FILE="${t}/${name}.json" bash "${script}" > "${t}/${name}.log" 2>&1) \
        || { echo "FAIL ${name}: script exited non-zero"; tail -20 "${t}/${name}.log"; exit 1; }
    local status
    status="$(jq -r .status "${t}/${name}.json")"
    [[ "${status}" == "${expected}" ]] || { echo "FAIL ${name}: ${status} != ${expected}"; exit 1; }
    echo "ok ${name}: ${status}"
}

run draft draft_pr LANDING=draft
staging="$(git -C origin.git rev-parse ci/rebase/master)"
run draft-rerun draft_pr LANDING=draft
[[ "$(git -C origin.git rev-parse ci/rebase/master)" == "${staging}" ]] || { echo "FAIL: staging re-pushed"; exit 1; }
run auto landed LANDING=auto
[[ "$(git -C origin.git rev-parse master)" == "${staging}" ]] || { echo "FAIL: master != staging"; exit 1; }
git -C origin.git cat-file commit master | grep -q 'SSH SIGNATURE' || { echo "FAIL: landed commit unsigned"; exit 1; }
run auto-rerun fresh LANDING=auto
(cd upstream && echo z > jito && git add . && git commit -qm "agave 3 touches jito")
run conflict conflict LANDING=auto
(cd upstream && git rm -q jito && git commit -qm "agave 4 drops jito")
git clone -q origin.git mover
run stale-lease stale LANDING=auto MOVE_ON_API="${t}/mover"
echo "all ok"
