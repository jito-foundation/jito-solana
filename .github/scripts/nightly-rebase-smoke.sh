#!/usr/bin/env bash
# Smoke test for nightly-rebase.sh against throwaway local repos and a fake gh.
# Needs bash >= 4.2, git, jq, python3 with PyYAML, ssh-keygen.
# Covers: draft staging/reuse, signing failure, carry order, phased auto
# landing, statusless CI retriggering, conflict, timeout, and lease validation.
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
if [[ "$1 $2" == "pr create" || "$1 $2" == "pr edit" ]]; then
    args=("$@")
    for (( i = 0; i < ${#args[@]}; i++ )); do
        if [[ "${args[i]}" == "--body-file" ]]; then
            cp "${args[i + 1]}" "${GH_PR_BODY}"
            break
        fi
    done
fi
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
        echo "${GH_STATUS:-success}" ;;
esac
EOF
chmod +x bin/gh
cat > bin/curl <<'EOF'
#!/usr/bin/env bash
while (( $# )); do
    if [[ "$1" == "--data" ]]; then
        printf '%s' "$2" > "${SLACK_PAYLOAD_FILE}"
        exit
    fi
    shift
done
exit 1
EOF
chmod +x bin/curl
export PATH="${t}/bin:${PATH}" GH_LOG="${t}/gh.log" GH_PR_BODY="${t}/pr-body"

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

check_status() {
    local -r name="$1" expected="$2"
    local status
    status="$(jq -r .status "${t}/${name}.json")"
    [[ "${status}" == "${expected}" ]] || {
        echo "FAIL ${name}: ${status} != ${expected}"
        exit 1
    }
    echo "ok ${name}: ${status}"
}

run_stage() {
    local -r name="$1" expected="$2"
    shift 2
    (cd work && git checkout -q master && { git remote remove agave 2>/dev/null || true; } \
        && env "$@" RESULT_FILE="${t}/${name}.json" \
            STATE_FILE="${t}/${name}-state.json" \
            GITHUB_OUTPUT="${t}/${name}-output" \
            bash "${script}" stage > "${t}/${name}.log" 2>&1) || {
        echo "FAIL ${name}: stage exited non-zero"
        tail -20 "${t}/${name}.log"
        exit 1
    }
    check_status "${name}" "${expected}"
}

run_stage_fails() {
    local -r name="$1" expected="$2"
    shift 2
    if (cd work && git checkout -q master && { git remote remove agave 2>/dev/null || true; } \
        && env "$@" RESULT_FILE="${t}/${name}.json" \
            STATE_FILE="${t}/${name}-state.json" \
            GITHUB_OUTPUT="${t}/${name}-output" \
            bash "${script}" stage > "${t}/${name}.log" 2>&1); then
        echo "FAIL ${name}: stage exited zero"
        exit 1
    fi
    check_status "${name}" "${expected}"
}

run_wait() {
    local -r name="$1" expected="$2"
    shift 2
    (cd work && env "$@" RESULT_FILE="${t}/${name}.json" \
        STATE_FILE="${t}/${name}-state.json" \
        GITHUB_OUTPUT="${t}/${name}-wait-output" \
        bash "${script}" wait > "${t}/${name}-wait.log" 2>&1) || {
        echo "FAIL ${name}: wait exited non-zero"
        tail -20 "${t}/${name}-wait.log"
        exit 1
    }
    check_status "${name}" "${expected}"
}

run_land() {
    local -r name="$1" expected="$2"
    shift 2
    (cd work && env "$@" RESULT_FILE="${t}/${name}.json" \
        STATE_FILE="${t}/${name}-state.json" \
        bash "${script}" land > "${t}/${name}-land.log" 2>&1) || {
        echo "FAIL ${name}: land exited non-zero"
        tail -20 "${t}/${name}-land.log"
        exit 1
    }
    check_status "${name}" "${expected}"
}

run_stage draft draft_pr LANDING=draft
run_stage_fails signing-failure failed LANDING=draft GIT_CONFIG_COUNT=1 \
    GIT_CONFIG_KEY_0=gpg.ssh.program GIT_CONFIG_VALUE_0=false
staging="$(git -C origin.git rev-parse ci/rebase/master)"
run_stage draft-rerun draft_pr LANDING=draft
[[ "$(git -C origin.git rev-parse ci/rebase/master)" == "${staging}" ]] || { echo "FAIL: staging re-pushed"; exit 1; }
(cd work && git checkout -q master && echo second > second-carry && git add . \
    && git commit -qm "Second Jito Patch" && git push -q origin master)
run_stage ordered-series draft_pr LANDING=draft
ordered_staging="$(git -C origin.git rev-parse ci/rebase/master)"
base="$(git -C work merge-base origin/master agave/master)"
mapfile -t carries < <(git -C work rev-list --reverse "${base}..origin/master")
(cd work && git checkout -q -B reordered "${base}" \
    && git cherry-pick "${carries[1]}" "${carries[0]}" >/dev/null \
    && git push -q --force origin HEAD:master)
run_stage reordered-series draft_pr LANDING=draft
[[ "$(git -C origin.git rev-parse ci/rebase/master)" != "${ordered_staging}" ]] || {
    echo "FAIL: reordered carry series reused staging"
    exit 1
}
reviewed_channel="$(git -C origin.git rev-parse master)"
awk '/^```bash$/ { command = 1; next } command && /^```$/ { exit } command' \
    "${GH_PR_BODY}" > "${t}/reviewed-command"
run_stage statusless draft_pr LANDING=draft GH_STATUS=missing \
    GIT_COMMITTER_DATE=2000-01-01T00:00:00Z
[[ "$(git -C origin.git rev-parse ci/rebase/master)" != "${staging}" ]] || { echo "FAIL: statusless staging reused"; exit 1; }
if (cd work && bash "${t}/reviewed-command" > "${t}/reviewed-command.log" 2>&1); then
    echo "FAIL: saved draft command accepted moved staging"
    exit 1
fi
[[ "$(git -C origin.git rev-parse master)" == "${reviewed_channel}" ]] || { echo "FAIL: saved draft command changed channel"; exit 1; }
staging="$(git -C origin.git rev-parse ci/rebase/master)"
(cd work && git checkout -q master && git commit --amend -qm "Jito Patch amended" \
    && git push -q --force origin master)
run_stage history-change draft_pr LANDING=draft
[[ "$(git -C origin.git rev-parse ci/rebase/master)" != "${staging}" ]] || { echo "FAIL: changed history reused staging"; exit 1; }

run_stage auto staged LANDING=auto
staging="$(jq -r .staging_sha "${t}/auto-state.json")"
run_wait auto failed LANDING=auto GH_STATUS=success
grep -qx 'ready-to-land=true' "${t}/auto-wait-output" || { echo "FAIL: auto not ready to land"; exit 1; }
run_land auto landed LANDING=auto
[[ "$(git -C origin.git rev-parse master)" == "${staging}" ]] || { echo "FAIL: master != staging"; exit 1; }
git -C origin.git cat-file commit master | grep -q 'SSH SIGNATURE' || { echo "FAIL: landed commit unsigned"; exit 1; }
run_stage auto-rerun fresh LANDING=auto

(cd upstream && echo c > c && git add . && git commit -qm "agave 3")
run_stage timeout staged LANDING=auto
run_wait timeout ci_timeout LANDING=auto GH_STATUS=pending CI_TIMEOUT_MINUTES=0
[[ "$(git -C origin.git rev-parse master)" != \
    "$(jq -r .staging_sha "${t}/timeout-state.json")" ]] || { echo "FAIL: timeout landed"; exit 1; }

(cd upstream && echo z > jito && git add . && git commit -qm "agave 4 touches jito")
run_stage conflict conflict LANDING=auto
(cd upstream && git rm -q jito && git commit -qm "agave 5 drops jito")

root="$(git -C upstream rev-list --max-parents=0 HEAD)"
git -C origin.git update-ref refs/heads/master "${root}"
git -C origin.git update-ref -d refs/heads/ci/rebase/master
git -C origin.git update-ref -d refs/heads/ci/rebase/master-trigger
run_stage no-carry-first staged LANDING=auto GH_STATUS=missing
run_stage no-carry-retrigger draft_pr LANDING=draft GH_STATUS=missing
clean_sha="$(jq -r .staging_sha "${t}/no-carry-retrigger-state.json")"
ci_sha="$(jq -r .ci_sha "${t}/no-carry-retrigger-state.json")"
[[ "${ci_sha}" != "${clean_sha}" ]] || { echo "FAIL: no-carry CI was not retriggered"; exit 1; }
[[ "$(git -C origin.git rev-parse ci/rebase/master-trigger)" == "${ci_sha}" ]] || { echo "FAIL: wrong trigger SHA"; exit 1; }
[[ "$(git -C origin.git rev-parse "${ci_sha}^{tree}")" == \
    "$(git -C origin.git rev-parse "${clean_sha}^{tree}")" ]] || { echo "FAIL: trigger tree differs"; exit 1; }
git -C origin.git cat-file commit "${ci_sha}" | grep -q 'SSH SIGNATURE' || { echo "FAIL: trigger unsigned"; exit 1; }
grep -q "builds?commit=${ci_sha}" "${GH_PR_BODY}" || { echo "FAIL: draft PR links clean SHA instead of CI trigger"; exit 1; }

cp "${t}/no-carry-retrigger-state.json" "${t}/trigger-moved-state.json"
old_trigger="$(git -C origin.git rev-parse ci/rebase/master-trigger)"
git -C origin.git update-ref refs/heads/ci/rebase/master-trigger \
    "$(git -C origin.git rev-parse master)" "${old_trigger}"
run_land trigger-moved failed LANDING=auto
[[ "$(jq -r .detail "${t}/trigger-moved.json")" == "CI trigger no longer matches staging; landing skipped" ]] || { echo "FAIL: wrong moved-trigger detail"; exit 1; }

git -C origin.git update-ref refs/heads/ci/rebase/master-trigger "${old_trigger}"
cp "${t}/no-carry-retrigger-state.json" "${t}/tree-mismatch-state.json"
bad_ci="$(git -C work rev-parse agave/master~1)"
jq --arg sha "${bad_ci}" '.ci_sha = $sha' "${t}/tree-mismatch-state.json" \
    >| "${t}/tree-mismatch-state.tmp"
mv "${t}/tree-mismatch-state.tmp" "${t}/tree-mismatch-state.json"
git -C origin.git update-ref refs/heads/ci/rebase/master-trigger "${bad_ci}" "${old_trigger}"
run_land tree-mismatch failed LANDING=auto
[[ "$(jq -r .detail "${t}/tree-mismatch.json")" == "CI trigger no longer matches staging; landing skipped" ]] || { echo "FAIL: wrong trigger-tree detail"; exit 1; }
git -C origin.git update-ref refs/heads/ci/rebase/master-trigger "${old_trigger}" "${bad_ci}"

run_wait no-carry-retrigger failed LANDING=auto GH_STATUS=success
run_land no-carry-retrigger landed LANDING=auto
[[ "$(git -C origin.git rev-parse master)" == "${clean_sha}" ]] || { echo "FAIL: trigger commit landed"; exit 1; }

(cd upstream && echo after > after && git add . && git commit -qm "agave 6")
run_stage stale-lease staged LANDING=auto GH_STATUS=success
run_wait stale-lease failed LANDING=auto GH_STATUS=success
git clone -q origin.git mover
(cd mover && echo moved > moved && git add . && git commit -qm "merged during CI" \
    && git push -q origin master)
run_land stale-lease stale LANDING=auto

workflow="$(dirname "${script}")/../workflows/nightly-rebase.yml"
summary_script="${t}/summary.sh"
python3 - "${workflow}" "${summary_script}" <<'PY'
import sys
import yaml

with open(sys.argv[1]) as f:
    workflow = yaml.safe_load(f)

rebase = workflow["jobs"]["rebase"]
assert rebase["permissions"] == {"contents": "read", "statuses": "read"}
steps = {step["name"]: step for step in rebase["steps"]}
checkout = steps["Check out channel"]["with"]
assert checkout["fetch-depth"] == 1
assert checkout["fetch-tags"] is False
assert checkout["filter"] == "blob:none"
assert checkout["token"] == "${{ steps.stage-token.outputs.token }}"
assert sum(step.get("uses") == "actions/create-github-app-token@v2"
           for step in rebase["steps"]) == 2
assert steps["Stage nightly rebase"]["run"].endswith(" stage")
assert steps["Wait for Buildkite"]["run"].endswith(" wait")
assert steps["Wait for Buildkite"]["env"]["GH_TOKEN"] == "${{ github.token }}"
assert steps["Land nightly rebase"]["run"].endswith(" land")
assert steps["Land nightly rebase"]["env"]["GH_TOKEN"] == "${{ steps.land-token.outputs.token }}"

summary = next(step for step in workflow["jobs"]["summary"]["steps"]
               if step["name"] == "Post Slack summary")
assert summary["env"]["CHANNELS"] == "${{ needs.channels.outputs.channels }}"
with open(sys.argv[2], "w") as f:
    f.write(summary["run"])
PY

mkdir "${t}/results"
printf '%s\n' '{"channel":"master","status":"landed","detail":"ok","url":""}' \
    > "${t}/results/nightly-rebase-master.json"
printf '%s\n' '{not-json' > "${t}/results/nightly-rebase-v4.3.json"
printf '%s\n' '{"channel":"bad-types","status":[],"detail":{},"url":false}' \
    > "${t}/results/nightly-rebase-bad-types.json"
printf '%s\n' \
    '{"channel":"wrong","status":"landed","detail":"wrong","url":""}' \
    '{"channel":"multi","status":"landed","detail":"second","url":""}' \
    > "${t}/results/nightly-rebase-multi.json"
channels='[{"channel":"master"},{"channel":"v4.3"},{"channel":"v4.4"},{"channel":"bad-types"},{"channel":"multi"}]'
SLACK_PAYLOAD_FILE="${t}/slack-payload.json" \
CHANNELS="${channels}" CHANNELS_RESULT=success REBASE_RESULT=failure \
RESULTS_DIR="${t}/results" SLACK_WEBHOOK_URL=https://example/slack \
GITHUB_SERVER_URL=https://example GITHUB_REPOSITORY=o/r GITHUB_RUN_ID=1 \
    bash "${summary_script}"
summary="$(jq -r .text "${t}/slack-payload.json")"
grep -Fqx 'master: landed - ok' <<< "${summary}" || { echo "FAIL: valid summary result missing"; exit 1; }
grep -Fqx 'v4.3: failed - Job failed before producing a result' <<< "${summary}" || { echo "FAIL: malformed result not named"; exit 1; }
grep -Fqx 'v4.4: failed - Job failed before producing a result' <<< "${summary}" || { echo "FAIL: missing result not named"; exit 1; }
grep -Fqx 'bad-types: failed - Job failed before producing a result' <<< "${summary}" || { echo "FAIL: wrong result types accepted"; exit 1; }
grep -Fqx 'multi: failed - Job failed before producing a result' <<< "${summary}" || { echo "FAIL: multiple result objects accepted"; exit 1; }
echo "all ok"
