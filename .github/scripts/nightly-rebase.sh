#!/bin/bash
#
# Rebase one jito-solana channel onto its upstream branch.
#
# LANDING=draft: push the rebased head to ci/rebase/<channel> and refresh a
# draft PR for a human to land by force-pushing the channel.
# LANDING=auto: stage, wait for Buildkite with `wait`, then invoke `land` with
# fresh credentials. The channel lease makes a concurrent merge abort landing.
#
# Conflicts are never resolved here; they open or refresh an issue.

set -o noclobber  # Prevent overwriting existing files with >
set -o nounset    # Treat unset variables as an error
set -o errexit    # Exit immediately if a command exits with non-zero status
set -o pipefail   # Prevent errors in a pipeline from being masked
set -o errtrace   # Ensure that any error traps are inherited by functions

: "${CHANNEL:?CHANNEL is required}"
: "${UPSTREAM_CHANNEL:?UPSTREAM_CHANNEL is required}"
: "${CHANNEL_OWNER:?CHANNEL_OWNER is required}"
: "${LANDING:?LANDING is required (auto|draft)}"
: "${GH_REPO:?GH_REPO is required}"
: "${RESULT_FILE:?RESULT_FILE is required}"
: "${STATE_FILE:?STATE_FILE is required}"
: "${GITHUB_RUN_ID:?GITHUB_RUN_ID is required}"
: "${GITHUB_SERVER_URL:?GITHUB_SERVER_URL is required}"
: "${GITHUB_RUN_ATTEMPT:=1}"
: "${GITHUB_OUTPUT:=/dev/null}"
: "${UPSTREAM_REPO:=https://github.com/anza-xyz/agave.git}"
: "${CI_CONTEXT:=buildkite/jito-solana}"
: "${CI_TIMEOUT_MINUTES:=150}"
: "${CI_POLL_SECONDS:=60}"

declare -g result_url=""
declare -g channel_sha=""
declare -g upstream_sha=""
declare -g staging_sha=""
declare -g ci_sha=""

declare -gr staging_branch="ci/rebase/${CHANNEL}"
declare -gr staging_ref="refs/remotes/origin/${staging_branch}"
declare -gr trigger_branch="${staging_branch}-trigger"
declare -gr trigger_ref="refs/remotes/origin/${trigger_branch}"
declare -gr conflict_issue_title="Nightly rebase conflict: ${CHANNEL}"
declare -gr run_url="${GITHUB_SERVER_URL}/${GH_REPO}/actions/runs/${GITHUB_RUN_ID}"

write_result() {
    local -r status="$1" detail="$2"
    jq -n \
        --arg channel "${CHANNEL}" \
        --arg status "${status}" \
        --arg detail "${detail}" \
        --arg url "${result_url}" \
        --arg channel_sha "${channel_sha}" \
        --arg upstream_sha "${upstream_sha}" \
        '{
            channel: $channel,
            status: $status,
            detail: $detail,
            url: $url,
            channel_sha: $channel_sha,
            upstream_sha: $upstream_sha
        }' >| "${RESULT_FILE}"
}
write_result failed "Run failed before producing a result"

write_state() {
    jq -n \
        --arg channel_sha "${channel_sha}" \
        --arg upstream_sha "${upstream_sha}" \
        --arg staging_sha "${staging_sha}" \
        --arg ci_sha "${ci_sha}" \
        '{
            channel_sha: $channel_sha,
            upstream_sha: $upstream_sha,
            staging_sha: $staging_sha,
            ci_sha: $ci_sha
        }' >| "${STATE_FILE}"
}

load_state() {
    channel_sha="$(jq -er .channel_sha "${STATE_FILE}")"
    upstream_sha="$(jq -er .upstream_sha "${STATE_FILE}")"
    staging_sha="$(jq -er .staging_sha "${STATE_FILE}")"
    ci_sha="$(jq -er .ci_sha "${STATE_FILE}")"
}

find_open_pr() {
    gh pr list \
        --base "${CHANNEL}" \
        --head "${staging_branch}" \
        --json number \
        --jq '.[0].number // empty'
}

find_conflict_issue() {
    gh issue list \
        --search "in:title \"${conflict_issue_title}\"" \
        --json number \
        --jq '.[0].number // empty'
}

close_conflict_issue() {
    local -r resolution_url="$1"
    local issue_number
    issue_number="$(find_conflict_issue)"

    if [[ -n "${issue_number}" ]]; then
        gh issue close "${issue_number}" \
            --comment "Resolved by ${resolution_url}."
    fi
}

# open_pr_action <gh pr verb> <args...>: acts on the channel's staging PR if any.
open_pr_action() {
    local pr_number
    pr_number="$(find_open_pr)"

    if [[ -n "${pr_number}" ]]; then
        gh pr "$1" "${pr_number}" "${@:2}"
    fi
}

write_conflict_report() {
    local -r last_staging_sha="$1"
    local -r conflict_files="$2"
    local issue_number
    local issue_url
    local report_file

    report_file="$(mktemp)"

    cat >| "${report_file}" <<EOF
The nightly rebase of \`${CHANNEL}\` onto
\`agave/${UPSTREAM_CHANNEL}\` did not apply cleanly.

Channel tip: \`${channel_sha}\`
Agave tip: \`${upstream_sha}\`
Last staging tip: \`${last_staging_sha:-none}\`

Conflicting files:

\`\`\`text
${conflict_files}
\`\`\`

[Workflow run](${run_url})
EOF

    issue_number="$(find_conflict_issue)"
    if [[ -n "${issue_number}" ]]; then
        gh issue edit "${issue_number}" \
            --body-file "${report_file}" \
            --add-assignee "${CHANNEL_OWNER}"
        issue_url="${GITHUB_SERVER_URL}/${GH_REPO}/issues/${issue_number}"
    else
        issue_url="$(gh issue create \
            --title "${conflict_issue_title}" \
            --body-file "${report_file}" \
            --assignee "${CHANNEL_OWNER}")"
    fi

    result_url="${issue_url}"
    write_result conflict "Rebase conflict"
}

write_pr_body() {
    local -r body_file="$3"
    local -r pr_staging_sha="$1"
    local -r pr_ci_sha="$2"
    local carry_text upstream_count range_diff_text old_carry_base

    carry_text="$(git log --format="- \`%h\` %s" \
        "agave/${UPSTREAM_CHANNEL}..origin/${CHANNEL}")"
    carry_text="${carry_text:-- None}"
    upstream_count="$(git rev-list --count \
        "origin/${CHANNEL}..agave/${UPSTREAM_CHANNEL}")"
    old_carry_base="$(git merge-base "origin/${CHANNEL}" "agave/${UPSTREAM_CHANNEL}")"
    if [[ "${old_carry_base}" == "${channel_sha}" ||
        "${upstream_sha}" == "${pr_staging_sha}" ]]; then
        range_diff_text="No comparable range: at least one side has no Jito carry commits."
    else
        range_diff_text="$(git range-diff --no-color \
            "${old_carry_base}..origin/${CHANNEL}" \
            "agave/${UPSTREAM_CHANNEL}..${pr_staging_sha}")"
    fi

    cat >| "${body_file}" <<EOF
Automated nightly rebase of \`${CHANNEL}\` onto
\`agave/${UPSTREAM_CHANNEL}\`.

- Channel tip: \`${channel_sha}\`
- Agave tip: \`${upstream_sha}\`
- Upstream delta: ${upstream_count} commits
- Staging branch: \`${staging_branch}\`
- [Buildkite](https://buildkite.com/jito/jito-solana/builds?commit=${pr_ci_sha})

Do not merge this PR. Landing rewrites \`${CHANNEL}\` to keep Agave
ancestry, so a jito-solana team member force-pushes the approved
staging head:

\`\`\`bash
git fetch origin ${staging_branch}:refs/remotes/origin/${staging_branch}
test "\$(git rev-parse origin/${staging_branch})" = ${pr_staging_sha} || {
    echo "Staging moved; refresh this PR before landing." >&2
    exit 1
}
git push --force-with-lease=refs/heads/${CHANNEL}:${channel_sha} origin \\
    ${pr_staging_sha}:refs/heads/${CHANNEL}
\`\`\`

Jito carry commits:

${carry_text}

<details>
<summary>Patch invariance: range-diff of the carry commits before and after</summary>

Every intra-patch change must trace to an upstream commit.

\`\`\`text
${range_diff_text}
\`\`\`

</details>

- [ ] Review the upstream delta
- [ ] Review the range-diff
- [ ] Confirm Buildkite is green
- [ ] Force-push \`${CHANNEL}\` to the staging head

[Workflow run](${run_url})
EOF
}

upsert_pr() {
    local -r body_file="$1"
    local -r title="Nightly rebase: ${CHANNEL} onto agave/${UPSTREAM_CHANNEL}"
    local pr_number
    local pr_url

    pr_number="$(find_open_pr)"
    if [[ -n "${pr_number}" ]]; then
        gh pr edit "${pr_number}" \
            --title "${title}" \
            --body-file "${body_file}"
        pr_url="${GITHUB_SERVER_URL}/${GH_REPO}/pull/${pr_number}"
    else
        pr_url="$(gh pr create \
            --draft \
            --base "${CHANNEL}" \
            --head "${staging_branch}" \
            --title "${title}" \
            --body-file "${body_file}")"
    fi

    result_url="${pr_url}"
    write_result draft_pr "Draft PR refreshed"
    close_conflict_issue "${pr_url}"
}

ci_state() {
    gh api "repos/${GH_REPO}/commits/$1/status" \
        --jq "[.statuses[] | select(.context == \"${CI_CONTEXT}\")][0].state // \"missing\""
}

has_ci_status() {
    local state
    state="$(ci_state "$1")" || return
    [[ "${state}" != "missing" ]]
}

# Poll the commit status Buildkite reports to GitHub for the staging head.
# Prints success, failure, or timeout.
wait_for_ci() {
    local -r sha="$1"
    local -r deadline=$((SECONDS + CI_TIMEOUT_MINUTES * 60))
    local state

    while (( SECONDS < deadline )); do
        state="$(ci_state "${sha}")"
        case "${state}" in
            success) echo success; return ;;
            failure | error) echo failure; return ;;
        esac
        sleep "${CI_POLL_SECONDS}"
    done
    echo timeout
}

same_carry_series() {
    local range_diff
    range_diff="$(git range-diff --no-color --no-patch \
        "agave/${UPSTREAM_CHANNEL}..$1" \
        "agave/${UPSTREAM_CHANNEL}..$2" 2>/dev/null)" || return 1
    [[ -n "${range_diff}" ]] &&
        awk '$3 != "=" || $1 != $4 { exit 1 }' <<< "${range_diff}"
}

wait_for_landing() {
    load_state
    result_url="https://buildkite.com/jito/jito-solana/builds?commit=${ci_sha}"

    case "$(wait_for_ci "${ci_sha}")" in
        success)
            write_result failed "Buildkite passed but landing did not complete"
            echo 'ready-to-land=true' >> "${GITHUB_OUTPUT}"
            ;;
        failure)
            write_result ci_failure "Buildkite failure; ${staging_branch} left for review"
            ;;
        timeout)
            write_result ci_timeout "Buildkite timeout; ${staging_branch} left for review"
            ;;
    esac
}

land_channel() {
    load_state
    result_url="https://buildkite.com/jito/jito-solana/builds?commit=${ci_sha}"

    if ! git fetch --no-tags origin \
        "+refs/heads/${staging_branch}:${staging_ref}" ||
        [[ "$(git rev-parse "${staging_ref}")" != "${staging_sha}" ]]; then
        write_result failed "Staging moved after Buildkite; landing skipped"
        return
    fi

    if [[ "${ci_sha}" != "${staging_sha}" ]]; then
        if ! git fetch --no-tags origin \
            "+refs/heads/${trigger_branch}:${trigger_ref}" ||
            [[ "$(git rev-parse "${trigger_ref}")" != "${ci_sha}" ]] ||
            [[ "$(git rev-parse "${ci_sha}^{tree}")" != \
                "$(git rev-parse "${staging_sha}^{tree}")" ]]; then
            write_result failed "CI trigger no longer matches staging; landing skipped"
            return
        fi
    fi

    # The lease pins the channel tip we rebased from: a merge that landed
    # during CI makes this push fail instead of dropping that merge.
    if git push \
        --force-with-lease="refs/heads/${CHANNEL}:${channel_sha}" \
        origin "${staging_sha}:refs/heads/${CHANNEL}"; then
        result_url="${GITHUB_SERVER_URL}/${GH_REPO}/commit/${staging_sha}"
        write_result landed "Rebased ${CHANNEL} onto agave/${UPSTREAM_CHANNEL}; previous tip ${channel_sha:0:10}"
        close_conflict_issue "${result_url}"
    else
        write_result stale "${CHANNEL} moved during CI; retrying next run"
    fi
}

stage() {
    local previous_staging_sha=""
    local old_trigger_sha=""
    local candidate_sha
    local body_file
    local rebase_status
    local conflict_files
    local -a origin_fetch_args=(--no-tags --filter=blob:none)

    git remote add agave "${UPSTREAM_REPO}"
    if [[ "$(git rev-parse --is-shallow-repository)" == true ]]; then
        origin_fetch_args+=(--unshallow)
    fi
    git fetch "${origin_fetch_args[@]}" origin \
        "+refs/heads/${CHANNEL}:refs/remotes/origin/${CHANNEL}"
    git -c http.https://github.com/.extraheader= fetch \
        --no-tags --filter=blob:none agave \
        "+refs/heads/${UPSTREAM_CHANNEL}:refs/remotes/agave/${UPSTREAM_CHANNEL}"

    channel_sha="$(git rev-parse "origin/${CHANNEL}")"
    upstream_sha="$(git rev-parse "agave/${UPSTREAM_CHANNEL}")"

    if git merge-base --is-ancestor \
        "agave/${UPSTREAM_CHANNEL}" "origin/${CHANNEL}"; then
        open_pr_action close --comment "Channel now contains agave/${UPSTREAM_CHANNEL}."
        close_conflict_issue \
            "${GITHUB_SERVER_URL}/${GH_REPO}/commit/${channel_sha}"
        write_result fresh "Channel already contains the Agave tip"
        return
    fi

    if git fetch --no-tags origin \
        "+refs/heads/${staging_branch}:${staging_ref}" 2>/dev/null; then
        previous_staging_sha="$(git rev-parse "${staging_ref}")"
    fi

    git checkout -B "rebase-candidate/${CHANNEL}" "origin/${CHANNEL}"
    if git rebase --gpg-sign "agave/${UPSTREAM_CHANNEL}"; then
        :
    else
        rebase_status="$?"
        conflict_files="$(git diff --name-only --diff-filter=U)"
        if [[ -n "${conflict_files}" ]]; then
            write_conflict_report "${previous_staging_sha}" "${conflict_files}"
            git rebase --abort
            open_pr_action comment --body "Tonight's rebase conflicted: ${result_url}. This staging head is still valid, just stale."
            return
        fi
        write_result failed "Rebase failed without conflicts"
        git rebase --abort || true
        return "${rebase_status}"
    fi
    candidate_sha="$(git rev-parse HEAD)"
    ci_sha="${candidate_sha}"

    # Keep yesterday's staging head when tonight's rebase produced the same
    # tree on the same upstream tip, so its Buildkite result stays attached.
    if [[ "${previous_staging_sha}" == "${candidate_sha}" &&
        "${candidate_sha}" == "${upstream_sha}" ]] &&
        ! has_ci_status "${candidate_sha}"; then
        if git fetch --no-tags origin \
            "+refs/heads/${trigger_branch}:${trigger_ref}" 2>/dev/null; then
            old_trigger_sha="$(git rev-parse "${trigger_ref}")"
        fi
        git checkout --detach "${candidate_sha}"
        git commit --allow-empty --gpg-sign \
            -m "ci: retrigger ${CHANNEL} rebase (${GITHUB_RUN_ID}.${GITHUB_RUN_ATTEMPT})"
        ci_sha="$(git rev-parse HEAD)"
        git push --force-with-lease="refs/heads/${trigger_branch}:${old_trigger_sha}" \
            origin "${ci_sha}:refs/heads/${trigger_branch}"
        staging_sha="${candidate_sha}"
    elif [[ -n "${previous_staging_sha}" ]] &&
        git diff --quiet "${previous_staging_sha}" "${candidate_sha}" &&
        git merge-base --is-ancestor \
            "agave/${UPSTREAM_CHANNEL}" "${previous_staging_sha}" &&
        same_carry_series "${previous_staging_sha}" "${candidate_sha}" &&
        has_ci_status "${previous_staging_sha}"; then
        staging_sha="${previous_staging_sha}"
        ci_sha="${previous_staging_sha}"
    else
        git push --force-with-lease="refs/heads/${staging_branch}:${previous_staging_sha}" \
            origin "${candidate_sha}:refs/heads/${staging_branch}"
        staging_sha="${candidate_sha}"
    fi

    write_state
    if [[ "${LANDING}" == "auto" ]]; then
        write_result staged "Rebase staged; waiting for Buildkite"
        echo 'needs-landing=true' >> "${GITHUB_OUTPUT}"
        return
    fi

    body_file="$(mktemp)"
    write_pr_body "${staging_sha}" "${ci_sha}" "${body_file}"
    upsert_pr "${body_file}"
}

case "${1:-}" in
    stage) stage ;;
    wait) wait_for_landing ;;
    land) land_channel ;;
    *) echo "usage: $0 {stage|wait|land}" >&2; exit 2 ;;
esac
