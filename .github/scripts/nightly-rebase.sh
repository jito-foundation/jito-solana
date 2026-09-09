#!/bin/bash
#
# Rebase one jito-solana channel onto its upstream branch.
#
# LANDING=draft: push the rebased head to ci/rebase/<channel> and refresh a
# draft PR for a human to land by force-pushing the channel.
# LANDING=auto:  push the staging branch, wait for Buildkite, then force-push
# the channel with --force-with-lease so a concurrent merge aborts the landing.
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
: "${GITHUB_RUN_ID:?GITHUB_RUN_ID is required}"
: "${GITHUB_SERVER_URL:?GITHUB_SERVER_URL is required}"
: "${UPSTREAM_REPO:=https://github.com/anza-xyz/agave.git}"
: "${CI_CONTEXT:=buildkite/jito-solana}"
: "${CI_TIMEOUT_MINUTES:=240}"
: "${CI_POLL_SECONDS:=60}"

declare -g result_status="failed"
declare -g result_detail="Run failed before producing a result"
declare -g result_url=""
declare -g channel_sha=""
declare -g upstream_sha=""

declare -gr staging_branch="ci/rebase/${CHANNEL}"
declare -gr staging_ref="refs/remotes/origin/${staging_branch}"
declare -gr conflict_issue_title="Nightly rebase conflict: ${CHANNEL}"
declare -gr run_url="${GITHUB_SERVER_URL}/${GH_REPO}/actions/runs/${GITHUB_RUN_ID}"

buildkite_url() {
    echo "https://buildkite.com/jito/jito-solana/builds?commit=$1"
}

write_result() {
    jq -n \
        --arg channel "${CHANNEL}" \
        --arg status "${result_status}" \
        --arg detail "${result_detail}" \
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
write_result  # a crash leaves this failed result for the summary

find_open_pr() {
    gh pr list \
        --repo "${GH_REPO}" \
        --state open \
        --base "${CHANNEL}" \
        --head "${staging_branch}" \
        --json number \
        --jq '.[0].number // empty'
}

find_conflict_issue() {
    gh issue list \
        --repo "${GH_REPO}" \
        --state open \
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
            --repo "${GH_REPO}" \
            --comment "Resolved by ${resolution_url}."
    fi
}

# open_pr_action <gh pr verb> <args...>: acts on the channel's staging PR if any.
open_pr_action() {
    local pr_number
    pr_number="$(find_open_pr)"

    if [[ -n "${pr_number}" ]]; then
        gh pr "$1" "${pr_number}" --repo "${GH_REPO}" "${@:2}"
    fi
}

write_conflict_report() {
    local -r last_staging_sha="$1"
    local conflict_files
    local issue_number
    local issue_url
    local report_file

    conflict_files="$(git diff --name-only --diff-filter=U)"
    report_file="$(mktemp)"

    if [[ -z "${conflict_files}" ]]; then
        conflict_files="No unmerged paths reported"
    fi

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
            --repo "${GH_REPO}" \
            --body-file "${report_file}" \
            --add-assignee "${CHANNEL_OWNER}"
        issue_url="$(gh issue view "${issue_number}" \
            --repo "${GH_REPO}" --json url --jq .url)"
    else
        issue_url="$(gh issue create \
            --repo "${GH_REPO}" \
            --title "${conflict_issue_title}" \
            --body-file "${report_file}" \
            --assignee "${CHANNEL_OWNER}")"
    fi

    result_status="conflict"
    result_detail="Rebase conflict"
    result_url="${issue_url}"
    write_result
}

write_pr_body() {
    local -r carry_file="$1"
    local -r upstream_count="$2"
    local -r range_diff_file="$3"
    local -r staging_sha="$4"
    local -r body_file="$5"

    cat >| "${body_file}" <<EOF
Automated nightly rebase of \`${CHANNEL}\` onto
\`agave/${UPSTREAM_CHANNEL}\`.

- Channel tip: \`${channel_sha}\`
- Agave tip: \`${upstream_sha}\`
- Upstream delta: ${upstream_count} commits
- Staging branch: \`${staging_branch}\`
- [Buildkite]($(buildkite_url "${staging_sha}"))

Do not merge this PR. Landing rewrites \`${CHANNEL}\` to keep Agave
ancestry, so a jito-solana team member force-pushes the approved
staging head:

\`\`\`bash
git fetch origin ${staging_branch}
git push --force-with-lease=refs/heads/${CHANNEL}:${channel_sha} origin \\
    origin/${staging_branch}:refs/heads/${CHANNEL}
\`\`\`

Jito carry commits:

$(cat "${carry_file}")

<details>
<summary>Patch invariance: range-diff of the carry commits before and after</summary>

Every intra-patch change must trace to an upstream commit.

\`\`\`text
$(cat "${range_diff_file}")
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
            --repo "${GH_REPO}" \
            --title "${title}" \
            --body-file "${body_file}"
        pr_url="$(gh pr view "${pr_number}" \
            --repo "${GH_REPO}" --json url --jq .url)"
    else
        pr_url="$(gh pr create \
            --repo "${GH_REPO}" \
            --draft \
            --base "${CHANNEL}" \
            --head "${staging_branch}" \
            --title "${title}" \
            --body-file "${body_file}")"
    fi

    result_status="draft_pr"
    result_detail="Draft PR refreshed"
    result_url="${pr_url}"
    write_result
    close_conflict_issue "${pr_url}"
}

# Poll the commit status Buildkite reports to GitHub for the staging head.
# Prints success, failure, or timeout.
wait_for_ci() {
    local -r sha="$1"
    local -r deadline=$((SECONDS + CI_TIMEOUT_MINUTES * 60))
    local state

    while (( SECONDS < deadline )); do
        state="$(gh api "repos/${GH_REPO}/commits/${sha}/status" \
            --jq "[.statuses[] | select(.context == \"${CI_CONTEXT}\")][0].state // \"pending\"")"
        case "${state}" in
            success) echo success; return ;;
            failure | error) echo failure; return ;;
        esac
        sleep "${CI_POLL_SECONDS}"
    done
    echo timeout
}

land_channel() {
    local -r staging_sha="$1"
    local ci_state
    ci_state="$(wait_for_ci "${staging_sha}")"

    result_url="$(buildkite_url "${staging_sha}")"
    if [[ "${ci_state}" != "success" ]]; then
        result_status="ci_${ci_state}"
        result_detail="Buildkite ${ci_state}; ${staging_branch} left for review"
        write_result
        return
    fi

    # The lease pins the channel tip we rebased from: a merge that landed
    # during CI makes this push fail instead of dropping that merge.
    if git push \
        --force-with-lease="refs/heads/${CHANNEL}:${channel_sha}" \
        origin "${staging_sha}:refs/heads/${CHANNEL}"; then
        result_status="landed"
        result_detail="Rebased ${CHANNEL} onto agave/${UPSTREAM_CHANNEL}; previous tip ${channel_sha:0:10}"
        result_url="${GITHUB_SERVER_URL}/${GH_REPO}/commit/${staging_sha}"
        write_result
        close_conflict_issue "${result_url}"
    else
        result_status="stale"
        result_detail="${CHANNEL} moved during CI; retrying next run"
        write_result
    fi
}

main() {
    local staging_sha=""
    local candidate_sha
    local upstream_count
    local carry_file
    local range_diff_file
    local body_file

    git remote add agave "${UPSTREAM_REPO}"
    git -c http.https://github.com/.extraheader= fetch --no-tags agave \
        "+refs/heads/${UPSTREAM_CHANNEL}:refs/remotes/agave/${UPSTREAM_CHANNEL}"
    git fetch --no-tags origin \
        "+refs/heads/${CHANNEL}:refs/remotes/origin/${CHANNEL}"

    channel_sha="$(git rev-parse "origin/${CHANNEL}")"
    upstream_sha="$(git rev-parse "agave/${UPSTREAM_CHANNEL}")"

    if git fetch --no-tags origin \
        "+refs/heads/${staging_branch}:${staging_ref}" 2>/dev/null; then
        staging_sha="$(git rev-parse "${staging_ref}")"
    fi

    if git merge-base --is-ancestor \
        "agave/${UPSTREAM_CHANNEL}" "origin/${CHANNEL}"; then
        open_pr_action close --comment "Channel now contains agave/${UPSTREAM_CHANNEL}."
        close_conflict_issue \
            "${GITHUB_SERVER_URL}/${GH_REPO}/commit/${channel_sha}"
        result_status="fresh"
        result_detail="Channel already contains the Agave tip"
        write_result
        return
    fi

    carry_file="$(mktemp)"
    git log --format="- \`%h\` %s" \
        "agave/${UPSTREAM_CHANNEL}..origin/${CHANNEL}" >| "${carry_file}"
    if [[ ! -s "${carry_file}" ]]; then
        echo "- None" >| "${carry_file}"
    fi

    git checkout -B "rebase-candidate/${CHANNEL}" "origin/${CHANNEL}"
    if ! git rebase --gpg-sign "agave/${UPSTREAM_CHANNEL}"; then
        write_conflict_report "${staging_sha}"
        git rebase --abort
        open_pr_action comment --body "Tonight's rebase conflicted: ${result_url}. This staging head is still valid, just stale."
        return
    fi
    candidate_sha="$(git rev-parse HEAD)"

    # Keep yesterday's staging head when tonight's rebase produced the same
    # tree on the same upstream tip, so its Buildkite result stays attached.
    if [[ -n "${staging_sha}" ]] &&
        git diff --quiet "${staging_sha}" "${candidate_sha}" &&
        git merge-base --is-ancestor "agave/${UPSTREAM_CHANNEL}" "${staging_sha}"; then
        candidate_sha="${staging_sha}"
    else
        git push --force-with-lease="refs/heads/${staging_branch}:${staging_sha}" \
            origin "${candidate_sha}:refs/heads/${staging_branch}"
    fi

    if [[ "${LANDING}" == "auto" ]]; then
        land_channel "${candidate_sha}"
        return
    fi

    upstream_count="$(git rev-list --count \
        "origin/${CHANNEL}..agave/${UPSTREAM_CHANNEL}")"
    range_diff_file="$(mktemp)"
    git range-diff --no-color \
        "$(git merge-base "origin/${CHANNEL}" "agave/${UPSTREAM_CHANNEL}")..origin/${CHANNEL}" \
        "agave/${UPSTREAM_CHANNEL}..${candidate_sha}" >| "${range_diff_file}"
    body_file="$(mktemp)"
    write_pr_body "${carry_file}" "${upstream_count}" "${range_diff_file}" \
        "${candidate_sha}" "${body_file}"
    upsert_pr "${body_file}"
}

main "${@}"
