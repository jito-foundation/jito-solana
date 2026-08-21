#!/bin/bash

set -o noclobber  # Prevent overwriting existing files with >
set -o nounset    # Treat unset variables as an error
set -o errexit    # Exit immediately if a command exits with non-zero status
set -o pipefail   # Prevent errors in a pipeline from being masked
set -o errtrace   # Ensure that any error traps are inherited by functions

on_fatal_error() {
    local -r exit_code="${?}"
    { set +x; } 2>/dev/null
    local -r line_number="${1:-}"
    local -r file="${BASH_SOURCE[0]}"
    echo "FATAL: Failed at ${file}:${line_number}: ${BASH_COMMAND}" \
        "(exit code: ${exit_code})" >&2

    if declare -F on_fatal_cleanup >/dev/null; then
        trap - ERR
        set +o errexit
        on_fatal_cleanup
        set -o errexit
    fi

    [[ "${exit_code}" -eq 0 ]] && exit 1
    exit $((exit_code))
}
trap 'on_fatal_error ${LINENO}' ERR

: "${CHANNEL:?CHANNEL is required}"
: "${UPSTREAM_CHANNEL:?UPSTREAM_CHANNEL is required}"
: "${CHANNEL_OWNER:?CHANNEL_OWNER is required}"
: "${COMMITTER_EMAIL:?COMMITTER_EMAIL is required}"
: "${COMMITTER_NAME:?COMMITTER_NAME is required}"
: "${GH_REPO:?GH_REPO is required}"
: "${RESULT_FILE:?RESULT_FILE is required}"
: "${GITHUB_RUN_ID:?GITHUB_RUN_ID is required}"
: "${GITHUB_SERVER_URL:?GITHUB_SERVER_URL is required}"
: "${UPSTREAM_REPO:=https://github.com/anza-xyz/agave.git}"

declare -g result_status="failed"
declare -g result_detail="Run failed before producing a result"
declare -g result_url=""
declare -g channel_sha=""
declare -g upstream_sha=""

declare -gr staging_branch="ci/rebase/${CHANNEL}"
declare -gr staging_ref="refs/remotes/origin/${staging_branch}"
declare -gr conflict_issue_title="Nightly rebase conflict: ${CHANNEL}"

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

on_fatal_cleanup() {
    result_status="failed"
    result_detail="Unexpected workflow failure"
    write_result
}

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
        --limit 100 \
        --json number,title |
        jq -r --arg title "${conflict_issue_title}" \
            'map(select(.title == $title))[0].number // empty'
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

close_open_pr() {
    local -r comment="$1"
    local pr_number
    pr_number="$(find_open_pr)"

    if [[ -n "${pr_number}" ]]; then
        gh pr close "${pr_number}" \
            --repo "${GH_REPO}" \
            --comment "${comment}"
    fi
}

write_conflict_report() {
    local -r last_staging_sha="$1"
    local conflict_files
    local git_status
    local issue_number
    local issue_url
    local report_file

    conflict_files="$(git diff --name-only --diff-filter=U)"
    git_status="$(git status --short | sed -n '1,100p')"
    report_file="$(mktemp)"

    if [[ -z "${conflict_files}" ]]; then
        conflict_files="No unmerged paths reported"
    fi
    if [[ -z "${git_status}" ]]; then
        git_status="No status entries reported"
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

\`git status --short\`:

\`\`\`text
${git_status}
\`\`\`

[Workflow run](${GITHUB_SERVER_URL}/${GH_REPO}/actions/runs/${GITHUB_RUN_ID})
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
    local -r body_file="$3"
    local carry_commits
    carry_commits="$(cat "${carry_file}")"

    cat >| "${body_file}" <<EOF
Automated nightly rebase of \`${CHANNEL}\` onto
\`agave/${UPSTREAM_CHANNEL}\`.

- Channel tip: \`${channel_sha}\`
- Agave tip: \`${upstream_sha}\`
- Upstream delta: ${upstream_count} commits
- Staging branch: \`${staging_branch}\`

Landing method: update \`${CHANNEL}\` to this staging head to preserve
Agave ancestry.

Jito carry commits:

${carry_commits}

- [ ] Review the upstream delta
- [ ] Review the Jito carry commits
- [ ] Confirm Buildkite is green
- [ ] Update \`${CHANNEL}\` to the approved staging head

[Workflow run](${GITHUB_SERVER_URL}/${GH_REPO}/actions/runs/${GITHUB_RUN_ID})
EOF
}

upsert_pr() {
    local -r body_file="$1"
    local -r title="Nightly rebase: ${CHANNEL} onto agave/${UPSTREAM_CHANNEL}"
    local pr_number
    local pr_url
    local is_draft

    pr_number="$(find_open_pr)"
    if [[ -n "${pr_number}" ]]; then
        gh pr edit "${pr_number}" \
            --repo "${GH_REPO}" \
            --title "${title}" \
            --body-file "${body_file}"
        is_draft="$(gh pr view "${pr_number}" \
            --repo "${GH_REPO}" --json isDraft --jq .isDraft)"
        if [[ "${is_draft}" != "true" ]]; then
            gh pr ready "${pr_number}" --repo "${GH_REPO}" --undo
        fi
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

main() {
    local staging_sha=""
    local candidate_sha
    local new_staging_sha
    local upstream_count
    local carry_file
    local body_file

    git config user.email "${COMMITTER_EMAIL}"
    git config user.name "${COMMITTER_NAME}"
    git config commit.gpgsign true

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
        close_open_pr "Channel now contains agave/${UPSTREAM_CHANNEL}."
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
        close_open_pr "Superseded by ${result_url}."
        return
    fi
    candidate_sha="$(git rev-parse HEAD)"
    new_staging_sha="${candidate_sha}"

    if [[ -n "${staging_sha}" ]]; then
        if git diff --quiet "${staging_sha}" "${candidate_sha}" &&
            git merge-base --is-ancestor \
                "agave/${UPSTREAM_CHANNEL}" "${staging_sha}"; then
            new_staging_sha="${staging_sha}"
        elif ! git merge-base --is-ancestor \
            "${staging_sha}" "${candidate_sha}"; then
            # Preserve the prior staging tip as the parent so pushes stay
            # fast-forward-only. The candidate parent preserves Agave history.
            new_staging_sha="$(
                printf 'Refresh nightly rebase for %s\n' "${CHANNEL}" |
                    git commit-tree -S "${candidate_sha}^{tree}" \
                        -p "${staging_sha}" -p "${candidate_sha}"
            )"
        fi
    fi

    git branch -f "${staging_branch}" "${new_staging_sha}"
    if [[ "${new_staging_sha}" != "${staging_sha}" ]]; then
        git push origin \
            "refs/heads/${staging_branch}:refs/heads/${staging_branch}"
    fi

    upstream_count="$(git rev-list --count \
        "origin/${CHANNEL}..agave/${UPSTREAM_CHANNEL}")"
    body_file="$(mktemp)"
    write_pr_body "${carry_file}" "${upstream_count}" "${body_file}"
    upsert_pr "${body_file}"
}

main "${@}"
