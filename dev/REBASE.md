# Keeping jito-solana rebased on Agave

Every weekday at 19:00 UTC the `Nightly Agave Rebase` workflow rebases each
channel in `.github/rebase-channels.json` onto its Agave branch and posts one
Slack summary. What you do next depends on the channel's `landing` mode and
the status in that summary.

## master (`landing: auto`)

The bot rebases master's carry commits (the Jito Patch plus anything merged
since) onto `agave/master`, pushes `ci/rebase/master`, waits for Buildkite,
then force-pushes master. Nothing to do unless Slack reports a problem.

Merging your PR to master works as before: squash-merge it. It becomes a carry
commit and is replayed every night. Keep carry commits few and self-contained;
they are what conflicts with upstream.

After a landing, master's history is rewritten. Rebase open branches with the
previous tip from the Slack line:

```bash
git fetch origin
git rebase --onto origin/master <previous tip>
```

## v4.2, v4.3 (`landing: draft`)

The bot pushes `ci/rebase/vX.Y` and opens or refreshes one draft PR titled
`Nightly rebase: vX.Y onto agave/vX.Y`. Do not merge it. To land:

1. Read the upstream delta and the `range-diff` in the PR body. Every change
   inside a carry commit must trace to an upstream commit.
2. Confirm Buildkite is green on the staging head.
3. Run the force-push command from the PR body. It uses `--force-with-lease`
   pinned to the tip the bot rebased from, so it fails if the channel moved.

GitHub marks the draft PR merged once its head is on the channel. The next
nightly run reports `fresh`.

## Conflicts

The bot never resolves conflicts. It opens or refreshes an issue titled
`Nightly rebase conflict: <channel>`, assigned to the channel owner, listing
the conflicting files. Resolve it by hand:

```bash
git fetch origin <channel>
git fetch agave <channel>
git checkout -B ex/<channel>_rebase origin/<channel>
git rebase agave/<channel>            # resolve, then git rebase --continue
git range-diff "$(git merge-base origin/<channel> agave/<channel>)..origin/<channel>" agave/<channel>..HEAD
git push --force-with-lease=refs/heads/<channel>:<channel tip> origin HEAD:<channel>
```

`<channel tip>` is in the issue. The next run sees the channel contains the
Agave tip and closes the issue. The `/rebase` skill covers conflict resolution
in depth, including the consensus-path rules for `svm/`, `runtime/`,
`ledger/`, and `core/src/banking_stage/`.

## Slack statuses

| Status | Meaning | Action |
|---|---|---|
| `fresh` | Channel already contains the Agave tip | none |
| `landed` | Auto channel rebased and pushed | rebase your open branches |
| `draft_pr` | Draft channel staged | land it when reviewed |
| `conflict` | Rebase stopped, issue filed | owner resolves by hand |
| `ci_failure` | Buildkite red on the staging head | check the build; staging branch is left in place |
| `ci_timeout` | Buildkite did not report within 4 hours | check Buildkite, rerun the workflow |
| `stale` | Channel moved during CI, landing skipped | none, next run retries |
| `failed` | Script crashed | read the run log |

## Channels

Edit `.github/rebase-channels.json`. Each entry names the channel branch, the
Agave branch, an owner for conflict issues, and `landing`. Add a channel when
Agave cuts a release branch; drop it when Agave stops backporting to it.

## Setup

The workflow needs a GitHub App (Contents, Issues, Pull requests: write)
installed on the repo, with `REBASE_APP_ID` and `REBASE_APP_PRIVATE_KEY` as
secrets, plus `GPG_PRIVATE_KEY`, `GPG_PASSPHRASE`, and `SLACK_WEBHOOK_URL`.
For every `auto` channel the App must be an `always` bypass actor on the
rulesets protecting that branch. Keep it off the `v*.*` rulesets so the bot
cannot rewrite release lines.
