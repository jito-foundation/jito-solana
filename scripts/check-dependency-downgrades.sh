#!/usr/bin/env bash
#
# check-dependency-downgrades.sh — flag third-party dependency DOWNGRADES between two git refs.
#
# Compares the resolved versions recorded in Cargo.lock at BASE vs HEAD and reports any
# crate whose highest resolved version went *down*. This catches the class of bug where a
# rebase / channel cut silently reverts a dependency to an older version (e.g. prio-graph
# 0.3.0 -> =0.1.0 between v4.0 and v4.1), which a normal `git diff Cargo.toml` review misses
# because the manifest constraint syntax can change too (e.g. "0.3.0" -> "=0.1.0").
#
# It reads Cargo.lock (resolved versions), NOT Cargo.toml (constraints), so it sees what the
# build actually links.
#
# Usage:
#   scripts/check-dependency-downgrades.sh <base-ref> [head-ref] [upstream-ref]
#
#   <base-ref>      Required. Baseline to compare against, e.g. the previous Jito release tag
#                   (v4.0.0-jito) or the pre-rebase branch state (origin/v4.1).
#   [head-ref]      Optional. Defaults to the working tree's Cargo.lock (use HEAD for a commit).
#   [upstream-ref]  Optional. If given, each downgrade is classified:
#                     [upstream too] — upstream also has this lower version (probably benign)
#                     [JITO-ONLY]    — upstream does NOT, so the rebase introduced it (suspicious)
#
# Examples:
#   # channel cut review (what would have caught the prio-graph regression):
#   scripts/check-dependency-downgrades.sh v4.0.0-jito v4.1.0-beta.1-jito v4.1.0-beta.1
#
#   # nightly rebase CI: compare the freshly-rebased branch against what was shipping:
#   scripts/check-dependency-downgrades.sh origin/$BRANCH $REBASE_BRANCH upstream/$BRANCH
#
# Exit codes: 0 = no downgrades, 1 = downgrades found, 2 = usage / lookup error.

set -euo pipefail

LOCKFILE="${LOCKFILE:-Cargo.lock}"

if [[ $# -lt 1 || "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  sed -n '2,40p' "$0" | sed 's/^#//; s/^ //'
  exit 2
fi

BASE_REF="$1"
HEAD_REF="${2:-}"
UPSTREAM_REF="${3:-}"

# Emit "name<TAB>maxversion" for every package in a Cargo.lock, taking the highest resolved
# version when a crate appears multiple times. Reads from a git ref, or the working tree if
# ref is the literal "WORKTREE".
versions_at() {
  local ref="$1" content
  if [[ "$ref" == "WORKTREE" ]]; then
    content="$(cat "$LOCKFILE")"
  else
    if ! content="$(git show "${ref}:${LOCKFILE}" 2>/dev/null)"; then
      echo "error: cannot read ${LOCKFILE} at ref '${ref}'" >&2
      exit 2
    fi
  fi
  # Parse [[package]] stanzas: pair each name with its version, keep the max version per name.
  printf '%s\n' "$content" | awk '
    /^name = "/    { gsub(/^name = "|"$/, ""); name=$0; next }
    /^version = "/ { gsub(/^version = "|"$/, ""); if (name != "") { print name "\t" $0 }; name="" }
  ' | sort | awk -F'\t' '
    { if ($1 != cur) { if (cur != "") print cur "\t" max; cur=$1; max=$2 }
      else { if (cmp($2, max) > 0) max=$2 } }
    END { if (cur != "") print cur "\t" max }
    function cmp(a, b,  na, nb, i) {
      # numeric-dotted semver-ish compare; falls back to string for pre-release tails.
      split(a, na, /[.+-]/); split(b, nb, /[.+-]/)
      for (i = 1; i <= 3; i++) {
        if ((na[i]+0) > (nb[i]+0)) return 1
        if ((na[i]+0) < (nb[i]+0)) return -1
      }
      if (a > b) return 1; if (a < b) return -1; return 0
    }'
}

BASE_FILE="$(mktemp)"; HEAD_FILE="$(mktemp)"; UP_FILE="$(mktemp)"
trap 'rm -f "$BASE_FILE" "$HEAD_FILE" "$UP_FILE"' EXIT

versions_at "$BASE_REF" > "$BASE_FILE"
versions_at "${HEAD_REF:-WORKTREE}" > "$HEAD_FILE"
if [[ -n "$UPSTREAM_REF" ]]; then
  versions_at "$UPSTREAM_REF" > "$UP_FILE"
else
  : > "$UP_FILE"
fi

echo "Dependency downgrade check"
echo "  base:     ${BASE_REF}"
echo "  head:     ${HEAD_REF:-<working tree>}"
[[ -n "$UPSTREAM_REF" ]] && echo "  upstream: ${UPSTREAM_REF}"
echo

# Join base+head on crate name, report where head's max version < base's max version.
FOUND="$(join -t$'\t' "$BASE_FILE" "$HEAD_FILE" | awk -F'\t' '
  function cmp(a, b,  na, nb, i) {
    split(a, na, /[.+-]/); split(b, nb, /[.+-]/)
    for (i = 1; i <= 3; i++) { if ((na[i]+0) > (nb[i]+0)) return 1; if ((na[i]+0) < (nb[i]+0)) return -1 }
    if (a > b) return 1; if (a < b) return -1; return 0
  }
  { if (cmp($3, $2) < 0) print $1 "\t" $2 "\t" $3 }')"

if [[ -z "$FOUND" ]]; then
  echo "✓ No dependency downgrades detected."
  exit 0
fi

status=1
echo "⚠ Dependency downgrades detected (resolved version went DOWN):"
echo
printf '  %-32s %-14s %-14s %s\n' "crate" "base" "head" "classification"
printf '  %-32s %-14s %-14s %s\n' "-----" "----" "----" "--------------"
while IFS=$'\t' read -r name base head; do
  cls=""
  if [[ -n "$UPSTREAM_REF" ]]; then
    up="$(awk -F'\t' -v n="$name" '$1==n{print $2}' "$UP_FILE")"
    if [[ -z "$up" ]]; then
      cls="JITO-ONLY (upstream dropped it) — review"
    elif [[ "$up" == "$head" ]]; then
      cls="upstream too (likely benign)"
    else
      cls="JITO-ONLY (upstream has $up) — review"
    fi
  fi
  printf '  %-32s %-14s %-14s %s\n' "$name" "$base" "$head" "$cls"
done <<< "$FOUND"
echo
echo "Review each: a downgrade on a rebase usually means an older snapshot won a merge conflict."
exit $status
