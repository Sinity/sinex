#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" -ne 0 ]]; then
  echo "verify-quick accepts no caller-controlled arguments" >&2
  exit 2
fi

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

if git show-ref --verify --quiet refs/remotes/origin/master; then
  base_ref=origin/master
elif git show-ref --verify --quiet refs/heads/master; then
  base_ref=master
else
  echo "verify-quick requires origin/master or master to determine the merge base" >&2
  exit 1
fi
merge_base="$(git merge-base HEAD "$base_ref")"

mapfile -d '' changed_rust_paths < <(
  {
    git diff --name-only -z "$merge_base"...HEAD -- '*.rs'
    git diff --name-only -z -- '*.rs'
    git diff --cached --name-only -z -- '*.rs'
    git ls-files --others --exclude-standard -z -- '*.rs'
  } | sort -zu
)
rust_files=()
for path in "${changed_rust_paths[@]}"; do
  if [[ -f "$path" ]]; then
    rust_files+=("$path")
  fi
done

if [[ "${#rust_files[@]}" -gt 0 ]]; then
  echo "Checking formatting for ${#rust_files[@]} changed Rust file(s)..."
  rustfmt --edition 2024 --config skip_children=true --check "${rust_files[@]}"
else
  echo "No changed Rust files; skipping rustfmt."
fi

echo "Scanning blocking ast-grep rules..."
scan_status=0
scan_output="$(
  ast-grep scan \
    --config .config/ast-grep/sgconfig.yml \
    --json=stream \
    --include-metadata \
    --globs '!**/tests/**' \
    --globs '!**/tests.rs' \
    --globs '!**/*_test.rs' \
    --globs '!**/test_*.rs' \
    --globs '!**/build.rs' \
    .
)" || scan_status=$?
if [[ "$scan_status" -gt 1 ]]; then
  printf '%s\n' "$scan_output" >&2
  echo "ast-grep failed with exit code $scan_status" >&2
  exit "$scan_status"
fi

error_findings="$(jq -r -s '
  [.[] | select(.severity == "error") |
    "\(.file):\(.range.start.line):\(.range.start.column) [\(.ruleId)] \(.message)"]
  | .[]
' <<<"$scan_output")" || {
  echo "could not parse ast-grep JSON output" >&2
  exit 1
}
if [[ -n "$error_findings" ]]; then
  printf '%s\n' "$error_findings" >&2
  echo "blocking ast-grep findings detected" >&2
  exit 1
fi

echo "Checking Git diff whitespace..."
git diff --check "$merge_base"...HEAD
git diff --check --cached
git diff --check

echo "Quick static verification passed (Rust formatting, blocking ast-grep rules, Git diff whitespace)."
