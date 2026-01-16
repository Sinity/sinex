#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  scripts/bench-nextest.sh [args]

Thin wrapper around `cargo xtask bench`.
Examples:
  scripts/bench-nextest.sh --profile fast --threads 16,32 --runs 3
  scripts/bench-nextest.sh --mode refine --threads 8,16,24
USAGE
}

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
  usage
  exit 0
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if command -v direnv >/dev/null 2>&1 && [[ -f "$repo_root/.envrc" ]]; then
  exec direnv exec "$repo_root" cargo xtask bench "$@"
else
  exec cargo xtask bench "$@"
fi
