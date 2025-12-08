#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root_dir"

echo "🧪 checking .sqlx cache freshness..."

if ! command -v sqlx >/dev/null 2>&1; then
  echo "sqlx not found on PATH; install sqlx-cli or run via devenv." >&2
  exit 1
fi

# Run prepare in offline mode to refresh metadata quickly.
SQLX_OFFLINE=1 sqlx prepare --workspace -- --all-targets --all-features >/dev/null

if ! git diff --quiet -- .sqlx; then
  echo ".sqlx metadata changed. Please commit updated files." >&2
  git status --short .sqlx
  exit 1
fi

echo "✅ .sqlx cache is up to date."
