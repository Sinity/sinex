#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root_dir"

echo "🧪 checking .sqlx cache freshness..."

if ! command -v sqlx >/dev/null 2>&1; then
  echo "sqlx not found on PATH; install sqlx-cli or run via devenv." >&2
  exit 1
fi

# Run prepare in offline check-only mode to validate metadata without rewriting it.
if ! SQLX_OFFLINE=1 cargo sqlx prepare --workspace --check -- --all-targets --all-features; then
  echo "sqlx prepare --check failed (offline). Regenerate with 'devenv tasks run sqlx:prepare' when schema changes." >&2
  exit 1
fi

echo "✅ .sqlx cache matches current schema."
