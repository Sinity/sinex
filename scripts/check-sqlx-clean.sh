#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root_dir"

echo "🧪 checking .sqlx cache freshness..."

if ! command -v sqlx >/dev/null 2>&1; then
  echo "sqlx not found on PATH; install sqlx-cli or run via devenv." >&2
  exit 1
fi

# Check .sqlx metadata against current schema hash without rewriting.
if ! cargo xtask sqlx-check; then
  exit 1
fi

echo "✅ .sqlx cache matches current schema."
