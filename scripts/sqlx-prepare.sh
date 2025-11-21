#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$PROJECT_ROOT/target/sqlx-prepare}"

echo "Preparing SQLx metadata using workspace-wide build..."

scripts/ci-postgres.sh <<'SQLX'
pushd crate/lib/sinex-schema >/dev/null
DATABASE_URL="$DATABASE_URL_SUPERUSER" cargo run -- up
popd
cargo sqlx prepare --workspace -- --all-targets --all-features
for crate in crate/lib/sinex-test-utils crate/lib/sinex-satellite-sdk crate/lib/sinex-core; do
  pushd "$crate" >/dev/null
  if [[ "$crate" == *sinex-test-utils ]]; then
    cargo sqlx prepare -- --all-targets
  elif [[ "$crate" == *sinex-core ]]; then
    cargo sqlx prepare -- --all-targets --all-features
  else
    cargo sqlx prepare -- --all-targets --all-features
  fi
  popd
  if [ -d "$crate/.sqlx" ]; then
    rsync -a "$crate/.sqlx/" .sqlx/
    rm -rf "$crate/.sqlx"
  fi
done
SQLX

echo "✅ SQLx metadata refreshed in .sqlx/"
