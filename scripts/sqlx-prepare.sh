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
for crate in crate/lib/sinex-test-utils crate/lib/sinex-satellite-sdk crate/lib/sinex-core crate/lib/sinex-schema crate/core/sinex-gateway; do
  pushd "$crate" >/dev/null
  case "$crate" in
    *sinex-test-utils)
      cargo sqlx prepare -- --all-targets
      ;;
    *sinex-core)
      cargo sqlx prepare -- --all-targets --all-features
      ;;
    *sinex-schema)
      cargo sqlx prepare -- --all-targets --all-features
      ;;
    *sinex-gateway)
      cargo sqlx prepare -- --all-targets --all-features
      ;;
    *)
      cargo sqlx prepare -- --all-targets --all-features
      ;;
  esac
  popd
  if [ -d "$crate/.sqlx" ]; then
    rsync -a "$crate/.sqlx/" .sqlx/
    rm -rf "$crate/.sqlx"
  fi
done
SQLX

echo "✅ SQLx metadata refreshed in .sqlx/"
