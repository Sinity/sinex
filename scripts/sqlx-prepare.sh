#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$PROJECT_ROOT/target/sqlx-prepare}"

rm -rf .sqlx
mkdir -p .sqlx

echo "Preparing SQLx metadata using workspace-wide build..."

scripts/ci-postgres.sh <<'SQLX'
DATABASE_URL="$DATABASE_URL_SUPERUSER" \
  cargo run \
    --manifest-path crate/lib/sinex-schema/Cargo.toml \
    --bin sinex-schema -- \
    up
cargo sqlx prepare --workspace -- --all-targets --all-features
extra_crates=(
  crate/lib/sinex-test-utils
  crate/lib/sinex-satellite-sdk
  crate/lib/sinex-core
  crate/lib/sinex-schema
  crate/lib/sinex-services
  crate/core/sinex-gateway
  crate/core/sinex-ingestd
  crate/satellites/sinex-desktop-satellite
)

for crate in "${extra_crates[@]}"; do
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
done

find crate -type d -name '.sqlx' | while read -r sqlx_dir; do
  if [ -d "$sqlx_dir" ] && [ "$sqlx_dir" != "./.sqlx" ]; then
    rsync -a "$sqlx_dir/" .sqlx/
  fi
done
SQLX

echo "✅ SQLx metadata refreshed in .sqlx/"
