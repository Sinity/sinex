# GitHub Actions Workflows

This directory documents the live workflows and how to rerun them locally. Everything is driven by `cargo xtask`; there are no bespoke helper scripts beyond the small CI wrappers under `scripts/`.

## Active Workflows

- **`ci.yml`** — Main gate on pushes/PRs. Boots Postgres via `scripts/ci-postgres.sh`, migrates with `sinex-schema`, runs `cargo xtask schema check-ready`, `cargo xtask lint-forbidden`, schema drift check, smoke fixtures (`cargo nextest run -p sinex-e2e-tests --profile fast`), and the reliable Nextest profile (`cargo xtask test --profile reliable`).
- **`db-checks.yml`** — Path-filtered database checks. When schemas change, runs `cargo xtask schema check-ready` plus a generate/sync smoke. When SQLx inputs change, runs `cargo xtask sqlx-prepare`, verifies `.sqlx/` is clean, and does an offline workspace check.
- **`schema-compatibility.yml`** — PR guard that runs `cargo xtask schema compat --base ${{ github.base_ref }}` and comments on failures.
- **`schema-management.yml`** — Validates JSON schemas, regenerates from code, and (on `master` pushes) deploys with `cargo xtask schema deploy` if the production DB secret is present.
- **`schema-auto-update.yml`** — Scheduled drift catch-up: regenerates schemas and `.sqlx/` via `cargo xtask` and opens auto-PRs against the default branch.

## Local Reproduction

Run workflow steps inside `nix develop`:

```bash
# Main CI gate locally (same as ci.yml body)
scripts/ci-devenv.sh bash <<'DEVENV'
set -euo pipefail
scripts/ci-postgres.sh <<'POSTGRES'
set -euo pipefail
DATABASE_URL="$DATABASE_URL_SUPERUSER" \
  cargo run --manifest-path crate/lib/sinex-schema/Cargo.toml --bin sinex-schema -- up
cargo xtask schema check-ready
cargo xtask lint-forbidden
cargo xtask schema generate
cargo nextest run -p sinex-e2e-tests --profile fast
cargo xtask test --profile reliable
POSTGRES
DEVENV

# Schema compat check (matches schema-compatibility.yml)
CI_BASE_BRANCH=master cargo xtask schema compat

# SQLx refresh (matches db-checks/sqlx path)
cargo xtask sqlx-prepare
```

Workflows assume `SQLX_OFFLINE=1` for checks unless explicitly testing online generation. Use `cargo xtask check` for the fastest local preflight.
