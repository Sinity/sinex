# GitHub Actions Workflows

This directory documents the live workflows and how to rerun them locally. Everything is driven by `xtask`; CI spins up Postgres via `xtask ci postgres` and runs the same xtask pipelines developers use.

## Active Workflows

- **`ci.yml`** — Main gate on pushes/PRs. Boots Postgres with `xtask ci postgres -- xtask ci workspace`, which migrates with `sinex-schema`, runs `xtask schema check-ready`, `xtask lint-forbidden`, a schema drift check, smoke fixtures (`xtask test --profile default -- -p sinex-e2e-tests`), and the CI Nextest profile (`xtask test --profile ci --prime`).
- **`db-checks.yml`** — Path-filtered database checks. When schemas change, runs `xtask schema check-ready` plus a generate/sync smoke.
- **`schema-compatibility.yml`** — PR guard that runs `xtask schema compat --base ${{ github.base_ref }}` and comments on failures.
- **`schema-management.yml`** — Validates JSON schemas, regenerates from code, and (on `master` pushes) deploys with `xtask schema deploy` if the production DB secret is present.
- **`schema-auto-update.yml`** — Scheduled drift catch-up: regenerates schemas via `xtask` and opens auto-PRs against the default branch.

## Local Reproduction

Run workflow steps inside `nix develop`:

```bash
# Main CI gate locally (same as ci.yml body)
nix develop --accept-flake-config --no-pure-eval --command \
  xtask ci postgres -- \
  xtask ci workspace

# Schema compat check (matches schema-compatibility.yml)
CI_BASE_BRANCH=master xtask schema compat

```

Use `xtask check` for the fastest local preflight.
