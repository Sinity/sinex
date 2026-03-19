# GitHub Actions Workflows

This directory documents the live workflows and how to rerun them locally. The
default GitHub Actions gate is narrower than the full VM/e2e story: today it is
the Postgres-backed workspace gate through `xtask ci`, plus a handful of
separate scheduled/manual workflows.

## Active Workflows

- **`ci.yml`** — Main gate on pushes/PRs. Runs
  `xtask xtr ci postgres -- xtask xtr ci workspace`.

  The workspace stage applies declarative schema, checks contract tables, runs
  `cargo deny`, runs `xtask check` and `xtask lint-forbidden` in parallel, enforces
  workspace cleanliness, runs the `sinex-e2e-tests` package, then runs the rest of
  the test suite with `sinex-e2e-tests` excluded. It does **not** run the NixOS VM
  suite in `tests/e2e/nixos-vm/`.
- **`db-checks.yml`** — Path-filtered database checks. When schemas change, runs a
  narrower schema-focused pipeline rather than the full CI gate.
- **`verify-perf.yml`** — Nightly / on-demand performance verification. Runs
  `xtask test bench --contracts` inside an ephemeral Postgres environment and
  uploads the perf-verification artifacts.
- **`n1-compat.yml`** — Weekly / on-demand N-1 protocol compatibility check. Brings
  up current gateway plus the latest released terminal ingestor and verifies the
  rolling-update path still moves events.
- **`fuzz.yml`** — Nightly / on-demand fuzzing for selected `sinex-primitives` and
  `sinex-db` targets. Crash artifacts are uploaded and the summary job fails if any
  crashes are found.
- **`schema-compatibility.yml`** — PR guard that runs contract compatibility checks
  against the base branch.
- **`schema-management.yml`** — Validates JSON schemas, regenerates from code, and
  on default-branch pushes deploys via `xtask schema deploy` if the production DB
  secret is present.
- **`schema-auto-update.yml`** — Scheduled schema drift catch-up that regenerates
  schemas via `xtask` and opens auto-PRs against the default branch.

## Local Reproduction

Run workflow steps inside `nix develop`:

```bash
# Postgres-backed workspace gate from ci.yml
nix develop --accept-flake-config --no-pure-eval --command \
  xtask xtr ci postgres -- \
  xtask xtr ci workspace

# Schema contract check (matches schema-compatibility.yml)
CI_BASE_BRANCH=master xtask contracts compat
```

Use `xtask check` for the fastest local preflight. Use
`./tests/e2e/nixos-vm/run-vm-tests.sh` separately when a change touches the VM
deployment path, because the default GitHub Actions gate does not execute that suite.
