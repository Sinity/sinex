# GitHub Actions Workflows

This directory documents the parked GitHub Actions workflows and how to rerun
them locally. Hosted GitHub Actions are intentionally not an automatic gate for
this repository: the account is on a no-spend posture, and automatic
push/pull-request/scheduled workflows can burn paid minutes before useful
feedback is produced.

All workflows in this directory are manual `workflow_dispatch` jobs. PRs should
record the local `xtask` verification that was run, and reviewers should treat
that recorded local evidence as the closure signal unless a workflow is
deliberately invoked by hand.

## Active Workflows

- **`ci.yml`** — Manual main gate. Runs
  `xtask ci postgres -- xtask ci workspace`.

  The workspace stage applies declarative schema, checks contract tables, runs
  `cargo deny`, runs `xtask check` plus the internal forbidden-pattern lane in
  parallel, enforces workspace cleanliness, runs the `sinex-e2e-tests` package,
  then runs the rest of the test suite with `sinex-e2e-tests` excluded. The
  closest public local equivalent is `xtask check --forbidden` or `xtask check --full`.
  It does **not** run the NixOS VM suite in `tests/e2e/nixos-vm/`.
- **`db-checks.yml`** — Manual database checks. Runs a schema-focused pipeline.
- **`verify-perf.yml`** — On-demand performance verification. Runs
  `xtask test bench --contracts` inside an ephemeral Postgres environment and
  uploads the perf-verification artifacts.
- **`n1-compat.yml`** — On-demand N-1 protocol compatibility check. Brings
  up current gateway plus the latest released terminal ingestor and verifies the
  rolling-update path still moves events.
- **`fuzz.yml`** — On-demand fuzzing for selected `sinex-primitives` and
  `sinex-db` targets. Crash artifacts are uploaded and the summary job fails if any
  crashes are found.
- **`schema-compatibility.yml`** — Manual contract compatibility checks
  against the base branch.
- **`schema-management.yml`** — Validates JSON schemas, regenerates the checked-in
  schema bundle from the Rust registry, and can deploy via `xtask infra
  schema-apply` from a manual default-branch run if the production DB secret is
  present.

## Local Reproduction

Run workflow steps inside `nix develop`:

```bash
# Postgres-backed workspace gate from ci.yml
nix develop --accept-flake-config --no-pure-eval --command \
  xtask ci postgres -- \
  xtask ci workspace

# Schema contract check (matches schema-compatibility.yml)
xtask ci compat --base master
```

Use `xtask check` for the fastest local preflight. Use
`xtask test vm --category smoke` (or `--category integration`) when a change
touches the VM deployment path, because the default GitHub Actions gate does
not execute that suite.
