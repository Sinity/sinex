# Verification Workflow

`xtask verify` is the unified verifier surface for conformance, replay determinism, and performance budgets.

## Commands

- `xtask verify conformance`
  - Runs core conformance checks on xtask command shape and stream-kernel invariants.

- `xtask verify replay-lab [--seed <N>]`
  - Runs deterministic replay envelope checks.

- `xtask verify perf [--profile fast] [--runs 2] [--threads 12,24]`
  - Executes benchmark sweeps, stores run history, evaluates budget contracts, and writes artifacts.

- `xtask verify all`
  - Runs `conformance`, `replay-lab`, and `perf` sequentially.

## Performance Contracts

Contracts are loaded from:

- `config/verify/perf-contracts.toml`

Supported thresholds per scenario (with defaults + per-scenario overrides):

- `max_median_ms`
- `max_p95_ms`
- `min_throughput_runs_per_sec`
- `median_regression_pct`
- `p95_regression_pct`
- `throughput_regression_pct`
- `enforce_baseline`

## Perf Artifacts

`xtask verify perf` writes:

- JSON report: `.../verify-perf-report.json`
- Prometheus metrics: `.../verify-perf-metrics.prom`
- latest pointer: `$SINEX_STATE_DIR/verify-perf-latest.json` (or default state dir)

It also emits benchmark markdown/html reports under the bench output directory.

## CI Usage

Quick checks:

- `xtask verify conformance`
- `xtask verify replay-lab --seed 42`

Scheduled perf gate:

- `xtask verify perf --profile fast --runs 2 --threads 12,24`
