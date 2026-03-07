# Verification Workflow

`xtask verify` is the performance-verification surface.

## Commands

- `xtask verify perf [--profile fast] [--runs 2] [--threads 12,24]`
  - Runs benchmark sweeps, evaluates contract thresholds, and emits artifacts.
- `xtask verify report [--report <path>]`
  - Prints a summary for a generated perf report (`latest` by default).
- `xtask verify compare --current <path> --previous <path>`
  - Compares two perf reports.
- `xtask verify all`
  - Alias for the perf flow (same options as `verify perf`).

## Contracts

Perf contracts are loaded from:

- `config/verify/perf-contracts.toml`

Supported scenario thresholds:

- `max_median_ms`
- `max_p95_ms`
- `min_throughput_runs_per_sec`
- `median_regression_pct`
- `p95_regression_pct`
- `throughput_regression_pct`
- `enforce_baseline`

## Artifacts

`xtask verify perf` writes:

- JSON report: `.../verify-perf-report.json`
- Prometheus metrics: `.../verify-perf-metrics.prom`
- latest pointer: `$SINEX_STATE_DIR/verify-perf-latest.json` (or default state dir)

## Non-Perf Verification

Conformance and functional checks are run through:

- `xtask check --full`
- `xtask test`
- `xtask xtr ci workspace`
