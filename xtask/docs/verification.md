# Verification Workflow

The public verification surface is currently split between:

- `xtask test bench` for performance contracts and stored perf reports
- `xtask ci ...` for schema/bootstrap/workspace validation

There is still an internal `xtask/src/commands/verify.rs` module, but the top-level
CLI does not expose a `verify` command today.

## Perf Verification

Performance verification lives under `xtask test bench`. Use benchmark mode for
performance sweeps, contracts, and report handling:

```bash
# Benchmark sweeps
xtask test bench

# Enforce perf budgets from config/verify/perf-contracts.toml
xtask test bench --contracts

# Print a stored report
xtask test bench --report path/to/report.json

# Compare two stored reports
xtask test bench --compare path/to/current.json path/to/previous.json
```

Useful options:

- `--profile fast|ci|...` selects the nextest profile.
- `--runs N` controls repetitions per scenario.
- `--threads 12,24` sweeps concurrency settings.
- `--target <pkg|workspace>` narrows the benchmark scope.

## Non-Perf Verification

Use the `ci` command family for broader compile/lint/test validation:

```bash
xtask ci schema-only
xtask ci postgres -- xtask ci workspace
```

`xtask ci workspace` currently does all of the following:

- applies declarative schema
- verifies the core contract tables exist
- runs `cargo deny check`
- runs `xtask check` and `xtask lint-forbidden` in parallel
- fails if the workspace is left dirty by generated output
- runs the `sinex-e2e-tests` package
- runs the remainder of the test suite with `sinex-e2e-tests` excluded

The GitHub Actions workflow runs the workspace gate through
`xtask xtr ci postgres -- xtask xtr ci workspace`.

Use `xtask ci schema-only` when you want the schema apply + readiness path without
the broader compile/test stages.

## Contracts

Perf contracts are loaded from:

- `config/verify/perf-contracts.toml`

Reference `xtask test bench --help` for the current perf option surface.
