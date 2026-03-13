# Verification Workflow

Performance verification lives under `xtask test bench`, not a separate
`xtask verify` surface.

## Perf Verification

Use benchmark mode for performance sweeps, contracts, and report handling:

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

Use the standard workspace checks for compile, lint, and test validation:

```bash
xtask check --full
xtask test
xtask ci workspace
```

Use `xtask ci schema-only` when you want a schema-focused validation pass.

## Contracts

Perf contracts are loaded from:

- `config/verify/perf-contracts.toml`

Reference `xtask test bench --help` for the current option surface.
