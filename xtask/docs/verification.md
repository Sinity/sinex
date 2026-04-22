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

# Enforce perf budgets from xtask/config/perf-contracts.toml
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

Product/runtime resource-shape benchmarks can also live as proof-carrying
scenario tests when the important output is a structured measurement artifact
rather than the wall-clock duration of a whole nextest run. For example:

```bash
xtask test -p sinex-node-sdk --scenario-tag frame_amplification
xtask test -p sinex-node-sdk --scenario-tag storage_profile --heavy
```

These scenario artifacts are observed/advisory by default. Promote a resource
metric into `xtask/config/perf-contracts.toml` only when the threshold is tied
to a documented correctness or operational invariant rather than a local timing
sample.

## Non-Perf Verification

Use `xtask docs schema-bundle` for the checked-in contract bundle, and the `ci`
command family for broader compile/lint/test validation:

```bash
# Regenerate or check the tracked JSON schema bundle
xtask docs schema-bundle
xtask docs schema-bundle --check

# Database/bootstrap validation only
xtask ci schema-only
xtask ci postgres -- xtask ci workspace
```

`xtask ci workspace` currently does all of the following:

- applies declarative schema
- verifies the core contract tables exist
- runs `cargo deny check`
- runs `xtask check` plus the internal forbidden-pattern lane in parallel
- fails if the workspace is left dirty by generated output
- runs the `sinex-e2e-tests` package
- runs the remainder of the test suite with `sinex-e2e-tests` excluded

The public local equivalent for that policy lane is `xtask check --forbidden`
or `xtask check --full`. Internally, the CI workspace lane runs the same
forbidden-pattern logic in parallel with `xtask check`. That logic now also
executes the repo's `ast-grep` rule catalog, but only `error`-severity ast-grep
matches are blocking today; `warning`/`hint` findings stay advisory until the
catalog is cleaned up enough to graduate them.

The GitHub Actions workflow runs the workspace gate through
`xtask ci postgres -- xtask ci workspace`.

Use `xtask ci schema-only` when you want the schema apply + readiness path without
the broader compile/test stages. Use `xtask docs schema-bundle` when you need to
refresh or verify the tracked `schemas/` contract bundle itself.

## Contracts

Perf contracts are loaded from:

- `xtask/config/perf-contracts.toml`

Reference `xtask test bench --help` for the current perf option surface.
