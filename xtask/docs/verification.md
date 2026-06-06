# Verification Workflow

The public verification surface is currently split between:

- `xtask test bench` for performance contracts and stored perf reports
- direct local commands such as `xtask check`, `xtask test`, and
  `xtask schema strict-diff` for schema/bootstrap/workspace validation

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

- `--profile <name>` selects the nextest profile.
- `--runs N` controls repetitions per scenario.
- `--threads 12,24` sweeps concurrency settings.
- `--target <pkg|workspace>` narrows the benchmark scope.

Product/runtime resource-shape checks should live as ordinary Rust tests when
the important output is a structured measurement artifact rather than the
wall-clock duration of a whole nextest run. For example:

```bash
xtask test -p sinexd -E 'test(material_assembler)'
xtask test -p sinex-e2e-tests -E 'test(source_material)'
```

These artifacts are observed/advisory by default. Promote a resource
metric into `xtask/config/perf-contracts.toml` only when the threshold is tied
to a documented correctness or operational invariant rather than a local timing
sample.

## Non-Perf Verification

Use `xtask docs schema-bundle` for the checked-in contract bundle, and direct
local commands for broader compile/lint/test validation:

```bash
# Regenerate or check the tracked JSON schema bundle
xtask docs schema-bundle
xtask docs schema-bundle --check

# Live schema drift against the checkout-local dev stack
xtask schema strict-diff

# Broad compile/lint/test surface
xtask check --full
xtask test --impact-mode=off --all
```

Hosted GitHub workflows have their own implementation details. Those are not
the normal desktop command surface. Use `xtask docs schema-bundle` when you need
to refresh or verify the tracked `schemas/` contract bundle itself.

## Contracts

Perf contracts are loaded from:

- `xtask/config/perf-contracts.toml`

Reference `xtask test bench --help` for the current perf option surface.
