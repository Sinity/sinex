# Testing

Assume the project devShell from [CONTRIBUTING.md](CONTRIBUTING.md) is already
active before running the commands below.

## Common Loops

```bash
# Fast compile + lint loop
xtask check
xtask check --lint
xtask check --full

# Main local test loop
xtask test
xtask test -p sinex-primitives
xtask test --debug -E 'test(name)'
xtask test --heavy

# Targeted e2e loop; simple test(name) filters infer the test binary automatically.
xtask test -p sinex-e2e-tests -E 'test(test_batch_large_payloads)'

# Impact planning and exact proof reuse
xtask impact explain
xtask impact audit --sample-skips 10
xtask impact seed -p xtask -E 'test(name)'
xtask impact seed-coverage -p xtask -E 'test(name)'
xtask test --impact-mode=off --all
xtask test --impact-mode=aggressive

```

`xtask test` is the primary test entrypoint. It handles the repo's preflight,
runtime binary preparation for e2e/sinexd tests, and nextest wiring; use
`xtask test --help` for the current option surface.
Bare `xtask test` uses machine-derived impact planning in balanced mode by
default. It runs affected package scopes unless the changed hunks have recorded
test-manifest, dependency-edge, or LLVM coverage-region evidence, records
accepted-risk decisions, and may reuse an exact previous proof only when the
manifest and input fingerprint match. When a balanced impact plan still falls
back to multiple package scopes, `xtask test` subtracts package scopes that
already have exact reusable proofs and runs only the unproven remainder. Direct
exact package/filter invocations also reuse matching proof units unless
`--no-reuse` is supplied. Use `--impact-mode=off --all` for a deliberate full
pass, `xtask impact seed` after broad runs to populate test entrypoint/dependency
evidence, and `xtask impact seed-coverage` for exact per-test LLVM line coverage.
Aggressive mode is available for local iteration when you accept hunk-coverage
gaps and want the planner to use partial evidence.

## Broad Local Validation

```bash
# Checked-in schema bundle drift
xtask docs schema-bundle --check

# Live schema drift against the checkout-local dev stack
xtask schema strict-diff

# Broad compile/lint/test surface for local pre-merge confidence
xtask check --full
xtask test --impact-mode=off --all
```

Hosted GitHub workflows have their own implementation details under
`.github/workflows/`. They are not the normal desktop command surface.

## Additional Test Surfaces

```bash
# Performance contracts
xtask test bench --contracts

# Coverage
xtask test coverage

# Mutation testing
xtask test mutants -p sinex-primitives

# Exported NixOS VM checks
xtask test vm --category smoke
xtask test vm --category integration
```

The default GitHub Actions gate does not run the NixOS VM suite; use the VM
commands separately when a change touches deployment/runtime behavior. VM tests
must report explicit outcomes through `TestRunner` and declare missing probes with
`require_evidence(...)` instead of returning early. Evidence kinds include DB,
NATS, process, logs, source-material, output-contract, fault-injection, and
custom proof artifacts; missing required evidence blocks the suite unless the test
marks the prerequisite as an operator-visible skip.

Do not model source-ingestion correctness as an `xtask exercise`. Source
material, source adapter, stream runtime, replay, and provenance behavior belong in
Rust tests and VM integration tests. `xtask` may orchestrate those tests, but it
does not own their semantics. The command-plane split is documented in
[`xtask/docs/runtime-target-boundaries.md`](xtask/docs/runtime-target-boundaries.md).

## Harness and Layout

- Most Rust/package tests run through `xtask test` and the `xtask::sandbox`
  infrastructure.
- Per-crate test details live alongside the owning crate under `crate/**/tests`
  and `crate/**/docs/`.
- Sandbox-specific guidance and patterns live in
  [`xtask/docs/sandbox/README.md`](xtask/docs/sandbox/README.md).
- Perf verification details live in
  [`xtask/docs/verification.md`](xtask/docs/verification.md).
- Ignored Rust tests must carry an operator-visible category prefix in the
  attribute reason: `heavy:`, `long:`, or `external:`. Heavy and long tests must
  name the `xtask test --heavy` route or the CI heavy slice; external tests must
  name their prerequisite with `requires ...`. The forbidden-pattern gate rejects
  bare, ambiguous, or unrouted ignores so skipped behavior remains visible.
