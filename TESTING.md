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
```

`xtask test` is the primary test entrypoint. It handles the repo's preflight and
nextest wiring; use `xtask test --help` for the current option surface.

## CI-Parity Validation

```bash
# Checked-in schema bundle drift
xtask docs schema-bundle --check

# Schema/bootstrap path only
xtask ci postgres -- xtask ci schema-only

# Main Postgres-backed workspace lane used in GitHub Actions
xtask ci postgres -- xtask ci workspace

# Schema compatibility against the default branch
xtask ci compat --base master
```

The workspace lane is broader than the default local loop: it applies schema,
checks contract tables, runs dependency/lint validation, enforces workspace
cleanliness, and runs the package test surfaces wired into CI.

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
commands separately when a change touches deployment/runtime behavior.

Do not model source-ingestion correctness as an `xtask exercise`. Source
material, SDK adapter, node runtime, replay, and provenance behavior belong in
Rust tests and VM integration tests. `xtask` may orchestrate those tests, but it
does not own their semantics. The command-plane split is documented in
[`docs/architecture/runtime-target-boundaries.md`](docs/architecture/runtime-target-boundaries.md).

## Harness and Layout

- Most Rust/package tests run through `xtask test` and the `xtask::sandbox`
  infrastructure.
- Per-crate test details live alongside the owning crate under `crate/**/tests`
  and `crate/**/docs/`.
- Sandbox-specific guidance and patterns live in
  [`xtask/docs/sandbox/README.md`](xtask/docs/sandbox/README.md).
- Perf verification details live in
  [`xtask/docs/verification.md`](xtask/docs/verification.md).
