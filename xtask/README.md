# xtask

`xtask` is the canonical task runner for the Sinex workspace. Use it instead of
bare `cargo` for build, test, infra, history, and diagnostics workflows.

## Start Here

```bash
xtask --help
xtask check
xtask test
xtask ci workspace
```

## Common Commands

```bash
# Fast compile check
xtask check

# Compile + clippy
xtask check --lint

# Full lint / fmt / forbidden scan
xtask check --full

# Run tests
xtask test

# Start local infrastructure
xtask infra start

# Run core services
xtask run core --logs

# Health checks
xtask doctor
xtask status --summary
```

## Documentation

- Command reference: `xtask/docs/README.md`
- Verification and perf contracts: `xtask/docs/verification.md`
- Testing sandbox: `xtask/docs/sandbox/README.md`
