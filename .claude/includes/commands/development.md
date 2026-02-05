## Development Workflows

```bash
# XTASK IS MANDATORY, BARE CARGO IS BLOCKED.

# Fast iteration (use between edits)
cargo xtask check                    # fmt + clippy + forbidden patterns (~10s)

# Before commit
cargo xtask check && cargo xtask test

# Full validation (before PR/merge)
cargo xtask ci workspace             # schema + lint + all tests

# Debugging a specific test
cargo xtask test --debug -E 'test(test_name)'

# Automatic fixing (fmt, clippy etc.)
cargo xtask fix

# Search through rg or your builtin tooling. bare grep is BLOCKED due to poor performance
```

---

## Testing Commands

```bash
# DEFAULTS: --affected (only changed packages), preflight ON (auto-start infra)
cargo xtask test                     # Runs affected packages (auto-starts Postgres/NATS)
cargo xtask test --all               # Run ALL packages (override --affected default)
cargo xtask test --debug             # Debug mode (1 thread, full output)
cargo xtask test --heavy             # Include #[ignore] tests
cargo xtask test --prime             # Prime database before testing
cargo xtask test --coverage          # Run with coverage collection
cargo xtask test --fuzz              # Run fuzz tests
cargo xtask test --mutants           # Run mutation tests
cargo xtask test --bench             # Run benchmarks
cargo xtask test -p PKG              # Single package (first-class flag)
cargo xtask test -E 'test(name)'     # Filter by test name (first-class flag)
cargo xtask test --skip-preflight    # Skip auto-start (if infra already running)
```

| Situation | Command |
|-----------|---------|
| Quick feedback | `cargo xtask test` (affected, auto-starts infra) |
| All tests | `cargo xtask test --all` |
| Debug failing test | `cargo xtask test --debug -E 'test(name)'` |
| Single package | `cargo xtask test -p sinex-primitives` |
| Heavy/ignored tests | `cargo xtask test --heavy` |
| Run benchmarks | `cargo xtask test --bench` |
| Skip auto-start | `cargo xtask test --skip-preflight` |

**Note:** `-p` and `-E` are first-class flags. Do NOT use `-- -p` or `-- -E` passthrough.
