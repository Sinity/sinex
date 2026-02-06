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

## Async Workflow Patterns

### Background Execution

```bash
# Spawn and continue working immediately
cargo xtask check --bg      # Returns job ID, runs in background
cargo xtask test --bg       # Tests compile and run while you work
cargo xtask build --bg      # Build while editing other files

# Monitor running jobs
cargo xtask jobs active              # What's running right now?
cargo xtask jobs list                # Recent job history
cargo xtask jobs status <ID>         # Status of specific job
cargo xtask jobs status <ID> --json  # Machine-parseable status

# Retrieve results
cargo xtask jobs output <ID>         # Full output
cargo xtask jobs wait <ID>           # Block until complete
```

### JSON for Agent Consumption

```bash
# Always use --json when parsing programmatically
cargo xtask check --json | jq '.status'           # "success" or "failed"
cargo xtask test --bg --json | jq '.data.job_id'  # Get job ID
cargo xtask jobs list --json | jq '.data.jobs[]'  # Iterate all jobs
```

### Decision Pattern

```
MATCH task:
  | Quick operation (< 5s)     → run foreground
  | Need result for next step  → run foreground, parse --json
  | Can work on other things   → run --bg, continue, check later
  | Multiple independent tasks → spawn all --bg in parallel
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
| Background test run | `cargo xtask test --bg -p PKG` |

**Note:** `-p` and `-E` are first-class flags. Do NOT use `-- -p` or `-- -E` passthrough.
