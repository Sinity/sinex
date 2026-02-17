## Development Workflows

```bash
# XTASK IS MANDATORY, BARE CARGO IS BLOCKED.

# Fast iteration (use between edits)
xtask check                    # fmt + clippy + forbidden patterns (~10s)

# Before commit
xtask check && xtask test

# Full validation (before PR/merge)
xtask ci workspace             # schema + lint + all tests

# Debugging a specific test
xtask test --debug -E 'test(test_name)'

# Automatic fixing (fmt, clippy etc.)
xtask fix

# Search through rg or your builtin tooling. bare grep is BLOCKED due to poor performance
```

---

## Async Workflow Patterns

### Background Execution

```bash
# Spawn and continue working immediately
xtask check --bg      # Returns job ID, runs in background
xtask test --bg       # Tests compile and run while you work
xtask build --bg      # Build while editing other files

# Monitor running jobs
xtask jobs active              # What's running right now?
xtask jobs list                # Recent job history
xtask jobs status <ID>         # Status of specific job
xtask jobs status <ID> --json  # Machine-parseable status

# Retrieve results
xtask jobs output <ID>         # Full output
xtask jobs wait <ID>           # Block until complete
```

### JSON for Agent Consumption

```bash
# Always use --json when parsing programmatically
xtask check --json | jq '.status'           # "success" or "failed"
xtask test --bg --json | jq '.data.job_id'  # Get job ID
xtask jobs list --json | jq '.data.jobs[]'  # Iterate all jobs
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
xtask test                     # Runs affected packages (auto-starts Postgres/NATS)
xtask test --all               # Run ALL packages (override --affected default)
xtask test --debug             # Debug mode (1 thread, full output)
xtask test --heavy             # Include #[ignore] tests
xtask test --prime             # Prime database before testing
xtask test --coverage          # Run with coverage collection
xtask test --fuzz              # Run fuzz tests
xtask test --mutants           # Run mutation tests
xtask test --bench             # Run benchmarks
xtask test -p PKG              # Single package (first-class flag)
xtask test -E 'test(name)'     # Filter by test name (first-class flag)
xtask test --skip-preflight    # Skip auto-start (if infra already running)
```

| Situation | Command |
|-----------|---------|
| Quick feedback | `xtask test` (affected, auto-starts infra) |
| All tests | `xtask test --all` |
| Debug failing test | `xtask test --debug -E 'test(name)'` |
| Single package | `xtask test -p sinex-primitives` |
| Heavy/ignored tests | `xtask test --heavy` |
| Run benchmarks | `xtask test --bench` |
| Skip auto-start | `xtask test --skip-preflight` |
| Background test run | `xtask test --bg -p PKG` |

**Note:** `-p` and `-E` are first-class flags. Do NOT use `-- -p` or `-- -E` passthrough.
