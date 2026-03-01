## Development Workflows

### !!! CARGO IS NEVER USED DIRECTLY !!!

Every `cargo` subcommand has an `xtask` equivalent that adds: history tracking, diagnostics
capture, coordination (dedup), preflight (auto-start DB/NATS), and JSON output. Using bare
`cargo` throws all of this away and leaves you blind to performance regressions.

**If you find yourself reaching for `cargo`, STOP. Find the xtask equivalent below.**

| You want to... | WRONG (bare cargo) | CORRECT (xtask) |
|---|---|---|
| Quick compile check | `cargo check -p PKG` | `xtask check -p PKG` |
| Compile + clippy | `cargo clippy -p PKG` | `xtask check -p PKG --lint` |
| Full lint + compile + fmt | `cargo clippy` | `xtask check --full` |
| Build a package | `cargo build -p PKG` | `xtask build -p PKG` |
| Build xtask itself | `cargo build -p xtask` | `xtask build -p xtask` |
| Run tests | `cargo test -p PKG` | `xtask test -p PKG` |
| Run specific test | `cargo test -- test_name` | `xtask test -E 'test(test_name)'` |
| Fix formatting | `cargo fmt` | `xtask fix` |
| Run clippy | `cargo clippy` | `xtask check --lint` (clippy only) |
| Run xtask command | `cargo run -p xtask -- CMD` | `xtask CMD` (binary is on PATH) |

**`cargo run -p xtask --` is especially wasteful** — it recompiles xtask from source (~30s) before
running the actual command. The `xtask` binary on PATH is pre-built.

**`| tail -N` on long commands is forbidden** — it hides ALL streaming output, leaving the user
blind for minutes. Run commands normally; xtask's output is designed to be useful.

---

### Quick Reference

```bash
# FAST ITERATION: compile check only (~3s warm, ~30s cold) — DEFAULT
xtask check
xtask check -p PKG                  # Single package

# WITH CLIPPY: compile + lint (~20s warm)
xtask check --lint
xtask check --lint -p PKG

# FULL PIPELINE: fmt + clippy + forbidden (~25s warm)
xtask check --full

# BEFORE COMMIT: full check + tests
xtask check --full && xtask test

# FULL VALIDATION: schema + lint + all tests
xtask ci workspace

# AUTOMATIC FIXING (fmt, clippy etc.)
xtask fix                      # Fix affected packages (smart default)
xtask fix --all                # Fix entire workspace
xtask fix -p PKG               # Fix specific package
xtask fix --smart              # Only fix packages with stored fixable diagnostics

# BUILDING
xtask build                    # Build affected packages (smart default)
xtask build -p PKG             # Build specific package
xtask build --all              # Build entire workspace
xtask build --release          # Build release mode

# RUNNING APPLICATIONS
xtask run list                 # List available binaries
xtask run node ingestor        # Run specific node
xtask run stack                # Run core services (gateway + ingestd)
xtask run ingestd --watch      # Run with hot reload
xtask run --bg stack           # Run stack in background
```

---

### Check Command Flags

```bash
xtask check                    # Default: cargo check only (affected packages, ~3s warm)
xtask check --lint             # Add clippy (~20s warm, subsumes cargo check)
xtask check --fmt              # Add formatting check (~1s extra)
xtask check --forbidden        # Add forbidden pattern scan (~1s extra)
xtask check --full             # All three: fmt + clippy + forbidden (~25s warm)
xtask check -p sinex-primitives  # Check specific package only
xtask check --all              # Check ALL packages (overrides --affected default)
xtask check --bg               # Run in background
xtask check --skip-tests       # Skip test/bench/example compilation
```

| Situation | Command |
|-----------|---------|
| Fastest "does it compile?" | `xtask check` (~3s warm) |
| Quick compile + clippy | `xtask check --lint` (~20s warm) |
| Full validation before commit | `xtask check --full` (~25s warm) |
| Just compilation check | `xtask check` (default!) |
| Single package | `xtask check -p sinex-primitives` |
| Skip test compilation | `xtask check --skip-tests` |
| Background (continue working) | `xtask check --bg` |

### Check Pipeline (empirical timing)

```
Pipeline: preflight → [fmt] → [clippy OR cargo check] → [forbidden]

xtask check:        ~3s warm, ~30s cold  (cargo check only, default)
xtask check --lint: ~20s warm, ~60s cold (clippy, subsumes cargo check)
xtask check --full: ~25s warm, ~90s cold (fmt + clippy + forbidden)
First run (migration cache miss): add ~1s (in-process migration via sinex-db)
```

**Architecture:** When `--lint` is active, clippy replaces cargo check — it runs the full
compiler before applying lint rules. The pipeline never runs both. The default (no flags)
runs only cargo check for maximum speed.

**Note:** `--affected` is default ON. Post-commit (no dirty files), it falls back to full workspace.

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
xtask test -E 'test(name)'    # Filter by test name (first-class flag)
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
