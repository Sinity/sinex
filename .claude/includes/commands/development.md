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
| Update insta snapshots | `INSTA_UPDATE=always cargo nextest run ...` | `xtask test --update-snapshots [flags]` |
| Inspect dependency tree | `cargo tree` | `xtask deps tree [PACKAGE]` (read-only, no history needed, both OK) |
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

# WORKFLOW SHORTCUT: runs minimum sequence (check → test), skips fresh steps
xtask work test           # check then test (check skipped if already fresh)
xtask work check          # check only

# FULL VALIDATION: schema + lint + all tests
xtask ci workspace

# AUTOMATIC FIXING (fmt, clippy etc.)
xtask fix                      # Fix affected packages (smart default)
xtask fix --all                # Fix entire workspace
xtask fix -p PKG               # Fix specific package
xtask fix --smart              # Only fix packages with stored fixable diagnostics
xtask fix --thorough           # Per-package iteration: catches more fixes (cached builds hide warnings)

# FIX + VERIFY IN ONE PASS (preferred over manual fix then check)
xtask check --fix              # xtask fix && xtask check --full (atomic)
xtask check --full --fix       # Same — auto-fix then run full pipeline
xtask check --fix-fmt          # Auto-fix formatting only, then recheck

# BUILDING
xtask build                    # Build affected packages (smart default)
xtask build -p PKG             # Build specific package
xtask build --all              # Build entire workspace
xtask build --release          # Build release mode

# RUNNING APPLICATIONS
xtask run list                 # List available binaries
xtask run node ingestor        # Run specific node
xtask run core                # Run core services (ingestd + gateway)
xtask run ingestd --watch      # Run with hot reload
xtask run --bg stack           # Run stack in background
```

---

### Check Command

| Situation | Command |
|-----------|---------|
| Fastest "does it compile?" | `xtask check` (default, compile-only) |
| Compile + clippy | `xtask check --lint` |
| Full validation before commit | `xtask check --full` (fmt + clippy + forbidden) |
| Full validation + auto-fix | `xtask check --fix` |
| Single package | `xtask check -p sinex-primitives` |
| Background (continue working) | `xtask check --bg` |

**Pipeline:** `preflight → [fmt] → [clippy OR cargo check] → [forbidden]`
When `--lint` is active, clippy replaces cargo check (subsumes it). Default (no flags)
runs only cargo check for maximum speed.

**Speed ordering:** `check` (fastest) → `check --lint` (moderate) → `check --full` (slowest).

**Note:** `--affected` is default ON. Post-commit (no dirty files), it falls back to full workspace.

See `xtask check --help` for all flags.

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
# DEFAULT: nextest, affected packages, preflight auto-starts infra
xtask test                               # Runs affected packages
xtask test --all                         # Run ALL packages
xtask test --debug -E 'test(name)'       # Debug specific test (1 thread, full output)
xtask test --heavy                       # Include #[ignore] tests
xtask test -p PKG                        # Single package
xtask test --update-snapshots            # Sets INSTA_UPDATE=always

# SPECIALIZED MODES (subcommands, not flags):
xtask test bench                         # Run benchmark sweeps
xtask test bench --contracts             # Bench + enforce perf budgets
xtask test bench --report <file>         # Print stored perf report
xtask test bench --compare <a> <b>       # Diff two perf reports
xtask test fuzz                          # Discover and list fuzz targets
xtask test fuzz sinex-primitives-fuzz::fuzz_event_roundtrip  # Run specific target
xtask test fuzz --max-time 120           # Time-limited fuzz run
xtask test coverage                      # HTML coverage report
xtask test coverage --enforce 80         # Enforce minimum coverage
xtask test mutants -p sinex-primitives   # Mutation testing
xtask test vm --category smoke           # NixOS VM tests (~5-10min)
```

| Situation | Command |
|-----------|---------|
| Quick feedback | `xtask test` (affected, auto-starts infra) |
| All tests | `xtask test --all` |
| Debug failing test | `xtask test --debug -E 'test(name)'` |
| Single package | `xtask test -p sinex-primitives` |
| Heavy/ignored tests | `xtask test --heavy` |
| Benchmarks | `xtask test bench` |
| Fuzz testing | `xtask test fuzz` |
| Coverage | `xtask test coverage` |
| Perf contracts | `xtask test bench --contracts` |
| Background test run | `xtask test --bg -p PKG` |

**Note:** `-p` and `-E` are first-class flags. Do NOT use `-- -p` or `-- -E` passthrough.

See `xtask test --help` and `xtask test <subcommand> --help` for all flags.
