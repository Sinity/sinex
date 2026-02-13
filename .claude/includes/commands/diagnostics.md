## Diagnostics

```bash
cargo xtask status --doctor --json   # Health check (Postgres, NATS, tools)
cargo xtask status --doctor --pipelines  # Health check + pipeline smoke tests
cargo xtask status --summary         # Compact one-line status (MOTD style)
cargo xtask status --watch           # Live-updating status display
cargo xtask check --json             # Lint + forbidden patterns (JSON output)
cargo xtask jobs active              # Show running background jobs
cargo xtask jobs list                # List recent jobs
```

---

## History (Execution Tracking)

### Top-Level Subcommands

```bash
cargo xtask history list [--limit N] [--command CMD]     # Recent invocations
cargo xtask history last --command CMD                    # Last invocation for a command
cargo xtask history stats --command CMD [--days N]        # Command statistics (success rate, avg time)
cargo xtask history prune [--older-than N]                # Prune entries older than N days (default: 90)
cargo xtask history export --limit N                      # Export invocations as JSON
cargo xtask history tests <subcommand>                    # Test result queries (see below)
cargo xtask history diagnostics [--level LEVEL] [--file PATTERN]  # Build diagnostics (warnings/errors)
```

### Test History Subcommands

```bash
cargo xtask history tests failures [--limit N] [--output] # Failing tests from most recent run
cargo xtask history tests analyze                          # Comprehensive analysis (buckets, timeouts, failures)
cargo xtask history tests output <pattern>                 # Show captured output for matching tests (pass or fail)
cargo xtask history tests slowest [--limit N]              # Slowest tests by avg duration (excludes timeouts)
cargo xtask history tests flaky [--limit N]                # Flaky tests (fail→pass on retry)
cargo xtask history tests getting-slower [--threshold-pct N] [--window N]  # Tests regressing in speed
cargo xtask history tests trends [--pattern X] [--package P] [--runs N]    # Duration trends over time
cargo xtask history tests eta                              # Estimated test suite runtime
```

**Note:** `slowest`, `getting-slower`, and `eta` only count passing tests — failed/timed-out tests
would inflate durations with timeout ceilings rather than reflecting real execution time.

**Note:** ALL test output (pass and fail) is stored in the history DB. Use `output` to retrieve it.

### Agent Debugging Workflow

```bash
# After a failed test run:
cargo xtask history tests analyze               # Overview: buckets, probable timeouts, failures by package
cargo xtask history tests failures --output     # Failing tests with captured stdout/stderr
cargo xtask history tests output test_name      # Get output for any test (pass or fail)
cargo xtask test --json | jq '.data.failures'   # Structured failure data with output

# Investigate specific patterns:
cargo xtask history tests flaky                 # Tests that pass on retry (infrastructure issues?)
cargo xtask history tests getting-slower        # Performance regressions
cargo xtask history tests trends --package sinex-ingestd  # Duration history for a package
```

---

## Dependency Analysis

```bash
cargo xtask deps list                # List workspace packages
cargo xtask deps tree [PACKAGE]      # Show dependency tree
cargo xtask deps duplicates          # Find duplicate versions
cargo xtask deps unused --json       # Detect unused dependencies
cargo xtask deps timings --json      # Analyze build times
cargo xtask deps impact [PACKAGE]    # Rebuild impact analysis
cargo xtask deps graph               # Visualize dependency graph
```

---

## CI Pipelines (via xtr umbrella)

```bash
cargo xtask xtr ci workspace         # Full validation (schema + lint + tests)
cargo xtask xtr ci postgres -- CMD   # Run CMD with ephemeral Postgres
cargo xtask xtr patterns -p '$X'     # AST-grep pattern search
cargo xtask xtr completions zsh      # Generate shell completions
```

Note: `xtr ci` requires the `sandbox` feature (used in CI environments, not default).
