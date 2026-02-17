## Diagnostics

```bash
xtask status --doctor --json   # Health check (Postgres, NATS, tools)
xtask status --doctor --pipelines  # Health check + pipeline smoke tests
xtask status --summary         # Compact one-line status (MOTD style)
xtask status --watch           # Live-updating status display
xtask check --json             # Lint + forbidden patterns (JSON output)
xtask jobs active              # Show running background jobs
xtask jobs list                # List recent jobs
```

---

## History (Execution Tracking)

### Top-Level Subcommands

```bash
xtask history list [--limit N] [--command CMD]     # Recent invocations
xtask history last --command CMD                    # Last invocation for a command
xtask history stats --command CMD [--days N]        # Command statistics (success rate, avg time)
xtask history prune [--older-than N]                # Prune entries older than N days (default: 90)
xtask history export --limit N                      # Export invocations as JSON
xtask history tests <subcommand>                    # Test result queries (see below)
xtask history diagnostics [--level LEVEL] [--file PATTERN]  # Build diagnostics (warnings/errors)
```

### Test History Subcommands

```bash
xtask history tests failures [--limit N] [--output] # Failing tests from most recent run
xtask history tests analyze                          # Comprehensive analysis (buckets, timeouts, failures)
xtask history tests output <pattern>                 # Show captured output for matching tests (pass or fail)
xtask history tests slowest [--limit N]              # Slowest tests by avg duration (excludes timeouts)
xtask history tests flaky [--limit N]                # Flaky tests (fail→pass on retry)
xtask history tests getting-slower [--threshold-pct N] [--window N]  # Tests regressing in speed
xtask history tests trends [--pattern X] [--package P] [--runs N]    # Duration trends over time
xtask history tests eta                              # Estimated test suite runtime
```

**Note:** `slowest`, `getting-slower`, and `eta` only count passing tests — failed/timed-out tests
would inflate durations with timeout ceilings rather than reflecting real execution time.

**Note:** ALL test output (pass and fail) is stored in the history DB. Use `output` to retrieve it.

### Agent Debugging Workflow

```bash
# After a failed test run:
xtask history tests analyze               # Overview: buckets, probable timeouts, failures by package
xtask history tests failures --output     # Failing tests with captured stdout/stderr
xtask history tests output test_name      # Get output for any test (pass or fail)
xtask test --json | jq '.data.failures'   # Structured failure data with output

# Investigate specific patterns:
xtask history tests flaky                 # Tests that pass on retry (infrastructure issues?)
xtask history tests getting-slower        # Performance regressions
xtask history tests trends --package sinex-ingestd  # Duration history for a package
```

---

## Dependency Analysis

```bash
xtask deps list                # List workspace packages
xtask deps tree [PACKAGE]      # Show dependency tree
xtask deps duplicates          # Find duplicate versions
xtask deps unused --json       # Detect unused dependencies
xtask deps timings --json      # Analyze build times
xtask deps impact [PACKAGE]    # Rebuild impact analysis
xtask deps graph               # Visualize dependency graph
```

---

## CI Pipelines (via xtr umbrella)

```bash
xtask xtr ci workspace         # Full validation (schema + lint + tests)
xtask xtr ci postgres -- CMD   # Run CMD with ephemeral Postgres
xtask xtr patterns -p '$X'     # AST-grep pattern search
xtask xtr completions zsh      # Generate shell completions
```

Note: `xtr ci` requires the `sandbox` feature (used in CI environments, not default).
