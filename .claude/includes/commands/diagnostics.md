## Diagnostics & History

### Quick Status

```bash
xtask doctor                   # Health check (Postgres, NATS, tools, TLS)
xtask doctor --fix             # Auto-remediate: start infra, invalidate stale cache
xtask doctor --runtime         # Runtime health (ingestd heartbeat, consumer lag)
xtask status --summary         # One-line status (MOTD style)
xtask analytics workspace-health  # Composite health score (0-100)
xtask analytics recommend      # Actionable recommendations
```

### After a Failed Check

```bash
xtask history diagnostics --level error              # Current errors (package-scoped)
xtask history diagnostics --fixable                   # Auto-fixable only
xtask history diagnostics --package sinex-primitives  # Filter by package
xtask history diagnostics --emit gcc                  # file:line:col: level: msg
xtask history diagnostics --trend                     # Error count trend
```

### After a Failed Test

```bash
xtask history tests analyze              # Overview: buckets, timeouts, failures
xtask history tests failures --output    # Failures with stdout/stderr
xtask history tests output test_name     # Output for any test (pass or fail)
xtask history tests slowest              # Slowest passing tests
xtask history tests flaky                # Tests that pass on retry
xtask history tests getting-slower       # Speed regressions
```

### History & Analytics

```bash
xtask history list [--command CMD]       # Recent invocations
xtask history stats --command CMD        # Success rate, avg time
xtask history progress                   # Live/final progress
xtask history eta check --phase compile  # ETA estimate

xtask analytics hotspots                 # Recurring diagnostics
xtask analytics reliability              # Test pass rates per package
xtask analytics velocity                 # Build/test time trends
```

### Dependency Analysis

```bash
xtask deps tree [PACKAGE]      # Dependency tree
xtask deps unused --json       # Unused dependencies
xtask deps impact [PACKAGE]    # Rebuild impact analysis
```

