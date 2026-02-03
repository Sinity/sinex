## Diagnostics

```bash
cargo xtask status --doctor --json   # Health check (Postgres, NATS, tools)
cargo xtask status --doctor --pipelines  # Health check + pipeline smoke tests
cargo xtask status --summary         # Compact one-line status (MOTD style)
cargo xtask status --watch           # Live-updating status display
cargo xtask check --json             # Lint + forbidden patterns (JSON output)
cargo xtask history tests slowest    # Find slow tests
cargo xtask history tests flaky      # Find flaky tests
cargo xtask history tests getting-slower  # Detect regressions
cargo xtask jobs active              # Show running background jobs
cargo xtask jobs list                # List recent jobs
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

Note: `cargo xtask ci` still works as a backwards-compatible alias.
