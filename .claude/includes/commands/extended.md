## Extended Commands

### Benchmarking

```bash
xtask test --bench                                        # Run benchmark lane
xtask verify perf                                         # Full benchmark/regression verification
```

### Running Binaries

```bash
xtask run ingestd                        # Run sinex-ingestd
xtask run gateway                        # Run sinex-gateway
xtask run --watch ingestd                # Hot-reload on file changes
xtask run --tether ingestd               # Tether to production NATS
xtask run --metrics ingestd              # Show periodic runtime metrics overlay (heartbeat, lag, latency)
xtask run core                          # Run full core services (ingestd + gateway)
xtask run all-nodes                      # Run all node binaries
```

### Documentation

```bash
xtask docs build                         # Generate rustdoc
xtask docs build --open                  # Generate and open in browser
xtask docs build --private --all-features  # Include private items
xtask docs serve                         # Serve docs at localhost:8080
xtask docs serve --build                 # Build then serve
```

### Codebase Snapshot

```bash
xtask snapshot                           # Generate AI context snapshot (via repomix)
xtask snapshot --output context.md       # Custom output path
xtask snapshot --compress                # Minified output
xtask snapshot --remove-comments         # Strip comments
xtask snapshot --include "crate/lib/**"  # Filter by glob
```

### Exercise (xtask Self-Validation)

```bash
xtask exercise --tier 1                  # Quick surface checks
xtask exercise --tier 2                  # Infrastructure exercises
xtask exercise --tier 3                  # Database + pipeline exercises
xtask exercise --tier 4                  # Heavy/stress exercises
xtask exercise --all                     # All tiers
xtask exercise --list                    # List available exercises
xtask exercise --exercise ID             # Run specific exercise
xtask exercise --dry-run                 # Preview without executing
```

### VM Testing

```bash
# NixOS compatibility gate — unified entry point via xtask test (Q1)
xtask test --vm                                # Run smoke tests (fast, ~5-10min)
xtask test --vm --category smoke               # Explicit: smoke=["basic"]
xtask test --vm --category integration         # Integration scenarios
xtask test --vm --category performance         # Performance scenarios
xtask test --vm --category all                 # Full suite
xtask test --vm --vm-parallel                  # Parallel execution
xtask test --vm --bg                           # Background execution

# VM lifecycle
xtask infra vm start minimal                   # Boot minimal NixOS VM
xtask infra vm start standard --persistent     # Persistent standard VM
xtask infra vm ssh                             # SSH into running VM
xtask infra vm stop                            # Shut down VM
xtask infra vm test --list                     # List available test scenarios
xtask infra vm test --validate                 # Check nix syntax of test files
xtask infra vm snapshot create NAME            # Save VM snapshot
xtask infra vm snapshot restore NAME           # Restore VM snapshot
```

### Privacy Engine

```bash
xtask privacy catalog                    # List all privacy rules
xtask privacy test "some text"           # Test text against privacy engine
xtask privacy decrypt <TOKEN>            # Decrypt an encrypted privacy token
xtask privacy key                        # Show privacy key information
xtask privacy config                     # Show or generate privacy configuration
```

### Verification

```bash
xtask verify perf                        # Run perf sweeps and enforce contract budgets
xtask verify report <FILE>               # Print summary from a perf report JSON
xtask verify compare <A> <B>             # Compare two perf reports
xtask verify all                         # Run all verification (currently perf only)
```

### CI Pipelines

```bash
xtask ci workspace                       # Full validation (schema + lint + tests)
xtask ci postgres -- CMD                 # Run CMD with ephemeral Postgres
xtask ci schema-only                     # Schema-only pipeline (apply, check-ready)
xtask ci check-ready                     # Verify required DB tables exist
xtask ci compat                          # Validate schema changes against base branch
```

**Note:** `xtask ci` requires the `sandbox` feature (used in CI environments, not default).

---

## Internal Commands (Invoked via Flags)

These are not standalone commands — they're invoked as flags on `xtask test` or `xtask check`:

| Flag | What it runs | Purpose |
|------|-------------|---------|
| `xtask test --coverage` | Coverage subcommands (html, lcov, summary, enforce) | Code coverage reporting |
| `xtask test --fuzz` | Fuzz lane (requires configured fuzz targets) | Security fuzzing |
| `xtask test --mutants` | Mutation testing | Code quality via mutation analysis |
| `xtask test --vm` | NixOS VM test runner (native Rust, no bash script) | NixOS compatibility gate |
| `xtask check --forbidden` | Forbidden pattern scanner | AST-grep pattern enforcement |
| `xtask check --lint` | Clippy with project config | Lint-only mode |
| `xtask check --nix` | `nix flake check --no-build` (~2-5s, eval only) | Nix flake evaluation check |
