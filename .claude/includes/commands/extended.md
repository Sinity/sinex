## Extended Commands

### Benchmarking

```bash
xtask bench                              # Run benchmarks (default mode)
xtask bench --mode sweeps --threads 8,12,16  # Thread-count sweep
xtask bench --mode refine --runs 5       # Refine with multiple runs
```

### Running Binaries

```bash
xtask run ingestd                        # Run sinex-ingestd
xtask run gateway                        # Run sinex-gateway
xtask run --watch ingestd                # Hot-reload on file changes
xtask run --tether ingestd               # Tether to production NATS
xtask run stack                          # Run full stack (ingestd + gateway)
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
xtask vm test                            # Run all VM tests
xtask vm test --category integration     # Filter by category
xtask vm test --parallel                 # Parallel execution
xtask vm start minimal                   # Boot minimal NixOS VM
xtask vm start standard --persistent     # Persistent standard VM
xtask vm ssh                             # SSH into running VM
xtask vm stop                            # Shut down VM
xtask vm snapshot create NAME            # Save VM snapshot
xtask vm snapshot restore NAME           # Restore VM snapshot
```

---

## Internal Commands (Invoked via Flags)

These are not standalone commands — they're invoked as flags on `xtask test` or `xtask check`:

| Flag | What it runs | Purpose |
|------|-------------|---------|
| `xtask test --coverage` | Coverage subcommands (html, lcov, summary, enforce) | Code coverage reporting |
| `xtask test --fuzz` | Fuzz targets (init, list, run, corpus) | Security fuzzing |
| `xtask test --mutants` | Mutation testing | Code quality via mutation analysis |
| `xtask check --forbidden` | Forbidden pattern scanner | AST-grep pattern enforcement |
| `xtask check --lint` | Clippy with project config | Lint-only mode |
