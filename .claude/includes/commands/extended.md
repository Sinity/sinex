## Extended Commands

### Running Binaries

```bash
xtask run ingestd                        # Run sinex-ingestd
xtask run gateway                        # Run sinex-gateway
xtask run node <NAME>                    # Run a specific node by name
xtask run --watch ingestd                # Hot-reload on file changes
xtask run --metrics ingestd              # Show periodic runtime metrics overlay (heartbeat, lag, latency)
xtask run core                          # Run full core services (ingestd + gateway)
xtask run all-ingestors                  # Run all ingestor nodes
xtask run all-automatons                 # Run all automaton nodes
xtask run tether                         # Connect to a remote environment via The Tether
xtask run list                           # List available binaries
```

See `xtask run --help` for all flags.

### Documentation

```bash
xtask docs build                         # Generate rustdoc
xtask docs build --open                  # Generate and open in browser
xtask docs build --private --all-features  # Include private items
xtask docs serve                         # Serve docs at localhost:8080
xtask docs serve --build                 # Build then serve
xtask docs agents                        # Generate AGENTS.md from CLAUDE.md
xtask docs snapshot                      # AI context snapshot (via repomix)
xtask docs snapshot --output context.md  # Custom output path
xtask docs snapshot --compress           # Tree-sitter structure extraction
xtask docs snapshot --include "crate/lib/**"  # Filter by glob
xtask docs snapshot --changed            # Include git-changed files
xtask docs snapshot --context           # Inject xtask state block
xtask docs snapshot --scope sinex-db    # Scope to crate + transitive deps
```

### Exercise (xtask Self-Validation)

```bash
xtask exercise --tier 1                  # Quick surface checks
xtask exercise --tier 2                  # Infrastructure exercises
xtask exercise --tier 3                  # Database + pipeline exercises
xtask exercise --tier 4                  # Heavy/stress exercises
xtask exercise --all                     # All tiers
```

See `xtask exercise --help` for all flags.

### VM Testing

```bash
# NixOS compatibility gate via `xtask test vm` subcommand
xtask test vm                            # Run smoke tests (fast, ~5-10min)
xtask test vm --category smoke           # Explicit smoke category
xtask test vm --category integration     # Integration scenarios
xtask test vm --category all             # Full suite
xtask test vm --parallel                 # Parallel execution

# VM lifecycle (infrastructure management)
xtask infra vm start minimal             # Boot minimal NixOS VM
xtask infra vm start standard --persistent  # Persistent standard VM
xtask infra vm ssh                       # SSH into running VM
xtask infra vm stop                      # Shut down VM
```

### Privacy Engine

```bash
xtask privacy catalog                    # List all privacy rules
xtask privacy test "some text"           # Test text against privacy engine
xtask privacy decrypt <TOKEN>            # Decrypt an encrypted privacy token
xtask privacy key                        # Show privacy key information
xtask privacy config                     # Show or generate privacy configuration
```

### Shell Completions

```bash
# Generate and install zsh completions (dynamic package/run-target values)
xtask completions zsh > ~/.zsh/completions/_xtask
compinit  # or restart your shell

# Other shells
xtask completions bash > ~/.bash_completion.d/xtask
xtask completions fish > ~/.config/fish/completions/xtask.fish

# The completion scripts use these hidden helpers at tab-complete time:
xtask completions list-packages          # Workspace package names (one per line)
xtask completions list-run-targets       # Run target names (one per line)
```

### CI Pipelines

```bash
xtask ci workspace                       # Full validation (schema + lint + tests)
xtask ci postgres -- CMD                 # Run CMD with ephemeral Postgres
```

**Note:** `xtask ci` requires the `sandbox` feature (used in CI environments, not default).
See `xtask ci --help` for all subcommands.

---

## Test Subcommands

Specialized test modes are subcommands of `xtask test`, not flags:

| Subcommand | What it runs | Purpose |
|------------|-------------|---------|
| `xtask test bench` | Benchmark sweeps (criterion via nextest) | Performance measurement |
| `xtask test bench --contracts` | Bench + perf contract enforcement | Regression gating |
| `xtask test fuzz` | libfuzzer targets | Security fuzzing |
| `xtask test coverage` | cargo-llvm-cov | Code coverage reporting |
| `xtask test mutants` | cargo-mutants | Mutation analysis |
| `xtask test vm` | NixOS VM test runner | NixOS compatibility gate |

Check-specific modes remain as flags on `xtask check`:

| Flag | Purpose |
|------|---------|
| `xtask check --lint` | Clippy with project config |
| `xtask check --forbidden` | AST-grep pattern enforcement |
| `xtask check --nix` | Nix flake evaluation check |
