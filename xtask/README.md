# xtask - Sinex Development Tools

Custom development automation for the Sinex workspace. This is the entry point for all common development workflows: testing, building, database operations, TLS setup, and dependency analysis.

## Installation

No installation needed - part of the workspace:

```bash
cd /realm/project/sinex
cargo xtask --help
```

## Available Commands

### Dependency Management

Analyze workspace dependencies and identify optimization opportunities.

```bash
# List all workspace packages
cargo xtask deps list [--format json]

# Show dependency tree (with depth control)
cargo xtask deps tree [PACKAGE] [--depth N]

# Find duplicate dependency versions (shows redundant versions across packages)
cargo xtask deps duplicates [--threshold N]

# Detect unused dependencies (requires cargo-machete)
cargo xtask deps unused [--ci] [--format json]

# Analyze build timings (shows which packages take longest to compile)
cargo xtask deps timings [--top N]

# Analyze rebuild impact (shows what breaks if this package changes)
cargo xtask deps impact [PACKAGE] [--format json]
```

### Graph Visualization

Visualize the dependency graph in different formats for analysis and debugging.

```bash
# ASCII tree (default, good for terminal)
cargo xtask deps graph

# Graphviz DOT format (can be rendered with Graphviz tools)
cargo xtask deps graph --format dot [-o graph.dot]

# JSON for D3.js visualization
cargo xtask deps graph --format json

# Focus on specific package (narrow scope)
cargo xtask deps graph --focus sinex-core [--reverse]

# Depth limiting (prevent overwhelming output)
cargo xtask deps graph --depth 3
```

Note: The old `cargo xtask graph deps` command still works but is deprecated. Use `cargo xtask deps graph` instead.

### Development Workflow

Essential commands for daily development.

```bash
# Fast iteration (use between edits)
cargo xtask check                    # fmt + clippy + forbidden patterns (~10s)

# Before commit
cargo xtask check && cargo xtask test

# Full CI validation (comprehensive)
cargo xtask ci workspace             # schema + lint + all tests
```

### Database Operations

Database setup and management.

```bash
cargo xtask db setup         # Create database + run migrations
cargo xtask db migrate       # Apply pending migrations
cargo xtask db status        # Check Postgres connectivity
```

### Testing Commands

Run tests with different options for different use cases.

```bash
# Standard run (multi-threaded with retries)
cargo xtask test

# Debug failing tests (1 thread, full output)
cargo xtask test --debug

# Include heavy/ignored tests
cargo xtask test --heavy

# Prime database before testing
cargo xtask test --prime

# Run with coverage collection
cargo xtask test --coverage

# Advanced filters
cargo xtask test -- -p sinex-core
cargo xtask test --debug -- -E 'test(my_test_name)'
```

### Running heavy / ignored tests

Some tests are marked `#[ignore = "long"]` or `#[ignore = "external"]` and are skipped by default.

```bash
# Run heavy/ignored tests
cargo xtask test --heavy --prime

# Include all ignored tests
cargo xtask test --heavy
```

Or use the helper script at `./scripts/run-heavy-tests.sh` or the provided VS Code task "Run heavy tests (include ignored)".

### Contracts (Schema Management)

JSON schema registry and validation.

```bash
# Generate JSON schemas from EventPayload types
cargo xtask contracts generate

# Verify core schema tables exist
cargo xtask contracts check-ready
```

### TLS Management

Generate and manage TLS certificates for development and testing.

```bash
# Generate CA, server, and client certificates
cargo xtask tls generate-dev-certs

# Verify TLS configuration (expiration, chain validity)
cargo xtask tls check

# Generate additional client certificate
cargo xtask tls generate-client-cert

# Generate .env.tls file with certificate paths
cargo xtask tls setup-env
```

### Diagnostics

Environment health checks and code quality scanning.

```bash
# Comprehensive health check (Postgres, NATS, required tools)
cargo xtask status --doctor --json

# Compact one-line status
cargo xtask status --summary

# Stack diagnostics
cargo xtask stack doctor

# Show currently running background jobs
cargo xtask jobs active
cargo xtask jobs list

# Query build history (test timing, flaky tests, etc.)
cargo xtask history list
cargo xtask history tests slowest
cargo xtask history tests flaky
cargo xtask history diagnostics        # Recent compiler warnings/errors
```

### xtr (Rarely Used Commands)

Commands under the `xtr` umbrella are less frequently used in day-to-day development.

```bash
# AST-grep pattern search
cargo xtask xtr patterns -p '$X.unwrap()' --limit 10

# CI pipelines
cargo xtask xtr ci workspace
cargo xtask xtr ci postgres -- cargo xtask test

# Shell completions
cargo xtask xtr completions zsh > ~/.zsh/completions/_xtask
cargo xtask xtr completions bash
cargo xtask xtr completions fish
```

### Code Quality

Linting and formatting checks (all included in `check`).

```bash
# Format + clippy + forbidden patterns
cargo xtask check

# JSON output for CI
cargo xtask check --json
```

### Benchmarking

Performance benchmarking and analysis.

```bash
# Benchmark with parameter sweeps
cargo xtask bench --mode sweeps --threads 8,12,16

# Refine previous results
cargo xtask bench --mode refine --runs 5
```

## Documentation

- **Command reference**: `xtask/docs/README.md` (full details)
- **Testing guide**: `../docs/current/testing/`
- **Configuration**: `../docs/current/configuration/`

## Implementation

Built with:

- **CLI**: clap v4 with derive API for command structure
- **Workspace analysis**: guppy v0.17 for dependency graphs
- **Output**: Human-readable format + JSON for CI integration
- **Testing**: assert_cmd + predicates for xtask tests

## Development

Run xtask tests locally:

```bash
cd xtask
cargo test --lib        # Unit tests
cargo test --test '*'   # Integration tests
```

Build xtask standalone (for quick feedback):

```bash
cargo build -p xtask
./target/debug/xtask --help
```

## Common Workflows

### Before Submitting a PR

```bash
# Ensure code quality
cargo xtask check

# Run tests
cargo xtask test

# Database schema consistency
cargo xtask contracts generate
cargo xtask db status

# Full validation
cargo xtask ci workspace
```

### Analyzing Build Performance

```bash
# See which packages compile slowly
cargo xtask deps timings --top 10

# Understand dependency structure
cargo xtask graph deps --render-format dot -o deps.dot
```

### Finding Dependency Issues

```bash
# List all packages and their counts
cargo xtask deps list

# Show duplicates and redundancies
cargo xtask deps duplicates

# Impact analysis (what breaks if I change X?)
cargo xtask deps impact sinex-core
```

### Environment Troubleshooting

```bash
# Quick health check
cargo xtask status --doctor

# Check database connectivity
cargo xtask db status --json

# Verify TLS setup
cargo xtask tls check

# Stack diagnostics
cargo xtask stack doctor
```

## Exit Codes

All commands return:

- **0** - Success
- **1** - Failure (invalid args, operation failed)
- **See error message** for details

## JSON Output

For CI integration and programmatic use, commands support `--json` flag:

```bash
cargo xtask check --json | jq '.status'
cargo xtask test --json | jq '.errors[]'
cargo xtask deps list --json | jq '.data.packages | length'
```

JSON schema for responses is documented in `CLAUDE.md` under xtask Commands section.

## Performance Notes

- **cargo xtask check**: ~10-20 seconds (incremental)
- **cargo xtask test**: ~30-60 seconds (multi-threaded)
- **cargo xtask ci workspace**: ~2-3 minutes (full validation)

Timing varies based on:

- Number of changed files
- Whether clean build or incremental
- System load and available memory
- Test profile selected

## Troubleshooting

### Command not found

```bash
# Ensure you're in the sinex workspace root
cd /realm/project/sinex

# Rebuild xtask if binary is stale
cargo build -p xtask

# Try again
cargo xtask --help
```

### Tests failing with database errors

```bash
# Recreate database schema
cargo xtask db setup

# Check connectivity
cargo xtask db status --json
```

### TLS certificate issues

```bash
# Regenerate development certificates
cargo xtask tls generate-dev-certs

# Verify configuration
cargo xtask tls check
```

### High build times

```bash
# Identify slow packages
cargo xtask deps timings --top 20

# Analyze specific package dependencies
cargo xtask deps tree sinex-core --depth 2

# Check for duplicates
cargo xtask deps duplicates
```

## See Also

- `/realm/project/sinex/xtask/docs/` - Detailed command documentation
- `/realm/project/sinex/xtask/src/` - Implementation
- `/realm/project/sinex/xtask/tests/` - Test coverage
