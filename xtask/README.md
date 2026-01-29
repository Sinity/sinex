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
cargo xtask graph deps

# Graphviz DOT format (can be rendered with Graphviz tools)
cargo xtask graph deps --render-format dot [-o graph.dot]

# JSON for D3.js visualization
cargo xtask graph deps --render-format json

# Focus on specific package (narrow scope)
cargo xtask graph deps --focus sinex-core [--reverse]

# Depth limiting (prevent overwhelming output)
cargo xtask graph deps --depth 3
```

### Development Workflow

Essential commands for daily development.

```bash
# Fast iteration (use between edits)
cargo xtask check                    # fmt --check + cargo check (~10s)

# Before commit
cargo xtask check && cargo xtask test --profile default

# Full CI validation (comprehensive)
cargo xtask ci-preflight             # lint + all tests + integration
```

### Database Operations

Database setup and management.

```bash
cargo xtask db setup         # Create database + run migrations
cargo xtask db migrate       # Apply pending migrations
cargo xtask db status        # Check Postgres connectivity
```

### Testing Commands

Run tests with different profiles optimized for different use cases.

```bash
# Quick feedback during development (12 threads, no retries)
cargo xtask test --profile fast

# CI validation (12 threads, 3 retries)
cargo xtask test --profile default

# Debug failing tests (1 thread, full output)
cargo xtask test --profile debug

# Include performance/stress tests
# These tests are marked `#[ignore]` by default to keep feedback fast. To run them:
# - Use the xtask alias: `cargo xtask test:heavy --prime`
# - Or include ignored tests directly: `cargo xtask test --include-ignored --prime`

# Advanced filters
cargo xtask test --profile default -- -p sinex-core
cargo xtask test --profile debug -- -E 'test(my_test_name)'
```

### Running heavy / ignored tests

Some tests are marked `#[ignore = "long"]` or `#[ignore = "external"]` and are skipped by default. To run only those heavy/external tests via xtask (recommended):

```bash
# Run tests that are annotated with #[ignore = "long"|"external"]
direnv exec /realm/project/sinex cargo xtask test:heavy --prime

# If you want to run *all* ignored tests (including flaky or platform-specific skips):
# direnv exec /realm/project/sinex cargo xtask test --include-ignored --prime
```

Or use the helper script at `./scripts/run-heavy-tests.sh` or the provided VS Code task "Run heavy tests (include ignored)".

### Schema Management

JSON schema registry and validation.

```bash
# Generate JSON schemas from EventPayload types
cargo xtask schema generate

# Verify core schema tables exist
cargo xtask schema check-ready
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
# Environment health check
cargo xtask doctor --json

# Check NATS/Postgres connectivity
cargo xtask doctor --pipelines

# Scan for forbidden patterns (unsafe, unwrap, expect)
cargo xtask lint-forbidden --json
```

### Code Quality

Linting and formatting checks.

```bash
# Format check only (no changes)
cargo xtask check

# Lint with Clippy (strict warnings)
cargo xtask lint
```

### Coverage

Code coverage reports.

```bash
# Generate HTML coverage report and open in browser
cargo xtask coverage html --open

# Summary coverage by file
cargo xtask coverage summary --files
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
cargo xtask lint

# Run tests
cargo xtask test --profile fast

# Database schema consistency
cargo xtask schema generate
cargo xtask db status

# Full validation
cargo xtask ci-preflight
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
cargo xtask doctor

# Check database connectivity
cargo xtask db status --json

# Verify TLS setup
cargo xtask tls check

# Check for forbidden patterns
cargo xtask lint-forbidden --json
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
cargo xtask test --profile fast --json | jq '.errors[]'
cargo xtask deps list --format json | jq '.packages | length'
```

JSON schema for responses is documented in `CLAUDE.md` under xtask Commands section.

## Performance Notes

- **cargo xtask check**: ~10 seconds (incremental)
- **cargo xtask test --profile fast**: ~30-60 seconds (12 parallel threads)
- **cargo xtask lint**: ~15 seconds
- **cargo xtask ci-preflight**: ~2-3 minutes (full validation)

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
