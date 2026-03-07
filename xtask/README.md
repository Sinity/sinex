# xtask - Sinex Development Tools

Custom development automation for the Sinex workspace. This is the entry point for all common development workflows: testing, building, database operations, TLS setup, and dependency analysis.

## Installation

No installation needed - part of the workspace:

```bash
cd /realm/project/sinex
xtask --help
```

## Available Commands

### Dependency Management

Analyze workspace dependencies and identify optimization opportunities.

```bash
# List all workspace packages
xtask deps list [--format json]

# Show dependency tree (with depth control)
xtask deps tree [PACKAGE] [--depth N]

# Find duplicate dependency versions (shows redundant versions across packages)
xtask deps duplicates [--threshold N]

# Detect unused dependencies (requires cargo-machete)
xtask deps unused [--ci] [--format json]

# Analyze build timings (shows which packages take longest to compile)
xtask deps timings [--top N]

# Analyze rebuild impact (shows what breaks if this package changes)
xtask deps impact [PACKAGE] [--format json]
```

### Graph Visualization

Visualize the dependency graph in different formats for analysis and debugging.

```bash
# ASCII tree (default, good for terminal)
xtask deps graph

# Graphviz DOT format (can be rendered with Graphviz tools)
xtask deps graph --format dot [-o graph.dot]

# JSON for D3.js visualization
xtask deps graph --format json

# Focus on specific package (narrow scope)
xtask deps graph --focus sinex-core [--reverse]

# Depth limiting (prevent overwhelming output)
xtask deps graph --depth 3
```

### Development Workflow

Essential commands for daily development.

```bash
# Fast iteration (use between edits)
xtask check                    # fmt + clippy + forbidden patterns (~10s)

# Before commit
xtask check && xtask test

# Full CI validation (comprehensive)
xtask ci workspace             # schema + lint + all tests
```

### Database Operations

Database setup and management.

```bash
xtask db setup         # Create database + apply declarative schema
xtask db apply         # Apply declarative schema
xtask db status        # Check Postgres connectivity
```

### Testing Commands

Run tests with different options for different use cases.

```bash
# Standard run (multi-threaded with retries)
xtask test

# Debug failing tests (1 thread, full output)
xtask test --debug

# Include heavy/ignored tests
xtask test --heavy

# Prime database before testing
xtask test --prime

# Run with coverage collection
xtask test --coverage

# Advanced filters
xtask test -- -p sinex-core
xtask test --debug -- -E 'test(my_test_name)'
```

### Running heavy / ignored tests

Some tests are marked `#[ignore = "long"]` or `#[ignore = "external"]` and are skipped by default.

```bash
# Run heavy/ignored tests
xtask test --heavy --prime

# Include all ignored tests
xtask test --heavy
```

Or use the helper script at `./scripts/run-heavy-tests.sh` or the provided VS Code task "Run heavy tests (include ignored)".

### Contracts (Schema Management)

JSON schema registry and validation.

```bash
# Generate JSON schemas from EventPayload types
xtask contracts generate

# Verify core schema tables exist
xtask contracts check-ready
```

### TLS Management

Generate and manage TLS certificates for development and testing.

```bash
# Generate CA, server, and client certificates
xtask xtr tls generate-dev-certs

# Verify TLS configuration (expiration, chain validity)
xtask xtr tls check

# Generate additional client certificate
xtask xtr tls generate-client-cert
```

### Diagnostics

Environment health checks and code quality scanning.

```bash
# Comprehensive health check (Postgres, NATS, required tools)
xtask status --doctor --json

# Compact one-line status
xtask status --summary

# Stack diagnostics
xtask status --doctor

# Show currently running background jobs
xtask jobs active
xtask jobs list

# Query build history (test timing, flaky tests, etc.)
xtask history list
xtask history tests slowest
xtask history tests flaky
xtask history diagnostics        # Recent compiler warnings/errors
```

### xtr (Rarely Used Commands)

Commands under the `xtr` umbrella are less frequently used in day-to-day development.

```bash
# AST-grep pattern search
xtask xtr patterns -p '$X.unwrap()' --limit 10

# CI pipelines
xtask xtr ci workspace
xtask xtr ci postgres -- xtask test

# Shell completions
xtask xtr completions zsh > ~/.zsh/completions/_xtask
xtask xtr completions bash
xtask xtr completions fish
```

### Code Quality

Linting and formatting checks (all included in `check`).

```bash
# Format + clippy + forbidden patterns
xtask check

# JSON output for CI
xtask check --json
```

### Benchmarking

Use the `--bench` flag on the `test` command:

```bash
xtask test --bench
```

## Architecture

The `xtask` crate is organized into several key modules:

- **`command` / `commands`**: The CLI framework and individual command implementations.
- **`infra`**: Management of the *local development stack*. This includes starting/stopping long-lived service processes (Postgres, NATS) used across many tests and for manual development.
- **`sandbox`**: Management of *isolated test environments*. The sandbox provides ephemeral resources (temporary database slots, isolated NATS namespaces) for integration and E2E tests. It depends on `infra` to ensure the underlying service managers are available.

### Infra vs Sandbox

- Use **`infra`** when you need to manage the lifecycle of a service manager itself (e.g., `xtask infra start`).
- Use **`sandbox`** (specifically `Sandbox` and `PipelineScope`) when writing tests that need isolated access to these services.

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
xtask check

# Run tests
xtask test

# Database schema consistency
xtask contracts generate
xtask db status

# Full validation
xtask ci workspace
```

### Analyzing Build Performance

```bash
# See which packages compile slowly
xtask deps timings --top 10

# Understand dependency structure
xtask deps graph --format dot -o deps.dot
```

### Finding Dependency Issues

```bash
# List all packages and their counts
xtask deps list

# Show duplicates and redundancies
xtask deps duplicates

# Impact analysis (what breaks if I change X?)
xtask deps impact sinex-core
```

### Environment Troubleshooting

```bash
# Quick health check
xtask status --doctor

# Check database connectivity
xtask db status --json

# Verify TLS setup
xtask xtr tls check

# Stack diagnostics
xtask status --doctor
```

## Exit Codes

All commands return:

- **0** - Success
- **1** - Failure (invalid args, operation failed)
- **See error message** for details

## JSON Output

For CI integration and programmatic use, commands support `--json` flag:

```bash
xtask check --json | jq '.status'
xtask test --json | jq '.errors[]'
xtask deps list --json | jq '.data.packages | length'
```

JSON schema for responses is documented in `CLAUDE.md` under xtask Commands section.

## Performance Notes

- **xtask check**: ~10-20 seconds (incremental)
- **xtask test**: ~30-60 seconds (multi-threaded)
- **xtask ci workspace**: ~2-3 minutes (full validation)

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
xtask --help
```

### Tests failing with database errors

```bash
# Recreate database schema
xtask db setup

# Check connectivity
xtask db status --json
```

### TLS certificate issues

```bash
# Regenerate development certificates
xtask xtr tls generate-dev-certs

# Verify configuration
xtask xtr tls check
```

### High build times

```bash
# Identify slow packages
xtask deps timings --top 20

# Analyze specific package dependencies
xtask deps tree sinex-core --depth 2

# Check for duplicates
xtask deps duplicates
```

## See Also

- `/realm/project/sinex/xtask/docs/` - Detailed command documentation
- `/realm/project/sinex/xtask/src/` - Implementation
- `/realm/project/sinex/xtask/tests/` - Test coverage
