# Environment Variables Reference

## Overview
Sinex uses environment variables for runtime configuration that varies between deployments or cannot be committed to version control (credentials, paths, feature flags).

## Naming Convention

### Application Variables: `SINEX_*` Prefix
All Sinex application variables **MUST** use the `SINEX_` prefix.

**Approved exceptions:**
- `DATABASE_URL` - Standard database connection string (SQLx/Diesel ecosystem)
- `RUST_LOG` - Logging configuration (standard Rust tracing/env_logger)
- `CARGO_*` - Build system variables (set by Cargo)
- `HOME`, `USER`, `SHELL`, `PATH`, `TERM` - System variables
- `XDG_*` - XDG Base Directory specification
- `NATS_*` - NATS ecosystem standards
- Build metadata: `node_*`, `GIT_*`, `SOURCE_DATE_EPOCH`

## Core Configuration Variables

### Gateway (sinex-gateway)

#### Network & TLS
```bash
# TCP listen address (default: 127.0.0.1:9999)
SINEX_GATEWAY_TCP_LISTEN="0.0.0.0:8080"

# TLS certificate paths
SINEX_GATEWAY_TLS_CERT="/path/to/cert.pem"
SINEX_GATEWAY_TLS_KEY="/path/to/key.pem"
SINEX_GATEWAY_TLS_CLIENT_CA="/path/to/ca.pem"  # Optional: for mTLS

# Require client TLS certificates (default: false)
SINEX_GATEWAY_REQUIRE_CLIENT_TLS=true
```

#### Authentication
```bash
# RPC authentication token (direct value)
SINEX_RPC_TOKEN="your-secret-token"

# RPC authentication token (file path)
SINEX_RPC_TOKEN_FILE="/run/secrets/rpc-token"

# Admin token file (elevated privileges)
SINEX_GATEWAY_ADMIN_TOKEN_FILE="/run/secrets/admin-token"
```

#### Limits
```bash
# Maximum blob event payload size in bytes (default: 10 MiB)
SINEX_GATEWAY_MAX_BLOB_BYTES=10485760
```

### Ingestd (sinex-ingestd)

#### NATS Connection
```bash
# NATS server URL (default: nats://localhost:4222)
SINEX_NATS_URL="nats://nats.example.com:4222"

# NATS authentication token
SINEX_NATS_TOKEN="your-nats-token"
```

### Database

#### Connection
```bash
# Primary database connection string (REQUIRED)
DATABASE_URL="postgresql://user:pass@localhost/sinex_dev"

# Alternative: Unix socket connection (devenv default)
DATABASE_URL="postgresql:///sinex_dev?host=/run/postgresql"
```

#### Connection Pool
```bash
# Connection pool size (default: 10)
SINEX_DB_POOL_SIZE=20

# Warn if pool acquisition takes longer than N ms (default: 500)
SINEX_POOL_ACQUIRE_WARN_MS=1000

# Timeout for pool acquisition in seconds (default: 30)
SINEX_POOL_ACQUIRE_TIMEOUT_SECS=60
```

### Storage & Paths

#### Annex Storage
```bash
# Path to annex storage directory
SINEX_ANNEX_PATH="/var/lib/sinex/annex"
```

#### Working Directories
```bash
# node working directory (default: /tmp/sinex)
SINEX_WORK_DIR="/var/lib/sinex/work"

# Data directory (default: $XDG_DATA_HOME/sinex or $HOME/.local/share/sinex)
SINEX_DATA_DIR="/var/lib/sinex/data"

# Custom configuration file path
SINEX_CONFIG="/etc/sinex/config.toml"
```

### Runtime Behavior

#### Environment
```bash
# Environment name: dev, staging, prod (default: dev)
SINEX_ENVIRONMENT=production
```

#### Feature Flags
```bash
# Enable edge mode processing
SINEX_EDGE_MODE=true

# Disable coordination between processors
SINEX_COORDINATION_DISABLED=true

# Dry-run mode (no side effects)
SINEX_DRY_RUN=true
```

#### Session & Identity
```bash
# Session identifier (for terminal node)
SINEX_SESSION_ID="unique-session-id"

# Application version override
SINEX_VERSION="1.2.3"

# Replay service namespace
SINEX_REPLAY_NAMESPACE="custom-namespace"
```

#### Process Management
```bash
# Heartbeat staleness threshold in seconds (default: 300)
SINEX_PROCESS_HEARTBEAT_STALE_SECS=600
```

### Logging
```bash
# Log level (uses RUST_LOG standard)
# Values: trace, debug, info, warn, error
RUST_LOG=sinex=debug,sqlx=warn

# Alternative: Sinex-specific log level
SINEX_LOG_LEVEL=debug
```

## node-Specific Variables

### Desktop node (sinex-desktop-node)
```bash
# Require Hyprland window manager (fail if not detected)
SINEX_DESKTOP_REQUIRE_HYPRLAND=true

# Skip DBus RPATH setting during build
SINEX_SKIP_DBUS_RPATH=true
```

### Terminal node (sinex-terminal-node)
```bash
# Session ID (auto-detected from TERM_SESSION_ID if not set)
SINEX_SESSION_ID="terminal-session-123"
```

## Testing & Development

### Test Infrastructure
```bash
# Enable TLS in integration tests
SINEX_TEST_USE_TLS=true

# NATS token for tests
SINEX_TEST_NATS_TOKEN="test-token"

# NATS shared key for tests
SINEX_TEST_NATS_SHARED_KEY="test-shared-key"

# NATS config file for tests
SINEX_TEST_NATS_CONFIG_FILE="/tmp/nats-test.conf"

# Test failure snapshot directory
SINEX_TEST_FAIL_DIR="/tmp/sinex-test-failures"

# Enable test optimizations (faster but less realistic)
SINEX_TEST_OPTIMIZATIONS=true

# Bypass replay control checks (TESTING ONLY)
SINEX_ALLOW_REPLAY_CONTROL_BYPASS=true
```

### Property Testing
```bash
# Number of property test cases (default: 256)
SINEX_PROPTEST_CASES=1000

# Property test RNG seed (for reproducibility)
SINEX_PROPTEST_SEED=12345

# Property test output directory
SINEX_PROPTEST_DIR="/tmp/proptest-output"

# Standard PropTest variable (fallback)
PROPTEST_CASES=1000
```

### Schema Migrations
```bash
# Allow destructive DOWN migrations (DANGEROUS)
SINEX_ALLOW_SCHEMA_DOWN=true
```

### Benchmarking
```bash
# Separate database for benchmarks
BENCH_DATABASE_URL="postgresql:///sinex_bench?host=/run/postgresql"
```

## Build-Time Variables

### node Build Metadata
```bash
# node version (default: CARGO_PKG_VERSION)
node_VERSION="1.2.3"

# Git commit hash
node_COMMIT_HASH="abc123def456"

# Git commit count
node_COMMIT_COUNT=42

# Git branch name
node_BRANCH="feature/xyz"

# Dirty working tree indicator
node_IS_DIRTY=true

# Build timestamp (RFC3339)
node_BUILD_TIMESTAMP="2026-01-16T02:00:00Z"

# Full version string (composite)
node_FULL_VERSION="1.2.3-42-gabc123def"

# Binary content hash
node_BINARY_HASH="sha256:..."
```

### Reproducible Builds
```bash
# Reproducible build timestamp (Unix epoch)
SOURCE_DATE_EPOCH=1705363200
```

## Ecosystem Standards (External)

### Database Testing
```bash
# Superuser connection for test setup/teardown
DATABASE_URL_SUPERUSER="postgresql://postgres@localhost/postgres"

# Application-specific connection for permission tests
DATABASE_URL_APP="postgresql://sinex_app@localhost/sinex_dev"
```

### NATS
```bash
# NATS credentials file (standard NATS variable)
NATS_CREDS="/path/to/nats.creds"

# NATS server binary path (for embedded test server)
NATS_SERVER_BIN="/usr/local/bin/nats-server"
```

### System Detection
```bash
# Hyprland window manager instance signature
HYPRLAND_INSTANCE_SIGNATURE="..."

# Kitty terminal IPC socket
KITTY_LISTEN_ON="unix:/tmp/kitty-socket"
```

## Priority & Precedence

Configuration sources are checked in this order (first found wins):

1. **Environment variables** (highest priority)
2. Configuration files (`SINEX_CONFIG` or default locations)
3. Compiled defaults (lowest priority)

Example:
```bash
# Environment overrides config file
SINEX_GATEWAY_TCP_LISTEN=0.0.0.0:8080 sinex-gateway
```

## Security Best Practices

### ✅ Do
- Use file paths for secrets: `SINEX_RPC_TOKEN_FILE` instead of `SINEX_RPC_TOKEN`
- Store secrets in `/run/secrets` or similar secure mount
- Use restrictive file permissions (0600) for credential files
- Rotate tokens regularly
- Use different credentials per environment

### ❌ Don't
- Commit credentials to version control
- Pass secrets on command line (visible in `ps`)
- Use production credentials in development
- Share credentials between services
- Log credential values

## Examples

### Development Setup
```bash
# Use devenv defaults (Unix sockets, local services)
export DATABASE_URL="postgresql:///sinex_dev?host=/run/postgresql"
export SINEX_NATS_URL="nats://localhost:4222"
export RUST_LOG="sinex=debug"

# Start services
devenv up nats ingestd gateway
```

### Production Deployment
```bash
# Use TCP connections, TLS, secrets
export DATABASE_URL="postgresql://sinex:PASSWORD@db.prod.internal/sinex"
export SINEX_NATS_URL="nats://nats.prod.internal:4222"
export SINEX_NATS_TOKEN="$(cat /run/secrets/nats-token)"
export SINEX_RPC_TOKEN_FILE="/run/secrets/rpc-token"
export SINEX_GATEWAY_TLS_CERT="/etc/sinex/tls/cert.pem"
export SINEX_GATEWAY_TLS_KEY="/etc/sinex/tls/key.pem"
export SINEX_GATEWAY_REQUIRE_CLIENT_TLS=true
export SINEX_ENVIRONMENT=production
export RUST_LOG="sinex=info,sqlx=warn"

# Start gateway
sinex-gateway
```

### Testing
```bash
# Run tests with custom database
export DATABASE_URL="postgresql:///sinex_test?host=/run/postgresql"
export SINEX_TEST_USE_TLS=true
export SINEX_PROPTEST_CASES=100

cargo xtask test --profile reliable
```

## See Also
- [Newtype Guide](./newtype-guide.md) - Type-safe configuration values
- [Environment Variable Audit](../../execution/env-var-audit.md) - Compliance audit
- [Configuration Guide](../architecture/configuration.md) - Overall config architecture
