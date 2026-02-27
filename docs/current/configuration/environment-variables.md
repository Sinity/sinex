# Environment Variables Reference

## Overview

Sinex uses environment variables for runtime configuration. This document covers shared variables; service-specific variables are documented in their respective crates.

## Naming Convention

All Sinex application variables **MUST** use the `SINEX_` prefix.

**Approved exceptions:**
- `DATABASE_URL` - Standard database connection string (SQLx ecosystem)
- `RUST_LOG` - Logging configuration (standard Rust tracing)
- `CARGO_*`, `HOME`, `USER`, `SHELL`, `PATH`, `TERM` - System variables
- `XDG_*` - XDG Base Directory specification
- `NATS_*` - NATS ecosystem standards
- Build metadata: `node_*`, `GIT_*`, `SOURCE_DATE_EPOCH`

## Per-Service Documentation

| Service | Location |
|---------|----------|
| Gateway | `crate/core/sinex-gateway/docs/environment.md` |
| Ingestd | `crate/core/sinex-ingestd/docs/environment.md` |
| Test utilities | `xtask/docs/sandbox/environment.md` |
| Desktop ingestor | `crate/nodes/sinex-desktop-ingestor/docs/environment.md` |
| Terminal ingestor | `crate/nodes/sinex-terminal-ingestor/docs/environment.md` |

## Shared Variables

### Database

```bash
# Primary database connection string (REQUIRED for most services)
DATABASE_URL="postgresql://user:pass@localhost/sinex_dev"

# Alternative: Unix socket connection (devenv default)
DATABASE_URL="postgresql:///sinex_dev?host=/run/postgresql"

# Connection pool settings
SINEX_DB_POOL_SIZE=20
SINEX_POOL_ACQUIRE_WARN_MS=1000
SINEX_DB_ACQUIRE_TIMEOUT_SECS=60
```

### Storage & Paths

```bash
# Annex storage directory
SINEX_ANNEX_PATH="/var/lib/sinex/annex"

# Working directory (default: /tmp/sinex)
SINEX_WORK_DIR="/var/lib/sinex/work"

# Data directory (default: $XDG_DATA_HOME/sinex)
SINEX_DATA_DIR="/var/lib/sinex/data"

# Configuration file path
SINEX_CONFIG="/etc/sinex/config.toml"
```

### Runtime Behavior

```bash
# Environment: dev, staging, prod (default: dev)
SINEX_ENVIRONMENT=production

# Edge mode (suppresses DATABASE_URL requirement, enables schema cache)
SINEX_EDGE_MODE=true

# Disable coordination between processors
SINEX_COORDINATION_DISABLED=true

# Dry-run mode
SINEX_DRY_RUN=true
```

### Logging

```bash
# Log level (standard RUST_LOG)
RUST_LOG=sinex=debug,sqlx=warn

# Alternative: Sinex-specific
SINEX_LOG_LEVEL=debug
```

## Build-Time Variables

```bash
# Node build metadata
node_VERSION="1.2.3"
node_COMMIT_HASH="abc123"
node_BRANCH="main"
node_BUILD_TIMESTAMP="2026-01-18T00:00:00Z"

# Reproducible builds
SOURCE_DATE_EPOCH=1705363200
```

## Priority & Precedence

1. **Environment variables** (highest priority)
2. Configuration files (`SINEX_CONFIG` or defaults)
3. Compiled defaults (lowest priority)

## Security Best Practices

**Do:**
- Use file paths for secrets: `SINEX_RPC_TOKEN_FILE` not `SINEX_RPC_TOKEN`
- Encode RPC role in the token value (`<token>:readonly|write|admin`)
- Store secrets in `/run/secrets` with 0600 permissions
- Rotate tokens regularly
- Use different credentials per environment

**Don't:**
- Commit credentials to version control
- Pass secrets on command line (visible in `ps`)
- Share credentials between services

## Examples

### Development

```bash
export DATABASE_URL="postgresql:///sinex_dev?host=/run/postgresql"
export SINEX_NATS_URL="nats://localhost:4222"
export RUST_LOG="sinex=debug"
devenv up nats ingestd gateway
```

### Production

```bash
export DATABASE_URL="postgresql://sinex:PASSWORD@db.prod.internal/sinex"
export SINEX_NATS_URL="tls://nats.prod.internal:4222"
export SINEX_RPC_TOKEN_FILE="/run/secrets/rpc-token"
export SINEX_ENVIRONMENT=production
sinex-gateway
```

## See Also

- Type-safe config values: `crate/lib/sinex-primitives/docs/newtypes.md`
- Security architecture: `docs/current/security-architecture.md`
