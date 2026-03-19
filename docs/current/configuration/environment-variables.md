# Environment Variables Reference

## Overview

Sinex uses environment variables for runtime configuration when services are started directly.
This document covers that direct-run surface; service-specific variables are documented in
their respective crates.

For NixOS deployments, prefer typed `services.sinex.*` module options for transport, secrets,
and other stable infrastructure wiring. In particular, gateway and NATS TLS should be configured
through the NixOS module surface, not by manually stuffing `SINEX_*` transport variables into
service environments.

## Canonical Ownership

- deployed systems should be configured through `services.sinex.*` NixOS options
- if a NixOS option renders an environment variable, the option is the owner and the env var is only process-level transport
- direct/manual runs may still set `SINEX_*` variables explicitly
- gateway TLS/auth options live under `services.sinex.core.gateway.*`
- shared NATS transport/auth options live under `services.sinex.nodes.nats.{servers,tls,auth}`

## NixOS-Managed Variables

These variables are not the preferred authoring surface on managed hosts.

| NixOS option owner | Rendered env vars |
|---|---|
| `services.sinex.core.gateway.tls*`, `requireClientTLS`, `autoGenerateTls` | `SINEX_GATEWAY_TLS_CERT`, `SINEX_GATEWAY_TLS_KEY`, `SINEX_GATEWAY_TLS_CLIENT_CA`, `SINEX_GATEWAY_REQUIRE_CLIENT_TLS` |
| `services.sinex.core.gateway.limits.*` | `SINEX_GATEWAY_MAX_CONCURRENCY`, `SINEX_GATEWAY_REQUEST_TIMEOUT_SECS`, `SINEX_GATEWAY_MAX_BODY_BYTES`, `SINEX_GATEWAY_MAX_BLOB_BYTES` |
| `services.sinex.nodes.nats.servers`, `monitoringPort` | `SINEX_NATS_URL`, `SINEX_NATS_MONITORING_PORT` |
| `services.sinex.nodes.nats.tls.*` | `SINEX_NATS_REQUIRE_TLS`, `SINEX_NATS_CA_CERT`, `SINEX_NATS_CLIENT_CERT`, `SINEX_NATS_CLIENT_KEY` |
| `services.sinex.nodes.nats.auth.*` | `SINEX_NATS_TOKEN_FILE`, `SINEX_NATS_CREDS_FILE`, `SINEX_NATS_NKEY_SEED_FILE` |
| `services.sinex.nodes.defaults.env` | arbitrary pass-through env-only flags |

## Env-Only Variables

These are not the primary declarative configuration surface:

- `DATABASE_URL`: direct-run database connection string; the NixOS module synthesizes it for managed services
- `RUST_LOG`: standard process logging override
- `SOURCE_DATE_EPOCH` and `node_*`: build metadata, not service configuration
- ad-hoc direct-run variables used outside NixOS service management, such as manual `sinex-gateway` or node launches

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

## Shared Variables For Direct Runs

### Database

```bash
# Primary database connection string (REQUIRED for most services)
DATABASE_URL="postgresql://user:pass@localhost/sinex_dev"

# Alternative: Unix socket connection (devenv default)
DATABASE_URL="postgresql:///sinex_dev?host=/run/postgresql"

# Additional DB tuning may exist per service; see the owning crate env docs.
```

### Storage & Paths

```bash
# Annex storage directory
SINEX_ANNEX_PATH="/var/lib/sinex/annex"

# Working directory (default: /tmp/sinex)
SINEX_WORK_DIR="/var/lib/sinex/work"

# Data directory (default: $XDG_DATA_HOME/sinex)
SINEX_DATA_DIR="/var/lib/sinex/data"
```

### Runtime Behavior

```bash
# Environment: dev, staging, prod (default: dev)
SINEX_ENVIRONMENT=production

# Edge mode (suppresses DATABASE_URL requirement, enables schema cache)
SINEX_EDGE_MODE=true

# Disable coordination between nodes
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

1. **NixOS module options** for deployed systems
2. **Environment variables** for direct/manual runs
3. Compiled defaults

The surviving file-based config surface is limited to justified local preference files such as
`sinexctl` user preferences. Core service deployment is not file-config-first.

## Security Best Practices

**Do:**
- Use file paths for secrets: `SINEX_RPC_TOKEN_FILE` not `SINEX_RPC_TOKEN`
- Use file paths for NATS auth in deployed systems:
  `SINEX_NATS_TOKEN_FILE`, `SINEX_NATS_CREDS_FILE`, or `SINEX_NATS_NKEY_SEED_FILE`
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
xtask infra start
xtask run core --logs
```

### Production

```bash
export DATABASE_URL="postgresql://sinex:PASSWORD@db.prod.internal/sinex"
export SINEX_NATS_URL="tls://nats.prod.internal:4222"
export SINEX_NATS_CREDS_FILE="/run/secrets/nats-client.creds"
export SINEX_RPC_TOKEN_FILE="/run/secrets/rpc-token"
export SINEX_ENVIRONMENT=production
sinex-gateway
```

For declarative hosts, use the NixOS module instead of exporting these values in shell profile
or service-manager glue. If an env var has a typed `services.sinex.*` owner, set the option, not the variable.

## See Also

- Type-safe config values: `crate/lib/sinex-primitives/docs/newtypes.md`
- Security model and posture: `docs/current/security.md`
- Gateway env surface: `crate/core/sinex-gateway/docs/environment.md`
- NATS env surface: `crate/core/sinex-ingestd/docs/environment.md`
- NixOS module surface: `nixos/modules/README.md`
