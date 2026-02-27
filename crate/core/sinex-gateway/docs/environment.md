# Gateway Environment Variables

Environment variables specific to `sinex-gateway`.

## Network & TLS

```bash
# TCP listen address (default: 127.0.0.1:9999)
SINEX_GATEWAY_TCP_LISTEN="0.0.0.0:8080"

# TLS certificate paths (REQUIRED for production)
SINEX_GATEWAY_TLS_CERT="/path/to/cert.pem"
SINEX_GATEWAY_TLS_KEY="/path/to/key.pem"
SINEX_GATEWAY_TLS_CLIENT_CA="/path/to/ca.pem"  # Optional: for mTLS

# Require client TLS certificates (default: false)
SINEX_GATEWAY_REQUIRE_CLIENT_TLS=true
```

## Authentication

```bash
# RPC authentication token (direct value)
# Required format: <token>:<role> where role is readonly|write|admin
SINEX_RPC_TOKEN="your-secret-token:admin"

# RPC authentication token (file path) - PREFERRED
SINEX_RPC_TOKEN_FILE="/run/secrets/rpc-token"

# Admin token file (elevated privileges)
SINEX_GATEWAY_ADMIN_TOKEN_FILE="/run/secrets/admin-token"
```

## Limits

```bash
# Maximum decoded blob payload size in bytes (default: 5 MiB)
SINEX_GATEWAY_MAX_BLOB_BYTES=5242880
```

## Timeouts

```bash
# Replay control request timeout in seconds (default: 30)
SINEX_REPLAY_CONTROL_TIMEOUT_SECS=30

# NATS consumer creation timeout in seconds (default: 10)
SINEX_NATS_CONSUMER_CREATE_TIMEOUT_SECS=10
```

## Rate Limiting

```bash
# Enable rate limiting (default: true)
SINEX_RPC_RATE_LIMIT_ENABLED=true

# In-memory mode: requests per second (default: 100)
SINEX_RPC_RATE_LIMIT_REQUESTS_PER_SEC=100

# In-memory mode: burst capacity (default: 50)
SINEX_RPC_RATE_LIMIT_BURST=50

# In-memory mode: idle timeout for per-token state in seconds (default: 3600)
SINEX_RPC_RATE_LIMIT_IDLE_TIMEOUT_SECS=3600

# Distributed mode: time window for rate counting in seconds (default: 60)
SINEX_RPC_RATE_LIMIT_WINDOW_SECS=60

# Distributed mode: maximum requests per minute (default: 6000)
SINEX_RPC_RATE_LIMIT_PER_MINUTE=6000
```

## Native Messaging

```bash
# Maximum native messaging payload size in bytes (default: 1048576 = 1 MiB)
SINEX_NATIVE_MESSAGING_MAX_SIZE_BYTES=1048576

# Per-extension role mapping for native messaging auth (JSON)
# Maps extension IDs to roles: "ReadOnly", "Write", or "Admin"
SINEX_NATIVE_MESSAGING_EXTENSION_ROLES='{"my-extension": "ReadOnly", "admin-ext": "Admin"}'
```

## Database Pool

```bash
# Pool acquire timeout in seconds (default: 5)
SINEX_GATEWAY_POOL_ACQUIRE_TIMEOUT_SECS=5
```

## Replay Control

```bash
# Make replay control optional (default: false)
# When set to 1/true/yes, the gateway starts even if NATS is unavailable.
# Replay approval workflow will be non-functional in this mode.
SINEX_REPLAY_CONTROL_OPTIONAL=false
```

## Quick Reference

| Variable | Required | Default | Purpose |
|----------|----------|---------|---------|
| `SINEX_GATEWAY_TCP_LISTEN` | No | `127.0.0.1:9999` | TCP listen address |
| `SINEX_GATEWAY_TLS_CERT` | Prod | - | TLS certificate path |
| `SINEX_GATEWAY_TLS_KEY` | Prod | - | TLS private key path |
| `SINEX_GATEWAY_TLS_CLIENT_CA` | No | - | Client CA for mTLS |
| `SINEX_RPC_TOKEN` | Yes* | - | Bearer token (direct) |
| `SINEX_RPC_TOKEN_FILE` | Yes* | - | Bearer token (file) |
| `SINEX_GATEWAY_MAX_BLOB_BYTES` | No | 5 MiB | Max decoded blob payload |
| `SINEX_REPLAY_CONTROL_TIMEOUT_SECS` | No | 30s | Replay request timeout |
| `SINEX_NATS_CONSUMER_CREATE_TIMEOUT_SECS` | No | 10s | Consumer creation timeout |
| `SINEX_RPC_RATE_LIMIT_ENABLED` | No | `true` | Enable rate limiting |
| `SINEX_RPC_RATE_LIMIT_REQUESTS_PER_SEC` | No | 100 | Token fill rate |
| `SINEX_RPC_RATE_LIMIT_BURST` | No | 50 | Token bucket capacity |
| `SINEX_RPC_RATE_LIMIT_IDLE_TIMEOUT_SECS` | No | 3600s | Client state TTL |
| `SINEX_RPC_RATE_LIMIT_WINDOW_SECS` | No | 60s | Distributed count window |
| `SINEX_RPC_RATE_LIMIT_PER_MINUTE` | No | 6000 | Max requests/min (distributed) |
| `SINEX_NATIVE_MESSAGING_MAX_SIZE_BYTES` | No | 1 MiB | Max native message size |
| `SINEX_NATIVE_MESSAGING_EXTENSION_ROLES` | No | - | Per-extension role map (JSON) |
| `SINEX_GATEWAY_POOL_ACQUIRE_TIMEOUT_SECS` | No | 5s | DB pool acquire timeout |
| `SINEX_REPLAY_CONTROL_OPTIONAL` | No | `false` | Make replay control optional |

*One of `SINEX_RPC_TOKEN` or `SINEX_RPC_TOKEN_FILE` required. Tokens must include a role suffix (`:readonly`, `:write`, or `:admin`).

## See Also

- Transport security: `docs/transport_security.md`
- Global env vars: `docs/current/configuration/environment-variables.md`
