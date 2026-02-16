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
SINEX_RPC_TOKEN="your-secret-token"

# RPC authentication token (file path) - PREFERRED
SINEX_RPC_TOKEN_FILE="/run/secrets/rpc-token"

# Admin token file (elevated privileges)
SINEX_GATEWAY_ADMIN_TOKEN_FILE="/run/secrets/admin-token"
```

## Limits

```bash
# Maximum blob event payload size in bytes (default: 10 MiB)
SINEX_GATEWAY_MAX_BLOB_BYTES=10485760
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
# Enable distributed rate limiting (default: false)
SINEX_RPC_RATE_LIMIT_ENABLED=true

# Requests per second (token bucket fill rate, default: 100)
SINEX_RPC_RATE_LIMIT_REQUESTS_PER_SEC=100

# Burst capacity (token bucket capacity, default: 200)
SINEX_RPC_RATE_LIMIT_BURST=200

# Idle timeout for per-client state in seconds (default: 300)
SINEX_RPC_RATE_LIMIT_IDLE_TIMEOUT_SECS=300

# Time window for distributed rate counting in seconds (default: 60)
SINEX_RPC_RATE_LIMIT_WINDOW_SECS=60

# Maximum requests per minute (distributed mode, default: 600)
SINEX_RPC_RATE_LIMIT_PER_MINUTE=600
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
# Allow bypassing replay approval workflow (default: false, DANGEROUS)
SINEX_ALLOW_REPLAY_CONTROL_BYPASS=false
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
| `SINEX_GATEWAY_MAX_BLOB_BYTES` | No | 10 MiB | Max blob payload |
| `SINEX_REPLAY_CONTROL_TIMEOUT_SECS` | No | 30s | Replay request timeout |
| `SINEX_NATS_CONSUMER_CREATE_TIMEOUT_SECS` | No | 10s | Consumer creation timeout |
| `SINEX_RPC_RATE_LIMIT_ENABLED` | No | `false` | Enable rate limiting |
| `SINEX_RPC_RATE_LIMIT_REQUESTS_PER_SEC` | No | 100 | Token fill rate |
| `SINEX_RPC_RATE_LIMIT_BURST` | No | 200 | Token bucket capacity |
| `SINEX_RPC_RATE_LIMIT_IDLE_TIMEOUT_SECS` | No | 300s | Client state TTL |
| `SINEX_RPC_RATE_LIMIT_WINDOW_SECS` | No | 60s | Distributed count window |
| `SINEX_RPC_RATE_LIMIT_PER_MINUTE` | No | 600 | Max requests/min (distributed) |
| `SINEX_NATIVE_MESSAGING_MAX_SIZE_BYTES` | No | 1 MiB | Max native message size |
| `SINEX_NATIVE_MESSAGING_EXTENSION_ROLES` | No | - | Per-extension role map (JSON) |
| `SINEX_GATEWAY_POOL_ACQUIRE_TIMEOUT_SECS` | No | 5s | DB pool acquire timeout |
| `SINEX_ALLOW_REPLAY_CONTROL_BYPASS` | No | `false` | Bypass replay approval |

*One of `SINEX_RPC_TOKEN` or `SINEX_RPC_TOKEN_FILE` required.

## See Also

- Transport security: `docs/transport_security.md`
- Global env vars: `docs/current/configuration/environment-variables.md`
