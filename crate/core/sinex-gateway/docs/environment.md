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

*One of `SINEX_RPC_TOKEN` or `SINEX_RPC_TOKEN_FILE` required.

## See Also

- Transport security: `docs/transport_security.md`
- Global env vars: `docs/current/configuration/environment-variables.md`
