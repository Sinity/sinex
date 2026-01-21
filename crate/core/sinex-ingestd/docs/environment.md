# Ingestd Environment Variables

Environment variables specific to `sinex-ingestd`.

## NATS Connection

```bash
# NATS server URL (default: nats://localhost:4222)
SINEX_NATS_URL="nats://nats.example.com:4222"

# NATS authentication token
SINEX_NATS_TOKEN="your-nats-token"

# Require TLS for NATS connections
SINEX_NATS_REQUIRE_TLS=true
```

## Quick Reference

| Variable | Required | Default | Purpose |
|----------|----------|---------|---------|
| `SINEX_NATS_URL` | No | `nats://localhost:4222` | NATS server URL |
| `SINEX_NATS_TOKEN` | Prod | - | NATS auth token |
| `SINEX_NATS_REQUIRE_TLS` | No | `false` | Enforce TLS validation |

## See Also

- Transport security: `docs/transport_security.md`
- Global env vars: `docs/current/configuration/environment-variables.md`
