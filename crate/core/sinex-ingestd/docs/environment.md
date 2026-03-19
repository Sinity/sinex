# Ingestd Environment Variables

Environment variables specific to direct `sinex-ingestd` runs.

If ingestd is managed through the NixOS module, prefer typed `services.sinex.*` options for
shared transport wiring and secrets. In particular, NATS TLS on NixOS should use:

- `services.sinex.nodes.nats.servers`
- `services.sinex.nodes.nats.tls.*`
- `services.sinex.nodes.nats.auth.*`

instead of ad hoc env injection.

## NATS Connection

```bash
# NATS server URL (default: nats://localhost:4222)
SINEX_NATS_URL="tls://nats.example.com:4222"

# Optional auth: pick exactly one mode
SINEX_NATS_TOKEN_FILE="/run/secrets/nats-token"
# or
SINEX_NATS_CREDS_FILE="/run/secrets/nats-client.creds"
# or
SINEX_NATS_NKEY_SEED_FILE="/run/secrets/nats-user.nk"

# TLS enforcement and material
SINEX_NATS_REQUIRE_TLS=1
SINEX_NATS_CA_CERT="/run/secrets/nats-ca.pem"
SINEX_NATS_CLIENT_CERT="/run/secrets/nats-client.pem"
SINEX_NATS_CLIENT_KEY="/run/secrets/nats-client-key.pem"
```

## Resource Monitoring

```bash
# Disk usage threshold percentage that triggers backpressure (default: 90)
SINEX_INGESTD_DISK_THRESHOLD_PERCENT=90
```

## Quick Reference

| Variable | Required | Default | Purpose |
|----------|----------|---------|---------|
| `SINEX_NATS_URL` | No | `nats://localhost:4222` | NATS server URL |
| `SINEX_NATS_TOKEN_FILE` | Optional | - | File containing NATS auth token |
| `SINEX_NATS_CREDS_FILE` | Optional | - | NATS credentials file (`.creds`) |
| `SINEX_NATS_NKEY_SEED_FILE` | Optional | - | File containing NATS NKey seed |
| `SINEX_NATS_REQUIRE_TLS` | No | `false` | Reject non-TLS NATS URLs |
| `SINEX_NATS_CA_CERT` | Optional | - | CA bundle for server verification |
| `SINEX_NATS_CLIENT_CERT` | Optional | - | Client certificate for mTLS |
| `SINEX_NATS_CLIENT_KEY` | Optional | - | Client private key for mTLS |
| `SINEX_INGESTD_DISK_THRESHOLD_PERCENT` | No | 90 | Disk backpressure threshold |

## See Also

- Transport security: `docs/transport_security.md`
- Global env vars: `docs/current/configuration/environment-variables.md`
- NixOS module surface: `nixos/modules/README.md`
