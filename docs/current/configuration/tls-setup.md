# TLS Setup

This is the generic TLS guide for Sinex. It covers the actual happy paths:

- local development
- direct gateway TLS
- NATS TLS

For declarative NixOS wiring, use [tls-nixos-integration.md](tls-nixos-integration.md).

## The Short Version

### Local development

```bash
xtask doctor --fix
```

That generates development certs in `.sinex/tls/` and `xtask` preflight wires the gateway
TLS env vars automatically when they are missing.

### Gateway

Gateway TCP always needs:

- `SINEX_GATEWAY_TLS_CERT`
- `SINEX_GATEWAY_TLS_KEY`

Gateway non-loopback binds also need:

- `SINEX_GATEWAY_TLS_CLIENT_CA`

### NATS

NATS TLS needs:

- `SINEX_NATS_URL=tls://...`
- optionally `SINEX_NATS_REQUIRE_TLS=1` to reject accidental plaintext fallback
- CA and client credentials only if your deployment requires them

## Development Certificates

Generated dev cert layout:

```text
.sinex/tls/
├── ca.pem
├── ca-key.pem
├── server.pem
├── server-key.pem
├── client.pem
└── client-key.pem
```

Regenerate them with:

```bash
xtask reset --yes --tls
```

## Gateway Examples

### Loopback-only development

```bash
export SINEX_GATEWAY_TLS_CERT=.sinex/tls/server.pem
export SINEX_GATEWAY_TLS_KEY=.sinex/tls/server-key.pem
export SINEX_GATEWAY_TCP_LISTEN=127.0.0.1:9999
```

### Remote / non-loopback bind

```bash
export SINEX_GATEWAY_TLS_CERT=/run/secrets/gateway.pem
export SINEX_GATEWAY_TLS_KEY=/run/secrets/gateway-key.pem
export SINEX_GATEWAY_TLS_CLIENT_CA=/run/secrets/clients-ca.pem
export SINEX_GATEWAY_TCP_LISTEN=0.0.0.0:9999
```

## NATS Examples

### Server verification only

```bash
export SINEX_NATS_URL=tls://nats.example.net:4222
export SINEX_NATS_REQUIRE_TLS=1
export SINEX_NATS_CA_CERT=/run/secrets/nats-ca.pem
```

### Mutual TLS

```bash
export SINEX_NATS_URL=tls://nats.example.net:4222
export SINEX_NATS_REQUIRE_TLS=1
export SINEX_NATS_CA_CERT=/run/secrets/nats-ca.pem
export SINEX_NATS_CLIENT_CERT=/run/secrets/nats-client.pem
export SINEX_NATS_CLIENT_KEY=/run/secrets/nats-client-key.pem
```

## Troubleshooting

### Gateway says cert/key are required

Set `SINEX_GATEWAY_TLS_CERT` and `SINEX_GATEWAY_TLS_KEY`, or run:

```bash
xtask doctor --fix
```

### Gateway says client CA is required

You are binding beyond loopback, or forcing client TLS explicitly. Supply:

```bash
export SINEX_GATEWAY_TLS_CLIENT_CA=/path/to/ca.pem
```

### NATS TLS should be on but plaintext still works

Set:

```bash
export SINEX_NATS_REQUIRE_TLS=1
```

This turns the URL scheme into an enforced policy instead of a suggestion.

### Handshake debugging

```bash
openssl s_client -connect localhost:9999 -CAfile .sinex/tls/ca.pem
```

```bash
nats server check -s tls://localhost:4222 --tlsca .sinex/tls/ca.pem
```

## Security Notes

- Do not commit private keys.
- Use real certificates outside local development.
- Prefer explicit TLS enforcement over relying on scheme alone.
- Treat loopback and non-loopback gateway binds differently; remote binds are a stronger trust boundary.
