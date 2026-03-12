# TLS Setup Guide

This guide covers TLS configuration for Sinex components including the gateway, NATS, and client authentication.

## Quick Start (Development)

Generate self-signed certificates for local development. TLS certificates are generated automatically by preflight when needed:

```bash
xtask doctor --fix
```

This creates a complete certificate hierarchy in `.sinex/tls/`:

```
.sinex/tls/
├── ca.pem           # Certificate Authority
├── ca-key.pem       # CA private key (keep secure!)
├── server.pem       # Server certificate
├── server-key.pem   # Server private key
├── client.pem       # Client certificate (for mTLS)
└── client-key.pem   # Client private key
```

To regenerate certificates:

```bash
xtask reset --yes --tls
```

## Certificate Generation

TLS certificates are generated automatically by the preflight system. To regenerate them:

```bash
# Regenerate certificates (stored in .sinex/tls/)
xtask reset --yes --tls

# Or via doctor with auto-fix
xtask doctor --fix
```

The generated certificates are valid for 365 days by default.

## Gateway Configuration

### Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| `SINEX_GATEWAY_TLS_CERT` | Path to server certificate | Yes (for TCP binding) |
| `SINEX_GATEWAY_TLS_KEY` | Path to server private key | Yes (for TCP binding) |
| `SINEX_GATEWAY_TLS_CLIENT_CA` | Path to CA for client verification | Required for mTLS |
| `SINEX_GATEWAY_REQUIRE_CLIENT_TLS` | Force mTLS even on loopback | No (default: false) |

### Basic TLS (Server Authentication)

```bash
export SINEX_GATEWAY_TLS_CERT=.sinex/tls/server.pem
export SINEX_GATEWAY_TLS_KEY=.sinex/tls/server-key.pem
export SINEX_GATEWAY_TCP_LISTEN=127.0.0.1:9999
```

### Mutual TLS (Client Authentication)

```bash
export SINEX_GATEWAY_TLS_CERT=.sinex/tls/server.pem
export SINEX_GATEWAY_TLS_KEY=.sinex/tls/server-key.pem
export SINEX_GATEWAY_TLS_CLIENT_CA=.sinex/tls/ca.pem
export SINEX_GATEWAY_TCP_LISTEN=0.0.0.0:9999
```

Note: mTLS is **required** when binding to non-loopback addresses.

## NATS Configuration

### Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| `SINEX_NATS_URL` | NATS server URL (use `tls://` for TLS) | Yes |
| `SINEX_NATS_REQUIRE_TLS` | Enforce TLS connection | Recommended |
| `SINEX_NATS_CA_CERT` | Path to CA certificate | For server verification |
| `SINEX_NATS_CLIENT_CERT` | Path to client certificate | For mTLS |
| `SINEX_NATS_CLIENT_KEY` | Path to client private key | For mTLS |

### NATS TLS Configuration

```bash
# Basic TLS (server verification only)
export SINEX_NATS_URL=tls://localhost:4222
export SINEX_NATS_REQUIRE_TLS=1
export SINEX_NATS_CA_CERT=.sinex/tls/ca.pem

# Full mTLS
export SINEX_NATS_URL=tls://localhost:4222
export SINEX_NATS_REQUIRE_TLS=1
export SINEX_NATS_CA_CERT=.sinex/tls/ca.pem
export SINEX_NATS_CLIENT_CERT=.sinex/tls/client.pem
export SINEX_NATS_CLIENT_KEY=.sinex/tls/client-key.pem
```

## Production Setup

### Using Let's Encrypt

For production deployments with Let's Encrypt:

1. **Obtain certificates** using certbot or acme.sh
2. **Point environment variables** to the certificate files:
   ```bash
   export SINEX_GATEWAY_TLS_CERT=/etc/letsencrypt/live/example.com/fullchain.pem
   export SINEX_GATEWAY_TLS_KEY=/etc/letsencrypt/live/example.com/privkey.pem
   ```
3. **Set up automatic renewal** with a post-hook to restart services

### Using Your Own CA

For internal deployments with a private CA:

1. **Generate a CA** (or use an existing organizational CA)
2. **Issue server certificates** for each service
3. **Distribute CA certificate** to all clients for verification
4. **Enable mTLS** for internal service-to-service communication

### Behind a Reverse Proxy

If running behind nginx, HAProxy, or a cloud load balancer:

1. **Configure TLS termination** at the proxy
2. **Bind gateway to loopback** (`127.0.0.1:9999`)
3. **Trust proxy headers** for client information

Example nginx configuration:

```nginx
server {
    listen 443 ssl;
    ssl_certificate /etc/ssl/server.pem;
    ssl_certificate_key /etc/ssl/server-key.pem;

    # Optional: client certificate verification
    ssl_client_certificate /etc/ssl/ca.pem;
    ssl_verify_client optional;

    location / {
        proxy_pass http://127.0.0.1:9999;
        proxy_set_header X-Client-Cert $ssl_client_cert;
    }
}
```

## Troubleshooting

### Certificate Expired

```bash
# Regenerate certificates
xtask reset --yes --tls
```

### Permission Denied on Key Files

Private key files should have mode 0600:

```bash
chmod 600 .sinex/tls/*-key.pem
```

### TLS Handshake Failures

Common causes:
- Certificate not trusted (add CA to trust store or use `--ca` flag)
- SAN mismatch (ensure hostname is in Subject Alternative Names)
- Expired certificate (regenerate with `xtask reset --yes --tls`)

Debug with OpenSSL:

```bash
openssl s_client -connect localhost:9999 -CAfile .sinex/tls/ca.pem
```

### NATS TLS Connection Issues

```bash
# Test NATS TLS connection
nats server check -s tls://localhost:4222 --tlsca .sinex/tls/ca.pem

# With client certificate
nats server check -s tls://localhost:4222 \
    --tlsca .sinex/tls/ca.pem \
    --tlscert .sinex/tls/client.pem \
    --tlskey .sinex/tls/client-key.pem
```

## CI/CD Integration

### GitHub Actions Example

```yaml
jobs:
  test:
    steps:
      - uses: actions/checkout@v4

      - name: Verify TLS configuration
        run: xtask doctor --json

      - name: Run tests
        run: xtask test
```

### NixOS Integration

TLS certificates can be managed via agenix or similar secret management:

```nix
{
  services.sinex.gateway = {
    enable = true;
    tls = {
      certFile = "/run/secrets/sinex-gateway-cert";
      keyFile = "/run/secrets/sinex-gateway-key";
      clientCaFile = "/run/secrets/sinex-ca";  # For mTLS
    };
  };
}
```

## Security Considerations

1. **Never commit private keys** - Add `.sinex/tls/` to `.gitignore`
2. **Rotate certificates regularly** - Before expiration
3. **Use strong key sizes** - Generated certificates use 2048-bit RSA
4. **Restrict key file permissions** - Mode 0600 for private keys
5. **Enable mTLS for production** - Especially for exposed services
6. **Monitor certificate expiration** - Use `xtask doctor` in CI
