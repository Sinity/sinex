# Gateway Transport Security

TLS and authentication requirements for the Gateway control plane.

## Trust Boundaries

The Gateway exposes RPC endpoints for user interaction and system control.

- **Localhost**: **MUST** use TLS (`https://...`); trust the local CA via `SINEX_RPC_CA_CERT` when using self-signed certs.
- **Network Exposed**: **MUST** be encrypted. Any TCP binding to a non-loopback interface requires TLS + mTLS (`SINEX_GATEWAY_TLS_CLIENT_CA`).

## Authentication

- **Bearer Token**: Required for all connections (default). Set via `SINEX_RPC_TOKEN` or `SINEX_RPC_TOKEN_FILE`.
- **mTLS**: Optional high-security mode. If enabled, client certificates serve as strong identity and may bypass or augment token auth.

## Enforcement

- **TCP**: TLS is mandatory; the gateway refuses to start without `SINEX_GATEWAY_TLS_CERT` and `SINEX_GATEWAY_TLS_KEY`.

## Environment Variables

| Variable | Purpose |
|----------|---------|
| `SINEX_GATEWAY_TLS_CERT` | TLS certificate path (required) |
| `SINEX_GATEWAY_TLS_KEY` | TLS private key path (required) |
| `SINEX_GATEWAY_TLS_CLIENT_CA` | Client CA for mTLS (optional) |
| `SINEX_RPC_TOKEN` | Bearer token (direct value) |
| `SINEX_RPC_TOKEN_FILE` | Bearer token (file path) |

## See Also

- System-wide security architecture: `docs/current/security-architecture.md`
- Gateway overview: `docs/overview.md`
