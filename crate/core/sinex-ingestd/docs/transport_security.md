# Ingestd Transport Security

NATS JetStream security requirements for the ingestion hub.

## Trust Boundaries

NATS JetStream is the central event bus (data plane).

- **Production/CI**: **MUST** be encrypted (`tls://`). Plaintext `nats://` is forbidden in non-dev environments.
- **Trusted nodes**: Entities with valid cryptographic identity (TLS Client Cert or NATS Creds) authorized to publish/subscribe.
- **Ad-hoc devices**: Temporary devices must enroll (exchange a bootstrap token for a cert/cred) before joining the mesh. Anonymous/open nodes are not supported.

## Enforcement

- **Config**: `require_tls: boolean` controls TLS enforcement.
- **Validation**: If `require_tls` is true, startup fails if `nats_url` scheme is not `tls://`.

## Environment Variables

| Variable | Purpose |
|----------|---------|
| `SINEX_NATS_URL` | NATS server URL (use `tls://` in production) |
| `SINEX_NATS_TOKEN` | NATS authentication token |
| `SINEX_NATS_REQUIRE_TLS` | Enforce TLS validation at startup |

## See Also

- System-wide security architecture: `docs/current/security-architecture.md`
- Node transport patterns: `crate/lib/sinex-node-sdk/docs/`
