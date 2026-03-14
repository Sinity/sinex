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
| `SINEX_NATS_TOKEN_FILE` | File containing the NATS auth token |
| `SINEX_NATS_CREDS_FILE` | NATS credentials file (JWT + seed) |
| `SINEX_NATS_NKEY_SEED_FILE` | File containing the NATS NKey seed |
| `SINEX_NATS_REQUIRE_TLS` | Enforce TLS validation at startup |

On NixOS, transport wiring should prefer typed module options over manual env assembly:

- `services.sinex.nodes.nats.servers`
- `services.sinex.nodes.nats.tls.*`
- `services.sinex.nodes.nats.auth.*`

## Authentication Modes

Production deployments should use one explicit NATS credential path per service:

- token auth via `SINEX_NATS_TOKEN_FILE`
- credentials-file auth via `SINEX_NATS_CREDS_FILE`
- NKey auth via `SINEX_NATS_NKEY_SEED_FILE`
- mTLS-style client identity via `SINEX_NATS_CLIENT_CERT` and `SINEX_NATS_CLIENT_KEY`

Configure exactly one NATS auth mode at a time.

## Role Separation

The still-relevant operational model for NATS authorization is:

- **Ingestors** should publish only the source-material and raw-event subjects they need
- **Automata / processors** should subscribe and publish only the event subjects they transform
- **Gateway / admin surfaces** may require broader access for management APIs

The exact subject patterns are deployment-specific, but the principle should stay the same:
grant each component family the narrowest NATS permissions that let it do its job.

## See Also

- System-wide security model: `docs/current/security.md`
- Node transport patterns: `crate/lib/sinex-node-sdk/docs/`
