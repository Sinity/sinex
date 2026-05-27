# Gateway Architecture

## Service Role

sinex-gateway is the **hardened external interface** for Sinex. It handles:

- Low-throughput, high-complexity request/response workloads
- External client communication (CLI tools, browser extensions)
- Zero-trust security boundary

## Why Gateway is Separate from Ingestd

| Dimension | Gateway | Ingestd |
|-----------|---------|---------|
| **Workload** | Low-throughput, complex queries | High-throughput, simple writes |
| **Protocol** | JSON-RPC/HTTP/S, native messaging | NATS JetStream (internal) |
| **Security** | Zero-trust boundary (hardened shell) | Internal trust zone |
| **Optimization** | Query logic and security | Write throughput |
| **Failure Impact** | Queries fail, data collection continues | Data pauses, queries still work |

## Security Posture

Gateway is the *only* component exposed to potentially untrusted clients:

- **Authentication**: Bearer token verification
- **Authorization**: Request-level access control
- **Rate Limiting**: Protect against abuse
- **Input Validation**: Strict schema enforcement

Forcing high-volume ingestion through these security layers would create massive bottlenecks.

## Failure Isolation

If gateway fails:
- Users cannot query or interact
- Data collection continues uninterrupted via ingestd
- System degrades gracefully

## Protocol Stack

1. **JSON-RPC over HTTP/S** - CLI and external tools
2. **Length-prefixed JSON** - Browser native messaging
3. **WebSocket** (planned) - Real-time subscriptions

See also:
- [rpc_server.md](./rpc_server.md) - RPC implementation details
- [native_messaging.md](./native_messaging.md) - Browser extension protocol
- [transport_security.md](./transport_security.md) - TLS configuration
