# sinexd Event Engine Architecture

## Service Role

`sinexd::event_engine` is the **high-speed ingestion engine** for Sinex. It handles:

- High-throughput, low-complexity stream ingestion
- Millions of events from trusted internal sources
- Write-optimized database persistence

## Why Event Engine and API Stay Separated

The old `sinex-ingestd` and `sinex-gateway` binaries have been folded into
`sinexd`, but the module boundary remains load-bearing.

| Dimension | Event engine module | API module |
|-----------|---------|---------|
| **Workload** | High-throughput, simple writes | Low-throughput, complex queries |
| **Protocol** | NATS JetStream (internal) | JSON-RPC/HTTP/S, native messaging |
| **Security** | Internal trust zone | Zero-trust boundary (hardened shell) |
| **Optimization** | Write throughput | Query logic and security |
| **Failure Impact** | Data pauses, queries still work | Queries fail, data collection continues |

## Internal Trust Model

The event engine operates in an internal trust zone:

- Clients (nodes) are trusted parts of the system
- Primary concern: performance and DoS prevention from buggy nodes
- No external authentication overhead

## Performance Architecture

Event processing pipeline:
1. **NATS Consumer** - Pull events from JetStream
2. **Batch Processor** - Accumulate events for efficient writes
3. **Database Writer** - Bulk insert with batching
4. **Ack Manager** - Acknowledge processed events

A long-running analytical query (via gateway) must not block ingestion of real-time events.

## Failure Isolation

If the event engine fails:
- Data collection pauses
- Users can still query all existing data via the API
- System remains read-only but useful

See also:
- [pipeline-design.md](./pipeline-design.md) - Detailed pipeline architecture
- [config.md](./config.md) - Configuration options
- [transport_security.md](./transport_security.md) - NATS TLS configuration
