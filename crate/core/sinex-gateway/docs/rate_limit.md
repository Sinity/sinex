# Rate Limiting

Per-token rate limiting strategy to protect the gateway from Denial-of-Service attacks and rogue clients.

## Strategy

The gateway implements a **per-token isolation** strategy using the Leaky Bucket algorithm (via the `governor` crate). This ensures that a single compromised or buggy token cannot monopolize system resources or degrade performance for other clients.

## Algorithm & Configuration

- **Algorithm**: Token bucket with linear refill.
- **Default Limit**: 100 requests per second per token.
- **Burst Capacity**: 50 requests (allows short spikes in activity).
- **State**: In-memory (per gateway instance).

## Security Analysis

### Attack Resistance
- **Single Token Spam**: **High**. A single token is strictly capped at 100 req/s.
- **Connection Exhaustion**: **High**. Rate limiting works in tandem with the connection concurrency limit (100) and request timeouts (30s).
- **Distributed Attack**: **Medium**. If an attacker compromises `N` tokens, they can achieve `N * 100` req/s total throughput.

### Performance Overhead
- **CPU**: ~200ns per check (atomic operations). Negligible impact on request latency.
- **Memory**: ~500 bytes per active token. State is automatically cleaned up after 1 hour of idleness to prevent memory leaks.

## Observability

Rate limit events are logged with the `token_prefix` (first 8 characters) to allow operators to identify the source of the traffic. Metrics track:
- `gateway_requests_rate_limited_total`
- `gateway_active_rate_limiters`
