# Rate Limiting

Per-token rate limiting strategy to protect the gateway from Denial-of-Service attacks and rogue clients.

## Strategy

The gateway implements **per-token isolation** with two runtime modes:

- **Distributed mode (preferred):** NATS KV-backed counters shared across gateway instances.
- **In-memory fallback:** `governor` token buckets when distributed mode cannot be initialized.

This ensures a single compromised token cannot monopolize gateway resources, and allows shared quota enforcement in multi-instance deployments.

## Algorithm & Configuration

- **Distributed defaults:** 6000 requests/minute per token, 60s window.
- **In-memory defaults:** 100 requests/second, burst 50, idle eviction 1 hour.
- **Startup behavior:** If NATS/KV setup fails, gateway falls back to in-memory mode.
- **Runtime distributed behavior:** KV read/update errors fail closed (request denied) to avoid accidental bypass.

## Security Analysis

### Attack Resistance
- **Single Token Spam**: **High**. A single token is strictly capped at 100 req/s.
- **Connection Exhaustion**: **High**. Rate limiting works in tandem with the connection concurrency limit (100) and request timeouts (30s).
- **Distributed Attack**: **Medium**. If an attacker compromises `N` tokens, they can achieve roughly `N * per-token-limit` throughput.

### Performance Overhead
- **CPU**: In-memory checks are very low overhead; distributed mode adds NATS KV round-trips when local reservations are exhausted.
- **Memory**: In-memory token state is evicted after idle timeout; distributed counters expire via KV `max_age`.

## Observability

Rate limit events are logged with `token_prefix` (first 8 characters) to identify source traffic. Metrics track:
- `gateway_requests_rate_limited_total`
- `gateway_active_rate_limiters`
