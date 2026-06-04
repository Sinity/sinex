# Gateway Coordination Architecture

The Sinex gateway implements comprehensive coordination features for zero-downtime upgrades and multi-instance deployments.

## Overview

The gateway maintains several types of state that must be coordinated during hot reload and across multiple instances:

1. **Active Connections** - In-flight HTTP/TLS requests
2. **Rate Limiting** - Per-token request quotas
3. **Metrics** - Request counters, latencies, error rates
4. **Replay Control** - NATS-based control plane state

## Graceful Shutdown with Connection Draining

### Problem

During hot reload or shutdown, the gateway must:

- Stop accepting new connections
- Wait for in-flight requests to complete
- Not kill active requests mid-processing

### Solution

**Connection Tracking** using atomic counters with RAII guards:

```rust
struct ConnectionGuard {
    counter: Arc<AtomicUsize>,
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::Relaxed);
    }
}
```

**Drain Loop** before shutdown:

```rust
// Stop accepting new connections
break; // Exit accept loop

// Wait for active connections to drain (up to 30s)
loop {
    let active = active_connections.load(Ordering::Relaxed);
    if active == 0 { break; }
    if timeout { warn!("Force exit"); break; }
    sleep(100ms);
}
```

**Benefits:**

- No killed requests during upgrades
- Configurable drain timeout (30s default)
- Falls back to force kill if timeout exceeded

## Distributed Rate Limiting via NATS KV

### Problem

In-memory rate limiting causes issues during hot reload:

- **Quota reset attack**: Client exhausts 90/100 quota → triggers reload → gets fresh 0/100 quota
- **Multi-instance inconsistency**: Each gateway has independent quotas
- **State loss**: All rate limit history lost on restart

### Solution

**Distributed Rate Limiter** using NATS KV for shared state:

```rust
pub struct DistributedRateLimiter {
    kv: async_nats::jetstream::kv::Store,
    config: DistributedRateLimitConfig,
}

impl DistributedRateLimiter {
    pub async fn check_and_increment(&self, token: &str) -> bool {
        let key = format!("token:{}", token);

        // Atomic read-increment-write
        let count = kv.get(&key).await?.unwrap_or(0);
        if count >= quota { return false; }

        kv.put(&key, (count + 1).to_string()).await?;
        true
    }
}
```

**KV Bucket Configuration:**

```rust
KvConfig {
    bucket: "sinex_api_rate_limits",
    max_age: Duration::from_secs(window_seconds * 2), // Auto-cleanup
    ..Default::default()
}
```

**Fallback Strategy:**

The gateway automatically falls back to in-memory rate limiting when:

- NATS is not available
- JetStream KV bucket creation fails
- NATS connectivity is lost

```rust
let (rate_limiter, cleanup_task) = match services.nats_client() {
    Some(nats) => {
        match DistributedRateLimiter::new(jetstream, config).await {
            Ok(limiter) => {
                info!("Using distributed rate limiting (NATS KV)");
                (RateLimiter::Distributed(Arc::new(limiter)), None)
            }
            Err(e) => {
                warn!("Falling back to in-memory rate limiting");
                let in_memory = Arc::new(TokenRateLimiter::from_env());
                (RateLimiter::InMemory(in_memory), Some(cleanup_task))
            }
        }
    }
    None => {
        info!("NATS not available - using in-memory rate limiting");
        // ...
    }
};
```

**Benefits:**

- ✅ Shared quota across all gateway instances
- ✅ State survives hot reload / rolling upgrades
- ✅ No quota reset bypass attacks
- ✅ Fails fast when NATS is absent at startup; live KV issues fall back in-process

**Environment Variables:**

- `SINEX_RPC_RATE_LIMIT_ENABLED` - Enable/disable rate limiting (default: true)
- `SINEX_RPC_RATE_LIMIT_PER_MINUTE` - Requests per minute per token (default: 6000)
- `SINEX_RPC_RATE_LIMIT_WINDOW_SECS` - Window duration in seconds (default: 60)

## TCP Listener Binding

### Current Behavior

The gateway binds its TCP listener through `tokio::net::TcpSocket` with
`SO_REUSEADDR` enabled before `bind`/`listen`:

```rust
let socket = TcpSocket::new_v4()?;
socket.set_reuseaddr(true)?;
socket.bind(addr)?;
let listener = socket.listen(128)?;
```

`SO_REUSEPORT` is not currently enabled. A replacement gateway process cannot
bind the same address while the old process still owns it. Hot reload therefore
needs an external handoff surface such as socket activation, a load balancer, or
an orchestrator step that drains/stops the old process before the replacement
binds.

## Integration with Hot Reload Orchestrator

The orchestrator in xtask now uses handoff-based restart:

```rust
async fn restart(&mut self) -> Result<()> {
    // 1. Build new binary
    let binary_path = self.build().await?;

    // 2. Start new instance WHILE old still running
    let new_child = self.spawn_new_instance(&binary_path).await?;

    // 3. Wait for initialization (NATS connect, listener bind)
    sleep(3s);

    // 4. Wait for old to exit gracefully (connection drain)
    if let Some(mut old_child) = self.child.take() {
        select! {
            _ = old_child.wait() => info!("Graceful exit"),
            _ = sleep(10s) => {
                warn!("Timeout, force kill");
                old_child.kill();
            }
        }
    }

    // 5. New instance now active
    self.child = Some(new_child);
    Ok(())
}
```

**Version Comparison for Leadership:**

Updated `NodeVersion::Ord` to include build metadata:

```rust
impl Ord for NodeVersion {
    fn cmp(&self, other: &Self) -> Ordering {
        // Primary: semver
        match self.version.cmp(&other.version) {
            Ordering::Equal => {
                // Secondary: dirty flag
                match (self.is_dirty, other.is_dirty) {
                    (false, true) => Ordering::Greater,
                    (true, false) => Ordering::Less,
                    _ => {
                        // Tertiary: commit count
                        match self.commit_count.cmp(&other.commit_count) {
                            Ordering::Equal => {
                                // Final: build timestamp (enables same-semver hot reload)
                                self.build_timestamp.cmp(&other.build_timestamp)
                            }
                            other => other,
                        }
                    }
                }
            }
            other => other,
        }
    }
}
```

This ensures newer builds win leadership even with same semver (critical for dev hot reload).

## State Coordination Summary

| State Type | Storage | Shared Across Instances | Survives Restart | Notes |
|------------|---------|------------------------|------------------|-------|
| **Rate Limits** | NATS KV | ✅ Yes | ✅ Yes | Falls back to in-memory only for live KV init failures after startup |
| **Active Connections** | In-memory counter | ❌ No | ❌ No | Tracked for graceful drain only |
| **Metrics** | In-memory counters | ❌ No | ❌ No | Emitted to NATS before shutdown |
| **Replay Control** | NATS KV | ✅ Yes | ✅ Yes | Already shared, no changes needed |

## Production Deployment Scenarios

### Single Instance with Hot Reload

```
Developer: xtask dev run gateway
    ↓
File change detected → Build new binary
    ↓
Drain/stop old instance before replacement binds
    ↓
Old instance: drains connections (30s timeout)
    ↓
New instance: takes all traffic
```

**State Continuity:**

- Rate limits: Preserved via NATS KV
- Active requests: Complete gracefully
- Metrics: Final snapshot emitted to NATS

### Multi-Instance Production Deployment

```
Load Balancer
    ↓
    ├── Gateway Instance 1 (port 9999)
    ├── Gateway Instance 2 (port 9999)
    └── Gateway Instance 3 (port 9999)
         ↓
    All share NATS KV for rate limits
```

**Rolling Upgrade:**

1. Deploy new binary to Instance 1
2. New process binds after the old process releases the listener, or via an external socket handoff
3. Old process drains and exits
4. Repeat for Instance 2, 3

**Consistency:**

- Rate limits: Shared across all instances via NATS KV
- Each instance tracks its own connections
- Metrics aggregated in NATS

## Enhancements

### Near-term (single-instance, actionable)

1. **Active Health Checks** — ✅ **Implemented** (`service_container.rs`):
   `ServiceContainer::probe_nats_active()` sends a PING/PONG flush to the broker
   with a 500ms timeout, catching stale connections that still report `Connected`
   in-process. `health_report()` aggregates DB, NATS, and replay-control status
   into a structured JSON response. The `/health` endpoint now returns that report
   rather than a plain 503/200 string; HTTP 200 now requires DB, NATS, and replay
   control to be live, while `status`, `healthy`, `serving`, and
   `degradation_reasons` distinguish full readiness from NATS / replay-control outages.

2. **Rate Limit Synchronization Batching** — ✅ **Implemented** (`distributed_rate_limit.rs`):
   Uses a local token reservation system (`DashMap`) to batch KV operations.
   Instead of incrementing NATS KV per-request, instances reserve batches of 50
   tokens via optimistic concurrency control (CAS loop). This reduces KV write
   traffic by ~50x under load while maintaining strict global limits (instances
   never reserve more than the remaining global capacity).

### Speculative (require multi-instance deployment first)

- **Sticky Sessions** — Route subsequent requests from the same client to the
  same gateway instance to improve cache locality.
- **Connection Prewarming** — New gateway instance announces itself to the pool
  and warms its connection before accepting queries.
- **Metrics Aggregation** — Combine per-instance metrics into a unified view via
  NATS KV or a shared aggregation endpoint.

## Configuration Reference

**Environment Variables:**

```bash
# Rate Limiting
SINEX_RPC_RATE_LIMIT_ENABLED=true
SINEX_RPC_RATE_LIMIT_PER_MINUTE=6000
SINEX_RPC_RATE_LIMIT_WINDOW_SECS=60

# TLS Binding
SINEX_API_TCP_LISTEN=127.0.0.1:9999
SINEX_API_TLS_CERT=/path/to/cert.pem
SINEX_API_TLS_KEY=/path/to/key.pem
SINEX_API_TLS_CLIENT_CA=/path/to/ca.pem  # Optional for mTLS

# NATS
SINEX_NATS_URL=nats://localhost:4222
```

**Graceful Shutdown:**

- Connection drain timeout: 30 seconds (hardcoded)
- Background task shutdown timeout: 30 seconds (hardcoded)
- Drain check interval: 100ms (hardcoded)

## References

- Hot Reload Orchestrator: `xtask/src/devtools/orchestrator.rs`
- Distributed Rate Limiter: `crate/sinexd/src/api/distributed_rate_limit.rs`
- Connection Tracking: `crate/sinexd/src/api/rpc_server.rs`
- Version Comparison: `crate/sinexd/src/node_sdk/version.rs`
- Node Coordination: `crate/sinexd/src/node_sdk/coordination.rs`
