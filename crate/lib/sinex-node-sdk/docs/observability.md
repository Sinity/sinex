# Observability Patterns

Sinex's self-hosting monitoring and checkpoint system.

## Journald-First Monitoring

**Architecture:**
```
Node emits JSON to stdout
      ↓
systemd captures in journald
      ↓
sinex-system-ingestor journal watchers emit events
      ↓
health automata and query views process them
      ↓
System health dashboard (queryable events)
```

**Benefits:**
- No separate monitoring infrastructure needed
- Heartbeats are regular events (fully queryable)
- Historical health data in event database
- Works out-of-box with systemd
- Structured JSON for parsing

### Heartbeat Structure

```rust
pub struct HeartbeatMetrics {
    pub service_name: String,
    pub status: ProcessStatus,        // Healthy | Degraded | Failed
    pub events_processed: u64,
    pub uptime_seconds: u64,
    pub memory_usage_mb: u32,
    pub cpu_usage_percent: f32,
    pub errors_count: u32,
    pub last_error_message: Option<String>,
    pub version: String,
    pub git_hash: String,
    pub timestamp: String,
    pub metadata: Option<serde_json::Value>,
}
```

### Status Determination

```rust
fn determine_status(recent_errors: usize) -> ProcessStatus {
    if recent_errors > 50 {
        ProcessStatus::Failed
    } else if recent_errors > 10 {
        ProcessStatus::Degraded
    } else {
        ProcessStatus::Healthy
    }
}
```

### Resource Monitoring

**Memory (VmRSS from /proc/self/status):**
```rust
fn get_memory_usage_mb(&self) -> u32 {
    std::fs::read_to_string("/proc/self/status")?
        .lines()
        .find(|line| line.starts_with("VmRSS:"))
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|kb_str| kb_str.parse::<u32>().ok())
        .map(|kb| kb / 1024)
        .unwrap_or(0)
}
```

**CPU (getrusage with proper unsafe handling):**
```rust
fn read_process_cpu_seconds() -> Option<f64> {
    let mut usage = MaybeUninit::<libc::rusage>::uninit();
    let result = unsafe {
        libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr())
    };
    if result == 0 {
        let usage = unsafe { usage.assume_init() };
        let cpu = usage.ru_utime.tv_sec as f64 + usage.ru_utime.tv_usec as f64 / 1_000_000.0
                + usage.ru_stime.tv_sec as f64 + usage.ru_stime.tv_usec as f64 / 1_000_000.0;
        Some(cpu)
    } else {
        None
    }
}
```

**Why Exemplary:**
- Self-hosting monitoring (heartbeats are events)
- No external metrics system required
- Historical queryability via event database
- Safe unsafe code (proper MaybeUninit usage - one of only 2 unsafe blocks in entire codebase)

---

## Unified Checkpoint System

### Type-Safe Checkpoint Enum

```rust
pub enum Checkpoint {
    /// No checkpoint (initial state)
    None,

    /// Internal event ID (automata)
    Internal {
        event_id: Uuid,
        message_count: u64,
    },

    /// External position (ingestors)
    External {
        position: serde_json::Value,  // Flexible external state
    },

    /// Stream message ID
    Stream {
        message_id: String,
        event_id: Option<Uuid>,
    },

    /// Timestamp-based checkpoint
    Timestamp {
        timestamp: DateTime<Utc>,
    },
}
```

### Checkpoint State

```rust
pub struct CheckpointState {
    pub checkpoint: Checkpoint,
    pub processed_count: u64,
    pub last_activity: DateTime<Utc>,
    pub data: Option<serde_json::Value>,  // Node-specific state
    pub version: u32,                     // Schema evolution (currently v2)
}
```

### Storage (NATS KV)

- **Bucket:** `sinex_checkpoints`
- **Key format:** `<node_name>.<consumer_group>.<consumer_name>`
- Atomic per-key updates (last write wins)
- Denormalized `last_activity` in payload for staleness detection

### Schema Evolution

Checkpoint payloads are versioned and stored as `CheckpointState` values in NATS KV.
Current semantics:
- Single abstraction for all checkpoint types
- Type-safe variants prevent mixing
- Flexible `External` variant for custom state
- Atomic updates via NATS KV
- Built-in staleness tracking

## Derived-Node Latency / Throughput Gauges

`DerivedNodeAdapter` emits the following gauges through `SelfObserver` on
every dispatch (one per processed event in `process_one`, one per batch
in `process_event_batch`). Operators see them via `sinexctl automata` and
the `sinex.metrics.gauge` event stream:

| Gauge | Source | Meaning |
|---|---|---|
| `derived.event_lag_ms` | last sample | wall delta from `event.ts_orig` to dispatch |
| `derived.event_lag_p50_ms` | reservoir | median over the last `DEFAULT_LATENCY_RESERVOIR` samples (1024) |
| `derived.event_lag_p99_ms` | reservoir | p99 over the same reservoir |
| `derived.tick_runtime_ms` | last sample | wall time inside `node.process_derived` |
| `derived.tick_runtime_p99_ms` | reservoir | p99 of per-tick runtime |
| `derived.throughput_eps` | sliding 60s | events per second over the live window |

Implementation lives in
`crate/lib/sinex-node-sdk/src/derived_node/histograms.rs`. Reservoir size
and window length are exported as `DEFAULT_LATENCY_RESERVOIR` and
`THROUGHPUT_WINDOW`. Heavy-lane scenario coverage:

```bash
xtask test -p sinex-node-sdk \
    -E 'binary(derived_telemetry_heavy_lane)' --heavy
```

The first slice (#571) shipped the point-in-time gauges; the second
slice (#561) added the reservoir-based percentiles, the throughput
window, and the heavy-lane scenario test.
