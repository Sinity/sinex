# Core Architecture Diagrams

## Leader/Standby Coordination

### PostgreSQL Advisory Locks Architecture

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                      LEADER/STANDBY COORDINATION                              │
│              PostgreSQL Advisory Locks + WorkTracker                          │
└──────────────────────────────────────────────────────────────────────────────┘

COORDINATION INFRASTRUCTURE
═══════════════════════════════════════════════════════════════════════════════

  ┌─────────────────────────────────────────────────────────────────────┐
  │                     PostgreSQL Advisory Locks                        │
  │                                                                       │
  │  ┌───────────────────────────────────────────────────────────────┐  │
  │  │                     Lock Registry                              │  │
  │  │                                                                 │  │
  │  │  Lock ID: hash("fs-watcher-01")                                │  │
  │  │  ┌──────────────────┐                                          │  │
  │  │  │ Owner: conn_425  │  ← Instance A connection                 │  │
  │  │  │ Acquired: T0     │                                          │  │
  │  │  └──────────────────┘                                          │  │
  │  │                                                                 │  │
  │  │  Lock ID: hash("terminal-node-01")                             │  │
  │  │  ┌──────────────────┐                                          │  │
  │  │  │ Owner: conn_531  │  ← Instance B connection                 │  │
  │  │  │ Acquired: T1     │                                          │  │
  │  │  └──────────────────┘                                          │  │
  │  │                                                                 │  │
  │  │  Guarantees:                                                    │  │
  │  │  - In-memory (fast)                                             │  │
  │  │  - Per-connection (auto-release on disconnect)                  │  │
  │  │  - Atomic acquire (pg_try_advisory_lock)                        │  │
  │  │  - No deadlock detection needed (non-blocking)                  │  │
  │  └───────────────────────────────────────────────────────────────┘  │
  └─────────────────────────────────────────────────────────────────────┘
```

### Instance State Machine

```
  ┌──────────┐
  │ Startup  │  Initial state on launch
  └────┬─────┘
       │
       │ Check for existing lock
       ↓
  ┌─────────────────┐
  │    Standby      │  Wait for leader to fail/release
  │                 │
  │ Actions:        │
  │ - Poll lock     │  Every 5s: SELECT pg_try_advisory_lock(id)
  │ - Monitor DB    │  Check for failure signals
  │ - Idle          │  No event processing
  └────┬─────┬──────┘
       │     ↑
       │     │ Lock acquisition failed
       │     │
       │ Lock acquired!
       ↓     │
  ┌─────────┴───────┐
  │ Transitioning   │  Brief state during handoff
  │                 │
  │ Actions:        │
  │ - Verify lock   │  Re-check lock ownership
  │ - Initialize    │  Set up consumers, load checkpoints
  └────┬────────────┘
       │
       │ Initialization complete
       ↓
  ┌──────────────────────────────────────────────────────────┐
  │                      Leader                               │
  │                                                           │
  │  ┌───────────────────────────────────────────────────┐   │
  │  │            Active Event Processing                 │   │
  │  │                                                     │   │
  │  │  ┌──────────────────────────────────────────────┐ │   │
  │  │  │ NATS Consumers                                │ │   │
  │  │  │ - Batch fetch events                          │ │   │
  │  │  │ - Process & persist                           │ │   │
  │  │  │ - ACK messages                                │ │   │
  │  │  └──────────────────────────────────────────────┘ │   │
  │  │                                                     │   │
  │  │  ┌──────────────────────────────────────────────┐ │   │
  │  │  │ WorkTracker                                   │ │   │
  │  │  │ - in_flight_operations: AtomicUsize           │ │   │
  │  │  │ - shutdown_requested: CoordinationPrimitive   │ │   │
  │  │  └──────────────────────────────────────────────┘ │   │
  │  │                                                     │   │
  │  │  ┌──────────────────────────────────────────────┐ │   │
  │  │  │ Heartbeat Emitter                             │ │   │
  │  │  │ - Emit every 60s to journald                  │ │   │
  │  │  │ - Status: Healthy/Degraded/Failed             │ │   │
  │  │  └──────────────────────────────────────────────┘ │   │
  │  └───────────────────────────────────────────────────┘   │
  │                                                           │
  │  Failure Detection:                                       │
  │  - DB connection lost → Lock auto-released                │
  │  - Process crash → Lock auto-released                     │
  │  - Signal (SIGTERM) → Graceful shutdown initiated         │
  └────────────┬──────────────────────────────────────────────┘
               │
               │ Graceful shutdown requested
               ↓
  ┌──────────────────────────────────────────────────────────┐
  │                     Draining                              │
  │                                                           │
  │  ┌───────────────────────────────────────────────────┐   │
  │  │ Shutdown Protocol:                                 │   │
  │  │                                                     │   │
  │  │ 1. request_shutdown()                              │   │
  │  │    - Set shutdown_requested flag                   │   │
  │  │    - Stop accepting new work                       │   │
  │  │                                                     │   │
  │  │ 2. Wait for in_flight_operations → 0              │   │
  │  │    - Timeout: 30 seconds                           │   │
  │  │    - Poll every 100ms                              │   │
  │  │                                                     │   │
  │  │ 3. Checkpoint state                                │   │
  │  │    - Save to NATS KV                               │   │
  │  │    - Flush pending writes                          │   │
  │  │                                                     │   │
  │  │ 4. Release advisory lock                           │   │
  │  │    - pg_advisory_unlock(lock_id)                   │   │
  │  │                                                     │   │
  │  │ 5. Close DB connection                             │   │
  │  │    - pool.close().await                            │   │
  │  └───────────────────────────────────────────────────┘   │
  └──────────────────────────────────────────────────────────┘
               │
               ↓
         ┌──────────┐
         │ Shutdown │  Process exits cleanly
         └──────────┘
```

### Failover Sequence Diagrams

```
Normal Operation (2 Instances)
───────────────────────────────

  Instance A          PostgreSQL           Instance B
      │                   │                     │
      │  Startup          │                     │  Startup
      │                   │                     │
      ├──try_lock()──────>│                     │
      │<─────OK───────────┤                     │
      │                   │                     │
      │  LEADER mode      │                     │
      │  Process events   │                     ├──try_lock()──────>
      │                   │                     │<─────FAIL─────────┤
      │                   │                     │
      │                   │                     │  STANDBY mode
      │                   │                     │  Sleep 5s
      │                   │                     │
      │                   │                     │  (retry loop)
      │                   │                     ├──try_lock()──────>
      │                   │                     │<─────FAIL─────────┤

Leader Failure (Automatic Failover)
────────────────────────────────────

  Instance A          PostgreSQL           Instance B
      │                   │                     │
      │  LEADER           │                     │  STANDBY
      │                   │                     │
      │  ──X              │                     │  (polling)
      │  [Crash!]         │                     │
      │                   │                     │
      │                   │  Lock auto-released │
      │                   │  (connection closed)│
      │                   │                     │
      │                   │                     ├──try_lock()──────>
      │                   │                     │<─────OK───────────┤
      │                   │                     │
      │                   │                     │  LEADER mode
      │                   │                     │  Load checkpoint
      │                   │                     │  Resume processing

Graceful Upgrade (Zero-Downtime)
─────────────────────────────────

  Instance A (v1.0)   PostgreSQL           Instance B (v1.1)
      │                   │                     │
      │  LEADER           │                     │  [Deploy]
      │                   │                     │  Startup
      │                   │                     │
      │                   │                     ├──try_lock()──────>
      │                   │                     │<─────FAIL─────────┤
      │                   │                     │
      │  [SIGTERM]        │                     │  STANDBY
      │  Start draining   │                     │  (waiting)
      │                   │                     │
      │  Wait for work    │                     │
      │  to complete...   │                     │
      │  (30s timeout)    │                     │
      │                   │                     │
      │  Save checkpoint  │                     │
      │  Release lock ────┤                     │
      │                   │  Lock released      │
      │  Exit cleanly     │                     │
      │                   │                     ├──try_lock()──────>
      │                   │                     │<─────OK───────────┤
      │                   │                     │
      │                   │                     │  LEADER mode
      │                   │                     │  Load checkpoint
      │                   │                     │  Resume at last event
```

## Monitoring & Observability Flow

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                    MONITORING & OBSERVABILITY ARCHITECTURE                    │
│                           Self-Hosting via Events                             │
└──────────────────────────────────────────────────────────────────────────────┘

  ┌─────────────────────────────────────────────────────────────────────┐
  │                     fs-watcher (Example)                             │
  │                                                                       │
  │  Every 60 seconds:                                                    │
  │  ┌───────────────────────────────────────────────────────────────┐  │
  │  │  HeartbeatEmitter::emit()                                      │  │
  │  │                                                                 │  │
  │  │  1. Collect metrics:                                            │  │
  │  │     - events_processed (since last heartbeat)                   │  │
  │  │     - errors_count (since last heartbeat)                       │  │
  │  │     - memory_usage_mb (VmRSS from /proc/self/status)            │  │
  │  │     - cpu_usage_percent (getrusage delta)                       │  │
  │  │     - uptime_seconds                                            │  │
  │  │                                                                 │  │
  │  │  2. Determine status:                                           │  │
  │  │     errors > 50  → Status::Failed                               │  │
  │  │     errors > 10  → Status::Degraded                             │  │
  │  │     else         → Status::Healthy                              │  │
  │  │                                                                 │  │
  │  │  3. Serialize to JSON                                           │  │
  │  │                                                                 │  │
  │  │  4. println!("{}", json)  ← To stdout                           │  │
  │  └───────────────────────────────────────────────────────────────┘  │
  └─────────────────┬───────────────────────────────────────────────────┘
                    │
                    ↓ stdout
  ┌─────────────────────────────────────────────────────────────────────┐
  │                           systemd                                    │
  │                                                                       │
  │  Unit file: fs-watcher.service                                       │
  │  StandardOutput=journal                                              │
  │  StandardError=journal                                               │
  │                                                                       │
  │  All stdout → journald automatically                                 │
  └─────────────────┬───────────────────────────────────────────────────┘
                    │
                    ↓ journald logs
  ┌─────────────────────────────────────────────────────────────────────┐
  │                      journald Storage                                │
  │                                                                       │
  │  Logs stored: /var/log/journal/{machine-id}/                        │
  │  Format: Binary, indexed by timestamp, unit, priority               │
  └─────────────────┬───────────────────────────────────────────────────┘
                    │
                    ↓ sinex-system-ingestor journal watcher reads
  ┌─────────────────────────────────────────────────────────────────────┐
  │         sinex-system-ingestor journal watcher (Event Capture)        │
  │                                                                       │
  │  1. journalctl --follow --output=json --unit=*.service              │
  │  2. Filter: MESSAGE matches heartbeat pattern                        │
  │  3. Parse JSON from MESSAGE field                                    │
  │  4. Emit as Sinex event (source: "journald", type: "heartbeat")     │
  └─────────────────┬───────────────────────────────────────────────────┘
                    │
                    ↓ NATS events.raw.system.heartbeat
  ┌─────────────────────────────────────────────────────────────────────┐
  │                        sinex-ingestd                                 │
  │                                                                       │
  │  Standard ingestion path → core.events                               │
  └─────────────────┬───────────────────────────────────────────────────┘
                    │
                    ↓ events.confirmations.{event_id}
  ┌─────────────────────────────────────────────────────────────────────┐
  │                   health-aggregator Automaton                        │
  │                                                                       │
  │  1. Subscribe to: events.confirmations.>                             │
  │  2. Aggregate metrics per service                                    │
  │  3. Detect anomalies (status changes, missing heartbeats)            │
  │  4. Store aggregated metrics in core.service_health                  │
  └─────────────────┬───────────────────────────────────────────────────┘
                    │
                    ↓ Query via gateway
  ┌─────────────────────────────────────────────────────────────────────┐
  │                      sinex-gateway (RPC)                             │
  │                                                                       │
  │  Endpoints:                                                           │
  │  - GET /health/services              (all services)                  │
  │  - GET /health/services/{name}       (specific service)              │
  │  - GET /health/history/{name}        (time-series)                   │
  │  - GET /health/alerts                (active alerts)                 │
  └─────────────────┬───────────────────────────────────────────────────┘
                    │
                    ↓ HTTP/JSON
  ┌─────────────────────────────────────────────────────────────────────┐
  │                    Dashboard / Monitoring UI                         │
  │                                                                       │
  │  Real-time view:                                                     │
  │  ┌──────────────────────────────────────────────────────────────┐   │
  │  │ Service          Status      Uptime   Events/s   Mem   CPU   │   │
  │  │ fs-watcher       ✅ Healthy  2d 4h    23.5       45MB  2.3%  │   │
  │  │ terminal-node    ✅ Healthy  2d 4h    8.2        32MB  1.1%  │   │
  │  │ desktop-node     ⚠️  Degraded 1d 2h    5.1        78MB  3.8%  │   │
  │  │ ingestd          ✅ Healthy  2d 4h    31.8       125MB 8.2%  │   │
  │  └──────────────────────────────────────────────────────────────┘   │
  └───────────────────────────────────────────────────────────────────────┘
```

### Benefits of Journald-First Approach

```
✅ Zero Configuration
   - Works out-of-box with systemd
   - No Prometheus, Grafana, Datadog setup needed

✅ Unified Storage
   - Heartbeats are events, stored in core.events
   - Query with same API as application events
   - Time-travel debugging

✅ Self-Hosting
   - No external monitoring dependencies
   - System monitors itself
   - Observability built into event model

✅ Historical Analysis
   - Full heartbeat history in database
   - SQL queries for complex analysis
   - Replay monitoring data

✅ Integration
   - Heartbeats flow through same pipeline as app events
   - Can correlate service health with event processing
   - Single source of truth
```

## See Also

- Patterns: `docs/current/architecture/distributed-patterns.md`
- Observability: `docs/current/architecture/observability.md`
- Type system: `docs/current/architecture/type-system-patterns.md`
