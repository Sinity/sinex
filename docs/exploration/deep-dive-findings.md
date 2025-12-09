# Sinex Deep Dive Findings

This document captures findings from a comprehensive exploration of the sinex codebase, focusing on cross-cutting concerns, critical code paths, and NixOS deployment coherency.

---

## Table of Contents

1. [Cross-Cutting Concerns](#cross-cutting-concerns)
   - [Idempotency Patterns](#idempotency-patterns)
   - [Backpressure Mechanisms](#backpressure-mechanisms)
   - [Graceful Shutdown](#graceful-shutdown)
   - [Configuration Precedence](#configuration-precedence)
2. [Critical Path Analysis](#critical-path-analysis)
   - [Ingestion Hot Path](#ingestion-hot-path)
   - [Provenance Enforcement](#provenance-enforcement)
   - [Checkpoint Lifecycle](#checkpoint-lifecycle)
   - [Three-Phase Startup](#three-phase-startup)
3. [NixOS Deployment Audit](#nixos-deployment-audit)
   - [Module Completeness](#module-completeness)
   - [Service Orchestration](#service-orchestration)
   - [Failure Recovery](#failure-recovery)
   - [Security Hardening](#security-hardening)
4. [Patterns Summary](#patterns-summary)
   - [Consistent Patterns](#consistent-patterns)
   - [Inconsistent/Missing Patterns](#inconsistentmissing-patterns)
5. [Recommendations](#recommendations)
   - [Immediate Actions](#immediate-actions)
   - [Future Improvements](#future-improvements)

---

## Cross-Cutting Concerns

### Idempotency Patterns

Idempotency is achieved through a **three-layer defense** across the system:

#### 1. NATS Message Deduplication

All satellites use `Nats-Msg-Id` headers for publisher-side deduplication:

```rust
// crate/lib/sinex-satellite-sdk/src/nats_publisher.rs
let msg_id = format!("{}:{}", satellite_id, event.id);
headers.insert("Nats-Msg-Id", msg_id);
```

JetStream maintains a deduplication window (default 2 minutes) to reject duplicate message IDs.

#### 2. Database-Level Idempotency

All event inserts use `ON CONFLICT DO NOTHING`:

```rust
// crate/core/sinex-ingestd/src/jetstream_consumer.rs:741
builder.push(" ON CONFLICT (id) DO NOTHING RETURNING id::uuid as \"id!\"");
```

This ensures duplicate ULID insertions are silently ignored, not errored.

#### 3. Confirmation Stream Compaction

The `sinex.events.confirmations` stream uses `max_msgs_per_subject: 1`:

```rust
// Configuration ensures only the latest confirmation per subject is retained
StreamConfig {
    max_msgs_per_subject: 1,  // Compacts to latest confirmation
    ...
}
```

This prevents automata from seeing duplicate confirmations for the same event.

#### Assessment: **CONSISTENT** ✅

Idempotency is uniformly implemented across all layers. The system achieves exactly-once semantics through this layered approach.

---

### Backpressure Mechanisms

Backpressure is coordinated across four layers:

#### 1. Gateway Layer

```rust
// crate/core/sinex-gateway/src/rpc_server.rs
ServiceBuilder::new()
    .layer(TimeoutLayer::new(Duration::from_secs(30)))
    .layer(ConcurrencyLimitLayer::new(100))
    .layer(RateLimitLayer::new(100, Duration::from_secs(1)))
```

- **Concurrency limit**: 100 concurrent requests
- **Timeout**: 30 seconds per request
- **Rate limit**: 100 requests/second

#### 2. JetStream Consumer Layer

```rust
// crate/core/sinex-ingestd/src/jetstream_consumer.rs
ConsumerConfig {
    max_ack_pending: 100,      // Flow control
    ack_wait: Duration::from_secs(30),
    max_deliver: 10,           // Retry limit before DLQ
    ...
}
```

**Note**: `max_ack_pending` is currently hardcoded, not configurable.

#### 3. Database Pool Layer

```rust
// Connection pool configuration
PgPoolOptions::new()
    .max_connections(10)
    .connect_timeout(Duration::from_secs(30))
```

#### 4. Internal Channel Bounds

```rust
// Typical bounded channel pattern
let (tx, rx) = tokio::sync::mpsc::channel(100);
```

#### Assessment: **MOSTLY CONSISTENT** ⚠️

Backpressure is well-coordinated, but there's a configuration mismatch:
- Config allows `batch_size: 1000` but consumer pulls only 100 messages
- `max_ack_pending` is hardcoded and should be configurable

---

### Graceful Shutdown

#### Signal Handling Patterns

**sinex-ingestd** (partial):
```rust
// Only catches SIGINT, missing SIGTERM
tokio::signal::ctrl_c().await?;
```

**Satellites** (complete):
```rust
// Catches both signals
let mut sigterm = signal(SignalKind::terminate())?;
let mut sigint = signal(SignalKind::interrupt())?;

tokio::select! {
    _ = sigterm.recv() => { /* shutdown */ }
    _ = sigint.recv() => { /* shutdown */ }
}
```

#### Shutdown Sequence

1. Signal received
2. Cancellation token triggered
3. In-flight messages completed (or NAK'd for redelivery)
4. Checkpoint saved to database
5. Connections closed

#### Polling-Based Shutdown Detection

```rust
// crate/lib/sinex-satellite-sdk/src/runtime/stream/mod.rs:778-782
tokio::select! {
    _ = async {
        while !self.should_stop() {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    } => { /* shutdown */ }
    result = self.run_ingestor_startup_sequence() => { /* completed */ }
}
```

**Issue**: 100ms polling introduces up to 100ms shutdown latency.

#### Assessment: **INCONSISTENT** ❌

- **ingestd only catches SIGINT** - systemd sends SIGTERM by default
- 100ms polling for shutdown is inefficient; should use channels or events
- Checkpoint saving happens, but `reset_checkpoint()` is NOT IMPLEMENTED

---

### Configuration Precedence

#### Loading Order

All services use Figment for configuration with clear precedence:

```rust
// Typical pattern across all services
Figment::new()
    .merge(Toml::file("config.toml"))       // 1. Config file (lowest)
    .merge(Env::prefixed("SINEX_"))         // 2. Environment variables
    .merge(Serialized::defaults(&cli_args)) // 3. CLI args (highest)
```

#### Environment Variable Prefixes

| Service | Prefix | Example |
|---------|--------|---------|
| Gateway | `SINEX_` | `SINEX_RPC_PORT` |
| Ingestd | `INGESTD_` | `INGESTD_BATCH_SIZE` |
| Satellites | `SATELLITE_` | `SATELLITE_POLL_INTERVAL` |

**Issue**: Inconsistent prefixes across services.

#### Secret Injection

```nix
# nixos/modules/secrets.nix
environment.SINEX_DB_PASSWORD = config.sops.secrets.db-password.path;
```

Secrets are injected via environment variables pointing to agenix-managed paths.

#### Assessment: **MOSTLY CONSISTENT** ⚠️

- Clear precedence (file → env → CLI)
- Inconsistent prefix naming conventions
- Secret handling is properly externalized

---

## Critical Path Analysis

### Ingestion Hot Path

**File**: `crate/core/sinex-ingestd/src/jetstream_consumer.rs`

#### Message Flow

```
NATS JetStream
    │
    ▼ pull_batch(100)
┌─────────────────────┐
│   process_batch()   │ ← Lines 334-647
│   ├── Deserialize   │
│   ├── Validate      │
│   ├── Parse ULID    │
│   └── Build batch   │
└─────────────────────┘
    │
    ▼
┌─────────────────────────────┐
│ persist_batch_optimized()   │ ← Lines 687-753
│ └── Multi-row INSERT        │
│     ON CONFLICT DO NOTHING  │
└─────────────────────────────┘
    │
    ▼ AFTER commit
┌─────────────────────────────┐
│ publish_confirmations()     │ ← Lines 598-605
│ └── To sinex.events.{id}    │
└─────────────────────────────┘
    │
    ▼
┌─────────────────────┐
│      ack_all()      │
└─────────────────────┘
```

#### Key Configuration

```rust
// Consumer configuration
ConsumerConfig {
    deliver_policy: DeliverPolicy::All,
    ack_policy: AckPolicy::Explicit,
    ack_wait: Duration::from_secs(30),
    max_deliver: 10,           // After 10 failures → DLQ
    max_ack_pending: 100,      // Flow control
    filter_subject: "sinex.events.*".to_string(),
}
```

#### Batch Processing

```rust
// Lines 362-380: Pull up to 100 messages with 5s timeout
let messages = consumer
    .fetch()
    .max_messages(100)
    .expires(Duration::from_secs(5))
    .messages()
    .await?;
```

#### Critical Invariant: Confirmations After Commit

```rust
// Lines 598-605: Order matters for exactly-once
// 1. DB transaction commits
// 2. THEN confirmations published
// 3. THEN messages ACK'd

// If we crash after commit but before ACK:
// - Messages redeliver (idempotent insert)
// - Confirmations republish (compacted stream)
// Result: No duplicates, no lost events
```

---

### Provenance Enforcement

Provenance enforces **audit trail integrity** via an XOR constraint: every event must have EITHER material provenance (external source) OR synthesis provenance (derived from other events), but never both or neither.

#### Application-Level Validation

```rust
// jetstream_consumer.rs:482-521
fn validate_provenance(raw_event: &RawEvent) -> Result<PreparedProvenance> {
    match (&raw_event.material_id, &raw_event.source_event_ids) {
        // Material provenance (from external source)
        (Some(material_id), None) => Ok(PreparedProvenance::Material {
            material_id: material_id.clone(),
            byte_offset_start: raw_event.byte_offset_start,
            byte_offset_end: raw_event.byte_offset_end,
        }),

        // Synthesis provenance (derived from other events)
        (None, Some(source_ids)) => Ok(PreparedProvenance::Synthesis {
            source_event_ids: source_ids.clone(),
        }),

        // XOR violation - both present
        (Some(_), Some(_)) => Err(anyhow!("Event has both material and synthesis provenance")),

        // Neither present - default to self-referential
        (None, None) => {
            warn!(event_id = %raw_event.id, "Event missing provenance; assuming self-referential");
            Ok(PreparedProvenance::Synthesis {
                source_event_ids: vec![raw_event.id.as_uuid()],
            })
        }
    }
}
```

#### Database-Level Constraint

```sql
-- From schema migrations
ALTER TABLE raw.events ADD CONSTRAINT provenance_xor CHECK (
    (material_id IS NOT NULL AND source_event_ids IS NULL) OR
    (material_id IS NULL AND source_event_ids IS NOT NULL)
);
```

#### Default Self-Referential Provenance

When neither provenance type is provided, the system defaults to self-referential synthesis (event is its own source). This is a **recovery mechanism**, not the intended path.

---

### Checkpoint Lifecycle

**File**: `crate/lib/sinex-satellite-sdk/src/checkpoint.rs`

#### CheckpointState Structure

```rust
// Lines 91-107
pub struct CheckpointState {
    /// Unified checkpoint data (External/Internal/Stream/Timestamp)
    pub checkpoint: Checkpoint,

    /// Total number of messages/events processed
    pub processed_count: u64,

    /// Last activity timestamp
    pub last_activity: chrono::DateTime<chrono::Utc>,

    /// Processor-specific state data
    pub data: Option<serde_json::Value>,

    /// Checkpoint version (for schema evolution)
    pub version: u32,  // Currently v2
}
```

#### Checkpoint Variants

```rust
pub enum Checkpoint {
    None,                              // Initial state
    Internal { event_id: Ulid, ... },  // Automata (event ULID)
    External { position: u64, ... },   // Ingestors (file offset, etc.)
    Stream { message_id: String, ... }, // NATS message ID
    Timestamp { at: DateTime, ... },   // Time-based processing
}
```

#### Load with Migration

```rust
// Lines 282-368
pub async fn load_checkpoint(&self) -> SatelliteResult<CheckpointState> {
    let row = self.pool.checkpoints().get_by_processor(...).await?;

    if let Some(row) = row {
        if row.checkpoint_data.is_some() {
            // Version 2+: Deserialize unified format
            let checkpoint: Checkpoint = serde_json::from_value(data)?;
            return Ok(CheckpointState { checkpoint, ... });
        } else {
            // Version 1: Migrate legacy format
            warn!("Migrating legacy checkpoint format");
            let legacy = LegacyCheckpointState { ... };
            let unified = CheckpointState::from(legacy);
            self.save_checkpoint(&unified).await?;  // Persist migration
            return Ok(unified);
        }
    }

    // No checkpoint found - start fresh
    Ok(CheckpointState::default())
}
```

#### Save with Atomic Upsert

```rust
// Lines 387-435
pub async fn save_checkpoint(&self, state: &CheckpointState) -> SatelliteResult<()> {
    let checkpoint_data = serde_json::to_value(&state.checkpoint)?;

    self.pool.checkpoints().upsert(
        CheckpointIdentity { processor, consumer_group, consumer_name },
        last_processed_id,
        processed_count,
        Some(checkpoint_data),
    ).await?;
}
```

#### NOT IMPLEMENTED Functions

```rust
// Lines 458-474: Reset checkpoint
pub async fn reset_checkpoint(&self) -> SatelliteResult<()> {
    warn!("Reset checkpoint not implemented in new API");
    Ok(())  // No-op!
}

// Lines 477-486: Get checkpoint stats
pub async fn get_checkpoint_stats(&self) -> SatelliteResult<CheckpointStats> {
    Ok(CheckpointStats {
        total_checkpoints: 0,
        max_processed: 0,
        last_update: None,
        first_checkpoint: None,
    })  // Returns empty stats!
}
```

---

### Three-Phase Startup

**File**: `crate/lib/sinex-satellite-sdk/src/runtime/stream/mod.rs`

#### Phase Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    INGESTOR STARTUP                         │
├─────────────────────────────────────────────────────────────┤
│  Phase 1: SNAPSHOT                                          │
│  └── Capture current state of external system               │
│      (if supports_snapshot capability)                      │
├─────────────────────────────────────────────────────────────┤
│  Phase 2: GAP-FILL                                          │
│  └── Process historical data since last checkpoint          │
│      (if supports_historical capability)                    │
├─────────────────────────────────────────────────────────────┤
│  Phase 3: CONTINUOUS                                        │
│  └── Real-time event streaming                              │
│      (if supports_continuous capability)                    │
└─────────────────────────────────────────────────────────────┘
```

#### Implementation

```rust
// Lines 609-668
async fn run_ingestor_startup_sequence(&mut self) -> SatelliteResult<()> {
    let capabilities = self.processor.capabilities();

    // Phase 1: Snapshot (if supported)
    if capabilities.supports_snapshot {
        info!("Phase 1: Starting snapshot capture");
        let checkpoint = self.checkpoint_manager.load_checkpoint().await?;
        self.processor.scan_snapshot(checkpoint).await?;
    }

    // Phase 2: Gap-filling (if supported and needed)
    if capabilities.supports_historical {
        info!("Phase 2: Starting historical gap-fill");
        let checkpoint = self.checkpoint_manager.load_checkpoint().await?;
        let report = self.processor.scan_historical(checkpoint).await?;
        self.checkpoint_manager.save_checkpoint(&report.checkpoint).await?;
    }

    // Phase 3: Continuous processing
    if capabilities.supports_continuous {
        info!("Phase 3: Starting continuous processing");
        self.run_continuous_processing().await?;
    }

    Ok(())
}
```

#### Capability-Driven Behavior

```rust
pub struct ProcessorCapabilities {
    pub supports_snapshot: bool,     // Can capture point-in-time state
    pub supports_historical: bool,   // Can backfill from checkpoint
    pub supports_continuous: bool,   // Can stream real-time events
    pub requires_confirmation: bool, // Needs DB confirmation before processing
}
```

Different satellites implement different capability sets:
- **File ingestor**: snapshot + historical + continuous
- **Desktop events**: continuous only
- **Health automaton**: continuous + requires_confirmation

---

## NixOS Deployment Audit

### Module Completeness

#### Available Modules (10 total)

| Module | Purpose | Config Options |
|--------|---------|----------------|
| `default.nix` | Service orchestration | enable, users, groups |
| `ingestd.nix` | Event ingestion daemon | batch_size, workers, nats_url |
| `gateway.nix` | HTTP/RPC gateway | port, auth, rate_limits |
| `nats.nix` | NATS JetStream server | jetstream, clustering |
| `blob-storage.nix` | Binary artifact storage | path, max_size |
| `satellite-services.nix` | Satellite systemd units | per-satellite config |
| `preflight-verification.nix` | Startup gates | health_checks, timeouts |
| `database.nix` | PostgreSQL + TimescaleDB | extensions, pools |
| `secrets.nix` | Agenix secret management | paths, permissions |
| `monitoring.nix` | Prometheus/Grafana | metrics, dashboards |

#### Option Coverage Assessment

Most production-critical options are exposed, but some are missing:
- `max_ack_pending` not configurable (hardcoded)
- `shutdown_timeout` not exposed
- Individual satellite enable/disable flags

### Service Orchestration

#### Startup Order

```nix
# Defined via systemd dependencies
postgresql.service
    └── nats.service
        └── sinex-ingestd.service
            └── sinex-gateway.service
                └── satellite-*.service
```

#### Dependency Declaration

```nix
# satellite-services.nix
systemd.services."satellite-${name}" = {
    after = [ "network.target" "nats.service" "sinex-ingestd.service" ];
    requires = [ "nats.service" ];
    wantedBy = [ "multi-user.target" ];
};
```

#### Health Checks

```nix
# preflight-verification.nix
ExecStartPre = [
    "${pkgs.bash}/bin/bash -c 'until pg_isready; do sleep 1; done'"
    "${pkgs.bash}/bin/bash -c 'until nats-server --help; do sleep 1; done'"
];
```

### Failure Recovery

#### Restart Policies

```nix
# Standard across all services
systemd.services.sinex-ingestd = {
    serviceConfig = {
        Restart = "on-failure";
        RestartSec = "5s";
        StartLimitBurst = 3;
        StartLimitIntervalSec = "60s";
    };
};
```

#### Preflight Gates

```nix
# preflight-verification.nix
systemd.services.sinex-preflight = {
    serviceConfig = {
        Type = "oneshot";
        RemainAfterExit = true;
    };
    script = ''
        # Verify all dependencies before main services start
        pg_isready -h localhost
        nats-server --check
        # Additional health checks...
    '';
};
```

### Security Hardening

#### CRITICAL FINDING: Missing Hardening ❌

**Only the preflight service has security hardening:**

```nix
# preflight-verification.nix (ONLY SERVICE WITH HARDENING)
serviceConfig = {
    ProtectSystem = "strict";
    ProtectHome = true;
    PrivateTmp = true;
    NoNewPrivileges = true;
    ProtectKernelTunables = true;
    ProtectKernelModules = true;
    ProtectControlGroups = true;
    RestrictAddressFamilies = [ "AF_UNIX" "AF_INET" "AF_INET6" ];
    RestrictNamespaces = true;
    RestrictRealtime = true;
    RestrictSUIDSGID = true;
    MemoryDenyWriteExecute = true;
    LockPersonality = true;
};
```

**Production services (ingestd, gateway, satellites) have ZERO hardening:**

```nix
# satellite-services.nix (NO HARDENING)
serviceConfig = {
    ExecStart = "${satellite}/bin/${name}";
    Restart = "on-failure";
    # NO security directives!
};
```

#### Secret Management

```nix
# secrets.nix
sops.secrets = {
    "sinex/db-password" = {
        owner = "sinex";
        group = "sinex";
        mode = "0400";
    };
};
```

Secrets are properly managed via agenix with appropriate permissions.

---

## Patterns Summary

### Consistent Patterns

| Pattern | Implementation | Assessment |
|---------|---------------|------------|
| **Idempotency** | NATS Msg-Id + ON CONFLICT + compaction | ✅ Excellent |
| **ULID Keys** | All entities use time-ordered ULIDs | ✅ Consistent |
| **Provenance XOR** | App + DB dual-layer enforcement | ✅ Robust |
| **Figment Config** | File → Env → CLI precedence | ✅ Clear |
| **Checkpoint Format** | Unified v2 with migration | ✅ Forwards-compatible |
| **Confirmation Flow** | Always AFTER DB commit | ✅ Exactly-once safe |
| **Secret Handling** | Externalized via agenix | ✅ Secure |

### Inconsistent/Missing Patterns

| Pattern | Issue | Impact |
|---------|-------|--------|
| **Signal Handling** | ingestd only catches SIGINT, not SIGTERM | 🔴 High - systemd shutdown may not work |
| **Security Hardening** | Zero hardening on production services | 🔴 Critical - attack surface exposed |
| **Shutdown Detection** | 100ms polling instead of channels | 🟡 Medium - latency/CPU waste |
| **Config Prefixes** | SINEX_ vs INGESTD_ vs SATELLITE_ | 🟡 Medium - confusing |
| **Checkpoint Reset** | `reset_checkpoint()` not implemented | 🟡 Medium - ops gap |
| **Checkpoint Stats** | `get_checkpoint_stats()` returns empty | 🟡 Medium - observability gap |
| **max_ack_pending** | Hardcoded, not configurable | 🟡 Medium - tuning limitation |
| **Batch Size Mismatch** | Config allows 1000, consumer pulls 100 | 🟢 Low - misleading config |

---

## Recommendations

### Immediate Actions

#### 1. Add SIGTERM Handler to ingestd

**Priority**: 🔴 High
**Effort**: Low
**File**: `crate/core/sinex-ingestd/src/main.rs`

```rust
// Current (broken)
tokio::signal::ctrl_c().await?;

// Fixed
let mut sigterm = signal(SignalKind::terminate())?;
let mut sigint = signal(SignalKind::interrupt())?;
tokio::select! {
    _ = sigterm.recv() => info!("SIGTERM received"),
    _ = sigint.recv() => info!("SIGINT received"),
}
```

#### 2. Apply Security Hardening to All Services

**Priority**: 🔴 Critical
**Effort**: Medium
**File**: `nixos/modules/satellite-services.nix`, `nixos/modules/ingestd.nix`, etc.

Copy the hardening from `preflight-verification.nix` to all production services:

```nix
serviceConfig = {
    # Existing config...

    # ADD THESE:
    ProtectSystem = "strict";
    ProtectHome = true;
    PrivateTmp = true;
    NoNewPrivileges = true;
    ProtectKernelTunables = true;
    ProtectKernelModules = true;
    ProtectControlGroups = true;
    RestrictAddressFamilies = [ "AF_UNIX" "AF_INET" "AF_INET6" ];
    RestrictNamespaces = true;
    RestrictRealtime = true;
    RestrictSUIDSGID = true;
    MemoryDenyWriteExecute = true;
    LockPersonality = true;
};
```

#### 3. Implement reset_checkpoint()

**Priority**: 🟡 Medium
**Effort**: Low
**File**: `crate/lib/sinex-satellite-sdk/src/checkpoint.rs`

```rust
pub async fn reset_checkpoint(&self) -> SatelliteResult<()> {
    self.pool.checkpoints().delete(
        &ProcessorName::new(&self.processor_name),
        &ConsumerGroup::new(&self.consumer_group),
        &ConsumerName::new(&self.consumer_name),
    ).await?;

    info!(
        processor = %self.processor_name,
        "Checkpoint reset successfully"
    );
    Ok(())
}
```

### Future Improvements

#### 1. Replace Polling with Channel-Based Shutdown

**File**: `crate/lib/sinex-satellite-sdk/src/runtime/stream/mod.rs`

Replace the 100ms polling loop with a `tokio::sync::watch` channel:

```rust
// Create shutdown channel
let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);

// In shutdown handler
shutdown_tx.send(true)?;

// In processing loop
tokio::select! {
    _ = shutdown_rx.changed() => break,
    result = process_next() => { ... }
}
```

#### 2. Standardize Environment Variable Prefixes

Adopt a single prefix (`SINEX_`) with service-specific suffixes:

| Current | Proposed |
|---------|----------|
| `INGESTD_BATCH_SIZE` | `SINEX_INGESTD_BATCH_SIZE` |
| `SATELLITE_POLL_INTERVAL` | `SINEX_SATELLITE_POLL_INTERVAL` |

#### 3. Make max_ack_pending Configurable

**File**: `crate/core/sinex-ingestd/src/jetstream_consumer.rs`

```rust
// Add to IngestdConfig
pub struct IngestdConfig {
    pub max_ack_pending: u32,  // Default: 100
    ...
}

// Use in consumer config
ConsumerConfig {
    max_ack_pending: config.max_ack_pending as i64,
    ...
}
```

#### 4. Implement get_checkpoint_stats()

**File**: `crate/lib/sinex-satellite-sdk/src/checkpoint.rs`

```rust
pub async fn get_checkpoint_stats(&self) -> SatelliteResult<CheckpointStats> {
    let stats = self.pool.checkpoints().get_stats(
        &ProcessorName::new(&self.processor_name),
    ).await?;

    Ok(CheckpointStats {
        total_checkpoints: stats.count,
        max_processed: stats.max_processed,
        last_update: stats.last_update,
        first_checkpoint: stats.first_created,
    })
}
```

---

## Appendix: File Reference

| File | Lines | Purpose |
|------|-------|---------|
| `crate/core/sinex-ingestd/src/jetstream_consumer.rs` | 860 | Ingestion hot path |
| `crate/lib/sinex-satellite-sdk/src/checkpoint.rs` | 509 | Checkpoint management |
| `crate/lib/sinex-satellite-sdk/src/runtime/stream/mod.rs` | 951 | Satellite runtime |
| `crate/lib/sinex-satellite-sdk/src/nats_publisher.rs` | ~200 | NATS publishing |
| `crate/core/sinex-gateway/src/rpc_server.rs` | ~400 | Gateway RPC |
| `nixos/modules/satellite-services.nix` | ~150 | Satellite systemd |
| `nixos/modules/preflight-verification.nix` | ~100 | Preflight gates |
| `nixos/modules/secrets.nix` | ~50 | Secret management |

---

*Document generated from deep exploration of sinex codebase, December 2024*
