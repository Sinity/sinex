# Comprehensive Issue Audit

This document consolidates all issues discovered during deep exploration of the sinex codebase.

**Total Issues Found: ~280+**

---

## Executive Summary

| Category | Critical | High | Medium | Low | Total |
|----------|----------|------|--------|-----|-------|
| Error Handling | 3 | 4 | 8 | 2 | 17 |
| Security | 0 | 1 | 0 | 0 | 1 |
| Technical Debt (TODO/Dead Code) | 4 | 8 | 10 | 5 | 127+ |
| Concurrency | 1 | 5 | 3 | 6 | 15 |
| Test Coverage Gaps | 0 | 6 | 4 | 2 | 50-60 |
| Configuration | 2 | 4 | 6 | 3 | 33 |
| Observability | 2 | 8 | 5 | 2 | 47 |
| NixOS Modules | 0 | 4 | 8 | 3 | 15 |

---

## Critical Issues (Fix Immediately)

### C1. Panic in Material Assembly Pipeline
**File**: `crate/core/sinex-ingestd/src/material_assembler.rs:474`
**Category**: Error Handling
```rust
let buf_path = state.buffered_slices
    .remove(&next_offset)
    .expect("buffer entry must exist");
```
**Impact**: Production crash if buffer state is corrupted. Data pipeline failure.
**Fix**: Replace with `ok_or_else(|| SinexError::...)` and proper error propagation.

---

### C2. Panic in Bulk Event Processing
**File**: `crate/lib/sinex-core/src/db/repositories/events.rs:1051`
```rust
for (i, event) in events.iter().enumerate() {
    let event_id = event.id.as_ref().unwrap();  // Could be None!
```
**Impact**: Silent panic processing event batches if any event lacks an ID.
**Fix**: Filter or validate events before iteration; return error for invalid events.

---

### C3. Terminal Satellite Production Panics
**File**: `crate/satellites/sinex-terminal-satellite/src/unified_processor.rs:785,839`
```rust
let event = timeout(...).await?.expect("terminal event emitted");
let payload_cmd = event.payload.get_command().expect("payload command present");
```
**Impact**: Production satellite crash on unexpected data format.
**Fix**: Replace `expect()` with proper Result error handling.

---

### C4. Config Integer Overflow Causes Runtime Panic
**File**: `crate/core/sinex-rpc-dispatcher/src/lib.rs:289`
```rust
let default_hours_i64: i64 = default_hours
    .try_into()
    .unwrap_or_else(|_| panic!("historical_scan_hours overflows i64"));
```
**Impact**: Production crash from config values.
**Fix**: Validate at config load time with proper error, not runtime panic.

---

### C5. Unbounded Memory Growth in Buffered Slices
**File**: `crate/core/sinex-ingestd/src/material_assembler.rs:461-510`
```rust
buffered_slices: BTreeMap::new(),  // No size limit
if offset > state.expected_offset {
    state.buffered_slices.insert(offset, buffer_path);  // Grows forever
}
```
**Impact**: DoS via memory exhaustion from out-of-order slices.
**Fix**: Add max buffer size with eviction or backpressure.

---

### C6. Silent Directory Creation Failure
**File**: `crate/satellites/sinex-system-satellite/src/journal_watcher.rs:569`
```rust
tokio::fs::create_dir_all(parent).await.ok();  // Error silently discarded
```
**Impact**: Cursor persistence silently fails, causing event duplication or loss.
**Fix**: Log warning or propagate error.

---

## High Priority Issues

### Error Handling (High)

#### EH1. Transaction Rollback Errors Ignored
**Files**:
- `crate/lib/sinex-core/src/db/distributed_locking.rs:324`
- `crate/lib/sinex-core/src/db/repositories/state.rs:483`
```rust
tx.rollback().await.ok();  // Error silently discarded
```
**Fix**: Log rollback failures at WARN level.

#### EH2. Health Check Failures Hidden
**File**: `crate/lib/sinex-core/src/db/repositories/state.rs:1109`
```rust
let processor_health = self.get_processor_health().await.ok();
```
**Fix**: Log health check failures; don't silently convert to None.

#### EH3. Error Context Lost in Validation
**Files**: `crate/core/sinex-ingestd/src/config.rs:404,431,438`
```rust
.map_err(|_| validator::ValidationError::new("invalid_work_dir"))
```
**Fix**: Include original error in message: `.map_err(|e| ...format!("... {e}"))`.

---

### Concurrency (High)

#### CC1. Database Queries Missing Timeouts
**File**: `crate/core/sinex-ingestd/src/jetstream_consumer.rs:743-750`
```rust
let rows = builder.build_query_as::<(Uuid,)>()
    .fetch_all(&self.pool).await  // No timeout!
```
**Impact**: Hung database blocks event processing indefinitely.
**Fix**: Wrap with `tokio::time::timeout()`.

#### CC2. RwLock Held During File I/O
**File**: `crate/core/sinex-ingestd/src/material_assembler.rs:382,434,760`
```rust
let mut states = self.assembler_state.write().await;
// ... performs file I/O while holding lock
```
**Impact**: Blocks all other material processing during slow disk I/O.
**Fix**: Clone/extract data, drop lock, then do I/O.

#### CC3. Unjoined Spawned Tasks
**File**: `crate/core/sinex-ingestd/src/service.rs:173,189,223,251,300`
```rust
tokio::spawn(async move { ... });  // No JoinHandle tracking
```
**Impact**: Task panics are logged but not handled; no graceful shutdown.
**Fix**: Store JoinHandles; join on shutdown.

#### CC4. Polling-Based Shutdown (100ms latency)
**File**: `crate/core/sinex-ingestd/src/service.rs:32-39`
```rust
loop {
    if shutdown_flag.load(Ordering::Relaxed) { break; }
    tokio::time::sleep(Duration::from_millis(100)).await;
}
```
**Fix**: Use `tokio::sync::watch` channel instead of polling.

#### CC5. JetStream Batch Processing No Timeout
**File**: `crate/core/sinex-ingestd/src/jetstream_consumer.rs:335-345`
**Impact**: Individual message processing can block entire batch indefinitely.
**Fix**: Add per-batch timeout wrapper.

---

### Security (High)

#### SEC1. TOCTOU Race on Unix Socket Permissions
**File**: `crate/core/sinex-gateway/src/rpc_server.rs:800-820`
```rust
let listener = tokio::net::UnixListener::bind(socket_path)?;
// Window here where socket has default permissions
tokio::fs::set_permissions(socket_path, Permissions::from_mode(0o600)).await;
```
**Impact**: Brief window for unauthorized socket access.
**Fix**: Use `umask()` before bind or accept risk as local-only.

---

### NixOS (High)

#### NIX1. Missing NATS Dependency for ingestd
**File**: `nixos/modules/satellite-services.nix:104-116`
```nix
after = [ "postgresql.service" ];  # Missing nats.service!
requires = [ "postgresql.service" ];
```
**Fix**: Add `"nats.service"` to both arrays.

#### NIX2. Missing Migration Assertion
**File**: `nixos/modules/default.nix`
**Issue**: Can enable ingestd without database migrations.
**Fix**: Add assertion requiring migrations when core services enabled.

#### NIX3. Missing NATS Assertion for Satellites
**File**: `nixos/modules/default.nix:1042-1051`
**Issue**: Can enable satellites without NATS.
**Fix**: Add assertion checking NATS enabled when satellites enabled.

#### NIX4. Missing maxLifetime Pool Option
**File**: `nixos/modules/default.nix:198-225`
**Issue**: Rust defaults to 1800s but NixOS can't configure it.
**Fix**: Add `maxLifetime` option to connectionPool submodule.

---

### Test Coverage (High)

#### TEST1. JetStreamEventConsumer - Zero Integration Tests
**File**: `crate/lib/sinex-satellite-sdk/src/jetstream_consumer.rs`
- 390 lines of source, ~10 lines of tests (0.03:1 ratio)
- No error injection tests
- No connection failure tests
- No malformed message tests

#### TEST2. LeaseManager - Only Stub Tests
**File**: `crate/lib/sinex-satellite-sdk/src/lease_manager.rs`
- ~200 lines source, ~15 lines tests (0.07:1 ratio)
- No multi-instance coordination tests
- No KV failure scenarios
- No lease expiration tests

#### TEST3. DlqRetryHandler - No Tests At All
**File**: `crate/lib/sinex-satellite-sdk/src/dlq_retry.rs`
- ~200 lines, 0 tests
- Critical for reliability

#### TEST4. CheckpointManager Error Paths Untested
**File**: `crate/lib/sinex-satellite-sdk/src/checkpoint.rs`
- Serialization failures not tested
- Concurrent updates not tested
- Database connection failures not tested

#### TEST5. EventProcessor Missing Integration Tests
**File**: `crate/lib/sinex-satellite-sdk/src/event_processor.rs`
- Batch timeout logic untested
- Shutdown with pending events untested

#### TEST6. 3 Property Tests Disabled
**File**: `crate/lib/sinex-core/tests/property/ulid_property_test.rs:359-571`
```rust
// Database test temporarily disabled due to direct sqlx usage
// TODO: Reimplement using repository pattern
```

---

### Observability (High)

#### OBS1. Silent DLQ Error Suppression
**File**: `crate/core/sinex-ingestd/src/jetstream_consumer.rs:804-814`
- DLQ routing errors logged without original error cause
- Missing material_id context
- No retry attempt number

#### OBS2. Missing Tracing Spans in Async Tasks
**File**: `crate/core/sinex-ingestd/src/material_assembler.rs:890,951,1048`
- Three `tokio::spawn()` without `#[tracing::instrument]`
- Cannot correlate logs across task boundaries

#### OBS3. Missing Batch Processing Metrics
**File**: `crate/core/sinex-ingestd/src/jetstream_consumer.rs`
- No per-operation timing (parse/validate/persist)
- No throughput metric (events/sec)
- Stats only every 60 seconds

#### OBS4. Unlogged Environment Variable Parsing Failures
**File**: `crate/core/sinex-gateway/src/rpc_server.rs:105-116`
```rust
.ok()  // Silently uses default on parse failure
```
**Fix**: Log WARN when env var parsing fails.

#### OBS5. Missing Transaction Boundary Logging
**File**: `crate/core/sinex-ingestd/src/material_assembler.rs:814-881`
- 5 sequential DB operations with no sequence logging
- Can't tell which operation failed

#### OBS6. No Backpressure/Lag Logging
**File**: `crate/core/sinex-ingestd/src/jetstream_consumer.rs`
- No logging when approaching max_ack_pending limit
- No consumer lag metrics

#### OBS7. Missing Request Timing in Gateway
**File**: `crate/core/sinex-gateway/src/rpc_server.rs:377`
```rust
let _start = std::time::Instant::now();  // Created but never used!
```

#### OBS8. Potential Sensitive Data in Logs
**File**: `crate/core/sinex-gateway/src/rpc_server.rs:373`
```rust
debug!("Received RPC request: method={}, params={:?}", ...)  // May log credentials
```

---

### Configuration (High)

#### CFG1. Localhost NATS Default Unsafe for Production
**Files**: Multiple
```rust
nats_url: "nats://localhost:4222"  // Fails in containers
```
**Fix**: Require explicit NATS_URL or fail fast in production.

#### CFG2. /tmp Socket Path Unsafe
**File**: `crate/core/sinex-gateway/src/rpc_server.rs:42`
```rust
DEFAULT_SOCKET_PATH: "/tmp/sinex-host.sock"
```
**Fix**: Use `/run/sinex/` for production sockets.

#### CFG3. Dev Environment Default Without Warning
**File**: `crate/lib/sinex-core/src/environment.rs:14`
```rust
DEFAULT_ENVIRONMENT: "dev"  // Silent fallback to dev mode
```
**Fix**: Require explicit SINEX_ENVIRONMENT in production.

#### CFG4. Inconsistent Pool Size Defaults
- `sinex-core/db/mod.rs:88`: `max_connections: 100`
- `sinex-ingestd/config.rs:34`: `default = 50`
- Confuses users about actual behavior

---

## Medium Priority Issues

### Technical Debt

#### TD1. System Satellite Processor Incomplete
**File**: `crate/satellites/sinex-system-satellite/src/unified_processor.rs:196`
```rust
// TODO(system-satellite): Complete implementation
// Needs: D-Bus, journal, and udev monitoring
```

#### TD2. Exploration Mode Non-Functional
**File**: `crate/lib/sinex-processor-runtime/src/runner.rs:318`
```rust
warn!("Exploration mode not yet fully implemented");
return Ok(());  // Does nothing!
```

#### TD3. reset_checkpoint() Not Implemented
**File**: `crate/lib/sinex-satellite-sdk/src/checkpoint.rs:458-474`
```rust
warn!("Reset checkpoint not implemented in new API");
Ok(())  // No-op!
```

#### TD4. get_checkpoint_stats() Returns Empty
**File**: `crate/lib/sinex-satellite-sdk/src/checkpoint.rs:477-486`
```rust
Ok(CheckpointStats { total_checkpoints: 0, max_processed: 0, ... })  // Always empty!
```

#### TD5. Content Analysis Returns Placeholder
**File**: `crate/satellites/sinex-content-automaton/src/lib.rs`
```rust
return Some(format!("File content analysis placeholder for: {}", path));
```

#### TD6. Satellite Processing Macros Disabled
**File**: `crate/lib/sinex-macros/src/lib.rs:169`
```rust
// Satellite processing macros temporarily disabled due to syn 2.x compatibility
```

#### TD7. 75+ `#[allow(dead_code)]` Attributes
**Impact**: Codebase hygiene; unclear what's intentionally unused vs forgotten.

#### TD8. Audit Bypass Mode Not Implemented
**File**: `crate/lib/sinex-core/src/db/repositories/events.rs:1952,1959`
```rust
// Note: Audit bypass mode not implemented - performing direct delete
```

---

### NixOS (Medium)

#### NIX5. Service Type Mismatch (sd_notify)
**File**: `nixos/modules/satellite-services.nix:74`
- ingestd configured with `Type = "notify"` but unclear if it sends sd_notify

#### NIX6. Missing TimeoutStartSec
**File**: `nixos/modules/satellite-services.nix:72-82`
- No startup timeout configured; relies on systemd default 90s

#### NIX7. Password File Handling Unclear
**File**: `nixos/modules/default.nix:185-189`
- passwordFile option exists but not documented how it's passed to services

#### NIX8. DLQ Rollback Incomplete
**File**: `nixos/modules/preflight-verification.nix:104-151`
- Backup created but restore on failure may not handle partial states

#### NIX9. Limited NATS Parameters
**File**: `nixos/modules/default.nix:518-535`
- Missing auth, TLS, retry configuration options

---

### Configuration (Medium)

#### CFG5. Hardcoded Pool Timeouts
**File**: `crate/core/sinex-ingestd/src/config.rs:294-296`
```rust
acquire_timeout: Duration::from_secs(30),  // Not configurable
idle_timeout: Duration::from_secs(600),
max_lifetime: Duration::from_secs(1800),
```

#### CFG6. Hardcoded JSON Limits
**File**: `crate/lib/sinex-core/src/types/mod.rs:70-79`
```rust
MAX_JSON_DEPTH: 100,
MAX_JSON_ELEMENTS: 50_000,
```
**Impact**: Can't tune for different payload requirements.

#### CFG7. Cascade Analysis Constants
**File**: `crate/lib/sinex-core/src/db/replay/config.rs:9-12`
```rust
DEFAULT_CASCADE_MAX_DEPTH: 100,
DEFAULT_CASCADE_BATCH_SIZE: 1000,
DEFAULT_CASCADE_MAX_MEMORY_BYTES: 100MB,
```

#### CFG8. Watch Intervals Hardcoded
**File**: `crate/lib/sinex-core/src/types/mod.rs:132-135`
```rust
DEFAULT_WATCH_INTERVAL: 100ms,
TERMINAL_SOCKET_WATCH_INTERVAL: 500ms,
```

#### CFG9. Retry Configuration Not Configurable
**File**: `crate/lib/sinex-core/src/db/replay/config.rs:17-18`
```rust
DEFAULT_REPLAY_MAX_RETRIES: 3,
DEFAULT_REPLAY_RETRY_DELAY_MS: 1000,
```

#### CFG10. Silent Fallback to /tmp
**File**: `crate/lib/sinex-satellite-sdk/src/config.rs:472-473`
```rust
.unwrap_or_else(|_| Utf8PathBuf::from("/tmp"))  // Hides errors
```

---

### Observability (Medium)

#### OBS9. Missing Recovery Summary Logging
**File**: `crate/core/sinex-ingestd/src/material_assembler.rs:155-323`
- No log of total materials restored, time taken, failures skipped

#### OBS10. Missing Cascade Analysis Timing
**File**: `crate/core/sinex-gateway/src/cascade_analyzer.rs:421-422`
- No query time or result size logging

#### OBS11. Missing Operation Correlation IDs
**Files**: Multiple
- No shared request ID across parse → validate → persist → confirm

---

## Low Priority Issues

### Error Handling

- Regex compilation unwrap (safe but could use const): `validation_chains.rs:57`
- JSON serialization unwrap without context: `schema_management.rs:47`
- Test code using unwrap instead of `?`: `sanitization.rs:256-258`

### Technical Debt

- Removed macro modules still commented: `sinex-macros/src/lib.rs:7-8,18`
- Hardcoded ULID bytes with expect: `dbus_watcher.rs:396+`
- Test-only panics in production lib: `event.rs:420`

### Configuration

- Different replay batch sizes: 500 vs 1000 in different modules
- Unit naming convention not documented

### Observability

- Stats using Relaxed atomic ordering (may miss updates): `service.rs`

---

## Recommended Priority Order

### Week 1: Critical Fixes
1. Fix all panic/expect in production code paths (C1-C4)
2. Add buffer size limits (C5)
3. Fix silent error suppression (C6)

### Week 2: High Priority
1. Add database query timeouts
2. Fix RwLock held during I/O
3. Add NATS dependencies to NixOS
4. Apply security hardening to all NixOS services

### Week 3: Testing
1. Add JetStreamEventConsumer integration tests
2. Add LeaseManager integration tests
3. Add DlqRetryHandler tests
4. Enable disabled property tests

### Week 4: Observability
1. Add missing tracing spans
2. Add batch processing metrics
3. Fix unlogged error paths
4. Add request timing to gateway

### Ongoing
- Clean up dead code attributes
- Implement stub functions (reset_checkpoint, etc.)
- Standardize configuration defaults
- Document all NixOS options

---

## Files Most Needing Attention

| File | Issues | Categories |
|------|--------|------------|
| `jetstream_consumer.rs` | 12 | Error, Concurrency, Observability |
| `material_assembler.rs` | 10 | Error, Concurrency, Observability |
| `checkpoint.rs` | 5 | Error, Technical Debt, Testing |
| `rpc_server.rs` | 6 | Security, Config, Observability |
| `satellite-services.nix` | 5 | NixOS Dependencies, Config |
| `default.nix` | 6 | NixOS Assertions, Options |
| `lease_manager.rs` | 4 | Testing, Concurrency |
| `dlq_retry.rs` | 3 | Testing |
| `config.rs` (multiple) | 8 | Configuration |

---

*Generated from comprehensive codebase exploration, December 2024*
