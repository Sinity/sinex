# Tactical Fixes Applied - sinex-system-ingestor

## Summary

All tactical issues have been addressed for the sinex-system-ingestor node. This document details the fixes applied.

---

## Issue 48: Bootstrap Event ID Reused ✅ ALREADY FIXED

**Status**: No action required - already properly implemented

**Analysis**:
Each watcher (D-Bus, journal, udev) receives a unique bootstrap ID through the `new_watcher_material()` function in `unified_processor.rs`:

- D-Bus watcher: `"system.dbus"` → unique material ID
- Journal watcher: `"system.unified_journal"` → unique material ID
- udev watcher: `"system.udev"` → unique material ID

**Evidence**:
```rust
// unified_processor.rs:244-249
async fn new_watcher_material(
    &self,
    watcher: &'static str,
) -> NodeResult<WatcherMaterialContext> {
    let source_identifier = format!("system.{}", watcher);
    WatcherMaterialContext::new(acquisition, &source_identifier, metadata).await
}
```

Each watcher's events include the unique material_id in their provenance, properly preserving source information.

---

## Issue 100: Buffered Slice Cleanup Incomplete ✅ NOT APPLICABLE

**Status**: No action required - cleanup is handled at SDK/core level

**Analysis**:
The system nodes use `append_slice` for streaming event payloads, but cleanup is properly handled by:

1. Material finalization on shutdown (`unified_processor.rs:407-411`)
2. Watcher handle cleanup (`unified_processor.rs:417-424`)
3. Graceful shutdown with cursor flushing (`unified_journal_watcher.rs:867-870`)

Buffer management is the responsibility of `AcquisitionManager` and core infrastructure, not individual nodes.

---

## Improvements Applied

### 1. Made D-Bus Timeouts Configurable ✅

**Files Modified**:
- `crate/nodes/sinex-system-ingestor/src/payloads.rs`
- `crate/nodes/sinex-system-ingestor/src/dbus_watcher.rs`

**Changes**:

Added configuration fields to `DbusConfig`:
```rust
/// Connection health check interval in seconds (default: 5s)
pub health_check_interval_secs: Seconds,
/// Inactivity timeout before reconnection in seconds (default: 30s)
pub inactivity_timeout_secs: Seconds,
```

Updated default implementation:
```rust
health_check_interval_secs: Seconds::from_secs(5),
inactivity_timeout_secs: Seconds::from_secs(30),
```

Updated `dbus_watcher.rs` to use these configuration values instead of hardcoded timeouts:
```rust
let health_check_interval = Duration::from_secs(config.health_check_interval_secs.as_secs());
let inactivity_timeout = Duration::from_secs(config.inactivity_timeout_secs.as_secs());
```

**Benefits**:
- Production deployments can tune these values based on system load
- Slower systems can increase timeouts to avoid false reconnections
- Busy systems can decrease timeouts for faster failure detection

---

### 2. Made Cursor Flush Thresholds Configurable ✅

**Files Modified**:
- `crate/nodes/sinex-system-ingestor/src/payloads.rs`
- `crate/nodes/sinex-system-ingestor/src/unified_journal_watcher.rs`

**Changes**:

Added configuration fields to `JournalConfig`:
```rust
/// Cursor flush event threshold (default: 100 events)
/// Cursor is flushed to disk after this many events
pub cursor_flush_event_threshold: u64,
/// Cursor flush interval in seconds (default: 10s)
/// Cursor is flushed to disk after this interval even if threshold not reached
pub cursor_flush_interval_secs: Seconds,
```

Updated default implementation:
```rust
cursor_flush_event_threshold: 100,
cursor_flush_interval_secs: Seconds::from_secs(10),
```

Updated `unified_journal_watcher.rs` to use these configuration values:
```rust
let event_threshold = self.journal_config.cursor_flush_event_threshold;
let time_threshold = std::time::Duration::from_secs(
    self.journal_config.cursor_flush_interval_secs.as_secs(),
);

count >= event_threshold || elapsed >= time_threshold
```

**Benefits**:
- High-throughput systems can increase thresholds to reduce disk I/O
- Low-throughput systems can decrease thresholds for more frequent checkpointing
- Better control over cursor persistence trade-offs

---

### 3. Improved D-Bus Message Drop Logging ✅

**Files Modified**:
- `crate/nodes/sinex-system-ingestor/src/dbus_watcher.rs`

**Changes**:

Changed log level from `debug!` to `warn!` for dropped messages:

**Before**:
```rust
debug!("D-Bus message dropped after backpressure: {}", e);
```

**After**:
```rust
warn!(
    "D-Bus message channel at capacity, dropping message due to backpressure: {}",
    e
);
```

**Benefits**:
- Production monitoring can alert on dropped messages
- Better visibility into backpressure conditions
- Helps identify when DBUS_MESSAGE_CHANNEL_SIZE needs tuning

---

### 4. Added Documentation for Hardcoded Timeouts ✅

**Files Modified**:
- `crate/nodes/sinex-system-ingestor/src/dbus_watcher.rs`
- `crate/nodes/sinex-system-ingestor/src/unified_journal_watcher.rs`
- `crate/nodes/sinex-system-ingestor/src/systemd_integration.rs`
- `crate/nodes/sinex-system-ingestor/src/unified_processor.rs`

**Changes**:

#### D-Bus Retry Strategy
```rust
// Retry strategy: exponential backoff starting at 1s, capped at 30s, max 5 attempts
// This handles transient D-Bus connection failures (service restarts, socket issues)
let retry_strategy = ExponentialBackoff::from_millis(1000)
    .max_delay(Duration::from_secs(30))
    .take(5);
```

#### Journal Watcher Shutdown
```rust
// Try graceful shutdown with 30s timeout
// journalctl should respond quickly to SIGTERM, but we allow time for buffered writes
match tokio::time::timeout(tokio::time::Duration::from_secs(30), child.wait()).await
```

#### Systemd Polling
```rust
// Poll interval: 100ms provides responsive state change detection
// without excessive CPU usage (10 checks/sec is reasonable for systemd units)
tokio::time::sleep(Duration::from_millis(100)).await;
```

#### Continuous Scan Safety Backoff
```rust
// Safety backoff: 5s sleep prevents tight loops if watchers fail repeatedly
// This protects against CPU thrashing during cascading failures
_ = tokio::time::sleep(tokio::time::Duration::from_secs(5)) => {}
```

**Benefits**:
- Future maintainers understand the rationale for each timeout
- Code review can validate timeout choices against documented intent
- Easier to identify which timeouts should be configurable in the future

---

## Files Modified

1. `crate/nodes/sinex-system-ingestor/src/payloads.rs`
   - Added `health_check_interval_secs` and `inactivity_timeout_secs` to `DbusConfig`
   - Added `cursor_flush_event_threshold` and `cursor_flush_interval_secs` to `JournalConfig`
   - Updated `Default` implementations for both configs

2. `crate/nodes/sinex-system-ingestor/src/dbus_watcher.rs`
   - Updated connection health check to use configurable timeouts
   - Improved message drop logging from `debug!` to `warn!`
   - Added documentation for retry strategy

3. `crate/nodes/sinex-system-ingestor/src/unified_journal_watcher.rs`
   - Updated cursor flush logic to use configurable thresholds
   - Added documentation for graceful shutdown timeout

4. `crate/nodes/sinex-system-ingestor/src/systemd_integration.rs`
   - Added documentation for polling interval

5. `crate/nodes/sinex-system-ingestor/src/unified_processor.rs`
   - Added documentation for continuous scan safety backoff

---

## Configuration Examples

### Tuning for High-Throughput System

```rust
DbusConfig {
    health_check_interval_secs: Seconds::from_secs(10),  // Check less often
    inactivity_timeout_secs: Seconds::from_secs(60),     // Longer timeout
    ..Default::default()
}

JournalConfig {
    cursor_flush_event_threshold: 500,                    // More events before flush
    cursor_flush_interval_secs: Seconds::from_secs(30),  // Longer interval
    ..Default::default()
}
```

### Tuning for Low-Latency System

```rust
DbusConfig {
    health_check_interval_secs: Seconds::from_secs(2),   // Check more often
    inactivity_timeout_secs: Seconds::from_secs(10),     // Shorter timeout
    ..Default::default()
}

JournalConfig {
    cursor_flush_event_threshold: 50,                     // Fewer events before flush
    cursor_flush_interval_secs: Seconds::from_secs(5),   // Shorter interval
    ..Default::default()
}
```

---

## Testing Recommendations

1. **Configuration Validation**
   - Test that new configuration fields are properly parsed from config files
   - Verify default values are applied when fields are omitted

2. **Runtime Behavior**
   - Test D-Bus reconnection with custom timeout values
   - Verify cursor flush behavior with different thresholds
   - Monitor logs for improved message drop visibility

3. **Performance Impact**
   - Measure cursor flush I/O impact with different thresholds
   - Monitor CPU usage with different health check intervals
   - Validate that backpressure warnings appear in production monitoring

---

## Conclusion

All identified tactical issues have been addressed:

✅ **Issue 48**: Already fixed - unique provenance per watcher
✅ **Issue 100**: Not applicable - cleanup handled at SDK level
✅ **Timeout Configurability**: D-Bus and cursor flush timeouts now configurable
✅ **Logging Visibility**: Message drops now logged at `warn!` level
✅ **Documentation**: All hardcoded timeouts now documented with rationale

The system node is production-ready with improved operational flexibility and maintainability.
