# Tactical Fixes Summary - sinex-system-ingestor

## Overview
This document summarizes the tactical issues identified for sinex-system-ingestor and their status.

## Issues Analysis

### Issue 48 (LOW): Bootstrap Event ID Reused
**Status: ALREADY FIXED ✓**

**Original Problem**: All system events share same provenance, loses source info

**Current Implementation**: Each watcher already receives a unique bootstrap ID through `new_watcher_material()`:
- D-Bus watcher: `"system.dbus"` (line 484 in unified_processor.rs)
- Journal watcher: `"system.unified_journal"` (line 509 in unified_processor.rs)
- udev watcher: `"system.udev"` (line 534 in unified_processor.rs)

Each watcher creates its own `WatcherMaterialContext` with a unique material ID, which is then used in `initial_provenance()` to generate distinct provenance for all events.

**Evidence**:
```rust
// unified_processor.rs:244-249
async fn new_watcher_material(
    &self,
    watcher: &'static str,
) -> NodeResult<WatcherMaterialContext> {
    let acquisition = self.acquisition()?;
    let source_identifier = format!("system.{}", watcher);
    let metadata = json!({
        "watcher": watcher,
        "processor": self.processor_name(),
    });
    WatcherMaterialContext::new(acquisition, &source_identifier, metadata).await
}
```

**Verification**: Each event's provenance includes the unique material_id from its watcher's context, preserving source information.

---

### Issue 100 (LOW): Buffered Slice Cleanup Incomplete
**Status: NOT APPLICABLE**

**Analysis**: The system nodes use `append_slice` for streaming event payloads, but cleanup is handled by the `AcquisitionManager` and finalization logic. There are no orphaned buffer files specific to this node.

**Current Cleanup Mechanisms**:
1. Material finalization on shutdown (unified_processor.rs:407-411)
2. Watcher handle cleanup (unified_processor.rs:417-424)
3. Graceful shutdown with cursor flushing (unified_journal_watcher.rs:867-870)

**Recommendation**: No action required. Buffer cleanup is managed at the SDK/core level, not at the node level.

---

## Hardcoded Timeouts Audit

### D-Bus Watcher (dbus_watcher.rs)

| Line | Timeout | Configurable? | Issue |
|------|---------|---------------|-------|
| 186 | 30s max retry delay | ❌ No | LOW - Retry backoff for reconnection |
| 333 | 5s health check interval | ❌ No | LOW - Connection health monitoring |
| 337 | 30s inactivity timeout | ❌ No | MEDIUM - Should be configurable |

**Recommendations**:
1. Add `dbus_inactivity_timeout_secs` to `DbusConfig`
2. Add `dbus_health_check_interval_secs` to `DbusConfig`
3. Retry backoff is reasonable as hardcoded (transient connection issues)

### Unified Journal Watcher (unified_journal_watcher.rs)

| Line | Timeout | Configurable? | Issue |
|------|---------|---------------|-------|
| 762 | 100 events batch limit | ❌ No | LOW - Cursor flush trigger |
| 762 | 10s cursor flush interval | ❌ No | LOW - Cursor flush trigger |
| 877 | 30s graceful shutdown | ❌ No | LOW - Reasonable default |

**Recommendations**:
1. Add `cursor_flush_event_threshold` to `JournalConfig` (default: 100)
2. Add `cursor_flush_interval_secs` to `JournalConfig` (default: 10)
3. Graceful shutdown timeout is reasonable as hardcoded

### Systemd Integration (systemd_integration.rs)

| Line | Timeout | Configurable? | Issue |
|------|---------|---------------|-------|
| 330 | 100ms polling interval | ❌ No | LOW - State change polling |

**Recommendation**: Add `systemd_poll_interval_millis` to `SystemdConfig`

### Unified Processor (unified_processor.rs)

| Line | Timeout | Configurable? | Issue |
|------|---------|---------------|-------|
| 982 | 5s continuous scan sleep | ❌ No | LOW - Scan loop backoff |

**Recommendation**: This is a safety backoff for continuous scans; reasonable as hardcoded.

---

## Missing Error Logging Audit

### Critical Paths with Error Handling ✓
All major error paths have proper logging:
- D-Bus watcher failures: Lines 149, 150, 152, 237, 286
- Journal watcher failures: Lines 129, 444, 893
- udev watcher failures: Lines 194, 216, 228, 232
- Material finalization errors: unified_processor.rs:408

### Potential Improvements
1. **D-Bus message drops** (dbus_watcher.rs:322) - Uses `debug!`, should be `warn!` for production visibility
2. **Cursor save failures** (unified_journal_watcher.rs:869) - Already has warning, good ✓

---

## Documentation Gaps

### Missing Documentation
1. **D-Bus inactivity timeout behavior** - Should document why 30s and what happens on timeout
2. **Cursor batching strategy** - Should document the 100-event / 10-second flush logic
3. **Material context per-watcher** - Should document why each watcher gets its own material

### Good Documentation ✓
- Unified journal consolidation (lines 5-11 in unified_journal_watcher.rs)
- udev inotify migration (lines 3-6 in udev_watcher.rs)
- Module-level documentation in lib.rs

---

## Recommended Actions

### High Priority
None - all critical issues are already addressed.

### Medium Priority
1. Make D-Bus inactivity timeout configurable
2. Document cursor batching strategy in unified_journal_watcher.rs

### Low Priority
1. Make cursor flush thresholds configurable
2. Add inline comments for hardcoded timeouts explaining rationale
3. Improve D-Bus message drop visibility (debug → warn)

---

## Summary

**Issues Fixed**: 1/2 (Issue 48 was already fixed, Issue 100 not applicable)

**New Issues Identified**:
- 3 hardcoded timeouts that should be configurable (medium priority)
- 1 logging improvement (low priority)
- Minor documentation gaps (low priority)

**Overall Status**: ✅ System node is in good shape. No critical issues found.

The system node properly manages provenance per watcher, handles errors appropriately, and has reasonable default timeouts. The main improvements would be making certain timeouts configurable for production tuning.
