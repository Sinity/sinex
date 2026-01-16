# Tactical Issues Backlog
**Extracted from:** `claude/deep-analysis-master-summary.md` (Nov 2025)
**Last Updated:** 2025-01-15
**Status:** Unresolved issues requiring implementation

---

## Overview

This document contains **140+ actionable tactical issues** extracted from the comprehensive deep analysis. These are implementation-level bugs, performance improvements, and operational gaps that should be addressed during Phase 5 (Ongoing Polish) of the unified plan.

**Excluded issues:**
- Issue 6: Advisory lock lost detection (✅ Resolved by KV migration)
- Coordination DB table issues (✅ Resolved by KV migration)

**Organization:** Issues are grouped by subsystem for easier planning and assignment.

---

## Event Flow & NATS JetStream (9 issues)

### Issue 1: No Backpressure on Event Publishing
- **File:** `crate/lib/sinex-node-sdk/src/nats_publisher.rs:54`
- **Severity:** HIGH
- **Impact:** node can hang if NATS is slow/down
- **Details:** Double-await pattern with no timeout on JetStream ack
- **Recommendation:** Add 5-10 second timeout, bounded publish queue

### Issue 2: Confirmation Timeout Silent Failures
- **File:** `crate/lib/sinex-node-sdk/src/confirmation_handler.rs:108`
- **Severity:** MEDIUM
- **Code:** `age.to_std().unwrap_or_default()`
- **Impact:** Clock skew causes false timeouts, no logging
- **Recommendation:** Explicit error handling with logging

### Issue 3: Stream Capacity Monitoring Missing
- **File:** `crate/core/sinex-ingestd/src/jetstream_consumer.rs:138`
- **Severity:** MEDIUM
- **Impact:** Events could be dropped silently when streams fill (10M limit)
- **Recommendation:** Add metrics, alert at 80% capacity

### Issue 76: NATS Batch Processing No Backpressure
- **File:** `crate/core/sinex-ingestd/src/material_assembler.rs:914-920`
- **Severity:** HIGH
- **Impact:** Memory exhaustion on message flood (batches of 200 with no rate limiting)
- **Recommendation:** Add rate limiting or bounded semaphore

### Issue 84: Panics in Spawned Tasks Not Propagated
- **File:** `crate/lib/sinex-node-sdk/src/coordination.rs:759`
- **Severity:** HIGH
- **Impact:** Heartbeat stops silently, no alerts
- **Recommendation:** Spawn with abort guard or periodic health check

### Issue 85: Material Assembler Consumer Panics Lose Data
- **File:** `crate/core/sinex-ingestd/src/material_assembler.rs:1179-1195`
- **Severity:** MEDIUM
- **Impact:** In-flight materials lost, no DLQ routing
- **Recommendation:** Restart consumers on panic, route failures to DLQ

### Issue 87: Abort Without Graceful Shutdown
- **File:** `crate/core/sinex-ingestd/src/material_assembler.rs:1181-1182`
- **Severity:** MEDIUM
- **Impact:** In-flight messages not acked, NATS redelivery
- **Recommendation:** Send shutdown signal, await graceful completion with timeout

### Issue 88: Lifecycle Heartbeat Abort Doesn't Flush
- **File:** `crate/lib/sinex-node-sdk/src/lifecycle.rs:320-322`
- **Severity:** LOW
- **Impact:** Last heartbeat metrics not emitted
- **Recommendation:** Emit final heartbeat before abort

### Issue 97: Lifecycle Status Change Ordering
- **File:** `crate/lib/sinex-node-sdk/src/lifecycle.rs:105-140`
- **Severity:** LOW
- **Impact:** Systemd might see old status if queried between updates
- **Recommendation:** Accept best-effort systemd notification

---

## Coordination & Distributed Systems (5 issues)

### Issue 4: WorkTracker Drain Has No Timeout
- **File:** `crate/lib/sinex-node-sdk/src/coordination.rs:98`
- **Severity:** HIGH
- **Impact:** Graceful shutdown can hang indefinitely
- **Recommendation:** Add configurable drain timeout with force-shutdown

### Issue 5: HandoffRequest Not Fully Implemented
- **File:** `crate/lib/sinex-node-sdk/src/coordination.rs:28`
- **Severity:** MEDIUM
- **Impact:** Version upgrades may not be zero-downtime
- **Status:** Struct defined, channel exists, but no send/receive logic
- **Recommendation:** Complete implementation or remove

### Issue 7: Clock Skew Between Client/Server ULIDs
- **File:** Multiple (ULID generation sites)
- **Severity:** MEDIUM
- **Impact:** Event ordering violations if clocks skewed
- **Recommendation:** Prefer DB-side generation, add skew detection

### Issue 90: Coordination Mode Check TOCTOU
- **File:** `crate/lib/sinex-node-sdk/src/coordination.rs:169-184`
- **Severity:** HIGH
- **Impact:** Two instances could both become leader
- **Recommendation:** Check mode INSIDE leadership acquisition transaction

### Issue 96: Coordination Shutdown Signal Ordering
- **File:** `crate/lib/sinex-node-sdk/src/coordination.rs:552-575`
- **Severity:** MEDIUM
- **Impact:** Standbys might not see failure signal in database
- **Recommendation:** Reverse order: poll database in monitoring loop

---

## Monitoring & Checkpoints (5 issues)

### Issue 8: Heartbeat Error Window Amnesia
- **File:** `crate/lib/sinex-node-sdk/src/heartbeat.rs:151-159`
- **Severity:** MEDIUM
- **Impact:** Error bursts appear fine 60s later, no historical context
- **Recommendation:** Implement 5-minute sliding window

### Issue 9: Hardcoded Health Thresholds
- **File:** `crate/lib/sinex-node-sdk/src/heartbeat.rs:151-159`
- **Severity:** LOW
- **Code:** `if recent_errors > 50` / `> 10`
- **Impact:** Cannot tune per-service, false alarms
- **Recommendation:** Make configurable via environment/config

### Issue 10: Resource Monitoring Silent Failures
- **File:** `crate/lib/sinex-node-sdk/src/heartbeat.rs:162`
- **Severity:** LOW
- **Impact:** Returns 0 on failure without logging
- **Recommendation:** Return `Option<T>`, log parse failures

### Issue 11: Checkpoint Type Auto-Detection Risk
- **File:** `crate/lib/sinex-node-sdk/src/checkpoint.rs:154`
- **Severity:** MEDIUM
- **Impact:** Stream ID that's valid ULID misclassified as Internal
- **Recommendation:** Explicit checkpoint type parameter

### Issue 12: No Checkpoint Cleanup
- **Severity:** LOW
- **Impact:** Table bloat over time from inactive processors
- **Recommendation:** 30-day TTL cleanup task

---

## Material Assembly & Blob System (9 issues)

### Issue 13: Unbounded Slice Buffer
- **File:** `crate/core/sinex-ingestd/src/material_assembler.rs:48`
- **Severity:** HIGH
- **Impact:** DoS via memory exhaustion, out-of-order slices accumulate indefinitely
- **Details:** `buffered_slices: BTreeMap<i64, PathBuf>` has no size limit
- **Recommendation:** Add MAX_BUFFERED_SLICES = 100, route to DLQ when exceeded

### Issue 14: No Slice Arrival Timeout
- **File:** `crate/core/sinex-ingestd/src/material_assembler.rs:180`
- **Severity:** MEDIUM
- **Impact:** Incomplete assemblies leak resources, never finalized or cleaned up
- **Recommendation:** 5-minute timeout, cleanup task for stale assemblies

### Issue 15: No git-annex Command Timeout
- **File:** `crate/lib/sinex-node-sdk/src/annex/git_annex.rs:98`
- **Severity:** MEDIUM
- **Impact:** `git-annex add` can hang indefinitely on large files or network issues
- **Recommendation:** Add 60-second timeout using tokio::time::timeout

### Issue 16: Missing Assembly Metrics
- **Severity:** LOW
- **Impact:** No observability into assembly progress, failures, or performance
- **Recommendation:** Add metrics for assembly duration, slice count, failures

### Issue 17: No Cleanup of Failed Assemblies
- **File:** `crate/core/sinex-ingestd/src/material_assembler.rs:192`
- **Severity:** MEDIUM
- **Impact:** Failed temp files accumulate in filesystem
- **Recommendation:** Explicit cleanup on errors, periodic temp dir scan

### Issue 18: BLAKE3 Hash Collision Handling
- **File:** `crate/lib/sinex-node-sdk/src/annex/blob_manager.rs:210`
- **Severity:** LOW
- **Impact:** Theoretically possible collision treated as duplicate (cryptographically unlikely)
- **Recommendation:** Document collision handling, consider size verification

### Issue 91: Material Assembler State Check TOCTOU
- **File:** `crate/core/sinex-ingestd/src/material_assembler.rs:382-389`
- **Severity:** MEDIUM
- **Impact:** Duplicate material states on concurrent begin messages
- **Recommendation:** Use entry() API for atomic check-and-insert

### Issue 93: Assembler State Not Synchronized with File Writes
- **File:** `crate/core/sinex-ingestd/src/material_assembler.rs:434-538`
- **Severity:** HIGH
- **Impact:** Crash between write and state update loses data
- **Recommendation:** Write-ahead log or atomic batch updates

### Issue 98: Potential Arc Cycle in MaterialAssembler
- **File:** `crate/core/sinex-ingestd/src/material_assembler.rs:1115-1126`
- **Severity:** HIGH
- **Impact:** Circular Arc references prevent cleanup
- **Recommendation:** Use Weak references in spawned tasks

---

## FS-Watcher node (8 issues)

### Issue 19: Event Queue Overflow
- **File:** `crate/nodes/sinex-fs-watcher/src/unified_processor.rs:124`
- **Severity:** HIGH
- **Impact:** Events silently dropped when 256-event buffer fills
- **Code:** `let (tx, mut rx) = mpsc::channel::<Event>(256);`
- **Recommendation:** Increase to 10,000, add dropped_events metric, use try_send

### Issue 21: TOCTOU Race in File Size Check
- **File:** `crate/nodes/sinex-fs-watcher/src/unified_processor.rs:178`
- **Severity:** MEDIUM
- **Impact:** File can grow between size check and read, violating max_capture_bytes
- **Recommendation:** Stream reading with cumulative size check

### Issue 22: No Retry on Transient Errors
- **File:** `crate/nodes/sinex-fs-watcher/src/unified_processor.rs:185`
- **Severity:** MEDIUM
- **Impact:** Transient read errors (file locked, in-use) cause permanent event loss
- **Recommendation:** Exponential backoff retry (3 attempts, 100ms/500ms/1s)

### Issue 23: Max Capture Bytes Not Atomic
- **File:** `crate/nodes/sinex-fs-watcher/src/unified_processor.rs:178`
- **Severity:** MEDIUM
- **Impact:** Large file partially captured before size check
- **Recommendation:** Check size before any read operation

### Issue 24: Missing Event Processing Metrics
- **Severity:** LOW
- **Impact:** No visibility into event rates, processing latency, drop rates
- **Recommendation:** Add comprehensive metrics for observability

### Issue 86: Filesystem Watcher Error Not Retried
- **File:** `crate/nodes/sinex-fs-watcher/src/unified_processor.rs:258-261`
- **Severity:** LOW
- **Impact:** Partial filesystem coverage if some paths fail to watch
- **Recommendation:** Exponential backoff retry for watcher errors

### Issue 89: Watch Handles Not Awaited on Shutdown
- **File:** `crate/nodes/sinex-fs-watcher/src/unified_processor.rs:408-411`
- **Severity:** HIGH
- **Impact:** File descriptors leaked, inotify watches remain
- **Recommendation:** Join after abort to ensure cleanup

### Issue 92: Filesystem Metadata TOCTOU
- **File:** `crate/nodes/sinex-fs-watcher/src/unified_processor.rs:565-588`
- **Severity:** LOW
- **Impact:** File could change/delete between check and read
- **Recommendation:** Open file, fstat, then read to ensure atomicity

---

## Terminal node (5 issues)

### Issue 25: Fish/Elvish History Not Supported
- **File:** `crate/nodes/sinex-terminal-node/src/shell_detection.rs:48`
- **Severity:** MEDIUM
- **Impact:** Fish/Elvish users get no terminal event capture
- **Details:** Fish uses SQLite, Elvish uses custom binary format, not plain text
- **Recommendation:** Implement SQLite parser for Fish, document Elvish limitation

### Issue 27: Polling Delay Latency
- **File:** `crate/nodes/sinex-terminal-node/src/unified_processor.rs:98`
- **Severity:** LOW
- **Impact:** 15-second default polling creates 0-15s capture latency
- **Details:** Commands not captured until next poll cycle
- **Recommendation:** Reduce to 5s or use inotify for real-time capture

### Issue 28: No Atomic State Persistence
- **File:** `crate/nodes/sinex-terminal-node/src/unified_processor.rs:156`
- **Severity:** MEDIUM
- **Impact:** State file corruption on crash loses position, may duplicate events
- **Recommendation:** Atomic write via temp file + rename

### Issue 29: No Terminal Event Metrics
- **Severity:** LOW
- **Impact:** No visibility into command rates, shell types, polling performance
- **Recommendation:** Add metrics for observability

### Issue 30: No Command Validation
- **File:** `crate/nodes/sinex-terminal-node/src/unified_processor.rs:218`
- **Severity:** LOW
- **Impact:** Malformed history lines (binary data, null bytes) processed as-is
- **Recommendation:** Validate UTF-8, reject binary data, add validation metrics

---

## Desktop node (6 issues)

### Issue 31: Clipboard Polling Latency
- **File:** `crate/nodes/sinex-desktop-node/src/clipboard.rs:116`
- **Severity:** MEDIUM
- **Impact:** 2-second polling = up to 2s capture latency, poor UX
- **Recommendation:** Reduce to 100ms or implement event-driven monitoring

### Issue 32: No Timeout on External Commands
- **File:** `crate/nodes/sinex-desktop-node/src/clipboard.rs:510`
- **Severity:** HIGH
- **Impact:** wl-paste/xclip/hyprctl can hang indefinitely, blocking monitoring
- **Recommendation:** Add 5-second timeout on all external commands

### Issue 35: No Clipboard Content Validation
- **File:** `crate/nodes/sinex-desktop-node/src/clipboard.rs:466`
- **Severity:** MEDIUM
- **Impact:** Binary data processed as text, potential corruption
- **Recommendation:** Validate UTF-8, check for null bytes, detect binary

### Issue 36: Single Window Manager Support
- **File:** `crate/nodes/sinex-desktop-node/src/window_manager.rs:16`
- **Severity:** MEDIUM
- **Impact:** Only Hyprland supported, unusable for most Linux users
- **Recommendation:** Add support for Sway, i3, GNOME, KDE

### Issue 37: No Unix Socket Read Timeout
- **File:** `crate/nodes/sinex-desktop-node/src/window_manager.rs:524`
- **Severity:** HIGH
- **Impact:** next_line() can block indefinitely, silent monitoring failure
- **Recommendation:** Add 30-second timeout with automatic reconnection

### Issue 38: Unbounded Window State Growth
- **File:** `crate/nodes/sinex-desktop-node/src/window_manager.rs:81`
- **Severity:** MEDIUM
- **Impact:** Missed closewindow events cause memory leak
- **Recommendation:** 48-hour TTL

---

## System node (9 issues)

### Issue 41: Duplicate journalctl Processes
- **File:** `journal_watcher.rs:273` + `systemd_watcher.rs:354`
- **Severity:** MEDIUM
- **Impact:** Two journalctl processes doing nearly identical work
- **Recommendation:** Consolidate into single JournalWatcher

### Issue 42: Udev 5-Second Polling
- **File:** `crate/nodes/sinex-system-node/src/udev_watcher.rs:177`
- **Severity:** HIGH
- **Impact:** Misses transient devices, 0-5s latency, inefficient
- **Recommendation:** Use inotify on /sys/class for real-time detection

### Issue 45: No D-Bus Message Read Timeout
- **File:** `crate/nodes/sinex-system-node/src/dbus_watcher.rs:~241`
- **Severity:** HIGH
- **Impact:** conn.next_msg() can block indefinitely
- **Recommendation:** Add 30-second timeout with reconnection

### Issue 46: Journal Cursor Saved on Every Event
- **File:** `crate/nodes/sinex-system-node/src/journal_watcher.rs:~350`
- **Severity:** MEDIUM
- **Impact:** Filesystem write per event, performance degradation
- **Recommendation:** Batch cursor saves (every 10s or 100 events)

### Issue 47: D-Bus Message Buffer Overflow
- **File:** `crate/nodes/sinex-system-node/src/dbus_watcher.rs:244`
- **Severity:** MEDIUM
- **Impact:** 1000-message buffer fills on busy systems
- **Recommendation:** Increase to 10,000, monitor buffer fill

### Issue 48: Bootstrap Event ID Reused
- **File:** All system node watchers
- **Severity:** LOW
- **Impact:** All system events share same provenance, loses source info
- **Recommendation:** Unique bootstrap ID per watcher

### Issue 49: No Atomic Cursor Persistence
- **File:** `crate/nodes/sinex-system-node/src/journal_watcher.rs:save_cursor`
- **Severity:** MEDIUM
- **Impact:** Crash during write = corrupted cursor, duplicate events
- **Recommendation:** Atomic write via temp file + rename

### Issue 99: Temp File Cleanup on Panic
- **File:** `crate/core/sinex-ingestd/src/material_assembler.rs:410-412`
- **Severity:** MEDIUM
- **Impact:** Temp directory fills over time
- **Recommendation:** Implement Drop guard for SourceMaterialHandle

### Issue 100: Buffered Slice Cleanup Incomplete
- **File:** `crate/core/sinex-ingestd/src/material_assembler.rs:502-509`
- **Severity:** LOW
- **Impact:** Orphaned buffer files accumulate
- **Recommendation:** Track failed cleanups, retry on next assembly

---

## Database Patterns & Query Optimization (23 issues)

### Issue 51: Format! for Query Building
- **File:** `crate/lib/sinex-core/src/db/repositories/common.rs:89`
- **Severity:** MEDIUM
- **Impact:** Safe here (compile-time constants), but sets dangerous precedent
- **Recommendation:** Add safety documentation

### Issue 52: BatchRepository Trait Unused
- **File:** `crate/lib/sinex-core/src/db/repositories/common.rs:126`
- **Severity:** LOW
- **Impact:** Dead code suggests incomplete bulk operation support
- **Recommendation:** Implement or remove

### Issue 53: Rollback Error Ignored
- **File:** `crate/lib/sinex-core/src/db/repositories/common.rs:165`
- **Severity:** MEDIUM
- **Code:** `let _ = tx.rollback().await;`
- **Impact:** Silent rollback failures
- **Recommendation:** Log rollback errors

### Issue 54: Macro Doesn't Enforce Schema Changes
- **File:** `crate/lib/sinex-core/src/db/repositories/events.rs:15`
- **Severity:** LOW
- **Impact:** Schema changes require manual macro updates
- **Recommendation:** Consider code generation

### Issue 55: Test Code in Production Path
- **File:** `crate/lib/sinex-core/src/db/repositories/events.rs:444`
- **Severity:** MEDIUM
- **Impact:** Bootstrap material insert in production code, error ignored
- **Recommendation:** Move to test utilities

### Issue 56: Pool Clone for Each Chunk
- **File:** `crate/lib/sinex-core/src/db/repositories/events.rs:970`
- **Severity:** MEDIUM
- **Impact:** Unnecessary Arc clones per batch chunk
- **Recommendation:** Pass &PgPool directly

### Issue 57: No Progress Reporting for Large Batches
- **File:** `crate/lib/sinex-core/src/db/repositories/events.rs:935`
- **Severity:** LOW
- **Impact:** Inserting 10,000 events = silent operation
- **Recommendation:** Emit metrics every 1000 events

### Issue 58: ILIKE on Payload Text is Slow
- **File:** `crate/lib/sinex-core/src/db/repositories/events.rs:811`
- **Severity:** HIGH
- **Code:** `AND payload::text ILIKE '%term%'`
- **Impact:** Full table scan on large datasets
- **Recommendation:** Use GIN index + to_tsvector()

### Issue 59: No Query Timeout
- **File:** All repositories
- **Severity:** MEDIUM
- **Impact:** Long-running queries block connection pool
- **Recommendation:** Set statement_timeout globally or per-query

### Issue 60: No TimescaleDB Retention Policy
- **File:** `crate/lib/sinex-schema/src/schema/events.rs:148`
- **Severity:** HIGH
- **Impact:** 90-day retention documented but not enforced, data accumulates indefinitely
- **Recommendation:** `SELECT add_retention_policy('core.events', INTERVAL '90 days');`

### Issue 61: No Chunk Size Configuration
- **File:** TimescaleDB hypertable
- **Severity:** MEDIUM
- **Impact:** Default 7-day chunks may not be optimal
- **Recommendation:** Analyze query patterns and set explicit interval

### Issue 62: Missing ts_ingest Index
- **File:** `crate/lib/sinex-schema/src/schema/events.rs:154`
- **Severity:** MEDIUM
- **Impact:** Most queries filter on ts_ingest but only ts_orig is indexed
- **Recommendation:** Add ix_events_ts_ingest DESC index

### Issue 63: Operation ID Can Be Forged
- **File:** `crate/lib/sinex-schema/src/schema/events.rs:255` (archive trigger)
- **Severity:** MEDIUM
- **Impact:** Any code can set sinex.operation_id and delete events
- **Recommendation:** Add pg_authid check or cryptographic signature

### Issue 64: No FK to operations_log
- **File:** `core.events` table schema
- **Severity:** LOW
- **Impact:** Events can reference non-existent operations
- **Recommendation:** Add optional operation_id column with FK

### Issue 65: Hardcoded Connection Math
- **File:** `crate/lib/sinex-test-utils/src/database_pool.rs:263`
- **Severity:** MEDIUM
- **Impact:** 480 connection budget doesn't adapt to PostgreSQL max_connections
- **Recommendation:** Query SHOW max_connections dynamically

### Issue 66: Infinite Loop on Database Acquisition
- **File:** `crate/lib/sinex-test-utils/src/database_pool.rs:797`
- **Severity:** HIGH
- **Impact:** Tests can hang forever if all slots permanently locked
- **Recommendation:** Add max attempts limit (currently has counter but doesn't exit)

### Issue 67: Lock Verification Race Window
- **File:** `crate/lib/sinex-test-utils/src/database_pool.rs:836`
- **Severity:** LOW
- **Impact:** Lock released between acquisition and verification (nanoseconds)
- **Recommendation:** Acceptable risk, or use SELECT FOR UPDATE

### Issue 68: Fingerprint Order Sensitivity
- **File:** `crate/lib/sinex-test-utils/src/database_pool.rs:165`
- **Severity:** LOW
- **Impact:** Reordering migration files = same hash but different result
- **Recommendation:** Hash (filename + content) in sorted order

### Issue 69: No Stamp File Cleanup
- **File:** `template_stamp.json`
- **Severity:** LOW
- **Impact:** Stamp files accumulate in target/ directory
- **Recommendation:** Cleanup files >7 days old

### Issue 70: FK Drop is Permanent
- **File:** `crate/lib/sinex-test-utils/src/database_pool.rs:992`
- **Severity:** MEDIUM
- **Impact:** legacy checkpoint FK dropped and never restored
- **Recommendation:** Use SET CONSTRAINTS DEFERRED instead

### Issue 71: No Cycle Detection in Cascade
- **File:** `core.expand_cascade` function
- **Severity:** HIGH
- **Impact:** Circular event dependencies cause infinite loop
- **Recommendation:** Add explicit cycle detection before expansion

### Issue 72: Unbounded Array Growth
- **File:** Cascade temp table parent_ids column
- **Severity:** MEDIUM
- **Impact:** Events with many parents = large array
- **Recommendation:** Consider separate cascade_edges table

### Issue 73: Redundant Existence Check
- **File:** `crate/lib/sinex-core/src/db/repositories/state.rs:278`
- **Severity:** MEDIUM
- **Impact:** Extra query before upsert (performance waste)
- **Recommendation:** Remove check, rely on ON CONFLICT alone

---

## Concurrency Patterns & Synchronization (20 issues)

### Issue 74: Handoff Channel Size Too Small
- **File:** `crate/lib/sinex-node-sdk/src/coordination.rs:292`
- **Severity:** MEDIUM
- **Impact:** Could block handoff requests if 10+ versions deployed simultaneously
- **Recommendation:** Increase to 100 or use unbounded with monitoring

### Issue 75: FS Watcher Channel Size Arbitrary
- **File:** `crate/nodes/sinex-fs-watcher/src/unified_processor.rs:492`
- **Severity:** LOW
- **Impact:** 256 buffer could drop events on burst file activity
- **Recommendation:** Document sizing rationale or make configurable

### Issue 77: Oneshot Receivers Accumulate
- **File:** `crate/lib/sinex-node-sdk/src/lifecycle.rs:152-153`
- **Severity:** MEDIUM
- **Impact:** Memory leak on repeated initialize/shutdown cycles
- **Recommendation:** Drop old sender before creating new one

### Issue 78: Filesystem Watcher Channel Not Closed
- **File:** `crate/nodes/sinex-fs-watcher/src/unified_processor.rs:492-496`
- **Severity:** LOW
- **Impact:** Receiver might not detect end-of-stream immediately
- **Recommendation:** Handle send errors, close channel on watcher drop

### Issue 80: std::Mutex Instead of parking_lot
- **File:** `crate/lib/sinex-node-sdk/src/lifecycle.rs:50`
- **Severity:** MEDIUM
- **Impact:** Slower than parking_lot, poison handling overhead
- **Recommendation:** Use parking_lot::Mutex or AtomicU8

### Issue 81: Double Lock in Coordination
- **File:** `crate/lib/sinex-node-sdk/src/coordination.rs:658-664`
- **Severity:** LOW
- **Impact:** Takes read lock twice in loop, minor overhead
- **Recommendation:** Hold lock across check or use atomics

### Issue 82: Potential Deadlock in Poisoned Mutex Recovery
- **File:** `crate/lib/sinex-node-sdk/src/lifecycle.rs:92-100`
- **Severity:** HIGH
- **Impact:** Service hangs on concurrent status access after panic
- **Recommendation:** Use parking_lot which doesn't poison, or make status atomic

### Issue 83: Missing Lock Ordering Documentation
- **File:** Multiple files with nested locks
- **Severity:** MEDIUM
- **Impact:** Hard to verify deadlock freedom
- **Recommendation:** Document global lock ordering

### Issue 94: Work Tracker Increment/Decrement Not Paired
- **File:** `crate/lib/sinex-node-sdk/src/coordination.rs:66-79`
- **Severity:** MEDIUM
- **Impact:** Work tracker counter drift
- **Recommendation:** Return guard from start_operation that auto-finishes on drop

### Issue 95: Heartbeat Counter Reset Race
- **File:** `crate/lib/sinex-node-sdk/src/heartbeat.rs:217-221`
- **Severity:** LOW
- **Impact:** Lost event counts
- **Recommendation:** Use fetch_and_add(0) to atomically read-and-reset

### Issue 101: No Connection Timeout in DB Pool Acquisition
- **File:** `crate/lib/sinex-test-utils/src/database_pool.rs:159-186`
- **Severity:** HIGH
- **Impact:** Test hangs forever (duplicate of Issue 66)
- **Recommendation:** Add max retries or overall timeout

### Issue 103: Reference Count Leak on Panic
- **File:** `crate/lib/sinex-test-utils/src/fixtures.rs:150-180`
- **Severity:** MEDIUM
- **Impact:** If `get_or_create` panics after incrementing ref count, count is never decremented
- **Recommendation:** Use RAII guard that decrements on drop

### Issue 104: Cleanup Panic Safety
- **File:** `crate/lib/sinex-test-utils/src/fixtures.rs:210-215`
- **Severity:** MEDIUM
- **Impact:** If cleanup.run() panics, fixture remains in cache with ref count 0
- **Recommendation:** Remove from cache AFTER successful cleanup

### Issue 106: Cache Key Collision Risk
- **File:** `crate/lib/sinex-test-utils/src/fixtures.rs:320`
- **Severity:** MEDIUM
- **Impact:** Simple string concatenation for cache keys can collide
- **Recommendation:** Use structured key with type information and parameter hash

### Issue 107: No Cleanup Timeout
- **File:** `crate/lib/sinex-test-utils/src/fixtures.rs:470-480`
- **Severity:** MEDIUM
- **Impact:** Cleanup can hang indefinitely, causing CI timeout
- **Recommendation:** Add timeout with `tokio::time::timeout`

### Issue 108: Cleanup Errors Swallowed
- **File:** `crate/lib/sinex-test-utils/src/fixtures.rs:210`
- **Severity:** MEDIUM
- **Impact:** Silent cleanup failures, resource leaks
- **Recommendation:** Log cleanup errors, consider cleanup failure registry

### Issue 109: No Dependency Tracking in Composite Fixtures
- **File:** `crate/lib/sinex-test-utils/src/fixtures.rs:600-650`
- **Severity:** HIGH
- **Impact:** Use-after-cleanup, potential panics or corrupted state
- **Recommendation:** Explicit dependency graph, reference counting includes dependents

### Issue 116: Cleanup in Drop May Panic
- **File:** `crate/lib/sinex-test-utils/src/lib.rs:80-95`
- **Severity:** HIGH
- **Impact:** Drop calls `block_on` which may panic if no runtime
- **Recommendation:** Use `Handle::try_current()` and spawn blocking task

### Issue 117: TempDir Not Cleaned on Panic
- **File:** `crate/lib/sinex-test-utils/src/lib.rs:20-40`
- **Severity:** LOW
- **Impact:** `/tmp` filled with test directories over time
- **Recommendation:** Use `defer!` macro or explicit cleanup guard

### Issue 118: Transaction Timeout Not Configurable
- **File:** `crate/lib/sinex-test-utils/src/lib.rs:200-250`
- **Severity:** LOW
- **Impact:** Long-running tests may hit timeout
- **Recommendation:** Add `.with_timeout(duration)` configuration

---

## Testing Infrastructure (12 issues)

### Issue 105: No Parameter Validation in Parameterized Fixtures
- **File:** `crate/lib/sinex-test-utils/src/fixtures.rs:300-350`
- **Severity:** LOW
- **Impact:** Duplicate fixtures created for semantically identical configs
- **Recommendation:** Canonical serialization or explicit validation

### Issue 110: Insufficient Edge Case Coverage in Property Strategies
- **File:** `crate/lib/sinex-test-utils/src/property_testing.rs:1-100`
- **Severity:** MEDIUM
- **Impact:** Missing Unicode, very long strings, deeply nested JSON edge cases
- **Recommendation:** Add explicit edge case generators

### Issue 111: No ULID Strategy
- **File:** `crate/lib/sinex-test-utils/src/property_testing.rs:50-150`
- **Severity:** LOW
- **Impact:** Can't test ULID-dependent code with property tests
- **Recommendation:** Add `SinexStrategies::ulid()` strategy

### Issue 112: Malicious Payloads Not Tested in CI
- **File:** `crate/lib/sinex-test-utils/src/property_testing.rs:250-400`
- **Severity:** HIGH
- **Impact:** Security vulnerabilities not tested despite infrastructure existing
- **Recommendation:** Add adversarial property tests using malicious strategies

### Issue 113: No Fuzzing Integration
- **File:** `crate/lib/sinex-test-utils/src/property_testing.rs`
- **Severity:** MEDIUM
- **Impact:** Missing continuous fuzzing in CI
- **Recommendation:** Add `fuzz/` directory with libFuzzer harnesses

### Issue 114: No Shrinking for Async Properties
- **File:** `crate/lib/sinex-test-utils/src/property_testing.rs:600-650`
- **Severity:** MEDIUM
- **Impact:** Harder to debug property test failures
- **Recommendation:** Use `TestCaseError::fail()` with proper shrinking hints

### Issue 115: Runtime Created Per Test Case
- **File:** `crate/lib/sinex-test-utils/src/property_testing.rs:620`
- **Severity:** LOW
- **Impact:** Slow property tests (~1ms overhead per case)
- **Recommendation:** Reuse runtime or use `#[tokio::test]` directly

### Issue 119: No Builder Pattern in Factories
- **File:** `crate/lib/sinex-test-utils/src/factories.rs:1-200`
- **Severity:** LOW
- **Impact:** Boilerplate duplication when customizing factories
- **Recommendation:** Implement builder pattern

### Issue 120: No Database Property Tests
- **File:** `crate/lib/sinex-core/tests/property_tests.rs`
- **Severity:** HIGH
- **Impact:** Database bugs not caught by property tests
- **Recommendation:** Add database property tests using TestContext

### Issue 121: No NATS Property Tests
- **File:** Test suite (missing tests)
- **Severity:** HIGH
- **Impact:** Message bus bugs not tested
- **Recommendation:** Add NATS property tests

### Issue 122: No node Property Tests
- **File:** Test suite (missing tests)
- **Severity:** MEDIUM
- **Impact:** node bugs not tested with randomized inputs
- **Recommendation:** Add node property tests

### Issue 124: No Adversarial Property Tests
- **File:** Test suite (malicious strategies defined but not used)
- **Severity:** MEDIUM
- **Impact:** SQL injection, XSS, path traversal not tested
- **Recommendation:** Add adversarial property tests using `SinexStrategies::malicious_payload()`

---

## Gateway and RPC Infrastructure (27 issues)

### Issue 125: RPC Dispatcher Completely Unimplemented
- **File:** `crate/lib/sinex-processor-runtime/src/cli.rs`
- **Severity:** CRITICAL
- **Impact:** Binary exists but provides no functionality, all methods return `NotImplemented`
- **Recommendation:** Implement or remove from codebase

### Issue 126: No Timeout on NATS Replay Requests
- **File:** `crate/core/sinex-gateway/src/replay_control.rs:48-54`
- **Severity:** HIGH
- **Impact:** NATS request can hang forever if server is slow/hung
- **Recommendation:** Wrap with `tokio::time::timeout(Duration::from_secs(30), ...)`

### Issue 127: Replay Control Silently Disabled on NATS Failure
- **File:** `crate/core/sinex-gateway/src/service_container.rs:75-81`
- **Severity:** HIGH
- **Impact:** Gateway appears healthy but replay commands fail with "not initialised"
- **Recommendation:** Make replay required or expose degraded state in health endpoint

### Issue 128: No Graceful Shutdown Mechanism
- **File:** `crate/core/sinex-gateway/src/main.rs:82-124`
- **Severity:** MEDIUM
- **Impact:** Cannot gracefully stop gateway, must kill process
- **Recommendation:** Add signal handling for SIGTERM/SIGINT

### Issue 129: No Connection Pool Configuration
- **File:** `crate/core/sinex-gateway/src/service_container.rs:38`
- **Severity:** MEDIUM
- **Impact:** Uses default pool settings, may exhaust connections under load
- **Recommendation:** Expose pool configuration (max connections, acquire timeout)

### Issue 130: Annex Path Defaults to /tmp
- **File:** `crate/core/sinex-gateway/src/service_container.rs:41-47`
- **Severity:** MEDIUM
- **Impact:** Blob storage lost on system restart
- **Recommendation:** Use persistent default like `~/.local/share/sinex/annex`

### Issue 131: Hardcoded Method Dispatch Table
- **File:** `crate/core/sinex-gateway/src/rpc_server.rs:290-357`
- **Severity:** LOW
- **Impact:** Adding methods requires editing core dispatch code
- **Recommendation:** Consider registry pattern with method metadata

### Issue 132: Concurrency Limit Too Low for Production
- **File:** `crate/core/sinex-gateway/src/rpc_server.rs:85`
- **Severity:** MEDIUM
- **Impact:** Default 32 concurrent requests is conservative, 33rd gets 429
- **Recommendation:** Increase default to 100-200 or make adaptive

### Issue 133: No Metrics on Load Shedding
- **File:** `crate/core/sinex-gateway/src/rpc_server.rs:469`
- **Severity:** MEDIUM
- **Impact:** Cannot observe when gateway is rejecting requests
- **Recommendation:** Add custom layer with metrics counter

### Issue 134: Unix Socket Permission Race Window
- **File:** `crate/core/sinex-gateway/src/rpc_server.rs:810-823`
- **Severity:** LOW
- **Impact:** Microsecond window where socket has world-readable permissions
- **Recommendation:** Use umask before bind or fchmod on descriptor

### Issue 135: Stale Socket Not Detected
- **File:** `crate/core/sinex-gateway/src/rpc_server.rs:798-808`
- **Severity:** LOW
- **Impact:** Two gateways could fight over same socket path
- **Recommendation:** Use flock or check if socket is active

### Issue 136: Hardcoded 1MB Native Messaging Limit
- **File:** `crate/core/sinex-gateway/src/native_messaging.rs:183`
- **Severity:** LOW
- **Impact:** Cannot send large blobs via native messaging
- **Recommendation:** Make limit configurable via environment variable

### Issue 137: No Constant-Time Secret Comparison in Native Messaging
- **File:** `crate/core/sinex-gateway/src/native_messaging.rs:119`
- **Severity:** MEDIUM (security)
- **Impact:** Extension secret can be brute-forced via timing attack
- **Recommendation:** Use constant-time comparison like in RPC auth

### Issue 138: Default Allows All Extensions
- **File:** `crate/core/sinex-gateway/src/native_messaging.rs:76-78`
- **Severity:** MEDIUM (security)
- **Impact:** If no allowlist configured, any browser extension can access Sinex
- **Recommendation:** Fail closed - require explicit allowlist

### Issue 139: No Timeout on Native Messaging Read
- **File:** `crate/core/sinex-gateway/src/native_messaging.rs:389`
- **Severity:** LOW
- **Impact:** Hung browser extension blocks gateway forever
- **Recommendation:** Add read timeout (e.g., 60 seconds)

### Issue 140: No Service-Level Caching
- **File:** All service implementations in `sinex-services`
- **Severity:** MEDIUM
- **Impact:** Every request hits database, repeated queries waste connections
- **Recommendation:** Add Redis or in-memory cache for hot data

### Issue 141: No Request Tracing
- **File:** Service layer (entire sinex-services crate)
- **Severity:** MEDIUM
- **Impact:** Cannot correlate logs across service boundaries
- **Recommendation:** Add OpenTelemetry tracing spans

### Issue 142: No Token Rotation Support
- **File:** `crate/core/sinex-gateway/src/rpc_server.rs:118-156`
- **Severity:** MEDIUM (security)
- **Impact:** Compromised token requires gateway restart to invalidate
- **Recommendation:** Watch token file for changes, reload on modification

### Issue 143: No Rate Limiting Per Token
- **File:** Authentication layer in gateway
- **Severity:** MEDIUM (security)
- **Impact:** Compromised token can DoS gateway
- **Recommendation:** Add per-token rate limiter

### Issue 144: Base64 Expansion Not Accounted in Body Limit
- **File:** `crate/core/sinex-gateway/src/handlers.rs:183-201`
- **Severity:** LOW
- **Impact:** 5MB blob (base64) exceeds 2MB body limit
- **Recommendation:** Ensure body limit >= blob limit * 1.4

### Issue 145: No Replay Control Metrics
- **File:** `crate/core/sinex-gateway/src/replay_control.rs`
- **Severity:** MEDIUM
- **Impact:** Cannot observe replay system health (latency, errors, queue depth)
- **Recommendation:** Add metrics for each replay operation type

### Issue 146: No Gateway Health Endpoint
- **File:** `crate/core/sinex-gateway/src/rpc_server.rs` (missing)
- **Severity:** MEDIUM
- **Impact:** Cannot monitor gateway health or detect degraded state
- **Recommendation:** Add `/health` endpoint showing component status

### Issue 147: No Prometheus Metrics Endpoint
- **File:** `crate/core/sinex-gateway/src/rpc_server.rs` (missing)
- **Severity:** MEDIUM
- **Impact:** Cannot integrate with Prometheus monitoring
- **Recommendation:** Add `/metrics` endpoint

### Issue 148: No Request ID in RPC Responses
- **File:** `crate/core/sinex-gateway/src/rpc_server.rs:390-402`
- **Severity:** LOW
- **Impact:** Hard to correlate requests in logs
- **Recommendation:** Add request ID middleware and x-request-id header

### Issue 149: No Graceful Degradation on DB Failure
- **File:** Service container initialization
- **Severity:** LOW
- **Impact:** DB connection failure crashes gateway, no fallback
- **Recommendation:** Add retry logic with exponential backoff

### Issue 150: No Connection Pool Health Checks
- **File:** `crate/core/sinex-gateway/src/service_container.rs:38`
- **Severity:** LOW
- **Impact:** Pool may serve stale connections
- **Recommendation:** Enable test_before_acquire in pool config

### Issue 151: No TLS Support for RPC Server
- **File:** `crate/core/sinex-gateway/src/rpc_server.rs:762-850`
- **Severity:** LOW
- **Impact:** Unencrypted RPC over network when using TCP binding
- **Recommendation:** Add TLS support via rustls

---

## Summary Statistics

**Total Unresolved Issues:** 142

**By Severity:**
- CRITICAL: 1
- HIGH: 25
- MEDIUM: 72
- LOW: 44

**By Category:**
- Event Flow & NATS: 9 issues
- Coordination: 5 issues
- Monitoring: 5 issues
- Material Assembly: 9 issues
- FS-Watcher: 8 issues
- Terminal node: 5 issues
- Desktop node: 6 issues
- System node: 9 issues
- Database: 23 issues
- Concurrency: 20 issues
- Testing: 12 issues
- Gateway/RPC: 27 issues

**High-Priority Focus Areas:**
1. Gateway RPC infrastructure (27 issues, several HIGH/CRITICAL)
2. Database query optimization (23 issues, includes retention policy)
3. Concurrency/race conditions (20 issues, multiple HIGH severity)
4. Testing infrastructure gaps (12 issues, HIGH priority property tests)
5. Material assembly reliability (9 issues, DoS risks)

---

## Integration with Unified Plan

These issues should be addressed during:
- **Phase 1.2:** Gateway RPC issues (125-151)
- **Phase 2:** node timeout/native API issues (19-49)
- **Phase 5:** All remaining tactical improvements

**Next Steps:**
1. Prioritize HIGH/CRITICAL issues for immediate attention
2. Group related issues for batch implementation
3. Create tracking tickets for each category
4. Assign during sprint planning

---

**Last Updated:** 2025-01-15
**Next Review:** After Phase 1 completion (March 2025)
