# Deep Analysis: Heartbeat System & Checkpoint Mechanisms

**Analysis Date:** 2025-11-16
**Focus:** Health monitoring, checkpoint persistence, progress tracking

---

## 🫀 Heartbeat System Architecture

### Design Philosophy: Journald-First Monitoring

**Core Concept:**

```
Satellite emits JSON to stdout
      ↓
systemd captures in journald
      ↓
journald-satellite ingests as events
      ↓
health-aggregator automaton processes
      ↓
System health dashboard
```

**Benefits:**

- ✅ No separate monitoring infrastructure
- ✅ Heartbeats are regular events (queryable)
- ✅ Historical health data in event database
- ✅ Works out-of-box with systemd

### Heartbeat Metrics Structure

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

### Status Determination Algorithm

```rust
fn determine_status(recent_errors: usize) -> ProcessStatus {
    if recent_errors > 50 {
        ProcessStatus::Failed    // Critical threshold
    } else if recent_errors > 10 {
        ProcessStatus::Degraded  // Warning threshold
    } else {
        ProcessStatus::Healthy   // Normal operation
    }
}
```

**Analysis:**

- ✅ Simple, clear thresholds
- ✅ Based on error rate, not error percentage
- ⚠️ **ISSUE:** Thresholds are hardcoded (not configurable)
- ⚠️ **ISSUE:** No time-based windowing (100 errors in 1 minute vs 1 hour treated same)
- ⚠️ **ISSUE:** Error counter resets after each heartbeat (loses context)

**Scenario Analysis:**

```
Heartbeat interval: 60 seconds
Error burst: 55 errors in first 10 seconds

Current behavior:
- Next heartbeat (60s later): reports 55 errors → Status: Failed
- Following heartbeat (120s later): reports 0 errors → Status: Healthy

Problem: No memory of recent failure, appears healthy immediately
```

**Recommendation:**

```rust
// Use sliding window approach
struct ErrorWindow {
    errors: VecDeque<(Instant, String)>,
    window_duration: Duration,
}

impl ErrorWindow {
    fn add_error(&mut self, msg: String) {
        self.errors.push_back((Instant::now(), msg));
        self.prune_old();
    }

    fn prune_old(&mut self) {
        let cutoff = Instant::now() - self.window_duration;
        while self.errors.front().map_or(false, |(t, _)| *t < cutoff) {
            self.errors.pop_front();
        }
    }

    fn count(&self) -> usize {
        self.prune_old(); // Const method limitation workaround
        self.errors.len()
    }
}
```

### Resource Monitoring Implementation

#### Memory Usage

**Implementation:**

```rust
fn get_memory_usage_mb(&self) -> u32 {
    if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
        for line in status.lines() {
            if line.starts_with("VmRSS:") {
                if let Some(kb_str) = line.split_whitespace().nth(1) {
                    if let Ok(kb) = kb_str.parse::<u32>() {
                        return kb / 1024; // KB → MB
                    }
                }
            }
        }
    }
    0 // Default on failure
}
```

**Analysis:**

- ✅ Uses VmRSS (Resident Set Size) - correct metric
- ✅ Simple, no external dependencies
- ⚠️ **ISSUE:** Returns 0 on failure (indistinguishable from "no memory")
- ⚠️ **ISSUE:** Linux-specific (fails on macOS, Windows)
- ⚠️ **ISSUE:** Silent fallback (no logging of parse failures)

**Recommendation:**

```rust
fn get_memory_usage_mb(&self) -> Option<u32> {
    let status = std::fs::read_to_string("/proc/self/status")
        .map_err(|e| {
            debug!("Failed to read /proc/self/status: {}", e);
            e
        })
        .ok()?;

    for line in status.lines() {
        if line.starts_with("VmRSS:") {
            let kb = line
                .split_whitespace()
                .nth(1)?
                .parse::<u32>()
                .ok()?;
            return Some(kb / 1024);
        }
    }

    warn!("VmRSS not found in /proc/self/status");
    None
}
```

#### CPU Usage

**Implementation:**

```rust
// Uses unsafe libc::getrusage
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

**Analysis:**

- ✅ **EXCELLENT:** Proper `MaybeUninit` usage (safe unsafe code)
- ✅ Checks `getrusage` return value
- ✅ Sums user + system time (correct)
- ✅ Handles microseconds properly
- ✅ This is one of the 2 unsafe blocks found in entire codebase

**CPU Percentage Calculation:**

```rust
fn get_cpu_usage_percent(&self) -> f32 {
    let current_cpu = Self::read_process_cpu_seconds()?;
    let now = Instant::now();
    let previous = *self.cpu_sample.lock();

    if let Some(prev) = previous {
        let cpu_delta = current_cpu - prev.cpu_seconds;
        let wall_delta = (now - prev.timestamp).as_secs_f64();

        if wall_delta > 0.0 && cpu_delta >= 0.0 {
            let utilization = (cpu_delta / wall_delta) * 100.0;
            let normalized = utilization / self.cpu_cores as f64;
            return normalized.clamp(0.0, 100.0) as f32;
        }
    }

    *self.cpu_sample.lock() = Some(CpuSample { current_cpu, now });
    0.0
}
```

**Analysis:**

- ✅ Delta-based calculation (correct)
- ✅ Normalizes by CPU count
- ✅ Clamps to [0, 100]
- ✅ Updates sample for next calculation
- ⚠️ **ISSUE:** Returns 0.0 on first call (no previous sample)
- ⚠️ **ISSUE:** Returns 0.0 if wall_delta <= 0 (clock went backwards?)
- 💡 **INSIGHT:** Normalized by cores means 100% = fully utilizing 1 core

### Heartbeat Emission

**Structured Log Format:**

```rust
let log_entry = json!({
    "level": "INFO",
    "message": "heartbeat",
    "target": "heartbeat",
    "module_path": "sinex_satellite_sdk::heartbeat",
    "service_name": metrics.service_name,
    "status": metrics.status,
    "events_processed": metrics.events_processed,
    "uptime_seconds": metrics.uptime_seconds,
    "memory_usage_mb": metrics.memory_usage_mb,
    "cpu_usage_percent": metrics.cpu_usage_percent,
    // ... more fields
});

println!("{}", log_entry); // → journald
```

**Analysis:**

- ✅ Structured JSON for easy parsing
- ✅ journald captures stdout automatically
- ✅ No external dependencies
- ⚠️ **QUESTION:** How does journald-satellite parse these?
- ⚠️ **QUESTION:** What if JSON is malformed?
- 💡 **INSIGHT:** Using `println!` (not `tracing`) ensures consistent format

---

## 📍 Checkpoint System Architecture

### Unified Checkpoint Types

```rust
pub enum Checkpoint {
    /// No checkpoint (initial state)
    None,

    /// Internal event ID (automata)
    Internal {
        event_id: Ulid,
        message_count: u64,
    },

    /// External position (ingestors)
    External {
        position: serde_json::Value,  // Flexible external state
    },

    /// Stream message ID
    Stream {
        message_id: String,
        event_id: Option<Ulid>,
    },

    /// Timestamp-based checkpoint
    Timestamp {
        timestamp: DateTime<Utc>,
    },
}
```

**Design Analysis:**

- ✅ **EXCELLENT:** Unified enum for all checkpoint types
- ✅ Type-safe variants prevent mixing checkpoint kinds
- ✅ Flexible `External` variant for custom state
- ✅ `Internal` embeds both ULID and count
- 💡 **INSIGHT:** Automata use `Internal`, ingestors use `External`/`Stream`

### Checkpoint State Structure

```rust
pub struct CheckpointState {
    pub checkpoint: Checkpoint,
    pub processed_count: u64,
    pub last_activity: DateTime<Utc>,
    pub data: Option<serde_json::Value>,  // Processor-specific state
    pub version: u32,                     // Schema evolution
}
```

**Features:**

- Version field enables schema migration (currently v2)
- Processor-specific `data` field for custom state
- `last_activity` for staleness detection
- Separate `processed_count` from checkpoint-embedded count

### Database Schema

**Table:** `core.processor_checkpoints`

**Columns:**

- `id`: ULID (primary key)
- `processor_name`: Satellite/automaton identifier
- `consumer_group`: Logical grouping
- `consumer_name`: Instance identifier (hostname + PID)
- `checkpoint_data`: JSON serialized CheckpointState
- `checkpoint_version`: Schema version (1 or 2)
- `processed_count`: Denormalized for queries
- `last_processed_id`: Denormalized for queries (v1 compat)
- Timestamps: `created_at`, `last_activity`, `updated_at`

**Indexing:** (Need to verify)

- Primary key on `id`
- Likely index on `(processor_name, consumer_group, consumer_name)`

### Atomic Checkpoint Updates

**Upsert Pattern:**

```sql
INSERT INTO core.processor_checkpoints (
    processor_name,
    consumer_group,
    consumer_name,
    checkpoint_data,
    checkpoint_version,
    processed_count,
    last_processed_id,
    last_activity
) VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
ON CONFLICT (processor_name, consumer_group, consumer_name)
DO UPDATE SET
    checkpoint_data = EXCLUDED.checkpoint_data,
    checkpoint_version = EXCLUDED.checkpoint_version,
    processed_count = EXCLUDED.processed_count,
    last_processed_id = EXCLUDED.last_processed_id,
    last_activity = NOW(),
    updated_at = NOW()
```

**Analysis:**

- ✅ Atomic upsert prevents race conditions
- ✅ Updates `last_activity` automatically
- ✅ Denormalizes frequently-queried fields
- ⚠️ **QUESTION:** Is there a unique constraint on (processor, group, consumer)?
- 💡 **INSIGHT:** Denormalization trades space for query speed

### Schema Evolution & Migration

**Version 1 → Version 2:**

```rust
impl From<LegacyCheckpointState> for CheckpointState {
    fn from(legacy: LegacyCheckpointState) -> Self {
        let checkpoint = match legacy.last_processed_id {
            Some(id) => {
                if let Ok(ulid) = id.parse::<Ulid>() {
                    Checkpoint::Internal {
                        event_id: ulid,
                        message_count: legacy.processed_count,
                    }
                } else {
                    Checkpoint::Stream {
                        message_id: id,
                        event_id: None,
                    }
                }
            }
            None => Checkpoint::None,
        };

        CheckpointState {
            checkpoint,
            processed_count: legacy.processed_count,
            last_activity: legacy.last_activity,
            data: legacy.data,
            version: 2,
        }
    }
}
```

**Analysis:**

- ✅ Automatic migration from v1 to v2
- ✅ Preserves all data during migration
- ✅ Graceful fallback for parse failures
- 💡 **INSIGHT:** ULIDs auto-detected, else treated as stream IDs

### Critical Checkpoint Logic

**Auto-Detection of Checkpoint Type:**

```rust
pub fn set_last_processed_id(&mut self, id: Option<String>) {
    self.checkpoint = match id {
        Some(id_str) => {
            // Try ULID parse first
            if let Ok(ulid) = id_str.parse::<Ulid>() {
                Checkpoint::Internal {
                    event_id: ulid,
                    message_count: self.processed_count,
                }
            } else {
                // Fallback to stream ID
                Checkpoint::Stream {
                    message_id: id_str,
                    event_id: None,
                }
            }
        }
        None => Checkpoint::None,
    };
}
```

**Analysis:**

- ✅ Automatic type detection (smart)
- ✅ Graceful fallback
- ⚠️ **ISSUE:** What if a stream ID happens to be valid ULID format?
- ⚠️ **ISSUE:** Silent type coercion could cause confusion
- 💡 **RECOMMENDATION:** Explicit checkpoint type rather than auto-detection

---

## 🔍 Critical Issues Found

### 1. **Heartbeat Error Window Amnesia** (MEDIUM)

**Issue:** Error counters reset after each heartbeat, losing historical context

**Impact:**

- Brief error bursts appear fine 60 seconds later
- No detection of recurring issues
- Status can flip rapidly: Failed → Healthy → Failed

**Recommendation:**
Implement sliding window error tracking (5-minute window)

### 2. **Hardcoded Health Thresholds** (LOW)

**Issue:** Error thresholds (10, 50) are hardcoded

**Impact:**

- Can't tune per-service
- High-volume services trigger false alarms
- Low-volume services hide problems

**Recommendation:**

```rust
pub struct HealthThresholds {
    pub degraded_errors_per_minute: u32,
    pub failed_errors_per_minute: u32,
}
```

### 3. **Resource Monitoring Silent Failures** (LOW)

**Issue:** Memory/CPU monitoring returns 0 on failure without logging

**Impact:**

- Can't distinguish "no resource usage" from "monitoring broken"
- Silent degradation of monitoring capability

**Recommendation:**
Return `Option<T>` and log failures

### 4. **Checkpoint Type Auto-Detection Risk** (MEDIUM)

**Issue:** Auto-detecting checkpoint type from string format

**Scenario:**

```
Stream ID: "01AN4Z07BY79KA1307SR9X4MV3" (happens to be valid ULID)
Auto-detected as: Checkpoint::Internal (wrong!)
Expected: Checkpoint::Stream
```

**Impact:**

- Incorrect checkpoint type
- Confusion in queries/debugging
- Potential replay errors

**Recommendation:**
Explicit checkpoint type rather than auto-detection

### 5. **Missing Checkpoint Cleanup** (LOW)

**Issue:** No automatic cleanup of old checkpoints

**Observation:**

- Checkpoints accumulate indefinitely
- Inactive processors leave stale data
- No TTL or retention policy

**Impact:**

- Table bloat over time
- Stale data confusion
- Query performance degradation

**Recommendation:**

```sql
-- Periodic cleanup of inactive checkpoints
DELETE FROM core.processor_checkpoints
WHERE last_activity < NOW() - INTERVAL '30 days';
```

---

## ✅ Strengths

1. **Unified Checkpoint System**
   - Single table for all checkpoint types
   - Type-safe enum variants
   - Automatic schema migration

2. **Atomic Checkpoint Updates**
   - No race conditions
   - Automatic timestamp updates
   - Denormalized for performance

3. **Journald-First Monitoring**
   - No separate infrastructure
   - Heartbeats as events
   - Historical queryability

4. **Safe Unsafe Code**
   - Proper `MaybeUninit` usage
   - Return value checking
   - Well-documented

5. **Version Evolution**
   - Schema version field
   - Automatic v1→v2 migration
   - Backwards compatibility

---

## ⚠️ Weaknesses & Recommendations

### Immediate (High Priority)

1. **Implement sliding window error tracking**
   - 5-minute window
   - Prevents amnesia
   - More accurate health status

2. **Return Option for resource monitoring**
   - Distinguish failure from zero
   - Log parsing errors
   - Better observability

### Short Term (Medium Priority)

3. **Make health thresholds configurable**
   - Per-service tuning
   - Environment variables
   - Runtime adjustment

4. **Explicit checkpoint type**
   - Remove auto-detection
   - Require explicit type
   - Prevent misclassification

5. **Add checkpoint cleanup**
   - 30-day retention policy
   - Periodic maintenance task
   - Configurable TTL

### Long Term (Nice to Have)

6. **Cross-platform resource monitoring**
   - Abstract platform differences
   - Support macOS, Windows
   - Consistent API

7. **Heartbeat aggregation metrics**
   - Fleet-wide health view
   - Anomaly detection
   - Trend analysis

---

**Analysis Status:** Partial
**Files Analyzed:** 3 (heartbeat.rs, checkpoint.rs, coordination.rs)
**Issues Found:** 5 issues cataloged
**Next:** Individual satellite implementations, testing patterns, concurrency
