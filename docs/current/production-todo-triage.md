# Production TODO Triage Report

**Generated:** 2026-01-30
**Status:** Complete Investigation

---

## Executive Summary

Out of 10 production TODOs investigated, the findings break down as:

- **🔴 Critical (3)**: Security/correctness issues requiring immediate fixes
- **🟡 Important (4)**: Production-readiness issues to fix before deployment
- **🟢 Nice-to-have (2)**: Improvements that can wait
- **✅ Fixed/Non-issue (1)**: Already resolved or not actionable

---

## 🔴 CRITICAL - Fix Immediately

### 1. Gateway Error Sanitization (SECURITY)

**Location:** `crate/core/sinex-gateway/src/rpc_server.rs:938`

**Severity:** 🔴 CRITICAL - Information Disclosure

**Problem:**
```rust
let data = serde_json::json!({
    "error_id": error_id.to_string(),
    "error": sinex_err,  // Full SinexError serialized to client!
});
```

The RPC error handler serializes the entire `SinexError` object (including internal context) to external clients. This leaks:
- Database connection details
- File paths
- Internal implementation details
- Stack traces
- Security-sensitive context values

**Impact:**
- Information disclosure vulnerability
- Aids attackers in reconnaissance
- Violates security best practices
- OWASP Top 10: A01:2021 - Broken Access Control

**Fix:**
```rust
// Sanitize error for production
let sanitized_message = match sinex_err {
    SinexError::Validation(_) => sinex_err.to_string(), // Safe to expose
    SinexError::NotFound(_) => sinex_err.to_string(),   // Safe to expose
    _ => format!("Internal error (ref: {error_id})"),   // Generic message
};

let data = serde_json::json!({
    "error_id": error_id.to_string(),
    "message": sanitized_message,
    // DO NOT include raw error object
});
```

**Priority:** P0 - Must fix before any production deployment

---

### 2. Native Messaging Read Timeout (DOS)

**Location:** `crate/core/sinex-gateway/src/native_messaging.rs:372`

**Severity:** 🔴 CRITICAL - Denial of Service

**Problem:**
```rust
// TODO: Add read timeout to prevent gateway hang if browser crashes
match stdin.read_exact(&mut len_bytes).await {
    Ok(_) => {}
    Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
    Err(e) => return Err(e.into()),
}
```

The gateway blocks indefinitely waiting for stdin input from browser extensions. If:
- Browser crashes mid-message
- Extension hangs
- Connection is left open

The gateway thread is permanently blocked, consuming resources.

**Impact:**
- Resource exhaustion (1 thread per hung connection)
- Gateway becomes unresponsive after enough hung connections
- No automatic recovery mechanism
- Single malicious/buggy extension can DoS the gateway

**Fix:**
```rust
use tokio::time::{timeout, Duration};

// Read with timeout (30s default, configurable via env var)
let read_timeout = std::env::var("SINEX_NATIVE_MSG_READ_TIMEOUT_SECS")
    .ok()
    .and_then(|s| s.parse().ok())
    .unwrap_or(30);

match timeout(
    Duration::from_secs(read_timeout),
    stdin.read_exact(&mut len_bytes)
).await {
    Ok(Ok(_)) => {}
    Ok(Err(e)) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
    Ok(Err(e)) => return Err(e.into()),
    Err(_) => {
        warn!("Native messaging read timeout after {read_timeout}s");
        return Err(eyre!("Read timeout - browser may have crashed"));
    }
}
```

**Priority:** P0 - Must fix before production

---

### 3. CORS Policy Too Permissive (SECURITY)

**Location:** `crate/core/sinex-gateway/src/rpc_server.rs:1197`

**Severity:** 🔴 CRITICAL - Security Misconfiguration

**Problem:**
```rust
// TODO: Review CORS policy for production (analysis/rpc_server.md Q-003)
.layer(CorsLayer::permissive())
```

`CorsLayer::permissive()` allows:
- All origins (`Access-Control-Allow-Origin: *`)
- All methods
- All headers
- Credentials from any origin

**Impact:**
- Any website can make authenticated requests to the gateway
- CSRF attacks possible
- Violates same-origin policy security model
- Data exfiltration from malicious sites

**Fix:**
```rust
use tower_http::cors::{CorsLayer, Any};

// Production CORS policy
let cors = CorsLayer::new()
    .allow_origin(
        std::env::var("SINEX_GATEWAY_CORS_ORIGINS")
            .unwrap_or_else(|_| "http://localhost:3000".to_string())
            .split(',')
            .map(|s| s.parse().unwrap())
            .collect::<Vec<_>>()
    )
    .allow_methods([Method::GET, Method::POST, Method::OPTIONS])
    .allow_headers([CONTENT_TYPE, AUTHORIZATION])
    .allow_credentials(true);

.layer(cors)
```

**Configuration:**
```bash
# Production
SINEX_GATEWAY_CORS_ORIGINS=https://sinex.example.com,https://app.example.com

# Development
SINEX_GATEWAY_CORS_ORIGINS=http://localhost:3000,http://localhost:8080
```

**Priority:** P0 - Must fix before production

---

## 🟡 IMPORTANT - Fix Before Production

### 4. Native Messaging Capability-Based Access

**Location:** `crate/core/sinex-gateway/src/native_messaging.rs:94`

**Severity:** 🟡 IMPORTANT - Security Enhancement

**Problem:**
```rust
// TODO: Implement capability-based access control (analysis/native_messaging.md)
let incoming_id = if let Some(id) = message.extension_id.as_deref() {
    id
} else { ... }
```

Current implementation only checks extension ID against allowlist. No fine-grained permissions like:
- Read-only vs read-write access
- Scoped to specific event types
- Rate limiting per extension
- API endpoint restrictions

**Impact:**
- All-or-nothing trust model
- Compromised extension has full access
- No defense in depth
- Harder to audit extension behavior

**Fix Strategy:**
1. Define capability model:
```rust
#[derive(Debug, Clone, Deserialize)]
struct ExtensionCapabilities {
    id: String,
    secret: Option<String>,
    allowed_methods: Vec<String>,  // ["search_events", "query_analytics"]
    read_only: bool,
    rate_limit_per_minute: u32,
    allowed_event_types: Option<Vec<String>>,
}
```

2. Enforce at method dispatch
3. Add capability validation middleware

**Priority:** P1 - Before production, but after P0 fixes

---

### 5. Cascade Analyzer Pagination

**Location:** `crate/core/sinex-gateway/src/cascade_analyzer.rs:405`

**Severity:** 🟡 IMPORTANT - Correctness/Performance

**Problem:**
```rust
// TODO: Remove hardcoded limit or implement pagination
let rows = repo
    .cascade_integrity_violations(table_name, 100)
    .await
```

Hardcoded limit of 100 violations means:
- If >100 violations exist, only first 100 are detected
- No way to discover remaining violations
- False sense of security ("only 100 violations found")

**Impact:**
- Incomplete integrity validation
- Data corruption may go undetected
- Operations team has no visibility into full scope

**Fix:**
```rust
// Option 1: Increase limit for now
let rows = repo
    .cascade_integrity_violations(table_name, 10_000)
    .await

// Option 2: Implement pagination (better)
let mut all_violations = Vec::new();
let mut offset = 0;
const BATCH_SIZE: i64 = 1000;

loop {
    let batch = repo
        .cascade_integrity_violations_paginated(table_name, BATCH_SIZE, offset)
        .await?;

    if batch.is_empty() {
        break;
    }

    offset += batch.len() as i64;
    all_violations.extend(batch);

    // Safety limit to prevent infinite loops
    if offset > 100_000 {
        warn!("Violation scan exceeded 100k rows, stopping");
        break;
    }
}
```

**Priority:** P1 - Before production operations

---

### 6. Cascade Analyzer Tarjan's Algorithm

**Location:** `crate/core/sinex-gateway/src/cascade_analyzer.rs:436`

**Severity:** 🟡 IMPORTANT - Correctness

**Problem:**
```rust
// TODO: In production, implement proper Tarjan's algorithm
let max_cycle_depth = self.config.max_depth.max(1);
let query = format!(r"
    WITH RECURSIVE cycle_check AS (
        ...
        WHERE NOT cc.has_cycle
        AND array_length(cc.path, 1) < {max_cycle_depth}
```

Recursive CTE approach for cycle detection:
- Only detects cycles up to `max_cycle_depth`
- False negatives for deeper cycles
- Performance degrades with graph depth
- Not the standard algorithm for cycle detection

**Impact:**
- Circular dependencies may go undetected
- Data integrity issues masked
- Archive operations may corrupt data

**Fix Strategy:**
Implement Tarjan's strongly connected components algorithm:

```rust
// Use petgraph crate
use petgraph::algo::tarjan_scc;
use petgraph::graphmap::DiGraphMap;

async fn detect_circular_dependencies_tx(
    &self,
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    table_name: &str,
) -> Result<Vec<CircularDependency>> {
    // 1. Load all edges from DB
    let edges = load_all_edges(tx, table_name).await?;

    // 2. Build graph
    let mut graph = DiGraphMap::new();
    for (from, to) in edges {
        graph.add_edge(from, to, ());
    }

    // 3. Find SCCs (O(V+E) guaranteed)
    let sccs = tarjan_scc(&graph);

    // 4. Convert to CircularDependency records
    sccs.into_iter()
        .filter(|scc| scc.len() > 1) // Cycles only
        .map(|scc| CircularDependency::from_scc(scc))
        .collect()
}
```

**Priority:** P1 - Required for data integrity guarantees

---

### 7. State Repository Missing Status Column

**Location:** `crate/lib/sinex-db/src/repositories/state.rs:708`

**Severity:** 🟡 IMPORTANT - Schema Deficiency

**Problem:**
```rust
/// Get all currently active processors.
pub async fn get_active_processors(&self) -> DbResult<Vec<ProcessorManifest>> {
    // TODO: The schema needs a 'status' or 'is_active' column.
    // For now, we return all processors as requested by the original (incorrect) implementation,
    // but note the missing filter.
    self.get_all_processors().await
}
```

The `core.processors` table has no way to distinguish:
- Active vs inactive processors
- Running vs stopped instances
- Latest version vs historical registrations

**Impact:**
- Can't query "which processors are currently running"
- Monitoring dashboards show stale data
- Coordination logic may use outdated processor info
- Operations team has no visibility

**Fix:**

1. **Schema migration:**
```sql
-- Migration: Add processor status tracking
ALTER TABLE core.processors
ADD COLUMN status TEXT NOT NULL DEFAULT 'registered'
    CHECK (status IN ('registered', 'active', 'inactive', 'failed'));

ALTER TABLE core.processors
ADD COLUMN last_heartbeat_at TIMESTAMPTZ;

CREATE INDEX idx_processors_status ON core.processors(status);
CREATE INDEX idx_processors_heartbeat ON core.processors(last_heartbeat_at DESC);
```

2. **Update repository:**
```rust
pub async fn get_active_processors(&self) -> DbResult<Vec<ProcessorManifest>> {
    sqlx::query_as!(
        ProcessorManifest,
        r#"
        SELECT *
        FROM core.processors
        WHERE status = 'active'
          AND (last_heartbeat_at IS NULL
               OR last_heartbeat_at > NOW() - INTERVAL '5 minutes')
        ORDER BY last_heartbeat_at DESC
        "#
    )
    .fetch_all(self.pool.as_ref())
    .await
    .map_err(Into::into)
}
```

**Priority:** P1 - Before production monitoring

---

## 🟢 NICE-TO-HAVE - Can Wait

### 8. Assembly Metrics

**Location:** `crate/core/sinex-ingestd/src/material_assembler/mod.rs:275`

**Severity:** 🟢 LOW - Observability Enhancement

**Problem:**
```rust
/// TODO: Add comprehensive assembly metrics for production observability:
/// - Assembly duration histogram (time from begin to end)
/// - Active assembly count gauge
/// - Slice count per material histogram
/// - Assembly failure counter by reason
/// - Buffer utilization histogram
/// - WAL replay duration on startup
```

Material assembler has no metrics instrumentation.

**Impact:**
- Limited observability during incidents
- Can't track performance degradation over time
- No alerting on assembly failures
- Harder to debug slowdowns

**Priority:** P2 - Post-launch improvement (existing logs sufficient for initial deployment)

**Recommendation:** Implement incrementally as part of observability initiative.

---

## ✅ NON-ISSUE

### 9. Hardcoded Embedding Dimension (RESOLVED)

**Location:** `crate/lib/sinex-schema/src/schema/embeddings.rs` (BUG-018)

**Severity:** ✅ RESOLVED

**Resolution:** Migration `m20260203_000018_dynamic_embedding_dimensions` switched embedding columns to dynamic `vector` type (no dimension constraint). The `embedding_models` table tracks dimensions per model; application-layer validation ensures consistency. See "Embedding Dimension Strategy" in `crate/lib/sinex-schema/docs/schema_design.md`.

---

### 10. Deprecated ValidateRecord Macro

**Location:** `crate/lib/sinex-macros/src/lib.rs:204` (BUG-019)

**Severity:** ✅ NON-ISSUE - Dead Code

**Problem:**
```rust
#[deprecated(note = "This macro is a no-op stub (BUG-019) and is unused.")]
#[proc_macro_derive(ValidateRecord, attributes(validate_against))]
pub fn validate_record(input: TokenStream) -> TokenStream { ... }
```

**Investigation:**
```bash
$ git grep -n "derive(ValidateRecord)" -- 'crate/**/*.rs'
# No usage found
```

**Status:** Already deprecated, no usage in codebase.

**Action:** Can be removed via cleanup PR, but not urgent. No impact on production.

**Priority:** P4 - Cleanup task

---

## Quick Fixes Applied

The following issues required no investigation and were fixed immediately:

### ✅ Code Formatting
- **Issue:** `cargo fmt --check` failed during triage
- **Fix:** Ran `cargo fmt`
- **Status:** ✅ Fixed

---

## Recommendations

### Immediate Actions (Before ANY Production Deployment)

1. **Fix P0 Critical Issues (1-3)**:
   - Error sanitization
   - Read timeout
   - CORS policy

   **Time estimate:** 2-4 hours

2. **Verify with security review**:
   ```bash
   cargo xtask check
   cargo xtask test
   # Manual security testing of gateway endpoints
   ```

### Before Production Launch (P1)

3. **Fix Important Issues (4-7)**:
   - Capability-based access (1-2 days)
   - Cascade analyzer fixes (4-6 hours)
   - Schema migration for processor status (2-4 hours)

4. **Add monitoring**:
   - Gateway error rate alerts
   - Native messaging timeout alerts
   - Cascade integrity violation alerts

### Post-Launch Improvements (P2-P4)

5. **Observability**:
   - Material assembler metrics (P2)

6. **Cleanup**:
   - Remove ValidateRecord macro (P4)
   - Address embedding dimensions when needed (P3)

---

## Risk Assessment

| Risk Category | Current State | After P0 Fixes | After P1 Fixes |
|---------------|---------------|----------------|----------------|
| Information Disclosure | 🔴 HIGH | 🟢 LOW | 🟢 LOW |
| Denial of Service | 🔴 HIGH | 🟢 LOW | 🟢 LOW |
| CSRF/Cross-Origin | 🔴 HIGH | 🟢 LOW | 🟢 LOW |
| Data Integrity | 🟡 MEDIUM | 🟡 MEDIUM | 🟢 LOW |
| Observability | 🟡 MEDIUM | 🟡 MEDIUM | 🟢 LOW |

**Deployment Readiness:** ❌ NOT READY (P0 issues must be fixed first)

---

## Next Steps

1. **Create GitHub issues** for P0-P1 items with this triage as reference
2. **Fix P0 issues** in order: error sanitization → read timeout → CORS
3. **Test security fixes** with attack scenarios
4. **Schedule P1 work** before production deployment
5. **Update deployment checklist** to verify all P0/P1 items completed

---

**End of Report**
