# Production TODO Triage Report

**Generated:** 2026-01-30
**Status:** Complete Investigation

---

## Executive Summary

Out of 10 production TODOs investigated, the findings break down as:

- **🔴 Critical (3)**: Security/correctness issues requiring immediate fixes
- **🟡 Important (2)**: Production-readiness issues to fix before deployment
- **🟢 Nice-to-have (2)**: Improvements that can wait
- **✅ Fixed/Non-issue (3)**: Already resolved or not actionable

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

### 4. Native Messaging Capability-Based Access (PARTIALLY RESOLVED)

**Location:** `crate/core/sinex-gateway/src/native_messaging.rs`

**Severity:** 🟡 IMPORTANT - Security Enhancement (partially addressed)

**Resolution (partial):** Per-extension role-based auth implemented via `SINEX_NATIVE_MESSAGING_EXTENSION_ROLES` env var (JSON map of extension ID → Role). Unknown extensions default to `ReadOnly` instead of `Admin`. `RpcAuthContext::extension()` constructor provides audit attribution. Commit: `377c93ff`.

**Remaining work:**
- Fine-grained method-level permissions (allowed_methods per extension)
- Per-extension rate limiting
- Event type scoping

**Priority:** P2 - Core role-based auth is in place; method-level capabilities are post-launch enhancement

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

### 6. Cascade Analyzer Tarjan's Algorithm (RESOLVED)

**Location:** `crate/core/sinex-gateway/src/cascade_analyzer.rs`

**Severity:** ✅ RESOLVED

**Resolution:** Replaced recursive CTE (O(n*d), max_depth bounded) with `petgraph::algo::tarjan_scc()` — O(V+E), guaranteed to find all cycles regardless of depth. Loads edges into `DiGraphMap<Uuid, ()>`, runs Tarjan's SCC, filters components with len > 1. Commit: `f4a88347`.

---

### 7. State Repository Missing Status Column (RESOLVED)

**Location:** `crate/lib/sinex-db/src/repositories/state.rs`

**Severity:** ✅ RESOLVED

**Resolution:** DB migration added `status` and `last_heartbeat_at` columns to `core.processors` with appropriate indexes. Repository methods (`get_active_processors`, `get_processor_health`, `update_processor_heartbeat`, `mark_processor_inactive`) implemented in `state.rs:668-794`. Four RPC handlers wired in `handlers/processors.rs` and registered in `rpc_registry.rs`. Commits: `14fbc268`, `221e7511`.

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
   xtask check
   xtask test
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
