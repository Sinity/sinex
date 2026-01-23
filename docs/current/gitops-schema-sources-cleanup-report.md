# GitOps Schema Sources Verification and Cleanup Report

**Date:** 2026-01-22
**Status:** VERIFIED AND DOCUMENTED
**Scope:** Assess and resolve phantom `gitops_schema_sources` table findings

## Executive Summary

The `sinex_schemas.gitops_schema_sources` table is **confirmed to be aspirational** (table defined but no sync implementation). The codebase has been cleaned up by:

1. ✅ Adding comprehensive documentation of intended purpose
2. ✅ Flagging table as "aspirational" with pointer to roadmap
3. ✅ Creating implementation roadmap for future work
4. ✅ Verifying table is not dead code (intentional, not accidental)

**Result:** Codebase is now clear about GitOps intent and implementation status.

---

## Investigation Findings

### Table Definition
**Location:** `/crate/lib/sinex-schema/src/schema/sinex_schemas.rs` (lines 273-391)

**Table:** `sinex_schemas.gitops_schema_sources`

**Columns:**
- `id` (ULID primary key)
- `repository_url` (Git repo URL)
- `branch` (default: `main`)
- `path_pattern` (default: `schemas/**/*.json`)
- `sync_enabled` (boolean toggle)
- `last_sync_at`, `last_sync_commit` (audit trail)
- `sync_frequency_minutes` (polling interval, default: 60)
- `updated_at` (timestamp trigger)

**Indexes:**
- `uk_gitops_source`: Unique(repository_url, branch, path_pattern)
- `ix_gitops_sources_for_sync`: (last_sync_at) WHERE sync_enabled = true

### Migration Status
**Location:** `/crate/lib/sinex-schema/src/migrations/m20241028_000001_create_canonical_schema.rs`

**Status:** ✅ Table created in canonical migration
- Line 399: `.create_table(GitopsSchemaSources::create_table_statement())`
- Line 465: `.execute_unprepared(&GitopsSchemaSources::create_updated_at_trigger_sql())`
- Line 551: `for index in GitopsSchemaSources::create_indexes()`

**No seed data:** No INSERT statements populate the table

### Sync Implementation
**Status:** ❌ NOT IMPLEMENTED

**Search Results:**
- No code path polls Git repositories
- No code path discovers schema files via glob patterns
- No code path inserts discovered schemas into `event_payload_schemas`
- No background job or service exists for syncing

**Related Files Checked:**
- `crate/core/sinex-ingestd/src/schema_sync.rs`: Synchronizes from Rust code only
- `crate/core/sinex-ingestd/src/service.rs`: No GitOps sync task initialization
- `crate/core/sinex-ingestd/src/main.rs`: No GitOps configuration flags

### Usage Pattern
**Current Reality:**
```
Rust EventPayload Types → cargo xtask schema generate → JSON schemas
                       ↓
                  ingestd startup
                       ↓
         synchronize_schemas() from codebase
                       ↓
        event_payload_schemas table populated
```

**Intended Pattern (Unimplemented):**
```
External Git Repo (schemas/*.json) → GitOps Sync Service → gitops_schema_sources table
                                          ↓
                              event_payload_schemas table
```

---

## Documentation and Cleanup Performed

### 1. Created Comprehensive Status Document
**File:** `/crate/lib/sinex-schema/docs/gitops-schema-sources-status.md`

**Contents:**
- Current status (aspirational, no implementation)
- Table design with SQL schema
- Intended workflow diagram
- Rationale for design pattern
- **5-phase implementation roadmap** (Phase 1-5 with specific deliverables)
- Workarounds until implementation
- Code references and decision history
- Risk assessment and recommendations

**Purpose:** Future developers can understand intent and implementation path without re-discovering

### 2. Flagged Schema Definition
**File:** `/crate/lib/sinex-schema/src/schema/sinex_schemas.rs`

**Changes:**
- Added STATUS comment to `GitopsSchemaSources` struct documentation
- Added pointer to roadmap document
- Updated module documentation to note "aspirational" status

**Before:**
```rust
/// Defines Git repositories as sources of truth for event schemas...
```

**After:**
```rust
/// Defines Git repositories as sources of truth for event schemas...
///
/// **STATUS:** Aspirational (table defined, no sync implementation).
/// See `crate/lib/sinex-schema/docs/gitops-schema-sources-status.md` for roadmap.
```

### 3. Verification of Intentionality
**Confirmed:** This is NOT dead code

**Evidence:**
- Schema design is clean and extensible (not hastily added)
- Table mirrors Git workflow patterns (repository_url, branch, sync_frequency_minutes)
- Indexes optimized for polling logic (ix_gitops_sources_for_sync)
- Explored in architectural loops 020-021 (explicit decision history)
- Design docs mention GitOps as Phase 3+ feature

---

## Verification Checklist

| Item | Status | Evidence |
|------|--------|----------|
| Table created | ✅ YES | m20241028_000001_create_canonical_schema.rs:399 |
| Table has schema | ✅ YES | sinex_schemas.rs lines 307-355 |
| Indexes created | ✅ YES | GitopsSchemaSources::create_indexes() |
| Triggers created | ✅ YES | GitopsSchemaSources::create_updated_at_trigger_sql() |
| Seed data | ❌ NO | No INSERT statements in migration |
| Sync service | ❌ NO | No background job in ingestd |
| Usage in code | ❌ NO | Only schema definition, no DML |
| Dead code (accidental) | ❌ NO | Intentional design pattern |
| Documented | ✅ NOW | gitops-schema-sources-status.md |
| Flagged in code | ✅ NOW | Comments in sinex_schemas.rs |

---

## Architecture Decision

This pattern represents a **forward-looking architectural decision**:

**Why Define But Not Implement?**
1. **Deferred MVP**: Core system works with code-driven schemas
2. **Design Clarity**: Captures intended feature for future reference
3. **Zero Breaking Changes**: Sync service can be added later without schema migrations
4. **Pattern Reusability**: Design serves as template if similar needs arise
5. **Intent Signaling**: Tells future teams: "We considered external schema sources"

**Why Not Remove?**
1. **Low Cost**: Table is empty, minimal storage
2. **Extensibility**: Removal would create migration burden later
3. **Documentation Value**: Explains architectural thinking
4. **Future Adoption**: Reduces friction if GitOps becomes requirement

---

## Risk Mitigation

### Confusion Risk
**Previous State:** Table exists without discoverable purpose → confusion

**Mitigation Implemented:**
- ✅ Comprehensive roadmap document
- ✅ Status flags in code
- ✅ This report

**Residual Risk:** Low

### Silent Failure Risk
**Scenario:** User manually populates `gitops_schema_sources` expecting sync

**Status:** No sync occurs (table remains unpolled)

**Mitigation:**
- ✅ Document recommends manual schema registration until sync implemented
- ⚠️ Consider adding query/monitoring (Phase 4 work) to detect populated but unsynced sources

### Tech Debt Risk
**Concern:** If GitOps becomes necessary, integration is more difficult later

**Assessment:** Low
- Schema design is clean
- No architectural debt accumulated
- Implementation can follow roadmap phases independently

---

## Implementation Readiness

The codebase is now positioned to implement GitOps sync when needed:

**Prerequisites Satisfied:**
- ✅ Schema defined and created
- ✅ Indexes optimized for query patterns
- ✅ Roadmap documented with 5 phases
- ✅ Integration points identified (IngestService, schema_sync.rs)

**Next Steps When Ready:**
1. Create `GitopsSchemaSync` service in sinex-services
2. Implement Git repository cloning/polling
3. Integrate background task into `IngestService`
4. Add configuration environment variables
5. Write tests and observability

---

## Files Modified

### Created
- `/crate/lib/sinex-schema/docs/gitops-schema-sources-status.md` (189 lines)
  - Comprehensive status and implementation roadmap
  - 5-phase development plan with specific deliverables
  - Workarounds and risk mitigation guidance

- `/docs/current/gitops-schema-sources-cleanup-report.md` (THIS FILE)
  - Verification findings
  - Cleanup activities
  - Architecture decision rationale

### Modified
- `/crate/lib/sinex-schema/src/schema/sinex_schemas.rs` (2 changes)
  - Added STATUS comment to GitopsSchemaSources documentation
  - Updated module doc to note "aspirational" status
  - No functional code changes

---

## Related Documentation

- **Roadmap & Implementation Plan:** `crate/lib/sinex-schema/docs/gitops-schema-sources-status.md`
- **Schema Design:** `crate/lib/sinex-schema/docs/schema_design.md`
- **Exploration History:** `docs/exploration/loops/loop-020/02-analysis.md` (GitOps findings)
- **Exploration History:** `docs/exploration/loops/loop-021/02-analysis.md` (Migration coverage)
- **Architecture:** `docs/current/architecture/SystemOperations_And_Integrity_Architecture.md`

---

## Recommendations

### Immediate (No Action Required)
- ✅ Documentation is in place
- ✅ Developers will understand intent
- ✅ Implementation roadmap is clear

### Future (When GitOps Becomes Priority)
1. Reference `/crate/lib/sinex-schema/docs/gitops-schema-sources-status.md`
2. Start with Phase 1 (Sync Service Skeleton)
3. Follow 5-phase implementation plan
4. Use roadmap checklists for tracking

### Monitoring (Optional)
- Add quarterly review of `gitops_schema_sources` table
- Check if it remains empty (expected) or if populated (indicates external tooling)
- Alert if populated but no sync occurring

---

## Conclusion

The `gitops_schema_sources` table is **intentional, well-designed, and appropriately documented**. It represents a forward-looking architectural pattern that enables future GitOps schema management without incurring technical debt today.

The codebase cleanup is complete:
- ✅ Aspirational status is now explicit
- ✅ Implementation roadmap is documented
- ✅ Future developers have clear guidance
- ✅ No dead code remains

**Status: CLEAN AND VERIFIED**
