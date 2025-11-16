# Documentation Analysis Index

**See full report:** `/realm/project/sinex/docs/DOCUMENTATION_CONSISTENCY_ANALYSIS.md`

## Quick Reference: Issues by Severity

### CRITICAL (Fix Immediately - 30 min)
- ✅ **Resolved:** `project-target-state.md` now carries an explicit historical banner so sensd/gRPC references are clearly marked as archival context.

### HIGH PRIORITY (Fix Soon - 15 min)
- ✅ **Resolved:** `docs/architecture/Core_Architecture.md` and `docs/vision/streaming-architecture.md` no longer reference deleted files; both now link to `docs/way.md`.

### MEDIUM PRIORITY (Fix This Sprint - 1-2 hours)
1. **Generic operational notes** – 7 vision docs still have blanket disclaimers instead of scoped section markers.
2. **Document scope mismatch** – `way.md` vs `project-target-state.md` purposes not explicitly stated beyond the new historical note.

## Key Statistics

| Metric | Value |
|--------|-------|
| Documents analyzed | 67 |
| Contradictions found | 6 |
| Broken references | 3 |
| Orphaned documents | 0 |
| Problematic docs | 1 |
| Critical issues | 1 |
| High priority issues | 2 |
| Medium priority issues | 3 |

## Contradictions Summary

| # | Title | Severity | Document(s) | Line(s) |
|---|-------|----------|-------------|---------|
| 1 | sensd temporal confusion | ✅ Resolved | project-target-state.md (historical banner refreshed 2025-11-13) | 2, 99-101, 225-247, 332-334 |
| 2 | Deleted file reference | ✅ Resolved | Core_Architecture.md (links updated) | 28 |
| 3 | Internal reference conflict | ✅ Resolved | streaming-architecture.md (See Also fixed) | 9-10, 96 |
| 4 | Phase status unknown | ✅ Resolved | way.md (Implementation + Last Verified sections) | 186-191 |
| 5 | Document scope mismatch | MEDIUM | way.md vs project-target-state.md | Various |
| 6 | Generic operational notes | MEDIUM | 7 vision documents | Top |

## Broken References

| Document | Line | References | Status |
|----------|------|-----------|--------|
| `Core_Architecture.md` | 28 | `docs/plan_v3.txt` | ✅ FIXED – link now targets `docs/way.md`. |
| `streaming-architecture.md` | 96 | `docs/TARGET_final.md` | ✅ FIXED – "See Also" references updated to `docs/way.md`. |
| `FUTURE_EVENT_PIPELINE_DESIGN.md` | (comment) | `docs/plan_v3.txt` | ✅ FIXED – inline comment now points to `docs/way.md`. |

## Reconciliation Checklist

### Phase 1: Broken References (30 minutes)
- [x] Update `Core_Architecture.md` line 28
- [x] Fix `streaming-architecture.md` line 96
- [x] Update `FUTURE_EVENT_PIPELINE_DESIGN.md` comment

### Phase 2: Temporal Clarity (1-2 hours)
- [x] Add temporal markers to `project-target-state.md` sensd sections
- [ ] Replace blanket operational notes with section annotations (7 docs)
- [x] Add progress status to `way.md`
- [ ] Add explicit purpose statements to vision documents

### Phase 3: Polish (2-3 hours, before release)
- [ ] Add "Last Verified" stamps to canonical docs (only once we have reproducible verification steps to cite)
- [ ] Update `docs/README.md` with document purposes and relationships
- [ ] Consolidate `project-target-state.md` sections by temporal frame
- [ ] Review and potentially archive exploratory documents

## Document Categories

### Canonical (9 documents)
- `docs/way.md` - AUTHORITATIVE
- `docs/architecture/Core_Architecture.md`
- `docs/architecture/provenance.md`
- `docs/architecture/event-taxonomy.md`
- `docs/architecture/security-architecture.md`
- `docs/architecture/SystemOperations_And_Integrity_Architecture.md`
- `docs/vision/manifesto.md`
- `docs/vision/project-target-state.md`
- `docs/README.md`

### Vision/Future (8 documents)
All flagged with operational notes directing to `docs/way.md`

### Roadmap (24 documents)
Feature specifications, exploratory

### Historical (7 documents)
Archived for reference only

### Miscellaneous (13 documents)
Exploratory brainstorming essays

## Architecture Insights from Analysis

**Historical State (Pre-Phase 5):**
```
Satellites → sensd (gRPC) → ingestd → Postgres → NATS JetStream → Automata
```

**Current State (JetStream-First):**
```
Satellites → NATS JetStream → ingestd → Postgres + NATS consumers
```

**Migration Progress:**
- 5 phases defined in `docs/way.md`
- Current phase: ✅ Phases 1–5 complete; follow-up work tracks in `docs/way_2.md`.
- Timeline: Documented per phase timelines in `docs/way.md`
- Owner: sinex-core JetStream team (gateway + ingestd maintainers)

## How to Use This Analysis

1. **Quick Overview:** Read this file
2. **Full Details:** See `DOCUMENTATION_CONSISTENCY_ANALYSIS.md`
3. **Implementation:** Follow reconciliation checklist above
4. **Questions:** Each contradiction has "Recommendation" section with examples

## File Locations

| File | Purpose |
|------|---------|
| `DOCUMENTATION_CONSISTENCY_ANALYSIS.md` | Full 646-line report |
| `ANALYSIS_INDEX.md` | This file - quick reference |
| `way.md` | Authoritative ingestion refactor plan |
| `Core_Architecture.md` | System overview (has broken reference) |
| `project-target-state.md` | Architecture snapshot (temporal confusion) |
| `streaming-architecture.md` | Backpressure guidance (internal contradiction) |

---

**Analysis Date:** 2025-11-13  
**Status:** Report completed, no changes made  
**Next Step:** Address Priority 1 (critical) issues
