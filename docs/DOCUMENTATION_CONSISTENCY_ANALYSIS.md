# Documentation Consistency Analysis

**Analysis Date:** 2025-10-25  
**Thoroughness Level:** Very Thorough  
**Status:** Report only (no changes made)

---

## Executive Summary

Analyzed all 67 markdown documents in `docs/` for consistency, broken references, and architectural contradictions. Found:

- **6 contradictions** (1 critical, 3 medium, 2 high severity)
- **3 broken file references** (deleted files still referenced in "See Also" sections)
- **0 true orphaned documents** (all have stated purposes)
- **1 problematic document** (project-target-state.md mixes present/future tense descriptions)

**Key Finding:** The migration from sensd/gRPC to JetStream is correctly tracked in `docs/way.md`, but supporting documents use inconsistent temporal language, creating confusion about "current state" vs. "target state".

---

## Document Inventory

### Total Documents: 67

| Category | Count | Status |
|----------|-------|--------|
| Core/Architecture | 8 | Canonical |
| Vision | 8 | With operational notes |
| Roadmap | 24 | Exploratory/future |
| Historical | 7 | Archived |
| Misc/High-level | 13 | Exploratory essays |
| Testing | 2 | Operational |
| Root | 3 | Guides/index |

### Canonical/Authoritative Documents

1. **`docs/way.md`** (2025-10-23)
   - Status: Explicitly marked "authoritative"
   - Scope: JetStream refactor playbook with 5-phase migration plan
   - Quality: Excellent - supersedes older plans clearly

2. **`docs/architecture/Core_Architecture.md`**
   - Status: Marked "canonical"
   - Issue: References deleted file `docs/plan_v3.txt` (see Contradiction 2)

3. **`docs/architecture/provenance.md`**
   - Status: Operational patterns document
   - Quality: Good - uses abstracted terminology ("Sensor" vs "sensd")

4. **`docs/architecture/event-taxonomy.md`**
   - Status: Marked "canonical"
   - Note: Consolidates taxonomy from deleted `TARGET_final.md` (acceptable)

5. **`docs/architecture/security-architecture.md`**
   - Status: Marked "canonical"
   - Quality: Good - coordinates properly with way.md

6. **`docs/architecture/SystemOperations_And_Integrity_Architecture.md`**
   - Version: 2.0, 2025-07-17
   - Status: Marked "OPERATIONAL"
   - Quality: Good - clearly states implementation status

7. **`docs/vision/manifesto.md`** (updated 2025-10-23)
   - Status: Strategic vision consolidation
   - Quality: Excellent - references way.md for authoritative ingestion plan

8. **`docs/vision/project-target-state.md`** (2025-10-23 ops note)
   - Status: Architecture snapshot with operational note
   - Issue: Mixes present/past/future tenses (see Contradiction 1)

9. **`docs/README.md`**
   - Status: Documentation index
   - Quality: Good - clearly categorizes docs by purpose

---

## Contradictions Found

### CONTRADICTION 1: sensd Status (Temporal Inconsistency)

**Severity:** CRITICAL  
**Location:** Multiple documents  
**Impact:** Readers confused about whether sensd is current or being retired

#### The Problem

**`docs/way.md`** (authoritative 2025-10-23) clearly states sensd is BEING REMOVED:
```
Line 5: "We are replacing the legacy 'satellites → sensd (gRPC) → ingestd'"
Line 9: "sensd and its gRPC surface disappear entirely"
Line 115: "Remove gRPC ingestion and the sensd crate"
Phase 5: "[  ] sensd removed, gRPC removed, docs updated"
```

**`docs/vision/project-target-state.md`** adds operational note at top (2025-10-23):
```
> JetStream ingestion is canonical (`docs/way.md`). 
> Any sensd/gRPC references here are historical context.
```

BUT the document body describes sensd in **PRESENT TENSE** as if it's currently operational:

```
Line 99-101: "sensd centralizes acquisition. It creates in‑flight Source Material registry rows"
Line 225-247: Section "sensd (Sensing)" with [ ] checklist items
Line 332-334: Diagram showing "[Satellites] → [sensd: Source Material + Ledger]"
```

#### Why This Matters

Readers don't know:
1. Is sensd already removed? (No)
2. Is sensd still active? (Yes, being migrated)
3. Should they use sensd in new code? (No, Phase 5 will remove it)
4. What's the "current" architecture? (Transitional: sensd + JetStream path)

#### Recommendation

Mark project-target-state.md sections with temporal clarity:

**Option A:** Add section headers
```markdown
## 4) Sensing (sensd) — Current State Being Migrated Away

[IMPORTANT: sensd is being retired in Phase 5 per `docs/way.md`. 
This section describes the CURRENT (being-transitioned) architecture.]

sensd centralizes acquisition. It creates in-flight Source Material registry rows...
```

**Option B:** Use conditional tense
```markdown
sensd [currently] centralizes acquisition. [It will be replaced by] 
Stage-as-You-Go in the SDK [per Phase 5]. It creates in-flight Source Material registry rows...
```

**Recommended:** Combine both - add disclaimer header AND refactor tense.

---

### CONTRADICTION 2: Core_Architecture.md References Deleted File

**Severity:** MEDIUM  
**Location:** `docs/architecture/Core_Architecture.md` line 28  
**Impact:** Undermines "canonical" status claim

#### The Problem

```markdown
Status: canonical
...
- See also: `docs/plan_v3.txt` for ingestion plan details
```

But `docs/plan_v3.txt` doesn't exist (superseded per `way.md` line 201).

#### Why This Matters

A document marked "canonical" should not reference files that don't exist. It breaks trust and sends readers looking for documents that were deleted.

#### Recommendation

**Change line 28 from:**
```
- See also: `docs/plan_v3.txt` for ingestion plan details
```

**To:**
```
- See also: `docs/way.md` (JetStream refactor plan and Phase-by-phase checklist)
```

---

### CONTRADICTION 3: streaming-architecture.md Has Internal Reference Conflict

**Severity:** HIGH  
**Location:** `docs/vision/streaming-architecture.md` lines 9-10 and 96  
**Impact:** Document contradicts itself about deleted files

#### The Problem

**Lines 9-10 (Accuracy Notice, 2025-07-24):**
```
> References to `docs/TARGET_final.md` and other legacy plans point to 
> files that were removed during documentation consolidation.
```

**Line 96 (See Also section):**
```
- docs/TARGET_final.md (authoritative blueprint)
```

The document acknowledges `TARGET_final.md` was deleted, then still lists it as an authoritative reference. This is contradictory.

#### Why This Matters

Readers won't know whether to look for `TARGET_final.md` or not. The "See Also" section seems to override the accuracy notice.

#### Recommendation

**Change line 96 from:**
```
- docs/TARGET_final.md (authoritative blueprint)
```

**To:**
```
- docs/way.md (JetStream ingestion plan)
```

OR remove the reference entirely with note:
```
- See `docs/way.md` for authoritative ingestion plan (replaces deleted TARGET_final.md)
```

---

### CONTRADICTION 4: Phase Completion Status Unknown

**Severity:** MEDIUM  
**Location:** `docs/way.md` lines 186-191  
**Impact:** Readers can't determine migration progress

#### The Problem

```markdown
## 8. Reference Checklist

- [ ] Phase 1 complete (events consumer + confirmation)
- [ ] Phase 2 complete (confirmation-aware runner)
- [ ] Phase 3 complete (Stage-as-You-Go + materials consumer)
- [ ] Phase 4 complete (leases/control plane)
- [ ] Phase 5 complete (sensd removed, gRPC removed, docs updated)
```

All boxes are unchecked. Document was updated 2025-10-23 (very recent) but provides no indication of actual progress.

#### Why This Matters

Engineers need to know:
- What phase are we in currently?
- What's the expected timeline?
- What's blocking progress?
- Who owns what?

The checklist format suggests progress tracking but doesn't provide the actual status.

#### Recommendation

Add status tracking after the checklist:

```markdown
## 8. Reference Checklist

- [ ] Phase 1 complete (events consumer + confirmation)
- [ ] Phase 2 complete (confirmation-aware runner)
- [ ] Phase 3 complete (Stage-as-You-Go + materials consumer)
- [ ] Phase 4 complete (leases/control plane)
- [ ] Phase 5 complete (sensd removed, gRPC removed, docs updated)

**Current Progress (2025-10-25):**
- Status: Phase [X] in progress
- Timeline: Expected completion [date]
- Owner: [team/person]
- Blockers: [list or "none"]
- Last update: [date] by [person]
```

---

### CONTRADICTION 5: Scope Mismatch Between way.md and project-target-state.md

**Severity:** MEDIUM  
**Location:** `docs/way.md` vs `docs/vision/project-target-state.md`  
**Impact:** Creates confusion about document purposes

#### The Problem

These are NOT actually contradictory, but serve different purposes:

**`docs/way.md`** = "How to migrate FROM sensd/gRPC TO JetStream"
- Narrow scope: ingestion refactor only
- Time horizon: implementation plan (months)
- Audience: engineers implementing Phases 1-5

**`docs/vision/project-target-state.md`** = "What is the complete system architecture"
- Broad scope: full system including sensd, ingestors, automata, gateway, agents, replay discipline, privacy, observability
- Time horizon: target state snapshot (cross-phase)
- Audience: architects, new team members understanding full design

#### Why This Matters

Readers don't understand:
1. Why two documents describe different things
2. Which document to use for different questions
3. How project-target-state.md relates to way.md

Compounded by project-target-state.md using present tense to describe sensd, readers think it describes "current architecture" rather than "architecture snapshot during migration period".

#### Recommendation

Make the purposes explicit. Add to project-target-state.md header:

```markdown
> **Document Purpose**  
> This is an ARCHITECTURE SNAPSHOT of the complete Sinex system during the 
> JetStream migration period (2025-07 to 2025-Q4). It describes the system 
> as it exists while Phase 1-5 of the migration (see `docs/way.md`) are underway.
>
> For implementation details of the migration itself, see `docs/way.md`.
> For latest operational status, see `docs/way.md` Phase checklist.
```

Make the temporal frame clear for each section:

```markdown
## 4) Sensing (sensd) and stage-as-you-go

[CURRENT STATE — Being retired in Phase 5 per `docs/way.md`]

sensd [currently] centralizes acquisition...
```

---

### CONTRADICTION 6: Operational Notes Too Generic

**Severity:** MEDIUM  
**Location:** All vision/* documents  
**Impact:** Notes provide cover-all disclaimers without specific guidance

#### The Problem

Multiple vision documents have been given generic operational notes (added 2025-10-23):

```markdown
> **Operational note (2025-10-23)**  
> JetStream ingestion is canonical (`docs/way.md`). Any sensd/gRPC 
> references here are historical context.
```

This appears in:
- `project-target-state.md`
- `streaming-architecture.md`
- `future-event-pipeline-design.md`
- `semantic-desktop-stream.md`
- `multi-device-sync-architecture.md`
- `TIM-HyprlandNativePluginDev.md`
- `emergent-insights-and-extensions.md`

#### Why This Matters

The blanket note says "references here are historical context" but:
1. Doesn't specify WHICH references/sections
2. Doesn't mark historical content with inline tags
3. Appears at top, easy to miss
4. Entire sections still describe sensd in detail with present tense

Readers either:
- Skip the note entirely (assume document is current)
- Read the note and get confused about which sections apply

#### Recommendation

Replace generic notes with inline annotations:

**Instead of:** Blanket note at top  
**Use:** Specific tags in sections:

```markdown
## 4) Sensing (sensd) and stage-as-you-go

[BEING-MIGRATED — See `docs/way.md` Phase 5 for retirement plan]

sensd [currently] centralizes acquisition...
```

And for JetStream sections:

```markdown
## 5) Ingester SDK and unified stream

[CANONICAL — Implementation tracked in `docs/way.md` Phase 1-2]

Unified stream API...
```

This makes it clear which parts are historical and which are current.

---

## Broken References Inventory

### Files That Were Deleted

| File | Status | Referenced In | Handling Quality |
|------|--------|---|---|
| `docs/plan.md` | Superseded | way.md lines 3, 200 | GOOD - Listed in "Historical Files" section |
| `docs/plan_v3.txt` | Superseded | way.md line 201, Core_Architecture.md line 28, roadmap/FUTURE_EVENT_PIPELINE_DESIGN.md | POOR - Core_Architecture.md references without note |
| `docs/TARGET_final.md` | Deleted during consolidation | streaming-architecture.md line 96, event-taxonomy.md line 4 | MIXED - Accuracy notice exists, but "See Also" still references it |
| `new-plan.md` | Unknown | misc/_new_ideas_discussion.md | UNCLEAR - File doesn't appear in repo |

### Documents With Unhandled References

1. **`docs/architecture/Core_Architecture.md` line 28**
   - References: `docs/plan_v3.txt`
   - Issue: No acknowledgment that file was deleted
   - Severity: MEDIUM (undermines "canonical" claim)
   - Fix: Change to `docs/way.md`

2. **`docs/vision/streaming-architecture.md` line 96**
   - References: `docs/TARGET_final.md` 
   - Issue: References deleted file in "See Also" section
   - Severity: HIGH (contradicts accuracy notice in same doc)
   - Fix: Change to `docs/way.md` or remove

3. **`docs/roadmap/FUTURE_EVENT_PIPELINE_DESIGN.md`**
   - References: `docs/plan_v3.txt` (in code comment)
   - Issue: Minor, in code example
   - Severity: LOW
   - Fix: Update comment to reference docs/way.md

### Documents Handling Deleted References Well

1. **`docs/way.md` lines 197-204**
   - Explicitly lists old files as "Historical Files / For context only"
   - Quality: EXCELLENT

2. **`docs/vision/streaming-architecture.md` lines 9-10**
   - Accuracy notice acknowledging deletion
   - Quality: GOOD (though "See Also" undermines it)

3. **`docs/architecture/event-taxonomy.md` line 4**
   - Mentions TARGET_final.md as source of consolidated content
   - Quality: GOOD (doesn't create expectation of accessing it)

---

## Orphaned Documents Assessment

### Definitions

- **Orphaned:** Not linked from any index or other documents; unclear purpose
- **Archived:** Intentionally set aside; clearly marked as historical
- **Exploratory:** Not linked from canonical docs; acknowledged as brainstorming

### Findings

**NO TRUE ORPHANS FOUND** — All 67 documents have stated purposes:

1. **Canonical docs** (9 docs) - Linked from README.md and each other
2. **Vision/Future docs** (8 docs) - Listed in README under "Vision & Roadmap"
3. **Roadmap docs** (24 docs) - Organized in roadmap/ with clear feature focus
4. **Historical docs** (7 docs) - In docs/historical/ with stated archival purpose
5. **Misc/high-level docs** (13 docs) - In misc/ folder, acknowledged as "exploratory brainstorming"
6. **Testing docs** (2 docs) - In testing/ with clear purpose
7. **Root/guides** (3 docs) - Documentation guidelines, index, security

**Status:** All documents have stated purposes and are discoverable.

---

## sensd References Summary

### Where sensd Appears

| Document | Count | Tense | Status | Issue |
|----------|-------|-------|--------|-------|
| `docs/way.md` | 12 | Future/planning | CORRECT | Clearly describes removal in Phase 5 |
| `docs/vision/project-target-state.md` | 11 | Present/mixed | PROBLEMATIC | Mixes current and target descriptions |
| `docs/vision/streaming-architecture.md` | 0 | N/A | OK | Has ops note acknowledging historical context |
| `docs/architecture/Core_Architecture.md` | 0 | N/A | OK | Focuses on JetStream |
| `docs/architecture/provenance.md` | 0 | N/A | OK | Uses generic "Sensor" terminology |
| Other vision docs | Varies | Mixed | ADEQUATE | All have ops notes; could be more specific |

### Key Insight

Only **`docs/vision/project-target-state.md`** has problematic temporal language mixing present/past/future without clear markers. All other documents either:
- Correctly describe sensd as being-removed (way.md)
- Use abstracted terminology (provenance.md)
- Have operational notes (other vision docs)

---

## Accuracy Notices Found

### By Document

| Document | Type | Date | Quality |
|----------|------|------|---------|
| `docs/way.md` | Status declaration | (implicit 2025-10-23) | EXCELLENT - "authoritative" with clear supersessions |
| `docs/vision/project-target-state.md` | Operational note | 2025-10-23 | ADEQUATE - Could specify which sections |
| `docs/vision/streaming-architecture.md` | Accuracy Notice | 2025-07-24 | GOOD - Acknowledges problem |
| `docs/vision/streaming-architecture.md` | Operational note | 2025-10-23 | ADEQUATE - Redundant with accuracy notice |
| `docs/architecture/Core_Architecture.md` | Status declaration | (implicit) | OK but references deleted files |

### Quality Assessment

**Pattern Observed:**
- New operational notes (2025-10-23) added to vision documents
- These notes redirect to `docs/way.md`
- Good intent, but too generic (see Contradiction 6)

**Missing:**
- No "Last Verified" dates on canonical architectural docs
- No clear owner/maintainer assignments
- No review/update schedule

---

## Documentation Quality Recommendations

### Priority 1 (CRITICAL — Fix Immediately)

1. **Update `docs/architecture/Core_Architecture.md` line 28**
   - Remove reference to deleted `docs/plan_v3.txt`
   - Replace with `docs/way.md`

2. **Fix `docs/vision/streaming-architecture.md` line 96**
   - Remove or replace deleted `docs/TARGET_final.md` reference
   - Replace with `docs/way.md`

3. **Add temporal markers to `docs/vision/project-target-state.md`**
   - Annotate sensd sections as "being-migrated"
   - Refactor tense to be consistent with temporal frame
   - Add document header explaining "snapshot during migration"

**Effort:** ~30 minutes  
**Impact:** High - Eliminates broken references and reduces confusion

---

### Priority 2 (HIGH — Fix Within Sprint)

1. **Add progress tracking to `docs/way.md`**
   - Current phase indicator
   - Timeline
   - Blockers
   - Responsible owner

2. **Make operational notes more specific**
   - Replace blanket notes with inline annotations
   - Mark historical vs. canonical sections
   - Link to specific way.md sections

3. **Add temporal frame to vision documents**
   - Make clear these describe "current state during migration"
   - Explain they will be updated as phases complete

**Effort:** ~1-2 hours  
**Impact:** High - Clarifies document purposes and reduces temporal confusion

---

### Priority 3 (MEDIUM — Fix Before Release)

1. **Add "Last Verified" dates to canonical docs**
   - `way.md`: 2025-10-23
   - `Core_Architecture.md`: [verify date]
   - `provenance.md`: [verify date]
   - `event-taxonomy.md`: 2025-07-24 (implied)

2. **Update `docs/README.md` index**
   - Add document purpose labels
   - Clarify relationships between docs
   - Add update schedules

3. **Consolidate `project-target-state.md`**
   - Separate sections by phase/temporal frame
   - Use consistent tense
   - Add "See also" cross-references

4. **Consider deprecating/archiving low-value docs**
   - 13 "misc/high-level" documents
   - 7 historical documents
   - Some may be candidates for archival if content is consolidated

**Effort:** ~2-3 hours  
**Impact:** Medium - Improves discoverability and long-term maintainability

---

## Cross-Document Reference Map

### Core Architecture Documentation Chain

```
docs/README.md (index)
├── docs/way.md (AUTHORITATIVE migration plan)
│   ├── docs/architecture/Core_Architecture.md [BROKEN REF: plan_v3.txt]
│   ├── docs/architecture/provenance.md
│   ├── docs/architecture/event-taxonomy.md
│   ├── docs/architecture/security-architecture.md
│   └── docs/architecture/SystemOperations_And_Integrity_Architecture.md
├── docs/vision/manifesto.md
├── docs/vision/project-target-state.md [PROBLEMATIC: mixed tenses]
│   └── crate/core/sinex-gateway/doc/overview.md [external doc]
├── docs/vision/streaming-architecture.md [BROKEN REF: TARGET_final.md]
└── Other vision docs
```

### Issue Locations

- **Red (Critical):** None
- **Orange (High):** streaming-architecture.md line 96
- **Yellow (Medium):** Core_Architecture.md line 28, project-target-state.md sections 4/B/D/F

---

## Conclusion

The documentation provides good architectural guidance and is mostly well-organized. The main issues are:

1. **Temporal confusion** around sensd status (being migrated but described in present tense)
2. **Broken references** to deleted files (3 locations)
3. **Inconsistent annotations** for what's current vs. historical
4. **Missing progress tracking** on the migration itself

These are **fixable with focused edits** totaling 2-3 hours of work. No major reorganization needed; the document structure itself is sound.

**Recommended Action:** Address Priority 1 items immediately (30 minutes), then Priority 2 items in next sprint (1-2 hours).

---

## Appendix A: Document Purpose Quick Reference

| Document | Purpose | Audience | Currency |
|----------|---------|----------|----------|
| `way.md` | Migration playbook for JetStream refactor | Engineers | Current (2025-10-23) |
| `Core_Architecture.md` | System overview | All | Current (canonical) |
| `provenance.md` | Ingestion patterns guide | Engineers | Current |
| `event-taxonomy.md` | Event families canon | Engineers | Current (2025-07-24) |
| `security-architecture.md` | Security model | Architects | Current |
| `SystemOperations_And_Integrity_Architecture.md` | Operations patterns | DevOps/Operators | Current (2025-07-17) |
| `manifesto.md` | Strategic vision | All | Current (2025-10-23) |
| `project-target-state.md` | Architecture snapshot | Architects | Transitional (update in Phase 5) |
| `streaming-architecture.md` | Backpressure patterns | Engineers | Contextual (see way.md) |
| `historical/` | Archived essays | Researchers | Historical (reference only) |
| `misc/` | Exploratory brainstorming | Researchers | Exploratory (not canonical) |
| `roadmap/` | Future features | Product | Planning (subject to change) |

---

**End of Report**
