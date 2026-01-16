# Unified Harmonized Plan: Meta Documentation
**Date:** 2025-01-15
**Status:** Complete
**Purpose:** Navigation, context, and reconciliation for the Sinex unified planning effort

---

## Document Overview

This meta-document combines three perspectives on the planning harmonization:

1. **Executive Summary** - What was done, key findings, timeline
2. **Navigation Guide** - How to use the planning documents
3. **Detailed Reconciliation** - Deep analysis of all plans against codebase state

**Primary Document:** [`UNIFIED_HARMONIZED_PLAN.md`](./UNIFIED_HARMONIZED_PLAN.md) - THE single source of truth for all work

---

# Part 1: Executive Summary

## What Was Done

I analyzed all planning documents against the current codebase state and created a unified, executable roadmap.

### Documents Analyzed
1. `_kv_plan.md` - NATS KV coordination (85% complete)
2. `consolidated-backlog.md` - Work item tracking (mixed status)
3. `next/naming-revamp.md` - node → Node vocabulary (0% complete, excellent plan)
4. `next/cli-rewrite-plan.md` - Python → Rust CLI (0% complete, excellent plan)
5. `next/sinex-dx-devops-deployment-paradigm-shift.md` - `sx` binary, SimpleProcessor (20% complete)
6. `claude/deep-analysis-master-summary.md` - 151 cataloged issues (many resolved)

### Result: Single Unified Plan

**UNIFIED_HARMONIZED_PLAN.md** (Version 2.0 - Comprehensive, 1813 lines)
- Synthesizes best insights from all plans
- Makes strategic decisions (finish infrastructure first, decisive cutover)
- Provides 5 clear phases with exit criteria
- Includes timelines, effort estimates, risk assessment
- Contains concrete automation (SSR recipes, `sd` commands, SQL migrations)
- Clarifies `sinexctl` vs `sx` distinction
- Details aspirational features (`SimpleProcessor`, Wasm runtime, aggregation runner)
- **Status:** Supersedes ALL Previous Plans

---

## Key Findings

### Current State (from Reconciliation)

**What's Actually Done:**
- ✅ KV Coordination: 85% complete (production-ready, minor gaps)
- ✅ Event Sourcing: Mature and correct
- ✅ Testing Infrastructure: Industry-leading
- ✅ Error Handling: Exemplary

**What's Not Done:**
- ❌ Naming Revamp: 0% (comprehensive plan exists, zero execution)
- ❌ CLI Rewrite: 0% (design document only, Python CLI active)
- ❌ Deployment Paradigm: 20% (foundation solid, DX missing)

**The Pattern:**
Strong execution on infrastructure (NATS, coordination, JetStream), near-zero execution on developer experience (naming, CLI, unified tooling).

### Strategic Insights

**Insight 1: The Hybrid State Problem**
The codebase is in a hybrid state where infrastructure migrations (KV, pipeline testing, config types) are 70-85% complete. This creates maintenance burden and confusion.

**Solution:** Finish infrastructure first (Phases 1-2) before starting DX work.

**Insight 2: Incremental Migration Doesn't Work Here**
Attempting to maintain compatibility during naming/CLI changes creates double work and extends the hybrid state indefinitely.

**Solution:** Decisive cutover (Phase 3) - break backend, replace frontend simultaneously.

**Insight 3: Planning Quality ≠ Execution**
All 5 planning documents are excellent, comprehensive, and actionable. Yet only KV coordination has significant progress.

**Solution:** Time-box planning, focus on execution. Unified plan provides clear exit criteria and timeline.

---

## The Unified Plan (High-Level)

### Phase 1: Complete Infrastructure (3-4 weeks)
**Goal:** Eliminate hybrid state

**Tasks:**
1. Finish KV coordination (85% → 100%)
   - Schema broadcast cache fallback
   - Migrate legacy lifecycle test
   - Gateway coordination helpers
   - Drop legacy tables

2. Gateway RPC API completeness
   - Add DLQ management endpoints
   - Add node operations endpoints
   - Add generic operations endpoints
   - Add audit endpoint

3. Test modernization
   - Eliminate all DB-only patterns (180+ call sites)
   - Split "god files" (events.rs, material_assembler.rs)
   - Remove `create_test_event`, `db_only()` helpers

4. Config hygiene
   - Complete `Seconds`/`Bytes` migration
   - Add serde support for "30s" syntax
   - Enforce `SINEX_` env prefix

### Phase 2: Native Integration & Hardening (4-6 weeks)
**Goal:** Move nodes from scripts to native Rust

**Tasks:**
1. Desktop node: Replace shell-outs with native APIs (arboard, wayland-client)
2. System node: Replace journalctl subprocess with native bindings
3. Security hardening: mTLS integration tests, token rotation, build stamping

### Phase 3: The Decisive Cutover (2-3 weeks)
**Goal:** Execute naming + CLI as atomic operation

**Strategic Decision:** Instead of incremental migration, break backend and replace frontend simultaneously. Forces completion.

**Tasks:**
1. Backend rename (breaks old CLI)
   - Filesystem: `crate/nodes/` → `crate/nodes/{capture,synthesis}/`
   - Traits: `StatefulStreamProcessor` → `Node`
   - Enums: `ProcessorType` → `NodeRole`
   - Database: `processor_manifests` → `node_manifests`
   - **Includes:** Concrete SSR recipes and `sd` automation commands

2. Rust CLI replacement (`sinexctl`)
   - Ship `sinexctl` binary with MVP commands
   - RPC-only (no direct DB access)
   - JSON output, exit codes, auth support

3. Python CLI deletion
   - `git rm -rf cli/`
   - Update docs to use `sinexctl`

4. NixOS module alignment
   - Rename files, options
   - Update Justfile targets

### Phase 4: Aspirational Features (4-8 weeks)
**Goal:** Advanced developer experience

**Tasks:**
1. `SimpleProcessor` trait - High-level abstraction for 90% of nodes
2. `sx` unified dev tool - `sx dev`, `sx deploy`, `sx monitor`
3. Wasm runtime integration - Hot-swappable plugins via Wasmtime
4. Aggregation runner - Native complex event processing

### Phase 5: Ongoing Polish
**Goal:** Clean docs, add CI guardrails

**Tasks:**
1. Documentation cleanup (global terminology sweep)
2. CI guardrails (vocabulary gate, link checker)
3. Observability enhancements
4. Performance optimization

---

## Timeline

| Phase | Duration | Parallelizable | Risk |
|-------|----------|----------------|------|
| Phase 1 | 3-4 weeks | High | Low |
| Phase 2 | 4-6 weeks | Medium | Medium |
| Phase 3 | 2-3 weeks | Low | Medium |
| Phase 4 | 4-8 weeks | High | Low |
| Phase 5 | Ongoing | High | Low |

**Total:** 13-21 weeks (3-5 months) to "production-ready with aspirational features"

---

## What This Solves

### Problem 1: Fragmented Plans
**Before:** 5 separate planning documents, unclear which to follow
**After:** Single unified plan with clear phases and exit criteria

### Problem 2: Unclear Status
**Before:** Hard to know what's done vs. what's planned
**After:** Reconciliation provides detailed status tables (Part 3 of this doc)

### Problem 3: The Indefinite Loop
**Before:** Starting new work before finishing existing migrations
**After:** Phases are sequential with exit criteria; must finish before moving on

### Problem 4: The Hybrid State
**Before:** Half-migrated tests, configs, nodes
**After:** Phase 1-2 explicitly finish all infrastructure migrations

### Problem 5: Compatibility Debt
**Before:** Shims and aliases prolong migrations
**After:** Phase 3 uses decisive cutover (break and replace)

---

## Success Metrics

**Phase 1 Complete (3-4 weeks):**
- [ ] All tests pass without legacy DB tables
- [ ] Gateway exposes all RPC endpoints
- [ ] Zero `create_test_event` call sites
- [ ] All configs use `Seconds`/`Bytes`

**Phase 2 Complete (4-6 weeks):**
- [ ] nodes use native APIs
- [ ] mTLS enforced in CI
- [ ] All events contain git revision

**Phase 3 Complete (2-3 weeks):**
- [ ] Codebase uses "node" vocabulary
- [ ] `sinexctl` ships
- [ ] Python CLI deleted

**Phase 4 Complete (4-8 weeks):**
- [ ] `SimpleProcessor` trait available
- [ ] `sx` tool ships with core commands
- [ ] Wasm runtime integrated
- [ ] Aggregation runner operational

**Phase 5 Complete (ongoing):**
- [ ] Docs use "node" vocabulary
- [ ] CI enforces vocabulary
- [ ] Performance benchmarks green

---

## Quality Attributes

**Planning Quality:**
- ⭐⭐⭐⭐⭐ Comprehensive, detailed, actionable
- All original plans were excellent

**Execution Quality:**
- ⭐⭐⭐⭐ Strong infrastructure work
- ⭐⭐ Weak DX work (naming, CLI not started)

**Harmonization Quality:**
- ✅ Unified plan resolves fragmentation
- ✅ Reconciliation provides clear status
- ✅ Strategic decisions made (no more planning paralysis)
- ✅ Timeline and exit criteria defined
- ✅ Concrete automation preserved (SSR, `sd`, SQL)
- ✅ Aspirational features detailed, not deferred vaguely

---

## Conclusion

The Sinex project has excellent planning and strong infrastructure execution, but suffers from:
1. Fragmented plans (now unified)
2. Unclear status (now reconciled)
3. Hybrid state (phases 1-2 fix this)
4. Incomplete DX work (phases 3-4 address decisively)

The unified plan provides a **clear, executable path** from current state to production-ready system in 13-21 weeks.

**Critical Success Factor:** Execute phases sequentially. Do not start Phase 2 until Phase 1 exit criteria are met.

**Next Action:** Review and approve unified plan, then start Phase 1.1 (Finish KV Coordination).

---

# Part 2: Navigation Guide

## Start Here

**If you want to execute work:**
→ Read [`UNIFIED_HARMONIZED_PLAN.md`](./UNIFIED_HARMONIZED_PLAN.md)

**If you want to understand current state:**
→ Read Part 3 of this document (Detailed Reconciliation below)

**If you want historical context:**
→ Read [`claude/deep-analysis-master-summary.md`](./claude/deep-analysis-master-summary.md)

---

## Document Hierarchy

### Level 1: Actionable Plan (READ THIS)

| Document | Purpose | Status | Last Update |
|----------|---------|--------|-------------|
| **[UNIFIED_HARMONIZED_PLAN.md](./UNIFIED_HARMONIZED_PLAN.md)** | Definitive roadmap with phases, timelines, exit criteria | ✅ Current | 2025-01-15 |

### Level 2: Context & Analysis

| Document | Purpose | Status |
|----------|---------|--------|
| **UNIFIED_HARMONIZED_PLAN_META.md** (this doc) | Navigation, executive summary, reconciliation | ✅ Current |
| [claude/deep-analysis-master-summary.md](./claude/deep-analysis-master-summary.md) | 151 cataloged issues (Nov 2025) | Reference |

### Level 3: Historical (Superseded)

All source planning documents have been **removed** and their content internalized into the unified plan:
- ~~`_kv_plan.md`~~ - KV coordination (now Phase 1.1)
- ~~`consolidated-backlog.md`~~ - Work items (now distributed throughout phases)
- ~~`next/naming-revamp.md`~~ - Naming vocabulary (now Phase 3.1 with SSR recipes)
- ~~`next/cli-rewrite-plan.md`~~ - CLI architecture (now Phase 3.2-3.3)
- ~~`next/sinex-dx-devops-deployment-paradigm-shift.md`~~ - `sx` vision (now Phase 4.2)

---

## Reading Paths

### Path 1: "I want to start working"
1. Read [`UNIFIED_HARMONIZED_PLAN.md`](./UNIFIED_HARMONIZED_PLAN.md) (45 min)
   - Understand phases, timelines, strategic decisions
   - Identify which phase you're working on
2. Check Part 3 of this document for current status details
3. Execute work following exit criteria in unified plan

### Path 2: "I want to understand current state"
1. Read Part 1 (Executive Summary) above (10 min)
2. Read Part 3 (Detailed Reconciliation) below (45 min)
   - Detailed status of each plan
   - What's done vs. what's remaining
   - Cross-cutting themes and patterns
3. Review unified plan for forward-looking roadmap

### Path 3: "I want historical context"
1. Start with `claude/deep-analysis-master-summary.md` (1 hour)
   - 151 issues cataloged with file references
   - Architectural strengths identified
   - Code metrics and quality assessment
2. Read Part 3 (Reconciliation) to see how issues were addressed
3. Review unified plan to see strategic decisions

---

## Key Concepts & Terminology

### Plans Status Legend
- ✅ **COMPLETE** - Fully implemented and tested
- ⚠️ **PARTIAL** - Significant progress but gaps remain
- ❌ **NOT STARTED** - Planning only, zero implementation
- 🟢 **DEFER** - Intentionally postponed

### Phase Overview (from Unified Plan)

**Phase 1: Complete Infrastructure (3-4 weeks)**
- Finish KV coordination (85% → 100%)
- Gateway RPC API completeness
- Test modernization
- Config hygiene

**Phase 2: Native Integration & Hardening (4-6 weeks)**
- Desktop node: native APIs
- System node: native journal
- Security hardening (mTLS, tokens)

**Phase 3: The Decisive Cutover (2-3 weeks)**
- Backend rename (node → node) with SSR automation
- Rust CLI replacement (`sinexctl`)
- Python CLI deletion
- NixOS module alignment

**Phase 4: Aspirational Features (4-8 weeks)**
- `SimpleProcessor` trait
- `sx` unified dev tool
- Wasm runtime integration
- Aggregation runner

**Phase 5: Ongoing Polish**
- Documentation cleanup
- CI guardrails
- Observability and performance

---

## Quick Reference: What to Read for Specific Topics

### KV Coordination & NATS
- Primary: [`UNIFIED_HARMONIZED_PLAN.md`](./UNIFIED_HARMONIZED_PLAN.md) Phase 1.1
- Status: Part 3 of this document, Section 1

### Naming (node → Node)
- Primary: [`UNIFIED_HARMONIZED_PLAN.md`](./UNIFIED_HARMONIZED_PLAN.md) Phase 3.1
- Status: Part 3 of this document, Section 2

### CLI (Python → Rust)
- Primary: [`UNIFIED_HARMONIZED_PLAN.md`](./UNIFIED_HARMONIZED_PLAN.md) Phase 3.2-3.3
- Status: Part 3 of this document, Section 3

### nodes (Desktop, System, Terminal)
- Primary: [`UNIFIED_HARMONIZED_PLAN.md`](./UNIFIED_HARMONIZED_PLAN.md) Phase 2.1-2.2
- Analysis: [`claude/deep-analysis-master-summary.md`](./claude/deep-analysis-master-summary.md)

### Testing Infrastructure
- Primary: [`UNIFIED_HARMONIZED_PLAN.md`](./UNIFIED_HARMONIZED_PLAN.md) Phase 1.3
- Patterns: `../current/testing/TEST_PATTERNS.md`

### Gateway & RPC
- Primary: [`UNIFIED_HARMONIZED_PLAN.md`](./UNIFIED_HARMONIZED_PLAN.md) Phase 1.2

### SimpleProcessor & sx Tool
- Primary: [`UNIFIED_HARMONIZED_PLAN.md`](./UNIFIED_HARMONIZED_PLAN.md) Phase 4.1-4.2

---

## Anti-Patterns to Avoid

Based on the reconciliation analysis, avoid these mistakes:

### 1. The Indefinite Loop
**Problem:** Starting new work before finishing existing migrations
**Solution:** Follow unified plan phases sequentially

### 2. The Hybrid State
**Problem:** Half-migrated patterns (DB-only + pipeline, old + new vocab)
**Solution:** Complete migrations fully before moving on

### 3. The Compatibility Shim
**Problem:** Adding aliases/compatibility layers that prolong migration
**Solution:** Phase 3 uses decisive cutover (break and replace)

### 4. The Planning Paralysis
**Problem:** Creating excellent plans but never executing
**Solution:** Time-box planning, focus on execution

### 5. The Scope Creep
**Problem:** Adding "nice to have" features during critical path work
**Solution:** Defer non-blocking features to Phase 4-5

---

## Communication

### For Team Members
**Starting work on Phase N:**
1. Announce in team chat/stand-up
2. Reference unified plan
3. Update plan document with "In Progress" status
4. Create tracking issue/PR

**Completing Phase N:**
1. Update unified plan with completion date
2. Mark all related items as complete
3. Announce completion + lessons learned

### For Future Contributors
**When onboarding:**
1. Start with unified plan (45 min read)
2. Read this meta doc for context (30 min read)
3. Identify current phase
4. Ask questions in team chat

**When confused:**
1. Check this document for navigation
2. Read unified plan for strategic context
3. Check reconciliation (Part 3) for detailed status
4. Ask in team chat if still unclear

---

## Document Maintenance

### Updating This Meta Document
When plans evolve:
1. Update Part 1 (Executive Summary) with new findings
2. Update Part 3 (Reconciliation) with current status
3. Keep Part 2 (Navigation) links current
4. Update "Last Updated" date below

### Document Lifecycle

**Active Plan:**
- Keep `UNIFIED_HARMONIZED_PLAN.md` updated as implementation progresses
- Add "Status Update" sections with dates
- Mark phases as complete when exit criteria met

**Meta Documentation:**
- Update this document after each phase completion
- Reconcile with deep-analysis issues quarterly
- Archive stale sections to `docs/exploration/archive/` when appropriate

---

## Last Updated
**Date:** 2025-01-15
**By:** Planning reconciliation and harmonization effort
**Next Review:** After Phase 1 completion (~February 2025)

---

# Part 3: Detailed Reconciliation

## Executive Summary

The Sinex project has **5 major planning documents** covering different aspects of evolution. Analysis reveals:

- **✅ KV Coordination**: 85% complete (production-ready, minor gaps)
- **❌ Naming Revamp**: 0% complete (comprehensive plan exists, zero execution)
- **❌ CLI Rewrite**: 0% complete (design document only, Python CLI active)
- **❌ Deployment Paradigm**: 20% complete (foundation solid, `sx` binary and `SimpleProcessor` missing)
- **⚠️ Issue Backlog**: Mix of completed, in-progress, and stale items

**Critical Finding:** The codebase has **strong execution on infrastructure** (NATS/KV, coordination, JetStream) but **zero execution on developer experience improvements** (naming, CLI, unified tooling).

---

## 1. KV Coordination Implementation Plan

**Status:** ✅ **85% COMPLETE** (Production-Ready)

### What's Done

✅ **KV Buckets Implemented & Active**
- `KV_sinex_instances` - node instance registry with heartbeat
- `KV_sinex_leadership` - CAS-based leadership election with 15s TTL
- `KV_sinex_checkpoints` - checkpoint persistence (already in use)
- Location: `crate/lib/sinex-core/src/coordination/kv_client.rs`

✅ **CoordinationKvClient Module** (186 lines, production-ready)
- `register_instance()`, `heartbeat()`, `acquire_leadership()`, `release_leadership()`
- Robust CAS semantics with deleted state detection
- Location: `crate/lib/sinex-core/src/coordination/kv_client.rs`

✅ **nodeCoordination Migrated to NATS**
- Constructor is **async** and takes NATS client + JetStream context
- **No database dependency** for coordination
- Leadership loop uses 5-second CAS interval
- Location: `crate/lib/sinex-node-sdk/src/coordination.rs` (640 lines)

✅ **Schema Broadcast Publishing**
- Ingestd publishes schemas to `system.schemas.active` on startup
- Location: `crate/core/sinex-ingestd/src/service.rs`

✅ **E2E Tests Migrated**
- `tests/e2e/tests/service_recovery_test.rs` - uses `CoordinationKvClient`
- `tests/e2e/tests/coordination_resilience_test.rs` - KV coordination tests
- Both verify leadership, heartbeat, failover via NATS KV

✅ **Edge Mode Support** (`SINEX_EDGE_MODE=1`)
- nodes can run without PostgreSQL
- Coordination via NATS KV only
- Checkpoints persist to KV store

### What's Incomplete

⚠️ **SchemaBroadcastCache Fallback Logic**
- Cache exists (`crate/lib/sinex-node-sdk/src/runtime/stream/mod.rs:43-64`)
- Listener subscribes to `system.schemas.active`
- **Gap:** Validators don't prefer cache over DB; no fallback documented
- **Impact:** Edge mode nodes can't validate schemas without DB

⚠️ **Legacy Test Using Old DB Path**
- `crate/lib/sinex-node-sdk/tests/integration/node_lifecycle_test.rs`
- Lines 136-147: Still uses old `nodeCoordination::new(instance, pool)` constructor
- Lines 412-417: Queries `core.node_instances` table directly
- **Action:** Migrate to async KV-based constructor

❌ **Gateway/CLI Coordination State Helpers**
- No RPC methods to query coordination state from KV
- CLI can't show current leader, instance health, or heartbeat age
- **Impact:** No observability into distributed coordination

❌ **Legacy Table Cleanup**
- `core.node_instances` still exists in schema (not actively used except legacy test)
- **Action:** Drop table after migrating `node_lifecycle_test.rs`

---

## 2. Naming Revamp (node → Node)

**Status:** ❌ **0% COMPLETE** (Comprehensive Plan, Zero Execution)

### Current State: 100% Old Vocabulary

**All old terminology remains in active use:**

- **Trait:** `StatefulStreamProcessor` (not `Node`)
- **Enum:** `ProcessorType::Ingestor | Automaton` (not `NodeRole::Capture | Synthesis`)
- **Crates:** `sinex-node-sdk`, `sinex-processor-runtime` (not `sinex-node-sdk`, `sinex-node-runtime`)
- **Directory:** `crate/nodes/` (not `crate/nodes/capture/` or `crate/nodes/synthesis/`)
- **Table:** `core.processor_manifests` with `processor_type` column (not `core.node_manifests` with `role`)
- **Check constraint:** `processor_type IN ('ingestor', 'automaton', 'agent', 'system')`

### Plan Quality

**The naming-revamp plan was excellent:**
- Comprehensive 9-phase migration strategy
- Concrete SSR recipes and automation toolkit
- Clear rationale for Option B ("Node / Pipeline")
- Deliverable checklist and CI guardrails

**These have been internalized into Phase 3.1 of the unified plan.**

### Why It Hasn't Started

**Reasons inferred from codebase state:**
1. **No blocking technical issue** - the current vocabulary works
2. **Large blast radius** - 400+ Rust files, schema, tests, docs, NixOS modules
3. **Requires coordination** - single-cutover change across entire stack
4. **Higher priority work** - KV coordination, JetStream migration took precedence

### Recommendation

**Execute in one sprint (Phase 3):**
- The automation is ready (SSR + `sd` scripts provided in unified plan)
- Estimated effort: **2-3 weeks** for complete migration if dedicated
- **Not blocking** any current functionality
- **High value** for developer onboarding and conceptual clarity

---

## 3. CLI Rewrite (Python → Rust)

**Status:** ❌ **0% COMPLETE** (Design Document Only)

### Current State: Python CLI Active

**Python CLI (`cli/exo.py`):**
- 183 KB, heavily used, recently updated (TLS hardening, Jan 2025)
- RPC-first design (default: `https://127.0.0.1:9999`)
- Fallback to direct PostgreSQL via `--use-db` flag
- Supporting files: `rpc_client.py`, `replay_commands.py`, `interactive.py`, `completion.py`

**Commands implemented:**
- `query`, `sources`, `stats` - RPC-backed
- `replay.*` - RPC-backed (complete FSM)
- `dlq.*` - **DB-only** (no RPC endpoints)
- `schema.*` - **DB-only**
- `blob.*` - Mixed RPC/DB

### Gateway RPC API Gaps

**Missing RPC methods that block Rust CLI:**

| Feature | Status | Impact |
|---------|--------|--------|
| DLQ management | ❌ No RPC | `dlq list/show/retry/resolve/purge` require SQL |
| Node operations | ❌ No RPC | No `nodes list/status/drain/set-horizon` API |
| Generic operations log | ⚠️ Partial | Replay ops via RPC; generic `ops start/ls` not exposed |
| Schema introspection | ⚠️ DB-only | `schema list/get` require `--use-db` |
| Audit trail | ❌ No API | No operation timeline endpoint |

### Plan Quality

**The cli-rewrite plan was production-ready:**
- Complete command tree design (`sinexctl`)
- Typed client architecture (`GatewayClient`, `AdminClient`, `NodeClient`)
- 6-phase migration strategy
- Explicit "thin client, thick services" principle
- Safety rails (dry-run, idempotency keys, version handshake)

**These have been internalized into Phase 3.2-3.3 of the unified plan.**

### Why It Hasn't Started

**Blockers identified:**
1. **Gateway API incomplete** - DLQ, node-admin, ops endpoints missing
2. **No forcing function** - Python CLI works well enough
3. **Higher priority work** - Core infrastructure (NATS, coordination) took precedence
4. **Large effort** - Estimated 2-3 months for MVP

### Recommendation

**Incremental approach:**
1. **First:** Add missing RPC endpoints to gateway (Phase 1.2)
2. **Then:** Ship `sinexctl` as thin RPC client (Phase 3.2)
3. **Finally:** Delete Python CLI once `sinexctl` reaches feature parity (Phase 3.3)

**Current risk:** The `--use-db` paths (DLQ, schema) create a second control plane that can diverge from gateway state machine.

---

## 4. Deployment Paradigm (`sx` Binary & SimpleProcessor)

**Status:** ⚠️ **20% COMPLETE** (Foundation Solid, DX Layer Missing)

### What's Implemented

✅ **Appliance Model**
- `devenv.nix` as single source of truth (Postgres 16, TimescaleDB, NATS JetStream, Git-Annex)
- Stateless binaries in Nix store, persistent state in `stateRoot`
- NixOS service module respects modularity

✅ **Event Sourcing Kernel**
- TimescaleDB hypertable for `core.events` with ULID, JSONB, provenance
- NATS JetStream as sole event transport
- `ingestd` as canonical DB writer

✅ **StatefulStreamProcessor SDK** (Mature)
- Trait-based architecture for all processors
- Checkpoint/KV coordination via NATS
- Event provenance (Material/Synthesis/Inference)
- Replay control and DLQ retry

✅ **nodes & Automata** (10+ implementations)
- Terminal, desktop, system, filesystem watchers
- Search, analytics, content, PKM automata
- Health aggregator

### What's Missing (Critical Path)

❌ **`sx` Unified Binary**
- **Expected:** `sx deploy`, `sx run seal`, `sx dev`, `sx dev --tether prod`
- **Current:** Individual binaries + `cargo xtask` (Check, Test, Db, Schema, Dev)
- **Impact:** Fragmented developer experience

❌ **`SimpleProcessor` Trait**
- **Expected:** High-level trait for 90% of use cases, auto-plumbing NATS/provenance
- **Current:** Only `StatefulStreamProcessor` exists (lower-level, manual checkpoint/state)
- **Impact:** Boilerplate for new nodes/automata

❌ **Wasm Runtime Integration**
- **Expected:** Wasmtime for memory-isolated refinement logic, hot-swappable plugins
- **Current:** Zero references to Wasmtime, WASI, or Wasm in codebase
- **Impact:** No plugin system

❌ **OCI Container Build**
- **Expected:** `sx deploy` builds containers from `devenv.nix`
- **Current:** No container build infrastructure
- **Impact:** Manual deployment process

❌ **Tether for Live Debugging**
- **Expected:** `sx dev --tether prod` tunnels to production with shadow consumers
- **Current:** No tunneling or shadow table infrastructure
- **Impact:** No safe production debugging

### Recommendation

**Prioritize `SimpleProcessor` abstraction (Phase 4.1):**
- Highest ROI for developer productivity
- Unblocks rapid node/automaton development
- Can wrap existing `StatefulStreamProcessor` (no breaking changes)

**Build `sx` unified tool (Phase 4.2):**
- `sx dev`, `sx deploy`, `sx monitor` commands
- Integrates with existing `cargo xtask` infrastructure
- Improves developer experience significantly

**Defer Wasm and advanced features (Phase 4.3-4.4):**
- Nice to have, not blocking core functionality
- Can be added incrementally after `SimpleProcessor` and `sx` ship

---

## 5. Consolidated Backlog

**Status:** ⚠️ **Mix of Complete, In-Progress, and Stale** (Now Internalized)

### Cross-Reference with Unified Plan

All backlog items have been internalized into the unified plan phases:

| Original Backlog Item | Status | Unified Plan Phase |
|----------------------|--------|-------------------|
| Complete KV coordination cleanup | ⚠️ 85% done | Phase 1.1 |
| Units/size/times use raw integers | ✅ Mostly done | Phase 1.4 (Config hygiene) |
| Hot-path clone/alloc audit | ❌ Not started | Phase 5 (Performance) |
| JetStream harness load/regression guard | ⚠️ Test exists, CI missing | Phase 5 (CI guardrails) |
| Schema drift control | ⚠️ CI runs, enforcement missing | Phase 5 (CI guardrails) |
| Consolidate HTTP dependency stacks | ⚠️ Confirmed issue | Phase 1.4 (Dependency cleanup) |
| Standardize test organization | ⚠️ Partial | Phase 1.3 (Test modernization) |

### Items Already Addressed by KV Migration

**These are now complete:**
- ✅ "Checkpoint persistence in KV (already partially adopted) as the only supported path"
- ✅ "Coordination mode uses NATS KV for node registration and leadership"
- ✅ "E2E coordination tests migrated to KV paths"

### Items Now Covered by Unified Plan

**These are superseded and internalized:**
- Vocabulary migration work → Phase 3.1
- CLI direct DB surgery issues → Phase 3.2-3.3
- RPC endpoint gaps → Phase 1.2

---

## 6. Deep Analysis Issues

**Status:** ⚠️ **151 Issues Cataloged** (Nov 2025 Analysis)

### Reconciliation with Current State

**HIGH PRIORITY Issues Still Valid:**

| Issue # | Category | Status | Notes |
|---------|----------|--------|-------|
| 1 | No backpressure on event publishing | ⚠️ Valid | `nats_publisher.rs:54` double-await with no timeout |
| 13 | Unbounded slice buffer | ⚠️ Valid | `material_assembler.rs:48` no MAX_BUFFERED_SLICES |
| 19 | FS-Watcher event queue overflow | ⚠️ Valid | 256-event buffer can drop events silently |
| 32 | No timeout on external commands (desktop) | ⚠️ Valid | `wl-paste`/`xclip` can hang indefinitely |
| 37 | No Unix socket read timeout (window manager) | ⚠️ Valid | `next_line()` blocks forever on Hyprland socket |
| 42 | Udev 5-second polling | ⚠️ Valid | Should use inotify for real-time detection |
| 58 | ILIKE on payload text is slow | ⚠️ Valid | Full table scan, needs GIN index |
| 60 | No TimescaleDB retention policy | ⚠️ Valid | 90-day retention documented but not enforced |
| 66 | Infinite loop on database acquisition | ⚠️ Valid | Test pool can hang forever |
| 76 | NATS batch processing no backpressure | ⚠️ Valid | Batches of 200 with no rate limiting |
| 82 | Potential deadlock in poisoned mutex recovery | ⚠️ Valid | Should use `parking_lot` |
| 98 | Potential Arc cycle in MaterialAssembler | ⚠️ Valid | Circular Arc prevents cleanup |

**Issues Resolved by KV Migration:**
- ✅ Issue 6: Advisory lock lost detection → KV coordination uses CAS, not advisory locks
- ✅ Coordination DB table queries → Coordination is now NATS KV-native

**Issues Superseded by Unified Plan:**
- Many architectural recommendations (naming, CLI, deployment) already captured in unified plan phases

### Recommendation

**Update deep-analysis-master-summary.md:**
1. Mark KV-related issues as resolved
2. Cross-reference naming/CLI issues with unified plan phases
3. Create new "2025-01 Status" section with reconciliation
4. Focus remaining issues on performance, security, and correctness (Phase 5)

---

## 7. Cross-Cutting Themes

### Theme 1: Infrastructure vs. Developer Experience

**Strong Infrastructure Execution:**
- ✅ NATS/KV coordination
- ✅ JetStream event transport
- ✅ Event sourcing with provenance
- ✅ TimescaleDB partitioning
- ✅ Distributed coordination (leadership, checkpoints)

**Weak Developer Experience Execution:**
- ❌ Naming vocabulary (node/processor terminology)
- ❌ CLI tooling (Python with DB fallbacks)
- ❌ Unified developer interface (`sx` binary)
- ❌ High-level SDK abstractions (`SimpleProcessor`)

**Analysis:** The team prioritizes **correctness and reliability** over **ergonomics**.

### Theme 2: Planning Quality vs. Execution

**High-Quality Plans:**
- All 5 planning documents were comprehensive, well-reasoned, and actionable
- Clear phases, deliverables, and automation toolkits provided
- Naming revamp even included SSR recipes and `sd` commands

**Low Execution Rate:**
- Only KV coordination has significant progress (85%)
- Naming, CLI, deployment paradigm were 0-20% complete
- Gap suggests **resource constraints** or **priority tradeoffs**

### Theme 3: Technical Debt vs. Aspirational Architecture

**Low Technical Debt:**
- Error handling is exemplary (`SinexError` with 19 variants, rich context)
- Testing is industry-leading (unit, integration, property, adversarial)
- Architecture is clean (event sourcing, CQRS, provenance)

**High Aspirational Gap:**
- Many "nice to have" features planned but not blocking
- Focus is on **shipping correct functionality** first
- DX improvements are secondary

---

## 8. Recommendations

### Immediate Actions (1-2 Weeks)

1. **Finish KV Coordination (85% → 100%)** [Phase 1.1]
   - Implement `SchemaBroadcastCache` fallback logic
   - Migrate `node_lifecycle_test.rs` to KV constructor
   - Add gateway RPC helpers for coordination state
   - Drop `core.node_instances` table

2. **Add Missing Gateway RPC Endpoints** [Phase 1.2]
   - DLQ management (`dlq list/retry/resolve/purge`)
   - Node operations (`nodes list/status/drain/set-horizon`)
   - Generic operations log (`ops start/ls/cancel`)
   - Audit trail (`core audit <operation-id>`)
   - **Impact:** Unblocks RPC-only CLI mode

### Short-Term (1-3 Months)

3. **Test Modernization** [Phase 1.3]
   - Eliminate all DB-only test patterns (180+ call sites)
   - Split "god files" (events.rs, material_assembler.rs)
   - Remove `create_test_event`, `db_only()` helpers

4. **Config Hygiene** [Phase 1.4]
   - Complete `Seconds`/`Bytes` migration
   - Add serde support for "30s" syntax
   - Enforce `SINEX_` env prefix

5. **Native Integration** [Phase 2]
   - Desktop node: native APIs (arboard, wayland-client)
   - System node: native journal bindings
   - Security hardening: mTLS, token rotation, build stamping

### Medium-Term (3-5 Months)

6. **Execute Naming Revamp** [Phase 3.1]
   - Use provided SSR recipes and automation toolkit
   - Single-cutover migration (2-3 week sprint)
   - Add CI vocabulary gate to prevent regression

7. **Ship Rust CLI** [Phase 3.2-3.3]
   - `sinexctl` as thin RPC client
   - MVP commands (query, replay, dlq, nodes)
   - Delete Python CLI once feature parity reached

8. **Implement `SimpleProcessor` Abstraction** [Phase 4.1]
   - Wrapper around `StatefulStreamProcessor` with auto-plumbing
   - 90% use case coverage for new nodes/automata
   - **Impact:** Reduces boilerplate, accelerates development

### Long-Term (5-8 Months)

9. **Build `sx` Unified Tool** [Phase 4.2]
   - `sx dev`, `sx deploy`, `sx monitor` commands
   - Integrates with existing infrastructure
   - Improves developer experience significantly

10. **Advanced Features** [Phase 4.3-4.4]
    - Wasm runtime integration
    - OCI container generation
    - Aggregation runner
    - Tether for live debugging

---

## 9. Plan Priority Matrix

| Plan Component | Completion | Priority | Effort | ROI |
|----------------|------------|----------|--------|-----|
| KV Coordination | 85% | 🔴 HIGH | 1-2 weeks | **High** (completes migration) |
| Gateway RPC Endpoints | 0% | 🔴 HIGH | 2-4 weeks | **High** (unblocks RPC-only CLI) |
| Test Modernization | 30% | 🟠 MEDIUM | 2-3 weeks | **Medium** (technical debt reduction) |
| Config Hygiene | 70% | 🟠 MEDIUM | 1-2 weeks | **Medium** (code quality) |
| Native Integration | 0% | 🟠 MEDIUM | 4-6 weeks | **Medium** (reliability) |
| Naming Revamp | 0% | 🟡 LOW | 2-3 weeks | **Medium** (clarity, onboarding) |
| Rust CLI | 0% | 🟡 LOW | 2-3 weeks | **Medium** (Python CLI functional) |
| SimpleProcessor | 0% | 🟠 MEDIUM | 2-3 weeks | **High** (developer productivity) |
| `sx` Tool | 0% | 🟢 DEFER | 4-6 weeks | **Medium** (DX polish) |
| Wasm/OCI/Tether | 0% | 🟢 DEFER | 4-8 weeks | **Low** (aspirational) |

**Legend:**
- 🔴 HIGH: Blocking or high-impact
- 🟠 MEDIUM: Important but not blocking
- 🟡 LOW: Nice to have
- 🟢 DEFER: Aspirational, low priority

---

## 10. Conclusion

**The Sinex project has strong technical foundations but weak developer experience tooling.**

**Strengths:**
- Event sourcing architecture is mature and correct
- NATS/KV coordination is nearly complete (85%)
- Testing infrastructure is industry-leading
- Error handling is exemplary

**Gaps:**
- Developer experience improvements are 0-20% complete
- All planning documents were comprehensive but not executed
- Resource constraints or priority tradeoffs favor core functionality over ergonomics

**Critical Path Forward:**
1. Finish KV coordination (2 weeks) [Phase 1.1]
2. Add gateway RPC endpoints (4 weeks) [Phase 1.2]
3. Test modernization (3 weeks) [Phase 1.3]
4. Config hygiene (2 weeks) [Phase 1.4]
5. Native integration (6 weeks) [Phase 2]
6. Decisive cutover (naming + CLI) (3 weeks) [Phase 3]
7. Aspirational features (8 weeks) [Phase 4]
8. Ongoing polish [Phase 5]

**Estimated Timeline to "Complete Plans":**
- **Infrastructure Complete:** 9-11 weeks (Phases 1-2)
- **Full Cutover:** 12-14 weeks (add Phase 3)
- **With Aspirational Features:** 20-22 weeks (add Phase 4)
- **Production Polish:** Ongoing (Phase 5)

The unified plan provides clear sequencing: **finish infrastructure first** (Phases 1-2), then **execute decisive cutover** (Phase 3), then **add aspirational features** (Phase 4), and **polish continuously** (Phase 5).

---

**End of Meta Documentation**
