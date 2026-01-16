# Phase 3+ Parallelization Strategy

**Date:** 2026-01-16
**Context:** Phase 1 & 2 parallel execution in progress (Streams A-C complete, D running)
**Goal:** Maximum parallelization for Phases 3, 4, 5

---

## Executive Summary

**Phase 1 & 2:** 70% parallelizable (5-6 weeks via parallel streams)
**Phase 3:** 40% parallelizable (2-3 weeks, limited by sequential dependencies)
**Phase 4:** 95% parallelizable (2-4 months, all features independent)
**Phase 5:** 100% parallelizable (continuous, fully independent workstreams)

**Total Timeline to Production (Phases 1-3):** 9-13 weeks
**With Max Parallelization:** 7-10 weeks (2-3 weeks savings)

---

## Phase 3: The Decisive Cutover (2-3 weeks)

### Dependency Graph

```
3.1 Backend Rename (1 week)
  ↓
3.2 Rust CLI (1-2 weeks) ──┐
  ↓                        │  Can run in parallel
3.3 Python Deletion (1 day)│
                           ↓
3.4 NixOS Module (2-3 days)
```

**Critical Path:** 3.1 → 3.2 → 3.3 (sequential)
**Parallel Opportunity:** 3.4 can overlap with 3.2-3.3

---

### Stream Breakdown

#### **Stream H: Backend Rename** (Phase 3.1)
**Priority:** CRITICAL (blocks all of Phase 3)
**Duration:** 1 week
**Parallelizable:** NO (must complete first)
**Branch:** `feat/backend-rename-node-to-node`

**Tasks:**
- H1: Filesystem restructure (`git mv crate/nodes → crate/nodes`)
- H2: Type system rename using rust-analyzer SSR
- H3: Database schema migration (`processor_manifests → node_manifests`)
- H4: JSON schema updates
- H5: Bulk string replacements with `sd`

**Exit Criteria:**
- ✅ `cargo build` succeeds with new names
- ✅ All tests pass using new vocabulary
- ✅ Database uses `node_manifests` table
- ✅ Python CLI fails (expected - will be replaced)

**Note:** This INTENTIONALLY breaks the old CLI. No compatibility shims allowed.

---

#### **Stream I: Rust CLI (`sinexctl`)** (Phase 3.2)
**Priority:** CRITICAL
**Duration:** 1-2 weeks
**Parallelizable:** YES (with Stream J)
**Depends On:** Stream H (backend rename must complete first)
**Branch:** `feat/rust-cli-sinexctl`

**Tasks:**
- I1: Crate structure setup (`cli/sinexctl`, `cli/sinex-cli`)
- I2: Gateway RPC client implementation
- I3: Command tree (node, replay, ops, dlq, core, gateway)
- I4: Output formatting (table, JSON, YAML)
- I5: Authentication (token + mTLS)
- I6: MVP command implementation
- I7: Integration tests (CLI ↔ gateway)

**MVP Commands Required:**
```bash
sinexctl gateway ping
sinexctl gateway version
sinexctl core health
sinexctl node list [--role capture|synthesis]
sinexctl node status <id>
sinexctl replay plan --query '...'
sinexctl replay submit <plan-id>
sinexctl replay watch <operation-id>
sinexctl replay ls
sinexctl dlq ls
sinexctl dlq peek <subject>
```

**Exit Criteria:**
- ✅ `sinexctl` binary ships with all MVP commands
- ✅ All commands work via RPC (no direct DB access)
- ✅ `--json` output format works
- ✅ Integration tests pass
- ✅ Authentication works (token + mTLS)

---

#### **Stream J: NixOS Module Alignment** (Phase 3.4)
**Priority:** HIGH
**Duration:** 2-3 days
**Parallelizable:** YES (with Stream I)
**Depends On:** Stream H (backend rename must complete first)
**Branch:** `feat/nixos-module-rename`

**Tasks:**
- J1: Rename module file (`node-services.nix → node-services.nix`)
- J2: Update module options (`.nodes.* → .nodes.capture.*`, `.nodes.synthesis.*`)
- J3: Service builders (`mkNodeService`, `mkCaptureNodeService`, `mkSynthesisNodeService`)
- J4: Justfile target updates
- J5: VM test scenario renames

**Exit Criteria:**
- ✅ NixOS module uses "node" vocabulary
- ✅ Service configuration works with new options
- ✅ Justfile targets updated
- ✅ VM test scenarios use new names
- ✅ All NixOS services start successfully

---

#### **Stream K: Python CLI Deletion** (Phase 3.3)
**Priority:** CRITICAL
**Duration:** 1 day
**Parallelizable:** NO (must wait for Stream I)
**Depends On:** Stream I (Rust CLI must work first)
**Branch:** (can be done on `feat/rust-cli-sinexctl` branch)

**Tasks:**
- K1: Delete all Python CLI code (`git rm -rf cli/`)
- K2: Update documentation (replace `exo.py` → `sinexctl`)
- K3: Create migration guide (`docs/migration/python-cli-to-sinexctl.md`)
- K4: Update README quickstart

**Exit Criteria:**
- ✅ No Python CLI code remains
- ✅ All documentation uses `sinexctl`
- ✅ Migration guide published
- ✅ README updated

---

### Phase 3 Execution Plan

**Option 1: Maximum Parallelization (RECOMMENDED)**

```
Week 1:     Stream H (Backend Rename)              [sequential - blocks everything]
Week 2:     Stream I (Rust CLI) + Stream J (NixOS) [2-way parallel]
Week 3:     Stream I (finish) + Stream J           [2-way parallel]
Week 3 end: Stream K (Python Deletion)             [sequential - 1 day]
```

**Total:** 2-3 weeks
**Parallel Efficiency:** ~40% (2 of 4 streams can overlap)

**Option 2: Conservative Sequential**

```
Week 1:   Stream H
Week 2-3: Stream I
Week 3:   Stream J
Week 3:   Stream K
```

**Total:** 3-4 weeks

---

## Phase 4: Aspirational Features (2-4 months)

### Dependency Graph

```
All streams are INDEPENDENT - full parallelization possible!

4.1 SimpleProcessor      ║
4.2 sx Tool              ║  All can run
4.3 Wasm Runtime         ║  in parallel
4.4 Aggregation Runner   ║
4.5 Additional Backlog   ║
```

**Critical Path:** None (all independent)
**Parallel Opportunity:** 100% (all 5 streams can run simultaneously)

---

### Stream Breakdown

#### **Stream L: SimpleProcessor Trait** (Phase 4.1)
**Priority:** HIGH (unblocks rapid node development)
**Duration:** 2-3 weeks
**Parallelizable:** YES (fully independent)
**Branch:** `feat/simple-processor-trait`

**Tasks:**
- L1: Define `SimpleProcessor` trait in node-sdk
- L2: Implement auto-wrapper for `Node` trait
- L3: Auto-plumbing features (NATS, checkpoints, provenance, DLQ)
- L4: Migrate terminal-canonicalizer to SimpleProcessor
- L5: Migrate health-aggregator to SimpleProcessor
- L6: Documentation + examples

**Value:** 90% of nodes don't need manual checkpoint/state management

**Exit Criteria:**
- ✅ `SimpleProcessor` trait defined in node-sdk
- ✅ Auto-wrapper implements full `Node` trait
- ✅ At least 2 nodes migrated
- ✅ Documentation + examples published

---

#### **Stream M: `sx` Unified Tool** (Phase 4.2)
**Priority:** MEDIUM
**Duration:** 2-4 months
**Parallelizable:** YES (fully independent)
**Branch:** `feat/sx-tool`

**Tasks:**
- M1: Holographic dev environment (`sx dev`)
- M2: The Tether (`sx dev --tether prod`)
- M3: Deployment artifact generation (`sx deploy --oci`, `sx deploy --systemd`)
- M4: Operations commands (wrap `sinexctl`)
- M5: Monitoring TUI (`sx monitor`)
- M6: Absorb xtask functionality

**Value:** Unified developer experience like `cargo` or `git`

**Note:** This is NOT `sinexctl` (Phase 3). `sx` is for local dev + deployment, `sinexctl` is for production ops.

**Exit Criteria:**
- ✅ `sx dev` auto-detects dependencies and starts services
- ✅ `sx dev --tether` connects to production safely
- ✅ `sx deploy --oci` builds container from devenv.nix
- ✅ `sx monitor` provides real-time TUI dashboard
- ✅ Documentation for all sx commands

---

#### **Stream N: Wasm Runtime Integration** (Phase 4.3)
**Priority:** MEDIUM
**Duration:** 3-4 months
**Parallelizable:** YES (fully independent)
**Branch:** `feat/wasm-plugin-runtime`

**Tasks:**
- N1: Embed Wasmtime runtime in Gateway
- N2: Plugin SDK (Rust → Wasm) with examples
- N3: Capability sandboxing (subjects, filesystem, memory)
- N4: Hot reload without gateway restart
- N5: Implement 1 production plugin (e.g., PDF text extractor)

**Value:** Hot-swappable plugins, memory isolation for refinement logic

**Exit Criteria:**
- ✅ Gateway embeds Wasmtime runtime
- ✅ Plugin SDK with examples
- ✅ Capability sandboxing works
- ✅ Hot reload functional
- ✅ At least 1 production plugin

---

#### **Stream O: Standard Aggregation Runner** (Phase 4.4)
**Priority:** MEDIUM
**Duration:** 2-3 weeks
**Parallelizable:** YES (fully independent)
**Branch:** `feat/standard-aggregation-runner`

**Tasks:**
- O1: Define `Aggregator` trait in node-sdk
- O2: Implement `AggregationRunner` universal logic
- O3: Migrate health aggregator
- O4: Migrate analytics aggregator
- O5: KV snapshot persistence
- O6: Replay isolation

**Value:** Eliminates bespoke reducer logic in stateful automata

**Exit Criteria:**
- ✅ `Aggregator` trait defined
- ✅ `AggregationRunner` implements universal logic
- ✅ At least 2 automata migrated
- ✅ Snapshots persist to KV automatically
- ✅ Replay isolation works

---

#### **Stream P: Additional Backlog Items** (Phase 4.5)
**Priority:** LOW
**Duration:** Variable (1-4 weeks per item)
**Parallelizable:** YES (items are independent)
**Branch:** `feat/backlog-<item-name>`

**Items (pick and choose):**
- P1: Transaction Isolation Audit (1 week)
- P2: Hot-Path Allocation Audit (2-3 weeks)
- P3: JetStream Soak Tests in CI (1 week)
- P4: Universal Event Processing Middleware (2 weeks)
- P5: Stateful Automata Sharding (3-4 weeks)
- P6: HTTP Dependency Consolidation (1 week)
- P7: sinex-core Flattening Guardrails (1 week)
- P8: Schema Drift Control (1 week)

**Exit Criteria:** Per item (see UNIFIED_HARMONIZED_PLAN.md)

---

### Phase 4 Execution Plan

**Option 1: Maximum Parallelization (5-way parallel)**

```
Month 1-2:  Stream L + M + N + O + P1-P3  [5-way parallel]
Month 2-3:  Stream M + N (finish)         [2-way parallel]
Month 3-4:  Stream N (finish)             [sequential]
```

**Total:** 2-4 months
**Parallel Efficiency:** 95% (all streams independent)

**Option 2: Prioritized Sequential (focus on high-value first)**

```
Weeks 1-3:  Stream L (SimpleProcessor)         [HIGH priority]
Weeks 4-7:  Stream O (Aggregation Runner)      [MEDIUM priority]
Months 2-4: Stream M (sx tool)                 [MEDIUM priority]
Months 2-5: Stream N (Wasm runtime)            [MEDIUM priority]
Variable:   Stream P (backlog items as needed) [LOW priority]
```

**Total:** 4-6 months

---

## Phase 5: Ongoing Polish (Continuous)

### Dependency Graph

```
All streams are INDEPENDENT - full parallelization possible!

5.1 Observability      ║
5.2 Performance        ║  All can run
5.3 Advanced Testing   ║  in parallel
```

**Critical Path:** None (all independent)
**Parallel Opportunity:** 100% (all 3 streams can run simultaneously)

---

### Stream Breakdown

#### **Stream Q: Observability & Tracing** (Phase 5.1)
**Priority:** LOW
**Duration:** 2-3 weeks
**Parallelizable:** YES (fully independent)
**Branch:** `feat/opentelemetry-integration`

**Tasks:**
- Q1: OpenTelemetry integration (tracing propagation)
- Q2: Metrics export (Prometheus format)
- Q3: OTLP/Jaeger integration
- Q4: Per-node metrics
- Q5: Distributed tracing dashboards

**Value:** Production debugging, performance insights

---

#### **Stream R: Performance Optimization** (Phase 5.2)
**Priority:** LOW
**Duration:** 2-4 weeks
**Parallelizable:** YES (fully independent)
**Branch:** `feat/performance-optimization`

**Tasks:**
- R1: Clone audit (reduce `.clone()` calls)
- R2: Zero-copy deserialization
- R3: Buffer pooling in material assembler
- R4: Batch processing optimization
- R5: CPU/memory profiling

**Value:** Improved throughput, lower latency

---

#### **Stream S: Advanced Testing** (Phase 5.3)
**Priority:** LOW
**Duration:** 2-4 weeks
**Parallelizable:** YES (fully independent)
**Branch:** `feat/advanced-testing`

**Tasks:**
- S1: Fuzzing with libFuzzer
- S2: Property tests for database
- S3: Chaos engineering framework
- S4: CI integration for fuzz/chaos

**Value:** Higher confidence, fewer regressions

---

### Phase 5 Execution Plan

**Option 1: Maximum Parallelization (3-way parallel)**

```
Weeks 1-3:  Stream Q + R + S  [3-way parallel]
```

**Total:** 2-4 weeks
**Parallel Efficiency:** 100% (all streams independent)

**Option 2: Prioritized Sequential**

```
Weeks 1-3:  Stream Q (Observability)
Weeks 4-6:  Stream R (Performance)
Weeks 7-9:  Stream S (Advanced Testing)
```

**Total:** 7-9 weeks

---

## Overall Timeline with Maximum Parallelization

### Current Status (2026-01-16)

**✅ COMPLETE:**
- Phase 1.1: KV Coordination (Stream A)
- Phase 1.2: Gateway RPC (Stream B)
- Phase 2.1: Desktop Native APIs (Stream C)

**🔄 IN PROGRESS:**
- Phase 1.3: Test Modernization (Stream D)

**⏳ WAITING:**
- Phase 1.3 completion (blocks Phase 2.2)
- Phase 2.2: System node (Stream E - depends on D)
- Phase 1.4: Config Hygiene (Stream F - ready to launch)
- Phase 2.3: Security Hardening (Stream G - ready to launch)

---

### Remaining Timeline

**Weeks 3-4: Finish Phase 1 & 2**
```
Stream D (Test Modernization)           [running now, 1-2 weeks]
  ↓
Stream E + F + G (3-way parallel)       [2-3 weeks after D completes]
```

**Weeks 5-7: Phase 3 (Decisive Cutover)**
```
Week 5:   Stream H (Backend Rename)              [1 week, sequential]
Week 6-7: Stream I (Rust CLI) + J (NixOS)        [2-way parallel]
Week 7:   Stream K (Python Deletion)             [1 day, sequential]
```

**Months 3-6: Phase 4 (Aspirational Features)**
```
Months 3-4: Stream L + M + N + O + P (5-way parallel)
Months 4-6: Stream M + N finish (2-way parallel)
```

**Continuous: Phase 5 (Ongoing Polish)**
```
Stream Q + R + S (3-way parallel, as needed)
```

---

## Parallelization Summary

### Phase 1 & 2: 70% Parallelizable
- **Streams:** A, B, C (3-way parallel) ✅ DONE
- **Streams:** D (sequential blocker) 🔄 IN PROGRESS
- **Streams:** E, F, G (3-way parallel) ⏳ WAITING FOR D
- **Duration:** 5-6 weeks total

### Phase 3: 40% Parallelizable
- **Streams:** H (sequential) → I + J (2-way parallel) → K (sequential)
- **Duration:** 2-3 weeks

### Phase 4: 95% Parallelizable
- **Streams:** L, M, N, O, P (5-way parallel)
- **Duration:** 2-4 months

### Phase 5: 100% Parallelizable
- **Streams:** Q, R, S (3-way parallel)
- **Duration:** Continuous

---

## Execution Recommendations

### For Phase 3 (Next)

**Week 5: Launch Stream H immediately**
- Backend rename is the critical path blocker
- No parallelization possible
- Must complete before I, J, K can start

**Week 6-7: Launch Streams I + J in parallel**
```bash
# Single message, 2 Task calls
claude-code "Start Streams I and J in parallel per plan"
```

**Week 7 end: Launch Stream K**
```bash
# After Stream I completes
claude-code "Start Stream K (Python CLI Deletion) per plan"
```

### For Phase 4 (Future)

**Month 3: Launch 5 streams in parallel**
```bash
# Single message, 5 Task calls
claude-code "Start Streams L, M, N, O, P in parallel per plan"
```

**Priority ranking if resources limited:**
1. **Stream L** (SimpleProcessor) - HIGH value, unblocks rapid development
2. **Stream O** (Aggregation Runner) - HIGH value, reduces boilerplate
3. **Stream M** (sx tool) - MEDIUM value, improves DX
4. **Stream N** (Wasm runtime) - MEDIUM value, enables plugins
5. **Stream P** (Backlog items) - LOW value, polish only

### For Phase 5 (Continuous)

**Launch all 3 streams in parallel when capacity available:**
```bash
claude-code "Start Streams Q, R, S in parallel per plan"
```

---

## Risk Mitigation

### Phase 3 Risks

**Risk 1: Stream H (Backend Rename) takes longer than 1 week**
- **Mitigation:** Start immediately after D completes, allocate buffer time
- **Impact:** Delays all of Phase 3 (critical path)

**Risk 2: Stream I (Rust CLI) incomplete after 2 weeks**
- **Mitigation:** Define strict MVP scope, cut non-essential commands
- **Impact:** Delays Stream K, but NixOS module (J) can proceed

**Risk 3: Integration issues between I and H**
- **Mitigation:** Comprehensive integration tests in Stream H
- **Impact:** Could require fixes to H after I starts

### Phase 4 Risks

**Risk 1: Stream M (sx tool) scope creep**
- **Mitigation:** Define MVP for each command, ship incrementally
- **Impact:** Low (not on critical path)

**Risk 2: Stream N (Wasm runtime) security issues**
- **Mitigation:** Thorough security review, capability sandboxing
- **Impact:** Low (aspirational feature)

**Risk 3: Multiple parallel streams compete for resources**
- **Mitigation:** Prioritize L and O (high value), defer P items
- **Impact:** Medium (may extend timeline)

---

## Success Metrics

### Phase 3 Complete:
- ✅ Codebase uses "node" vocabulary universally
- ✅ `sinexctl` binary ships with all MVP commands
- ✅ Python CLI deleted
- ✅ NixOS modules updated
- ✅ All tests pass
- ✅ Production deployment works with new names

### Phase 4 Progress:
- ✅ `SimpleProcessor` implemented and documented
- ✅ At least 2 nodes using SimpleProcessor
- ✅ `sx dev` functional (auto-detection works)
- ✅ Wasm plugin system demonstrated (1+ plugin)
- ✅ Aggregation runner migrated (2+ automata)

### Phase 5 Ongoing:
- ✅ OpenTelemetry tracing active in production
- ✅ Performance benchmarks tracked in CI
- ✅ Fuzzing runs continuously
- ✅ Zero regressions from optimization work

---

## Conclusion

**Maximum Parallelization Strategy:**

1. **Phase 1 & 2:** 70% parallel (5-6 weeks) - IN PROGRESS
2. **Phase 3:** 40% parallel (2-3 weeks) - 2-way for I+J
3. **Phase 4:** 95% parallel (2-4 months) - 5-way for L+M+N+O+P
4. **Phase 5:** 100% parallel (continuous) - 3-way for Q+R+S

**Total to Production (Phases 1-3):** 7-10 weeks with max parallelization
**Without Parallelization:** 11-15 weeks
**Time Savings:** 4-5 weeks (30-40% reduction)

**Next Actions:**
1. Wait for Stream D completion (1-2 weeks)
2. Launch Streams E+F+G (3-way parallel)
3. Launch Stream H immediately after E+F+G merge
4. Execute Phase 3 with I+J parallel
5. Launch Phase 4 with 5-way parallelization

This strategy maximizes throughput while respecting architectural dependencies.

---

🤖 Generated with [Claude Code](https://claude.com/claude-code)

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>
