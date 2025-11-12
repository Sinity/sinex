# Sinex Implementation Plan (Active Initiatives)

_Status: working draft. Captures all currently agreed technical decisions and the tasks required to land them. Update this file whenever scope changes. Authority flows from `docs/way.md` (JetStream refactor) → this plan → per-component READMEs._

---

## 1. Schema Pipeline Unification

**Decision:** Rust `derive(EventPayload)` definitions are the single source of truth. JSON files under `schemas/` exist as generated artifacts for GitOps distribution and downstream SDKs—never hand‑edit them.

### Tasks
- [x] Amend `schemas/README.md` with a “Source of Truth” section that states the rule above and explains how to regenerate artifacts.
- [x] Create a CI job (or extend schema-validation) that runs `schema-dev.sh generate` and fails when committed JSON is stale (treat like `cargo fmt`).
- [x] Teach contributors to commit regenerated JSON (README callouts + PR-template checkbox make this explicit; no merge bot planned).
- [x] Keep `scripts/check-schema-compatibility.sh`, `schema-validation.yml`, and `gitops_schema_sources` pointing at the generated bundle. Document the flow from Rust → JSON → Postgres → downstream consumers.
- [x] Document how non-Rust contributors propose schema changes (JSON PR as proposal, build fails until the matching Rust change lands).

**Exit criteria:** CI enforces zero drift; ingestd bootstraps via DB content created from the generated bundle; README explicitly warns against manual JSON edits.

---

## 2. Documentation Alignment (JetStream-only World)

**Decision:** All current docs must reflect the JetStream-only ingestion described in `docs/way.md`. Transactional outbox references move to historical sections.

### Tasks
- [x] Sweep docs for “transactional outbox,” “sensd,” or “gRPC ingestion” references and either delete or mark as historical context. (Historical banners added to the remaining references in reports/TODO files.)
- [x] Add short banners to historical docs (e.g., `/docs/historical/**`) to clarify their status.
- [x] Update `README.md` and satellite guides to mention JetStream confirmations, DLQ subjects, and the confirmation-aware automaton flow.

**Exit criteria:** No current doc suggests that the outbox or sensd are active components; contributors find a single, coherent story that matches `docs/way.md`.

---

## 3. Channel & Backpressure Hygiene

**Decision:** Follow the staging-stream guidance: satellites publish immediately to JetStream; local channels remain bounded/test-only.

### Tasks
- [ ] Audit satellites for in-process `Vec` accumulation or unbounded channel drains (e.g., `journal_watcher.rs`, clipboard watcher) and replace with streaming publishes/chunked processing.
- [ ] Annotate helper utilities like `ChannelReceiverExt::drain_all` as test-only, or move them under a testing feature so production code doesn’t rely on them.
- [ ] Ensure `docs/vision/streaming-architecture.md` explicitly links to the staging-stream implementation and references this policy.

**Exit criteria:** Production code no longer depends on arbitrary channel caps for flow control; tests/tools are the only consumers of the drain helpers.

---

## 4. CI & Tooling Hygiene

**Decision:** Keep CI deterministic and aligned with dev workflows.

### Tasks
- ✅ `.github/workflows/ci.yml` / `sqlx-cache.yml` / `sqlx-check.yml` run migrations from `crate/lib/sinex-schema` and all Postgres images are pinned to `timescale/timescaledb:2.15.2-pg16`.
- [x] Ensure the Nextest profile used in CI matches the documented “reliable” profile (or document the difference if we stick with default). (CI now runs `cargo nextest … --profile reliable` and coverage inherits it.)
- [x] Add linters/checks that fail CI when `#[tokio::test]` is used in workspace crates (outside proc-macro/test-harness contexts); everything should use `#[sinex_test]`.
- [x] Add lint or static analysis that forbids `sqlx::query(` and `sqlx::query_as(` (non-macro versions) so contributors stick to compile-time-checked macros (`query!`, `query_as!`, etc.).

**Exit criteria:** Fresh clone CI runs without touching removed paths; container images are pinned; lint guards protect against reintroducing deprecated patterns.

---

## 5. Gateway & Security Baseline

**Decision:** Gateway RPC endpoints must require authentication when exposed beyond localhost; CLI already expects `SINEX_RPC_TOKEN`.

### Tasks
- [ ] Implement token-based auth in `sinex-gateway` (Axum layer): reject unauthenticated JSON-RPC calls, allow binding to `127.0.0.1` without a token for dev shells.
- [ ] Extend CLI (`cli/exo.py`) to send the token header automatically when `SINEX_RPC_TOKEN` or `--rpc-token` are set (already partially wired).
- [ ] Add integration tests that exercise authenticated/unauthenticated flows.
- [ ] Document the default security stance in `README.md` / gateway docs.

**Exit criteria:** Gateway refuses unauthenticated requests unless explicitly configured; tests cover the path; docs state the requirement.

---

## 6. Processor Model Cleanup

**Decision:** `HotlogAutomaton` is deprecated—everything must implement `StatefulStreamProcessor`.

### Tasks
- [ ] Mark the Hotlog trait as `#[deprecated(note = "...")]` and add a lint/check to fail CI on new uses.
- [ ] Port remaining automata (e.g., health aggregator) to `StatefulStreamProcessor` + `processor_main!`.
- [ ] Remove Hotlog support code from the SDK once no crate depends on it.
- [ ] Update docs/tests to reflect the unified processor model.

**Exit criteria:** `rg HotlogAutomaton` returns zero outside deprecated shim files; all automata share one runner path; docs no longer mention Hotlog.

---

## 7. RPC Dispatcher Scan/Explore Completion

**Decision:** `sinex-rpc-dispatcher` must implement scan/explore modes per the SSP interface so CLI “scan/explore” commands work end-to-end.

### Tasks
- [ ] Flesh out the `scan` method for historical and continuous horizons (pull from Postgres logs or JetStream subjects as designed).
- [ ] Implement the `ExplorationProvider` methods (source state, ingestion history, coverage analysis, exports) with real data.
- [ ] Add integration tests covering pagination, checkpoint updates, and restart/resume scenarios.

**Exit criteria:** RPC dispatcher scan/explore commands function via CLI/automation; tests verify behaviour; NotImplemented errors are gone.

---

## 8. Documentation Consistency Fixes

**Decision:** Resolve the issues listed in `docs/ANALYSIS_INDEX.md` (sensd tense, broken links, missing status markers).

### Tasks
- [ ] Fix tense/temporal markers in `project-target-state.md`.
- [ ] Repair or remove references to deleted files (`docs/plan_v3.txt`, `docs/TARGET_final.md`, etc.).
- [ ] Add current-phase indicators to `way.md` or replace with an explicit “Completed” note.
- [ ] Add “Last Verified” stamps to canonical docs.

**Exit criteria:** `ANALYSIS_INDEX.md` items are checked off and the file reflects the updated status.

---

## 9. ✅ Developer Environment & Tooling Baseline

- `.env` is no longer tracked; developers copy from `.env.example`.
- `.cargo/config.toml` now lets Cargo auto-detect parallelism and only adds the `--check-cfg` flag by default.
- `.config/nextest.toml` uses `num-cpus` for the `ci-parallel` profile to avoid hardcoded thread counts.
- `devenv tasks run dev:test` executes the full workspace (`cargo nextest run --workspace --profile reliable`).
- `.vscode/tasks.json` exposes one check task, one full test gate, and explicit SQLx/DB helpers, all documented as dev-shell commands.
- Schema helper scripts now run directly in the current shell, skip global npm installs, add DB pre-flight checks, and the legacy `scripts/update_deps.sh` was removed.
- The legacy `scripts/test-analytics.sh` helper was removed—run `cargo nextest` (and `cargo llvm-cov`) directly.

---

## 10. Repository Hygiene & Narrative Alignment

**Decision:** The repo must describe the current architecture (sinex-core owns DB, JetStream-only ingestion) and enforce that story across templates/docs.

### Completed
- `README.md` now documents the actual crate layout (core/lib/satellites) and calls out sinex-core as the home of database code.
- `.github/pull_request_template.md` references the current abstractions (sinex-core repositories, `sqlx::query!`, shared validators) instead of legacy crates.
- Remaining references to `crate/sinex-db` in docs/scripts were updated to point at `crate/lib/sinex-core` / `sinex-schema`.
- Removed legacy scaffolding (abstractions workflow references, unused LLM/test analytics scripts) and refreshed `.github/workflows/README.md`.

**Exit criteria:** New contributors see a single, accurate description of the stack; PR reviewers no longer rely on stale checklists; stray legacy files are either deleted or clearly labelled.

---

## Change Control
- Update this file whenever a section is completed or new work is added.
- When a decision graduates from “plan” to “complete,” move details into the relevant canonical doc (e.g., `docs/way.md`, README, component guides) and trim this file accordingly.
