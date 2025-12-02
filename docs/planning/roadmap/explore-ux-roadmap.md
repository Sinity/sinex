# Explore UX Roadmap (MVP)

Purpose: define a minimal, consistent Explore experience to inspect history, understand provenance and changes, and safely run replays/curations. This reframes the prior "Explore UX Canon" from TARGET_final.md into a roadmap artifact.

MVP Panels
- Timeline
  - Goal: Navigate events over time; filter by families/sources; inspect provenance quickly.
  - Data hooks: event list (id, event_type, ts_orig, ts_ingest, payload_summary, material_id?, anchor_byte?, source_event_ids?), provenance overlays, gaps overlay (ledger continuity, recovered_partial).
  - Queries: time-bounded list + filters; per-event detail fetch.

- Replay Preview
  - Goal: Show what would change before executing replay; highlight risk.
  - Data hooks: planner output (counts, cascade histogram, anchor churn %, time-quality flips); safety gates.
  - Queries: preview summary persisted or computed on-demand; scope summary.

- Source Explorer
  - Goal: Monitor Source Material lifecycles and ledger integrity.
  - Data hooks: raw.source_material_registry (status, rotation, timing, host/user), raw.temporal_ledger (offsets, ts_capture, precision, clock, source_type).
  - Queries: materials list; ledger continuity; recovered segments.

- Curation Queue
  - Goal: Review proposals, make judgments, and produce confirmed state.
  - Data hooks: proposal events with evidence (source_event_ids), confidence, rationale; user.curation.judgment; finalizer outputs.
  - Queries: proposals awaiting judgment; evidence fetch; judgment submission via gateway/ingestd.

- Provenance Narrative
  - Goal: Explain why an event has this time/content.
  - Data hooks: external provenance (material_id, anchor_byte, offsets), internal provenance (source_event_ids), time-quality derivation, processor manifest (anchor_rule/version).
  - Queries: joins to material/ledger and parents; manifest lookup.

Operator Workflows (MVP)
- Safe Replay: select scope → preview → gated confirm → execute with operation_id → show results/audit.
- Gap Inspection: highlight gaps → jump to material/ledger slice → trigger historical replay if needed.
- Proposal Review: inspect evidence → accept/reject/modify → finalizer writes confirmed state.

Data Model Notes
- Planner runs in gateway/CLI for preview; execution reuses scope+operation_id.
- Reads from: core.events, audit.archived_events, raw.source_material_registry, raw.temporal_ledger, operations_log, processor_manifests.

Telemetry Hooks
- commit→publish latency; consumer lag; annex probe stats; anchor churn; coverage gaps; preview vs execution latency.

Non‑Goals (MVP)
- Full‑fidelity diffs for large payloads; cross‑environment aggregation; prescriptive UI framework.

