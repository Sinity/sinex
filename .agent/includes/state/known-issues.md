## Current Issue Summary

Canonical current-work tracking lives in GitHub:

- `#308` — Core hardening follow-up: SDK, test harness, runtime proof boundaries.

Scratch notes are not a backlog. If scratch content becomes durable project work, promote it into
GitHub issues or tracked docs and delete the scratch file.

This include keeps only the compressed memory surface for AGENTS consumers.

### Open Work Clusters

| Cluster | Issues | Meaning |
|---------|--------|---------|
| Transport and failure routing | `#326`, `#327`, `#338` | Define publish intent/QoS, split DLQ vs processing failure vs local spool semantics, and formalize node drain behavior. |
| Service-crate cleanup | `#328`, `#351` | Remove the retired `sinex-services` workspace/tooling/docs traces now that PKM and content ownership moved. |
| Content-store and SDK docs | `#313`, `#235` | Finish backend-neutral content-store naming and refresh node-SDK docs around the current framework/proof architecture. |
| Deployment and VM coverage | `#318`, `#234` | Make VM scenarios representative of deployment hardening and remove stale satellite-era VM surfaces. |
| Derived runtime rollout | `#334`, `#329`, `#331`, `#332` | Add operator-visible derived-node telemetry, deploy/session-detector surfaces, and unblock entity/document-layer research after derived-output proof. |
| Schema and temporal semantics | `#233`, `#325` | Unify schema-source bundles and decide late-arriving event coordination before expanding canonicalization/intelligence. |

### Recently Landed Work Worth Remembering

| Area | Current state |
|------|---------------|
| Source-material transport | Lifecycle frames now use one ordered SDK-owned stream instead of separate begin/slice/end streams. |
| Material hot path | WAL and staged-file syncs are batched on the slice path while begin/end boundaries remain crash-visible. |
| Logical record batching | SDK append streams batch many logical records into one physical material slice while returning exact byte anchors per event. |
| Metadata-only observations | Filesystem and system metadata events use buffered observation streams instead of creating tiny or zero-byte one-shot materials. |
| Small material storage | Small materials route through local CAS; large/long-lived content still uses annex-backed paths. |
| Blob persistence | Duplicate BLAKE3 inserts are deduplicated instead of redelivering batches forever. |
| Startup pressure | Continuous startup no longer performs unbounded browser or journal historical replay. |
| UUID validity | ingestd rejects malformed UUIDv7 variants before persistence; system ingestor emits deterministic valid UUIDv7 IDs. |
| Runtime target boundaries | `xtask`, `sinexctl`, test, benchmark, VM, dev, and deployed-runtime responsibilities are explicit in issues #309-#311/#322 and tracked docs. |
| Material assembler split | Restore planning, assembly transitions, durability policy, finalization, and redelivery decisions were extracted through #314/#339-#343. |
| Source-path proof | Historical backfill and browser-history ingestion were proved/hardened through the normal node/runtime plane in #319/#320. |
| Proof/scenario/evidence spine | #485/#323/#324/#316/#315/#317 landed the initial proof catalog, evidence envelopes, scenario taxonomy, source-material scenarios, and resource-shape benchmarks. |
| Automata proof | #321 verified deployed automata output quality, lag, and runtime budgets; operator-facing telemetry remains #334. |
| Agent memory | Scratch is no longer a durable backlog; tracked memory points at GitHub issues. |

### Architectural Fragilities Still Worth Remembering

| Fragility | Tracking |
|-----------|----------|
| Publish backpressure is not yet intent-aware across raw, control, DLQ, gateway, and telemetry paths. | `#326` |
| DLQ, processing failure, and local recovery spool semantics are still too easy to conflate. | `#327` |
| Blob/material APIs still expose annex-centric naming despite the hybrid local-CAS/annex backend. | `#313` |
| VM/deployment coverage still lags the current runtime-target and target-user bridge model. | `#318`, `#234` |
| Derived-node telemetry is not yet operator-visible enough through `sinexctl`/status surfaces. | `#334` |

### Clean Codebase Signals

- No known scratch-backed backlog remains outside GitHub issues or tracked docs.
- Generated AGENTS surface is derived from tracked includes, not from ignored scratch state.
- `master` currently contains the April 22 proof-carrying SDK merge through `d6724ef2e`.
