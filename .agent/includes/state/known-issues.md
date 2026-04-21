## Current Issue Summary

Canonical current-work tracking lives in GitHub:

- `#308` — Core hardening follow-up: SDK, test harness, runtime proof boundaries.

Scratch notes are not a backlog. If scratch content becomes durable project work, promote it into
GitHub issues or tracked docs and delete the scratch file.

This include keeps only the compressed memory surface for AGENTS consumers.

### Open Work Clusters

| Cluster | Issues | Meaning |
|---------|--------|---------|
| Runtime target and status boundaries | `#309`, `#310`, `#311`, `#322` | Decide command responsibilities and make dev/deployed/runtime status explicit instead of inferred from checkout-local state. |
| Transport, failure routing, and service ownership | `#326`, `#327`, `#328` | Define publish intent/QoS, split DLQ vs processing failure vs local spool semantics, and remove the awkward `sinex-services` layer. |
| SDK acquisition and content-store architecture | `#312`, `#313`, `#314`, `#323`, `#235` | Move record-source acquisition into SDK adapters, finish backend-neutral content-store naming, split material assembler states, and decide SQLite/WAL evidence lanes. |
| Source-path proof gaps | `#319`, `#320` | Prove historical backfill through the node/runtime plane and harden browser ingestion against real datasets. |
| Test harness, scenarios, VM, and resource proof | `#315`, `#316`, `#317`, `#318`, `#324`, `#234` | Make source-material scenarios systematic, failures evidence-rich, resource pressure measurable, and VM coverage representative. |
| Schema and derived runtime semantics | `#233`, `#263`, `#321`, `#325` | Unify schema-source bundles, schema automata outputs, verify deployed derived outputs, and decide late-arriving event coordination. |

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
| Agent memory | Scratch is no longer a durable backlog; tracked memory points at GitHub issues. |

### Architectural Fragilities Still Worth Remembering

| Fragility | Tracking |
|-----------|----------|
| Publish backpressure is not yet intent-aware across raw, control, DLQ, gateway, and telemetry paths. | `#326` |
| DLQ, processing failure, and local recovery spool semantics are still too easy to conflate. | `#327` |
| `sinex-services` still creates an awkward dependency and ownership boundary. | `#328` |
| Material assembler responsibilities are still concentrated even after hot-path fixes. | `#314` |
| Browser/live SQLite and export ingestion need stronger source-shaped proof on real datasets. | `#320`, `#323` |
| Test harness power is still below the complexity of the runtime incidents it should catch early. | `#315`, `#316`, `#317`, `#318`, `#324` |

### Clean Codebase Signals

- No known scratch-backed backlog remains outside GitHub issues or tracked docs.
- Generated AGENTS surface is derived from tracked includes, not from ignored scratch state.
- `master` currently contains the April 21 source-material/runtime hardening commits through
  `7e609bb70`.
