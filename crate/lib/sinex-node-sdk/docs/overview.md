# Sinex Node SDK: Architecture Overview

This document describes the current Sinex node runtime. It is about the code
that exists today: the traits, adapters, and runtime responsibilities that
ingestors and derived nodes actually use. Vision-only ideas live in
[`vision.md`](vision.md).

The SDK does **not** collapse ingestors and derived nodes into one authoring
model. They share infrastructure where the system benefits from uniformity, and
they keep separate traits where their work is genuinely different.

## 📐 Current Runtime Shape

The runtime has two authoring families built on shared plumbing:

### Ingestors

Use `SourceUnit` when a node reads from an external source and emits
material-provenance events.

- Owns the three-phase capture lifecycle: snapshot, historical catch-up, and
  continuous processing
- Uses SDK helpers for source acquisition, material staging, checkpointing, and
  shutdown
- Publishes provisional events that ingestd later persists and confirms

### Derived Nodes

Use `Transducer`, `Windowed`, or `ScopeReconciler` when a node
consumes confirmed events and emits derived-provenance events.

- `Transducer`: stateless 1:1 transformation or filtering
- `Windowed`: accumulate state until the implementation decides the window
  is complete
- `ScopeReconciler`: keep per-scope state and reconcile it into outputs

All three run through `AutomatonRuntime`, which handles checkpointing,
confirmation consumption, invalidation handling, health emission, and orderly
shutdown/drain behavior.

### Shared Runtime Infrastructure

Both families reuse the same operational pieces:

1. **Shared lifecycle phases**: snapshot, historical catch-up, and continuous processing.
2. **Shared checkpointing**: durable checkpoint/state management across node restarts.
3. **Shared runtime plumbing**: NATS transport, confirmations, health reporting, coordination, and shutdown handling.
4. **Reusable source adapters**: common input shapes like append-only UTF-8
   tail sources, checkpointed `SQLite` history readers, and incremental
   file-import roots live in the SDK so new ingestors extend the normal runtime
   plane instead of rebuilding bespoke readers or direct-import paths.
5. **Reusable material writers**: high-volume logical records can use
   SDK-managed rotating append streams, including buffered writers that
   coalesce records while returning exact per-record byte anchors.

## 🛰️ Runtime Boundaries

Sinex nodes communicate via a distributed event bus powered by NATS `JetStream`.

```text
┌─────────────────────┐     ┌─────────────────────┐     ┌─────────────────────┐
│   External World    │────▶│    Sinex Nodes      │────▶│   Data Substrate    │
│                     │     │ (Stateful Streams)  │     │                     │
│ • Files             │     │ • fs-ingestor       │     │ • core.events       │
│ • Terminal          │     │ • session-detector  │     │ • core.blobs        │
│ • Desktop           │     │ • health-automaton  │     │ • source materials  │
└─────────────────────┘     └──────────┬──────────┘     └──────────┬──────────┘
                                       │                           │
                            ┌──────────▼──────────┐                │
                            │   NATS JetStream    │◀───────────────┘
                            │                     │
                            │ • events.raw.*      │ (Submitted)
                            │ • events.confirm.*  │ (Persisted)
                            │ • events.dlq.*      │ (Failures)
                            └─────────────────────┘
```

### 🛡️ Data Integrity: The Single-Writer Pattern

Nodes do not write directly to `core.events`.

- Nodes publish provisional events to NATS.
- `sinex-ingestd` validates and persists them.
- `sinex-ingestd` publishes confirmations.
- Derived nodes consume confirmations, not speculative provisional events.

Replay is gateway-orchestrated. Nodes participate through snapshot and
historical scan surfaces, but replay planning and lifecycle control are not
owned by the node SDK.

## 🔄 Three-Phase Startup Pattern

The runtime exposes snapshot, historical, and continuous phases, but those
phases matter differently to the two node families:

### Ingestors

Ingestors use all three phases to keep capture complete:

1.  **Phase 1: Snapshot**
    *   Captures the *current* state of the external system.
    *   *Example*: Filesystem node scans all existing files.
2.  **Phase 2: Gap-Filling (Historical)**
    *   Processes events that occurred while the node was offline.
    *   Uses `TimeHorizon::Historical { end_time }`.
3.  **Phase 3: Continuous Processing**
    *   Real-time event monitoring and streaming.
    *   Receives `ContinuousStart`, whose checkpoint is only a live-tail resume cursor.
    *   Continues indefinitely until shutdown.

### Derived Nodes

Derived nodes run as long-lived consumers over confirmed event streams. They
still participate in lifecycle, checkpointing, invalidation, and shutdown, but
they do not implement "snapshot the outside world" authoring logic.

## 💾 Checkpoint & State Management

Nodes use a dual-destination persistence strategy:
- **Primary (NATS KV)**: Durable, distributed storage for crash recovery.
- **Secondary (Local File)**: Fast state serialization during shutdown so local
  restarts can resume without waiting on the distributed store.

Checkpoints support multiple anchoring strategies:
- `Timestamp`: Resuming based on wall-clock time (Journald).
- `Sequence`: Resuming based on internal `UUIDv7` IDs (NATS).
- `External`: Opaque cursor data for custom integrations.

## 🧬 Provenance & Lineage

The SDK automatically enforces data lineage:
- **Ingested Events**: Linked to `SourceMaterial` via byte offsets and hashes.
- **Synthesized Events**: Linked to parent events via `source_event_ids`.
- **Content Verification**: Material content is verified with BLAKE3 plus backend-aware digests when the storage backend exposes them.

For row-like or metadata-only observations, use the Record Source framework:
`RecordSources::*` for checkpointed reads, `BufferedRecordSourceHarness` for
the default read/process/materialize loop, and `BufferedRecordMaterializer` for
push-only observation streams. These APIs batch adjacent logical records into
fewer physical source-material slices, rotate through the normal SDK policy, and
return `SourceRecordAnchor` values for event provenance. This keeps events
byte-addressable without creating one tiny material or one fsync-heavy slice per
observation.

### Which adapter when?

The SDK ships eight built-in `RecordSource` adapters. Pick by input shape, not
by naming similarity — `polling()` and `incremental_dump()` look alike but
treat duplicates very differently.

| Adapter | Input shape | Checkpoint shape | Replay semantics | Typical use |
|---------|-------------|------------------|------------------|-------------|
| `append_only_utf8_file()` | UTF-8 file that grows; bytes never rewritten | Byte offset | Re-tail from offset — idempotent | Atuin history, logs, append-only journals |
| `sqlite()` | `SQLite` DB with monotone ROWID | `SqliteRowCheckpoint` | Re-read rows after `row_id` — idempotent; optional snapshot evidence | Browser history, Atuin store, dump files |
| `polling()` | Anything with a caller-defined poll fn | Caller-defined | Caller-defined | Custom polling adapters |
| `journal()` | systemd journald cursor | `JournalCursorCheckpoint` | Re-read from cursor — idempotent | System node, journald-style sources |
| `ipc_stream()` | `AsyncRead` ephemeral pipe / Unix socket | `IpcStreamCheckpoint` (reconnects, `last_seq`) | Snapshot/historical empty — ephemeral | D-Bus signal subscriptions, polkit, custom IPC |
| `one_time_dump()` | Single bounded `AsyncRead` | `OneTimeDumpCheckpoint` (consumed, `content_hash`) | Idempotent — same bytes produce same records | Single-shot CSV/JSON dumps, GDPR archives |
| `incremental_dump()` | Refreshable dump rewritten end-to-end | `IncrementalDumpCheckpoint` (`BTreeSet` of seen keys) | Emits only records whose key isn't in the checkpoint | Browser history exports, Reddit/Wykop GDPR refreshes |
| `api_fetch()` | Paginated remote API behind `ApiClient` | `ApiFetchCheckpoint` (cursor, etag, `fetched_at`) | Caller-driven — cursor advances forward, etag/last-fetched skip unchanged windows | Spotify, Goodreads, Lastpass, Raindrop |

Decision questions to ask, in order:

1. **Is the source bytes-addressable and append-only?** → `append_only_utf8_file()`.
2. **Is it a row store with a monotone key?** → `sqlite()`.
3. **Is it a stream that disappears after the connection drops?** → `ipc_stream()`.
4. **Is it a one-shot dump that arrives once?** → `one_time_dump()`.
5. **Is it a dump that gets rewritten end-to-end on every export?** → `incremental_dump()`.
6. **Is it a paginated remote API?** → `api_fetch()`.
7. **Is it journald?** → `journal()`.
8. **None of the above?** → `polling()` with a custom poll fn.

The four input-shape adapters (`ipc_stream`, `one_time_dump`,
`incremental_dump`, `api_fetch`) live as submodules under
`crate::record_source::*` and are also re-exported from the crate root.
A compact end-to-end example exercising each adapter is under
`crate/lib/sinex-node-sdk/examples/four_adapters.rs`.
## Shutdown patterns

Three shutdown primitives coexist in the SDK. They are not three competing
patterns — they are layered, and each call site picks the layer that matches
its lifetime.

| Primitive | Owner | When to use |
|-----------|-------|-------------|
| `RuntimeDrainController` (`runtime::stream::handles`) | Node lifecycle | Default for ingestor and derived-node loops. The command listener raises the drain edge, long-running phases subscribe to it, and the runner registers an abort handle for runtime-owned background tasks that must stop accepting work immediately. |
| `spawn_shutdown_task()` (`service_runtime`) | Service-level binaries | Use for xtask drivers, demos, and ad-hoc binaries that own their own process lifetime instead of running through the node runner. The factory returns a join handle wired to the same drain semantics. |
| `tokio::sync::watch::Receiver<bool>` | Leaf consumer | Not a third pattern. It is the wire shape that both primitives above hand to inner loops. Code that already holds a controller or service runtime should subscribe to it; nothing should construct a bare watch channel for shutdown of its own. |

When a node grows a new long-running task, subscribe to the existing
controller via `controller.subscribe()` — do not invent a parallel signal.

## Source-material staging APIs

Three SDK-level APIs cover source-material capture. They serve different
roles in the same pipeline and are intentionally distinct, not redundant:

| API | Module | Role |
|-----|--------|------|
| `batch_importer` | `crate::batch_importer` | Discovers files in a directory tree (FS-style ingestors that turn N files in a directory into N source materials in one pass). |
| `acquisition_manager` | `crate::acquisition_manager` | Owns the lifecycle of one source material: `begin → append slices → finalize`. Used by `StageAsYouGoContext` for streamed captures, and by ingestors that already have the bytes and need to register them as material. |
| `record_source` | `crate::record_source` | Reads logical records out of a backed material (or a raw input stream): append-only UTF-8 lines, `SQLite` rows, JSON-API pages. The output is the per-record byte anchors that feed event provenance. |

In a typical ingestor flow they layer top-down: the batch importer enumerates
files, the acquisition manager registers and writes each as a source material,
and the record source reads bytes for parsing. Direct callers of
`acquisition_manager` are also valid (single-file streamed captures), and
record sources can run against pre-registered materials without going through
the importer (one-shot dumps, API-fetched payloads). Pick the lowest layer
that matches the work; do not chain when a single layer suffices.

## 🚦 Error Handling, Raw DLQ, and Recovery

- **Automatic Retries**: SDK handles transient NATS, source-material, and database connectivity failures where the owning runtime path can retry safely.
- **Raw-Ingest DLQ**: Raw ingest/material failures are routed through the
  operator-facing DLQ stream/subjects for inspection and retry.
- **Processing-Failure Queue**: Derived/runtime processing failures are routed
  to a separate processing-failure stream instead of masquerading as raw ingest.
- **Recovery Spool**: If publishing that processing-failure payload fails, the
  node writes a per-work-dir recovery spool file for honest local recovery.
- **Backpressure**: The confirmation buffer applies backpressure via NAKs if the node falls too far behind.
