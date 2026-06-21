# SQLite Evidence Lane

## Status

Decision for [#323](https://github.com/Sinity/sinex/issues/323). The base
schema/link surface from [#494](https://github.com/Sinity/sinex/issues/494) and
the SDK capture/porting slice from
[#493](https://github.com/Sinity/sinex/issues/493) are implemented. Retention
policy and snapshot-backed reinterpretation remain follow-up work.

Update for [#1285](https://github.com/Sinity/sinex/issues/1285): SQLite
row-stream material is long-lived and rotates by size/time policy. The older
shape that created a source-material registry row for every tiny poll cycle, or
effectively for every row, is superseded.

## Decision

SQLite-backed sources should keep the existing SDK row-stream lane as the
canonical event-provenance path and add a complementary immutable snapshot
evidence lane.

Do not replace row-stream materials with database-file provenance. Events
emitted from Atuin, Fish SQLite, ActivityWatch, qutebrowser, and Chromium
history should continue to cite byte ranges inside SDK-managed JSONL stream
materials. Those bytes are the acquisition payload: the exact rows sinex read,
serialized in stable observation order, privacy-processed by the owning source,
and validated through the normal NATS -> event engine -> DB pipeline.

The row-stream material is not per poll and not per row. It is a growing
`AppendStreamAcquirer` material held across many adapter drain cycles and
rotated by policy. The default rotation policy is 100 MiB or one hour, with
tests allowed to use smaller thresholds to prove rotation behavior quickly.

Add periodic SQLite database snapshots as evidence materials linked to the
row-stream materials that were acquired from that live database. These snapshots
are stronger epistemic backing for mutable external stores: they preserve the
database substrate that existed near a polling interval, enable future schema
reinterpretation, and make replay/debugging less dependent on the live mutable
source still existing.

Defer SQLite WAL-frame capture. WAL capture is the most physically faithful
mutation log, but it is fragile because WAL files can be checkpointed or removed
by the owning application before sinex sees them. It also adds source-specific
filesystem timing assumptions and retention pressure. Row-stream materials plus
periodic immutable snapshots are the next useful step; WAL should only return as
a separate research issue if snapshot evidence proves insufficient.

## Target State

The SDK exposes a framework-like SQLite source shape with two lanes:

- `row_stream`: checkpointed rows read from the live database and appended as
  stable JSONL records into a rotating source-material stream.
- `snapshot_evidence`: an optional immutable SQLite snapshot captured at
  configured boundaries and stored as an ordinary source material.

The source author should declare source identity, query, checkpoint, material
encoding, privacy context, and snapshot policy once. The natural path should be
to construct a SQLite source through SDK descriptors or annotations rather
than manually combining a row reader, materializer, checkpoint state, and
evidence policy in source-local code.

The event provenance invariant remains unchanged:

- A row-derived event has `source_material_id = <row-stream material>`.
- The cited byte range identifies the stable serialized row material used by
  the source when creating the event.
- Snapshot materials are not crammed into `core.events.source_material_id`,
  because that would destroy the exact row anchor and violate the XOR
  provenance model's "one immediate substrate" semantics.

Snapshot evidence is represented as material-to-material evidence, not as a
second event provenance column. A row-stream material may link to one or more
snapshot materials with metadata such as `evidence_role = sqlite_snapshot`,
`source_path`, `snapshot_time`, `rowid_low`, `rowid_high`, `sqlite_page_size`,
`schema_fingerprint`, and `capture_method`.

## Why Row Streams Stay Canonical

The current implementation already has the right acquisition shape:

- `RecordSources::sqlite(...)` gives SQLite sources a typed rowid checkpoint.
- `BufferedRecordSourceHarness` applies one retry/skip cursor policy.
- `BufferedRecordMaterializer` appends stable per-record bytes and returns
  exact source-material anchors.
- `AdapterBackedSource` keeps one `AppendStreamAcquirer` across drain cycles,
  so low-volume polling creates `O(rotation_count)` materials rather than
  `O(poll_count)` materials.
- Terminal, desktop, and browser SQLite paths use this SDK path instead of
  registering the live external database file as the event material.

That shape is correct because the event is an interpretation of the row bytes the
source actually consumed. It also keeps hot capture incremental and bounded: sinex
does not copy an entire ActivityWatch or browser database for every new row.

It also avoids registry bloat. The historical per-poll/per-row material pattern
produced one registry row for each small polling cycle. That was honest
acquisition evidence, but it was the wrong lifecycle granularity for a source
that is observed continuously. The current row-stream lane preserves the same
per-record anchors inside fewer, policy-rotated source materials.

The weakness is epistemic, not operational. JSONL row-stream bytes prove what
sinex observed and emitted; they do not preserve the surrounding SQLite
database state, schema details, indexes, deleted rows, or non-selected columns
that may become relevant for future reinterpretation.

## Affected Sources

The initial SQLite evidence lane applies to:

- `terminal.atuin`: `~/.local/share/atuin/history.db`, table `history`, rowid
  checkpoint, command privacy context.
- `terminal.fish_sqlite`: explicitly configured Fish SQLite history, table
  `history`, rowid checkpoint.
- `desktop.activitywatch`: ActivityWatch SQLite database, tables `events` and
  `buckets`, rowid checkpoint over `events`.
- `browser.qutebrowser_native`: qutebrowser `history.sqlite`, table `History`.
- `browser.chromium_history`: Chromium-style `History`, tables `urls` and
  `visits`.

Future SQLite/cachew/HPI-like adapters should use the same source shape
instead of inventing local snapshot logic.

## Snapshot Capture Semantics

Snapshots should be captured through SQLite's online backup mechanism when
possible. The important property is not a raw filesystem copy; it is a consistent
immutable SQLite image that can be opened later without depending on the live
database and its transient WAL state. If a source cannot be opened for backup,
the SDK should emit structured evidence explaining the failure and continue the
row-stream lane.

Snapshot capture is not part of every poll. The default should be cadence and
boundary based:

- on first successful source observation after startup;
- after a configurable elapsed duration;
- after a configurable row-count delta;
- before or after explicit historical backfill windows;
- on clean shutdown when the previous snapshot is stale.

The source should surface this as a policy, not as ad-hoc timers inside
each source.

## Rotation Semantics

SQLite row streams rotate by source-material policy, not by adapter `open()` or
drain invocation. A source keeps its stream material open while polling and
appends each serialized row to the current material. Rotation finalizes the
current material and starts a new one only when a configured boundary is reached:

- maximum material size, default 100 MiB;
- maximum material age, default one hour;
- explicit forced rotation in tests or operator maintenance;
- shutdown finalization when the owning unit exits cleanly.

For low-volume pollers such as Atuin or ActivityWatch during idle periods, many
poll cycles should share the same material. The regression shape is: 1,000
polling cycles that emit one or two tiny records should produce one material
under the default policy, not approximately 1,000 registry rows.

## Storage and Lifecycle Policy

Expected storage profile:

- Atuin and Fish SQLite history databases are small enough for daily or
  startup/shutdown snapshots.
- qutebrowser native history is small; Chromium history can grow but remains
  practical with daily snapshots and content-addressed deduplication.
- ActivityWatch can be substantially larger and should default to coarser
  cadence, row-count thresholds, and retention windows.

Snapshots go through the normal source-material storage path. Small snapshots
benefit from local CAS by BLAKE3; larger snapshots route to the large-object
backend according to the existing content-store policy.

Lifecycle behavior must remain explicit package policy rather than source-local
habit. The SQLite evidence lane may declare material lifecycle boundaries such
as first-observation snapshots, latest-snapshot coverage, bounded historical
backfill evidence, and operator-requested preservation for a concrete replay or
debugging workflow. Those declarations must surface through package
completeness, coverage, and operation views; they must not become hidden
retention defaults inside a parser or SDK helper.

## Replay Semantics

There are two replay modes:

- Row-stream replay: reprocess the row-stream material currently cited by events.
  This is the normal event replay path and preserves exact event anchors.
- Snapshot-backed reinterpretation: use a snapshot evidence material to run a
  new source scan against an immutable copy of the database, producing new
  row-stream materials and fresh events through the normal pipeline.

Snapshot-backed reinterpretation is not bit-for-bit event mutation. It is a new
interpretation of preserved external evidence. Existing events remain immutable;
the replay/archive machinery decides which old events and derived descendants
are superseded.

Runtime-plane historical scan semantics are documented in
`crate/sinexd/docs/sources/historical_backfill_runtime_plane.md`.

The snapshot relation makes the proof chain explainable:

1. Event `E` cites row-stream material `R` at byte range `[a, b)`.
2. Row-stream material `R` records that rows from source `S` were observed over a
   capture window and rowid range.
3. Evidence material `D` is an immutable SQLite snapshot of source `S` near that
   observation window.
4. A future replay can either trust `R` as the exact acquisition payload or open
   `D` to reconstruct a stronger interpretation.

## Schema and API Implications

The current `raw.source_material_registry.metadata` field holds snapshot
metadata, and `raw.source_material_links` records first-class material
relations:

```text
raw.source_material_links(
  id uuid primary key default uuidv7(),
  from_material_id uuid not null,
  to_material_id uuid not null,
  relation_type text not null,
  metadata jsonb not null default '{}',
  created_at timestamptz not null default now()
)
```

The important invariant is direction:

- `row_stream_material --backed_by--> sqlite_snapshot_material`

The SDK API makes the relationship automatic for SQLite source contracts that enable
snapshots and provide a `SqliteSnapshotLinker`. RuntimeModule code supplies only the
runtime DB pool; it does not manually insert material links.

## Non-Goals

- Do not register only the external mutable SQLite path as a source material.
- Do not create one source-material row per poll cycle or per SQLite row.
- Do not make every SQLite poll copy the whole database.
- Do not make WAL capture a prerequisite for correct row-stream ingestion.
- Do not bypass NATS, event-engine persistence, schema validation, privacy processing, or normal
  event builders during snapshot-backed replay.
- Do not encode snapshot evidence by adding a second material column to
  `core.events`.

## Follow-Up Work

The initial implementation split into three slices:

- SDK SQLite source contracts with snapshot policy and snapshot evidence
  capture ([#493](https://github.com/Sinity/sinex/issues/493)).
- Source-material evidence links
  ([#494](https://github.com/Sinity/sinex/issues/494)).
- Scenario coverage proving row-stream anchoring and snapshot creation/linking
  for SDK, terminal, desktop, and browser paths.

Remaining follow-up work:

- Retention behavior for snapshot materials.
- Snapshot-backed reinterpretation against representative Atuin, ActivityWatch,
  and browser fixtures.
- Lineage/trace query exposure for material-to-material evidence links.
