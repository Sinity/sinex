# Record Source Framework

Record sources are the SDK path for ingestors that read durable row-like or
append-only inputs: `SQLite` tables, shell history files, browser history stores,
`ActivityWatch` history, journald-style cursors, and filesystem/system observation
streams.

## Responsibilities

Node code owns:

- source-specific parsing
- event payload semantics
- privacy context selection
- runtime configuration

The SDK owns:

- typed source descriptors
- typed checkpoints
- read horizons
- per-record cursor advancement
- retry vs skip policy
- stable source-material record bytes
- append-stream batching, rotation, anchoring, and finalization

## API Shape

Use `RecordSources` to construct a source adapter:

- `RecordSources::sqlite(...)` for rowid-backed `SQLite` sources.
- `RecordSources::append_only_utf8_file(...)` for append-only text histories.
  Each emitted text record carries the source byte range and a checkpoint at the
  end of that line, so generic retry/skip processing can advance precisely.
- `RecordSources::polling(...)` and `RecordSources::journal(...)` for
  custom cursor-based sources.

For checkpointed sources, prefer `BufferedRecordSourceHarness`. The harness
combines the source adapter with SDK-managed source-material bytes, then runs
one standard retry/skip cursor policy. It advances the checkpoint after
processed records and explicitly skipped warnings, holds the checkpoint before
retryable warnings, and advances to the source read frontier when the whole
returned batch completes. That lets readers acknowledge internally skipped
source rows without each node reinventing cursor policy.

For push-only observation streams, prefer `BufferedRecordMaterializer`. It
appends one stable logical record, returns a `SourceRecordAnchor`, and delegates
batching/rotation/finalization to the acquisition substrate. The default
constructors are:

- `BufferedRecordSourceHarness::buffered_default(source, acquisition)`
- `BufferedRecordSourceHarness::buffered(source, acquisition, config)`
- `BufferedRecordMaterializer::buffered_default(acquisition, source_identifier)`
- `BufferedRecordMaterializer::buffered(acquisition, source_identifier, config)`
- `BufferedRecordMaterializer::from_active_handle(...)`

## Default Node Pattern

```rust
let source = RecordSources::sqlite(
    path.clone(),
    checkpoint_key.clone(),
    |path, from_row_id, end_time| read_rows(path, from_row_id, end_time),
    |record: &MyRecord| record.row_id,
);

let harness = BufferedRecordSourceHarness::buffered_default(source, acquisition);
let mut checkpoint = SqliteRowCheckpoint::new(saved_row_id);

let report = harness
    .read_process_lenient(
        &mut checkpoint,
        RecordReadHorizon::Unbounded,
        |record, ctx| async move {
            let anchor = ctx.append_json_line(&record.raw_material()).await?;
            emit_event(record, anchor).await?;
            Ok(RecordProcessingOutcome::Processed)
        },
        |_| RecordWarningDisposition::Retry,
    )
    .await?;

harness.finalize("sqlite-history-scan").await?;
```

Persist `checkpoint.row_id` only after inspecting `report.warnings` and
finalizing the harness. This keeps retryable failures from skipping source
records and keeps source-material streams balanced.

## Lower-Level Substrate

`RecordSource::read_batch`, `process_record_batch_lenient`,
`AppendStreamAcquirer`, and `BufferedAppendStreamWriter` remain lower-level SDK
substrate. New node code should not assemble these directly for ordinary
checkpointed materialization. Use them only when implementing a new harness,
sink, source adapter, or a pipeline whose materialization context is deliberately
shared with another live path.

## Mutable SQLite Evidence

For mutable SQLite stores, the row-stream material produced by this framework is
the event's canonical acquisition payload. Events should cite byte ranges inside
that stable stream, not the live external database file. Stronger epistemic
backing should be modeled as complementary snapshot evidence linked to the
row-stream material through `raw.source_material_links` using the canonical
`backed_by` relation. See
[`docs/architecture/sqlite-evidence-lane.md`](../../../../docs/architecture/sqlite-evidence-lane.md).
