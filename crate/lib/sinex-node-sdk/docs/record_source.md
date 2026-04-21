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

Use `process_record_batch_lenient(...)` when processing can warn per record.
It advances the checkpoint after processed records and explicitly skipped
warnings, holds the checkpoint before retryable warnings, and advances to the
source read frontier when the whole returned batch completes. That lets readers
acknowledge internally skipped source rows without each node reinventing cursor
policy.

Use `RecordMaterializer<BufferedRecordSink>` for source-material bytes. The
materializer appends one stable logical record, returns a `SourceRecordAnchor`,
and delegates batching/rotation/finalization to the acquisition substrate.

## Default Node Pattern

```rust
let source = RecordSources::sqlite(
    path.clone(),
    checkpoint_key.clone(),
    |path, from_row_id, end_time| read_rows(path, from_row_id, end_time),
    |record: &MyRecord| record.row_id,
);

let mut checkpoint = SqliteRowCheckpoint::new(saved_row_id);
let batch = source
    .read_batch(&checkpoint, RecordReadHorizon::Unbounded)
    .await?;

let report = process_record_batch_lenient(
    &mut checkpoint,
    batch,
    |record| async move {
        let anchor = materializer.append_json_line(&record.raw_material()).await?;
        emit_event(record, anchor).await?;
        Ok(RecordProcessingOutcome::Processed)
    },
    |_| RecordWarningDisposition::Retry,
)
.await;
```

Persist `checkpoint.row_id` only after inspecting `report.warnings` and
finalizing the materializer. This keeps retryable failures from skipping source
records and keeps source-material streams balanced.

## Lower-Level Substrate

`AppendStreamAcquirer` and `BufferedAppendStreamWriter` remain the lower-level
source-material transport. New node code should not use them directly unless it
is implementing a new `RecordMaterialSink` or a genuinely lower-level SDK
primitive.
