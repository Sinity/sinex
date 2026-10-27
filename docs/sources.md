# Sources and stored formats

A source declares its parser, version, emitted event types, default class and
purpose in a `SourceSpec`. An input format declares how configured paths
become bindings and which reader or capture uses them. The
[source registry](../crates/sinex-sources/src/sources.rs) and
[shared source types](../crates/sinex-source-core/src/registry.rs) own these
declarations. `sinexctl sources` reports configured bindings; `sinexd admin
catalog` describes the supported declarations. The catalog is generated from
code rather than maintained as a second list of promised sources.

A binding names a source, an input and its settings. Live capture writes a
spool, which is read through the same path as backfill. Every admitted event
has the record location and parser reading that produced it. Reading stored
bytes again is [replay](architecture.md#identity-and-replay).
[Architecture](architecture.md#reading-sources) owns admission, source
progress and restart behavior.

| Input shape | Stored representation and reader |
|---|---|
| [Append file](../crates/sinex-source-core/src/drivers/append_file.rs) | Complete framed records from growing or rewritten files. A partial trailing record waits for more bytes. Alignment and parser carry preserve repeated records across a rewrite. |
| [SQLite rows](../crates/sinex-source-core/src/drivers/sqlite_rows.rs) | JSON records of changed rows, read in one committed database transaction. Row hashes identify changes; source parsers never open SQLite themselves. |
| [Directory](../crates/sinex-source-core/src/drivers/dir_snapshot.rs) | Changed files and their ranges in one stored bundle. Empty files have distinct anchors. |
| [Export archive](../crates/sinex-source-core/src/drivers/export_archive.rs) | The original file and decoded record parts. Compatible bindings share the file read; each has its own cursor and parser carry. |
| [API cursor](../crates/sinex-source-core/src/drivers/api_cursor.rs) | Response bytes with the provider position needed to continue. |
| [Live stream](../crates/sinex-source-core/src/drivers/live_stream.rs) | Received records with capture times, read from the capture spool. |
| [Media](../crates/sinex-source-core/src/drivers/media.rs) | Captured segment bytes and the metadata that locates them in time. |

## Record meanings

The parser and its tests own a provider's field mappings. Several examples
show why the stored format and the event are separate:

- [Atuin](../crates/sinex-sources/src/sources/atuin.rs) updates an existing
  command row when it finishes or is deleted. Negative running duration and
  exit values become unknown. The command's native ID stays its key.
- [zsh](../crates/sinex-sources/src/sources/zsh.rs) reads extended and plain
  history, decodes metafied bytes and escaped newlines, and counts repeated
  identical commands at the same second separately. A plain-history line
  has no invented start time.
- [ActivityWatch](../crates/sinex-sources/src/sources/activitywatch.rs) reads
  its SQLite events and buckets. A heartbeat can extend a presence or window
  span; its later reading replaces the earlier one. The parser also joins
  adjacent window spans whose titles differ only by a recognized spinner.
- [Chromium](../crates/sinex-sources/src/sources/chromium.rs) reads visits
  and URL rows separately. Each browser profile has its own binding and
  identity, and source times use Chromium's epoch.
- [journald](../crates/sinex-sources/src/sources/journald.rs) keeps every
  entry, including service errors and Sinex's own logs. Capture resumes from
  the journal cursor; that cursor also recognizes an entry seen again.
- [Goodreads](../crates/sinex-sources/src/sources/goodreads.rs) reads CSV
  book records. Date added and date read can each yield a `book.status`
  event. An unrated book has unknown rating, not a zero-star rating.
- [Takeout](../crates/sinex-sources/src/sources/google_takeout.rs) declares
  a source for each supported product over shared archives. Daily health
  figures are derivations; Fit still emits each source point.

Sinnix lake records are decoded by their
[lane parser](../crates/sinex-sources/src/sources/sinnix_lake.rs). A missing
raw file is not an empty successful read. Unknown typed fields and malformed
known fields follow [the typed-payload rules](architecture.md#typed-payloads).
Outside producers use [the producer protocol](architecture.md#outside-producers),
including Polylogue, which supplies its own parsed AI-session events.

The [Kestrel tests](../crates/sinexd/src/binding_reader/tests/kestrel.rs)
exercise synthetic inputs through binding reads, the writer, replay and
queries. Parser-specific [source tests](../crates/sinex-sources/src/sources)
cover the remaining field and framing cases.
