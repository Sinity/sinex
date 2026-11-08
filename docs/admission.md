# Admission and storage

Admission is the one way an event enters Sinex. This page owns the admission function and its writer, failures and progress, the commit log and positions, how the store represents events, liveness, raw bytes and read models, schema evolution, and the write path's performance targets. The durability order and crash states are in [architecture.md](architecture.md#durability-order); identity and the classification rules admission applies are in [events.md](events.md); the lifecycle operations it executes are in [lifecycle.md](lifecycle.md); the NATS ingress, the producer client and the confirmed stream are in [producers.md](producers.md).

## Admission and the writer

### Who calls it

**One admission function stores every event:** the daemon's own sources, derivations, replay, lifecycle operations, API requests, and the NATS ingress on behalf of every outside producer. Writes inside the daemon go straight to it, because routing them out to NATS and back would be a detour with nothing to gain; writes from outside arrive through the [ingress](producers.md).

### The transaction

One transaction admits a batch:

1. allocates commit positions from the counter row, as its first statement;
2. registers material references and extents;
3. checks kinds, schemas, origin, time and parents (each parent live, not tombstoned);
4. classifies every row against the live tables by its record coordinates and source record key ([events.md](events.md#identity)), resolving duplicates within the batch the same way;
5. inserts events, each carrying its commit position, `event_children` rows and live-table rows; writes archive changes with their chains and material status;
6. writes dirty keys, record failures, cursors, carry and reader memory, frame acknowledgements and the operation row;
7. checks cancellation once more, then commits.

A producer is told success only after commit. Detector hits are not written here: they are a [read model](#read-models).

**Outcomes** returned to the caller, per row or per batch: `admitted`, `repeated`, `superseded`, `older`, `removed`, `failed`, `superseded_in_batch`, dropped outputs (`superseded_input`, an output whose parent was archived meanwhile), the dirty keys, the committed position, and the ID mapping a caller needs to cite what it sent.

### One writer

- **A session advisory lock** refuses a second writer. The counter row makes allocation order equal commit order for any number of writers, so a reader never skips a position even if two writers ever run.
- **Priorities**: live before derived before bulk, decided at transaction boundaries. Bulk transactions are bounded by duration, with live batches served between them. Each binding reads a bounded turn, then yields. Turn and batch budgets schedule work and never cap input.
- **Set-based writes**: one binary `COPY` into a staging table, one classification snapshot, a fixed number of set-based statements; round trips never grow with the number of rows replaced.
- **Parsing** runs on blocking workers with permits, at most one batch parsed ahead of the batch awaiting its answer, so memory is bounded by waiting rather than by a cap on input.

## Failures and progress

- **A record a parser cannot read** becomes a record failure tied to its stored bytes (material, anchor, reason, parser version, run), written in the transaction that moves the cursor past it; the rest of the material continues. Retrying it is a [replay](lifecycle.md#replay) of those bytes.
- **Cursors commit with what they cover** and never move past data that was neither admitted nor recorded as failed.
- **A database or transport failure** is retryable with committed progress unchanged; it is never reported as a parse failure or as an empty success.
- **An all-suppressed or empty poll** still saves its legitimate progress.
- Refusals of frames from outside producers become record failures on their material and never stall capture; see [producers.md](producers.md).

## Commit log and positions

- **Commit positions** come from one counter row updated as the first statement of every admitting or lifecycle transaction. Values may skip after a rollback; readers rely only on no smaller value becoming visible after a larger one.
- **An event's position is on its own row**, found through the commit-order index on events. **`core.commit_log`** is an ordinary table (never a hypertable), written in the same transaction, with one row for each position that holds no event: lifecycle changes, tombstones and transactions that admit nothing. The canonical reader merges events, archive history and tombstone notices by the global position with deterministic order inside each position. It emits a terminal marker only after that position's data. The event-only derivation projection fetches events by `id`.
- **Pruning.** Rows below the smallest position of every commit-order reader are pruned. That is the one routine delete in Sinex; as-of reads and time lookups use events and the archive changes, so pruning loses nothing.
- **Partial positions.** Transfer paging uses a private within-position cursor. A scalar consumer checkpoint advances only over complete positions, after the confirmed publisher's cumulative completion barrier acknowledges. A tombstone allocates its own global position under the counter lock and keeps the exact original admission position in its skeleton; it never reuses the current head.
- **Every position is a PostgreSQL row committed with the work it covers**: derivation positions with their outputs, the publisher's and each read model's with their work, source cursors and carry with their events. No position lives in NATS.
- **Wakeups.** The writer publishes the latest committed position on a watch channel; readers compare it with their own position, with a polling backstop so a lost wakeup only delays.

The order in which bytes, rows and positions become durable is the [durability order](architecture.md#durability-order).

## Storage representation

### PostgreSQL and TimescaleDB

PostgreSQL with TimescaleDB is the one transactional store.

- `core.events` is a hypertable on `id` with daily chunks. Closed chunks are compressed, segmented by source and kind, ordered by `ts_orig, id`, with chunk skipping on `ts_orig` and bloom sparse indexes on `id` and `material_id`.
- Day queries over months of compressed chunks, and archive, restore, replay and tombstone on them, stay within the [targets](#performance).
- No foreign key points into a hypertable; admission checks references.
- **What each event writes** beyond its row: the primary key on `id`, `(event_type, ts_orig)` for kind and time questions, the commit-order index `(commit_seq, id)` that every commit-order reader follows, and a block-range index on `(material_id, anchor)`, since materials are UUIDv7 and anchors ascend within a material, so physical order follows record location. Per-source and all-kind time questions read `(event_type, ts_orig)` ranges, chunk skipping on `ts_orig`, and the per-source and per-run counters; events carry no per-source, plain time or per-run index. Only derived events enter the parents index.
- No DML touches a compressed chunk except tombstoning, which clears payloads. Archive and restore are logical: they change liveness and the archive changes, not the event rows.
- Activity aggregates bucket and select on `ts_orig`, and group only by bounded dimensions.
- Clusters are initialized with data checksums.

### Liveness

**Liveness is recorded per record, for every kind,** because replay works on arbitrary records within a material, not only whole materials.

- Two side tables are written in the event's transaction: one live reading per record coordinates, kind and output index; and one per `(source, kind, source record key)` for keyed kinds.
- The live row is narrow: record key hash, reading ID, reading order and record hash. The record hash makes suppress-or-supersede one comparison; the narrow row keeps the live tables small enough for the classification snapshot.
- What the states mean and how operations move readings between them is [lifecycle.md](lifecycle.md).

### Read models

Every read model is declared, rebuilt from events, and kept current through its own consumer position, following every lifecycle change (archive, restore, replay, tombstone).

- **Search.** Each kind declares `search: none | words | words_and_trigram`; no copy of the text is stored. The index covers live readings only, so a replayed or newly withheld reading never leaves old text searchable. The engine (a GIN variant, `pg_textsearch`, or tantivy as a consumer) is chosen by measurement on real-shaped data. `polylogue.message` and `polylogue.tool_call` are `none`.
- **Detector hits.** A consumer with its own position records which detector versions matched each field, follows every lifecycle change, and catches up from the start of the affected kinds when a detector is added or edited. Their use is [privacy.md](privacy.md).
- **Typed views.** Per-kind typed views are generated from the kind registry over live readings.
- **The SQL reader is an access boundary:** a database role that cannot read raw payloads, keys or hits, and sees withheld fields only as [privacy.md](privacy.md) allows.

### Raw bytes

Raw bytes are in the segment log of [hashed-blobs](https://github.com/Sinity/polylogue-sinex-common/blob/master/crates/hashed-blobs/README.md), written by the one writer in the [durability order](architecture.md#durability-order). Material extents and revision extents are segment extents. The numbered schema upgrade adds the extent index and committed segment ends. Existing per-file stores require a separately qualified predecessor exporter: its decoded streams are checked against the retained names and lengths, prepared as segments, then committed without changing material, event, key or cursor identities. Startup refuses catalog rows without a converted extent. Original files remain in custody after conversion; their retirement is a separate operational action. Only the segment reader runs in the new product. What a material is and how its bytes are read back is [materials.md](materials.md).

## Schema evolution

The schema is declared, never hand-altered. (ruling 2026-03-15, cc:11ccf3c6-2de1-4195-b2c6-0f44d825e6b5:6b6da473-0f02-456b-ada0-36e9e7809dfe).

- **One description in Rust** (`sinex-schema`) generates the fresh DDL and every structural upgrade; nothing structural is written by hand.
- **Upgrades preserve data.** Sinex holds captured data that cannot be fetched again, so it keeps numbered, data-preserving upgrades rather than recreating the store. (ruling 2026-03-08, cc:3a32c9d8-4a89-4424-887c-7a3c63af5c4d:4ba7e862-da6f-4705-a291-e6700d4d3248).
- **Data transformations are named steps** inside an upgrade. Upgrades are numbered when they land, so concurrent work never races for a number.
- **Explicit upgrade.** `sinexd admin upgrade` runs with the whole daemon stopped, in one transaction, and rolls back on failure; a writer holding the database refuses it. The NixOS module runs it as the `sinex-schema-upgrade` unit, which nothing starts on its own. A dry run reports what a step would change and writes nothing. `sinex-schema apply` is the same operation for development databases.
- **Work after start.** An upgrade does only what new writes need. Filling new tables from rows stored before it and building indexes over them are left to the daemon, which runs them after start as an ordered list of tasks: the publication key, the feed index, the event edges, the search rewrite. Each task has a mark table in `sinex_schema` that the upgrade creates and the task's last transaction drops, so a finished store matches a fresh one; each step is bounded and resumes after a restart; an index build an interruption left invalid is dropped and built again, and work another session still runs is waited for. `sinexctl status` lists the tasks that remain (`upgrade_work_after_start`). Readers stay correct meanwhile:
  - the publication key is built concurrently and swapped in; the publisher holds until then, and its saved position shows the wait;
  - event indexes are built one chunk per transaction (TimescaleDB refuses a concurrent build on a hypertable); feed pages scan the chunks not yet indexed; coverage reads source and run counters;
  - search rows stored before trigram participation was declared keep `trigram_indexed = true` until rewritten in event order. Each batch selects exact search-row IDs before fetching their event kinds through a separate selected-ID lookup. Inclusive UUID bounds prune chunks and compressed row groups; exact ID membership is still checked, so the bounds never widen the batch. Flag changes and the saved mark commit together. Distinct UUIDs within one millisecond remain distinct across batch boundaries. Search matches a row whatever its flag says (the flag only chooses between the trigram index and a scan), and the partial trigram index is then built concurrently. The concurrent build delays search maintenance, which the search position reports;
  - every walk over parent edges reads them through `core.event_children_of` and `core.event_parents_of`. While the edges are filled in event order, those functions also read the `parents` arrays of the events past the fill's mark, so walks answer as on a filled store; the last fill step restores their fresh definitions. A tombstone writes its event's edges before removing the row, so a reading tombstoned before the fill reached it keeps its parents; each fill batch locks the tombstone table, so a tombstone runs wholly before or after it.
- **Preparing before the stop.** What admission needs from its first write but takes long to rewrite is built beforehand by the new build while the old daemon serves: `sinexd admin upgrade --prepare` builds the equivalence-key table in its final shape beside the old one, mirrors every change to it through triggers, and copies the stored rows in key order; each batch holds a share lock on the table for its few milliseconds, so a concurrent change either preceded the batch or waits for it and is mirrored after the copy, and no row-lock order can deadlock admission. The upgrade swaps a complete copy in and checks the row counts; without one it converts the table in place. `--unprepare` removes the copy, its triggers and its functions.
- **Constraints on events are checked in the upgrade.** TimescaleDB 2.29 accepts `NOT VALID` constraints on a hypertable with compression enabled but refuses `VALIDATE CONSTRAINT` on it, and still checks a `NOT VALID` CHECK against compressed chunks. New event columns and their constraints are therefore added in the upgrade's transaction.
- **The transaction leaves event storage in place.** A widened event column is added beside the old one, whose declared default costs no write, and only rows holding another value are copied. Properties the declarations guarantee (the equivalence key encoding round-trips) are tests, not passes over stored rows, and the stored event types are read by index probes.

- **The daemon refuses any schema version other than its own** and names the command to run. Startup's schema step (`sinexd admin schema`) creates the schema in an empty database and keeps declarations current at its own version; it never upgrades, resets or moves a store aside.
- **Upgrades may pause capture.** Producers spool meanwhile; a deploy with zero lost capture time is not a requirement. (ruling 2026-10-11, cc:e3e37f29-2490-47ae-89ef-fd40b6242e9a:4e4fa8f3-a4b6-4258-8938-04c5b8513a40).
- **Declarations are single.** Kinds, CHECK lists, streams, consumers, derivations and sources are each declared once in Rust and exported for the NixOS module ([operations.md](operations.md#deployment)).

## Performance

The pipeline's throughput and live-latency targets are in [architecture.md](architecture.md#throughput-targets). The write path adds:

| Target | Measured as |
|---|---|
| live p99 at most 1 s while a bulk replay runs | submission to commit, paired bench runs |
| archiving one source's year on compressed chunks | writer-hold time and total time |
| segment log throughput | materials per second at a 1 s commit interval |
| day queries over three months on compressed chunks | latency of the day query, with archive, restore, replay and tombstone on the same chunks |

Figures are reported with bytes/s, bytes kept per event (every table's heap, TOAST and indexes, by table), WAL per event, syncs per group and stage utilisation. No test asserts elapsed time; measurement is the bench of [testing.md](testing.md). Batch and turn sizes are targets, never input ceilings.
