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
5. inserts events, commit-log rows, `event_children` rows and live-table rows; writes archive changes with their chains and material status;
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
- **`core.commit_log`** is an ordinary table (never a hypertable), written in the same transaction: one row for each event admitted, each archive change, and each position taken by a transaction that admits nothing. Commit-order readers scan it by primary key and fetch events by `id`.
- **Pruning.** Rows below the smallest position of every commit-order reader are pruned. That is the one routine delete in Sinex; as-of reads and time lookups use events and the archive changes, so pruning loses nothing.
- **Every position is a PostgreSQL row committed with the work it covers**: derivation positions with their outputs, the publisher's and each read model's with their work, source cursors and carry with their events. No position lives in NATS.
- **Wakeups.** The writer publishes the latest committed position on a watch channel; readers compare it with their own position, with a polling backstop so a lost wakeup only delays.

The order in which bytes, rows and positions become durable is the [durability order](architecture.md#durability-order).

## Storage representation

### PostgreSQL and TimescaleDB

PostgreSQL with TimescaleDB is the one transactional store.

- `core.events` is a hypertable on `id` with daily chunks. Closed chunks are compressed, segmented by source and kind, ordered by `ts_orig, id`, with chunk skipping on `ts_orig` and bloom sparse indexes on `id` and `material_id`.
- Day queries over months of compressed chunks, and archive, restore, replay and tombstone on them, stay within the [targets](#performance).
- No foreign key points into a hypertable; admission checks references.
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

- **Search.** Each kind declares `search: none | words | words_and_trigram` ([planned reference/kinds.md](reference/README.md#planned-pages)); no copy of the text is stored. The index covers live readings only, so a replayed or newly withheld reading never leaves old text searchable. The engine (a GIN variant, `pg_textsearch`, or tantivy as a consumer) is chosen by measurement on real-shaped data. `polylogue.message` and `polylogue.tool_call` are `none`.
- **Detector hits.** A consumer with its own position records which detector versions matched each field, follows every lifecycle change, and catches up from the start of the affected kinds when a detector is added or edited. Their use is [privacy.md](privacy.md).
- **Typed views.** Per-kind typed views are generated from the kind registry over live readings.
- **The SQL reader is an access boundary:** a database role that cannot read raw payloads, keys or hits, and sees withheld fields only as [privacy.md](privacy.md) allows.

### Raw bytes

Raw bytes are in the segment log of [hashed-blobs](https://github.com/Sinity/polylogue-sinex-common/blob/master/crates/hashed-blobs/README.md), written by the one writer in the [durability order](architecture.md#durability-order). Material extents and revision extents are segment extents. The numbered schema upgrade adds the extent index and committed segment ends. Existing per-file stores require a separately qualified predecessor exporter: its decoded streams are checked against the retained names and lengths, prepared as segments, then committed without changing material, event, key or cursor identities. Startup refuses catalog rows without a converted extent. Original files remain in custody after conversion; their retirement is a separate operational action. Only the segment reader runs in the new product. What a material is and how its bytes are read back is [materials.md](materials.md).

## Schema evolution

The schema is declared, never hand-altered. Ruling, 2026-03-15: "do not add alter table, wtf, stop ruining the codebase. the schema is supposed to be declarative." (cc:11ccf3c6-2de1-4195-b2c6-0f44d825e6b5:6b6da473-0f02-456b-ada0-36e9e7809dfe).

- **One description in Rust** (`sinex-schema`) generates the fresh DDL and every structural upgrade; nothing structural is written by hand.
- **Upgrades preserve data.** Sinex holds captured data that cannot be fetched again, so it keeps numbered, data-preserving upgrades rather than recreating the store. Ruling, 2026-03-08: "no data should be actually wiped out unrecoverably, ever!" (cc:3a32c9d8-4a89-4424-887c-7a3c63af5c4d:4ba7e862-da6f-4705-a291-e6700d4d3248).
- **Data transformations are named steps** inside an upgrade. Upgrades are numbered when they land, so concurrent work never races for a number.
- **Explicit apply.** `sinex-schema apply` runs with the whole daemon stopped, in one transaction, and rolls back on failure. It creates the schema in an empty database or upgrades a populated one; a dry run reports what a step would change and writes nothing.
- **The daemon refuses any schema version other than its own** and names the command to run. Ordinary startup never upgrades, resets or moves a store aside.
- **Upgrades may pause capture.** Producers spool meanwhile; a deploy with zero lost capture time is not a requirement. Ruling, 2026-10-11: "sure it is desirable properlyt I guess, but it is not a requirement anymore" (cc:e3e37f29-2490-47ae-89ef-fd40b6242e9a:4e4fa8f3-a4b6-4258-8938-04c5b8513a40).
- **Declarations are single.** Kinds, CHECK lists, streams, consumers, derivations and sources are each declared once in Rust and exported for the NixOS module ([operations.md](operations.md#deployment)).

## Performance

The pipeline's throughput and live-latency targets are in [architecture.md](architecture.md#throughput-targets). The write path adds:

| Target | Measured as |
|---|---|
| live p99 at most 1 s while a bulk replay runs | submission to commit, paired bench runs |
| archiving one source's year on compressed chunks | writer-hold time and total time |
| segment log throughput | materials per second at a 1 s commit interval |
| day queries over three months on compressed chunks | latency of the day query, with archive, restore, replay and tombstone on the same chunks |

Figures are reported with bytes/s, bytes kept per event, WAL per event, syncs per group and stage utilisation. No test asserts elapsed time; measurement is the bench of [testing.md](testing.md). Batch and turn sizes are targets, never input ceilings.

## Laws

- **one-transaction-or-nothing**: one transaction covers events, archive changes, dirty keys, record failures, cursors, acknowledgements and the operation row; a failure injected at any statement leaves none of them.
- **success-only-after-commit**: no producer or caller is told an event is admitted before its transaction commits.
- **single-writer**: a second writer blocks or is refused; under two concurrent admitting transactions no commit-order reader skips a position.
- **positions-never-regress**: no commit position smaller than one already visible becomes visible.
- **positions-commit-with-work**: every derivation, read-model, publisher and source position is in the transaction of the work it covers; a crash after the work and before the position is impossible.
- **failure-tied-to-bytes**: an unreadable record becomes a record failure naming its material and anchor in the transaction that advances the cursor past it, and the rest of the material is admitted.
- **cursor-never-past-unadmitted**: no cursor covers a record that was neither admitted nor recorded as failed.
- **transport-failure-not-parse-failure**: a database or transport failure during admission leaves committed progress unchanged and is reported as retryable, never as a record failure or an empty success.
- **empty-poll-saves-progress**: a poll whose records are all suppressed, or that finds none, still commits its cursor.
- **commit-log-prune-below-readers**: pruning removes only commit-log rows below every commit-order reader's position, and as-of reads at every retained position are unchanged by it.
- **search-follows-lifecycle**: after archive, restore, replay or tombstone, search returns exactly the live readings' text; no archived or withheld text is found.
- **read-models-rebuild-equal**: a read model rebuilt from events equals the one maintained incrementally.
- **reader-role-boundary**: the SQL reader role cannot read raw payloads, keys or hits.
- **fresh-equals-upgraded**: fresh DDL from the description equals the schema reached by applying every upgrade to a populated store from each supported version, with unaffected values identical.
- **daemon-refuses-mismatched-schema**: the daemon refuses to start on a schema version other than its own and names the command.
- **upgrade-rolls-back-whole**: a failing upgrade step leaves the store exactly at its previous version.
- **compressed-chunks-meet-targets**: day queries over three months and every lifecycle operation on compressed chunks meet the [performance](#performance) targets on real-shaped data.
- **live-latency-under-bulk**: live p99 stays at most 1 s while a bulk replay runs (a bench figure, not a test assertion).
