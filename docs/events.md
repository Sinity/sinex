# Events

An event is a **reading**: one interpretation of a **record** in a material, or of parent events. This page owns records and readings, the event and its fields, the hierarchy of events, time, identity (record coordinates, source record keys, reading order, record hash, removal at source, alignment of rewritten histories), citations, and producer versions, runs and manifests. Bytes and staging are in [materials](materials.md); moving readings between live, archived and tombstoned is [the lifecycle](lifecycle.md); grouping records from different sources is [fusion](derivations.md#fusion); how derivations run is [derivations](derivations.md).

## Records and readings

- **A record** is a datapoint in a material: a line, a row, a file, a message. It is located by its **record coordinates** `(material, anchor)`, the anchor being the byte offset where it starts, plus event type and **output index** when one record yields several events of one type.
- **A reading** is the event that one read of a record produced. Reading the same record again (replay, a new parser version) makes a new reading with a new ID.
- **At most one reading of each record is live.** A new live reading of a record archives the previous one; the others are kept as archived readings ([the lifecycle](lifecycle.md)). Ruling, 2026-10-27: "there cant be two distinct 'live' events for the same underlying" (cc-queued:ee52794c-8a9e-43b6-9fbc-9749c7e78e8f@2026-10-27T23:04Z).
- **History is implicit.** A record's history is its live and archived readings, grouped by record. There is no supersession pointer and no replacement table. Ruling, 2026-05-24: "you could take archived events and live core events as one table effectively, and group by the position within source material and source material id itself" (codex:019e309f-7614-7381-a8ac-f9080f304ee6@2026-05-24T17:18Z).

Output indices are zero-based unsigned 64-bit integers on public interfaces. Stored coordinates use nonnegative SQL `bigint`; admission and citation lookup check that boundary without wrapping. Widening the representation preserves existing coordinate values and decimal citation suffixes.

## The event

| Field | Meaning |
|---|---|
| `id` | UUIDv7, random, minted when the reading is made; new on every read |
| `source`, `event_type` | two fields, never one joined string split back |
| origin | exactly one of: a material origin (`material`, `anchor`, output index), or a non-empty list of parents |
| `ts_orig`, its quality and precision | when it happened, or unknown ([time](#time)) |
| `ts_coided` | computed from `id`: when Sinex made this reading |
| `ts_persisted` | when the row was written |
| `run` | the run that produced it, which names the producer and its version ([runs](#producer-versions-runs-and-manifests)) |
| `commit_seq` | the commit position of its transaction |
| `source_record_key` | per the kind's declaration, or none ([identity](#identity)) |
| `reading_order` | which of two readings of a record is newer ([reading order](#reading-order)) |
| `record_hash` | fingerprint compared on re-observation ([record hash](#record-hash)) |
| `record_state` | `present` or `removed` ([removed at source](#removed-at-source)) |
| `payload` | typed by the kind's registered schema |

**The ID is the reading's, never the content's.** There is no content-derived ID and no idempotency by ID, because a new read is a new observation. Ruling, 2026-05-07: "of course there's new uuidv7 - because it is new observaiton!" (cc:5fdffc35-be4a-457f-bb6b-4caa52b152c3:ae53f63b-8e16-4ec8-b34b-1fd830c88415). Ruling, 2026-03-06: "having new id is fine … if we reused id, that'd be a lie" (codex:019cbc74-e62a-7d93-987c-4848ca636a75@2026-03-06T00:00Z).

- **One origin** (the code's `one_origin`) is enforced four times: by the builder (no build without an origin), by the wire format (both and neither are refused), by a database CHECK, and by a non-empty parent-list type.
- **Events are built only through the builder,** so origin, time and key rules cannot be bypassed by a struct literal.
- **Stored events are never changed.** Anything about an event changes through a new reading or a lifecycle transition. Only the writer's database role inserts; a trigger refuses every UPDATE and every DELETE of an event row, with no session-flag exception. The one change an event row ever takes is [tombstoning](lifecycle.md#tombstone), which clears an archived reading's payload and keeps its skeleton.
- **Payloads are typed** against a registered kind schema; an unregistered type is refused ([kind registration](producers.md#kind-registration), [planned kinds](reference/README.md#planned-pages)). A changed payload is a new kind version, and stored events are read under the schema they were admitted with.
- **Unknown, not applicable and withheld are distinct.** A field Sinex could not observe is unknown; a field an output action removed is withheld ([privacy](privacy.md)); neither is shown as empty.
- **Nondeterministic inputs are events.** A model call, random choice or outside lookup a derivation depends on is recorded as an event, so replaying the derivation reads the same input.

## Hierarchy

Events are hierarchical interpretations, which the operator calls the core feature. Derived events sit above their parents at every level, and the finest grain is always kept.

- **Every level is a stored event with exact parents**, so it is citable, archivable and derivable from. Recipe answers, figures, findings, activity sessions, episodes, context packs, narratives and forecasts are stored events ([higher layer](higher-layer.md)).
- **Nothing at the top is ephemeral.** Results are computed when their inputs are admitted; a derivation computed on request says why.
- **Derived events are admitted through the same admission path** as read ones ([admission](admission.md)), recomputed on dirty keys ([derivations](derivations.md)).
- **Keyed aggregates cite read ranges, not every parent.** An aggregate (a day's figure, an episode) cites its parents as the read ranges it recorded (scope, kinds and a commit-position interval), so a figure over millions of inputs carries one parent entry per range. Per-event outputs keep exact parent IDs. How the lifecycle walks these ranges is in [whole chains](lifecycle.md#whole-chains-move-together); how ranges are recorded is in [derivations](derivations.md).

## Seven kinds of the same

Sinex distinguishes seven meanings of "the same". They share machinery but never a meaning:

| Same … | Decided by |
|---|---|
| bytes | content digest |
| record location | record coordinates |
| source record | source record key |
| newer version of that record | reading order within its namespace |
| reading | event ID |
| occurrence seen by several sources | a fusion rule, a shared identifier or the operator's `same` ([fusion](derivations.md#fusion)) |
| counted once in a metric | the metric's unit |

A parser fix re-reads old bytes into a new reading without making them a newer source state.

## Time

- **Time is decided once, when a record is framed.** The writer never sets or changes `ts_orig`. A producer outside the daemon supplies `ts_orig` with its quality, and it is honoured as given.
- **Quality** is declared per kind: `captured`, `intrinsic`, `file_mtime`, `operator`, `unknown`. A quality value can only be built from the field or evidence it names. The staging time is never `ts_orig`.
- **Unknown stays unknown.** A missing source time or a declared missing-value sentinel gives `unknown`; a present but unparseable time makes the record a record failure.
- **Precision is part of the time:** `instant`, `second`, `minute`, `day`, `month`, `year`, or `bounded` with an end. A coarse time stands for its whole period, starting in the zone the source meant. Precision comes from the declaration or per record, never inferred from a midnight.
- **The source's own representation is kept** in the payload beside the resolved `ts_orig`, offset or zone included.
- **Time evidence per byte range** for live streams: `(material, byte range, captured at, precision, clock, quality)`, append-only, read by framing on capture and on replay, so replay resolves the same times.
- **Plausibility is a label, never a rejection.** A stated time in the future or before the source existed is kept and labelled suspect with its reason. The one hard bound: a `captured` time from Sinex's own clock more than the declared skew (5 minutes) ahead of `ts_coided` is a bug, refused by a CHECK.
- **Ranges are half-open.** Ties among equal times break by record coordinates and output index, then by a per-record tie counter, never by commit order or event ID.
- **One time vocabulary in both products:** quality and precision on every time; published `polylogue.*` events carry both.
- **Derived `ts_orig`** is declared by the derivation from its inputs, never "now".

How queries use these clocks is in [query](query.md); the clocks themselves and the logical day are in [architecture](architecture.md#configuration-and-clocks).

## Identity

### Two identities and one key

- **Reading identity** is the event ID. Replay makes a new one.
- **Record identity** is the record coordinates. They are stable across replay, which re-reads the same material at the same offsets. They are stored in columns, never as the primary key.
- **The source record key** recognises the same source record seen again **in another material**: a re-staged file, an overlapping export, a row that changed after capture, a rewritten file, a deletion at the source. It is never the primary key and never the event's identity: within one material, the record coordinates identify a record. A derived event has no material, so its record is identified by its key.

### Key kinds

Each kind declares exactly one key kind, or explicitly none; the builder cannot omit the declaration. A source may declare its own key for its events of a kind. The key includes the source, so two sources' keys never meet. One shared function builds every key from exactly the declared fields: parts are length-encoded, never joined with a separator, and long keys are hashed whole with full equality kept (a short hash without collision equality is not a key).

| Key kind | Key | Used by |
|---|---|---|
| natural | a provider ID, a row's own ID column, a journal cursor, a commit hash, `(database, events.id)` | API syncs, SQLite rows, journald, git, ActivityWatch |
| input offset | `(input, offset in the logical input)`, the offset counted across all chained parts so re-chunking does not change it. For a one-shot file whose records have no identifying fields, the input is the file's **content digest**, so a byte-identical re-stage is recognised whatever its name. For a self-rewriting history whose records carry no time, the record's **first-seen** offset, carried across rewrites by [alignment](#alignment-of-rewritten-histories) | append-only files and streams without IDs; plain history files |
| path | `(source, path)`; a move is a path disappearing while another appears with the same content | directory snapshots |
| derived | the parents' keys (or, for a keyless parent, its logical-input offset) plus the window or scope; `query:<hash>` for request-made derived events | derivations, agent-written derived events |
| none | declared explicitly; record coordinates are the only identity, and a new capture is a new record | live streams with no natural identity (clipboard, notifications, window manager, D-Bus) |

A key never contains content, for a kind whose readings supersede (content belongs in the record hash); the material the record arrived in; the processing order or run; or the producer version. Any of these would make the same record look new.

### What admission does with a key

The comparison is always against the **live** reading of the key and of the record coordinates. Archived readings never suppress or block anything.

- **Suppress** a repeat: same key with the same record hash, or an older reading. The repeat still advances the stored reading order.
- **Supersede** with a changed, later reading: within one ordering namespace, a reading whose record hash differs and whose reading order is later archives the live reading with reason `newer_reading`, together with its whole derived chain ([whole chains](lifecycle.md#whole-chains-move-together)), and the new reading is admitted.
- **Removed at source** is a newer reading in state `removed` ([below](#removed-at-source)).

Readings of the same thing by different sources never meet under one key: they are separate records, and [fusion](derivations.md#fusion) groups them only for a defined family.

**Idempotency is per operation.** Admitting a record, a producer resend, recomputing a derivation and association are idempotent. Staging identical bytes is not: it makes a new material ([re-staging](materials.md#re-staging-identical-content)). Replay is not idempotent in its readings but converges to the same live state.

### Reading order

A number set when the record is read:

| Input | Reading order |
|---|---|
| tailed input | `(revision number << 40) \| offset` |
| polled source | the time the poll started |
| export | its export date; an export without a known date waits for one, given by `--export-date` |
| outside producer's keyed write | an order assigned or checked by Sinex from the producer's frame sequence in its namespace, never a host clock |

- **Orders compare only within one ordering namespace**: the revision chain of one tailed input; for polls and exports, the source; for outside producers, the producer identity's frame namespace.
- **A later unchanged reading still advances the stored order,** so a delayed older reading that differs cannot replace content a newer reading confirmed: readings at 10 (A), 30 (A), then a late 20 (B) leave A live.

### Record hash

The reading's revision fingerprint:

- BLAKE3 of the record's material bytes;
- when the source declares volatile fields, BLAKE3 of the parsed record without them, as sorted-key JSON;
- for a derived event, BLAKE3 of the sorted-key payload.

It is computed over source bytes, before and independent of any privacy rule, so a rule never changes what counts as a repeat.

### Removed at source

When the source reports a deletion (a soft delete, a row gone from a full pass, a mail deleted on the server), admission records a newer reading in state `removed`, ordered like any revision, so an older export cannot bring the record back. It is never ignored, never turned into an addition, and never a lifecycle act. Ordinary views and counts leave out records whose live reading is removed, and show `removed at source` on request. A later reading where the record reappears is a newer `present` reading. A line missing after a shell rewrote its history file is expiry, not removal.

### Alignment of rewritten histories

A rewritten revision's records are aligned with the previous revision's. A matched record keeps its key; an unmatched one gets a new key; ambiguity only ever creates records, never merges them. Alignment covers the whole history at any length, with no tail window. The alignment procedure per input shape is in [sources](sources.md).

## Citations

Readings are cited by event ID, records by coordinates, things by source record key, and spans by time. A record citation resolves to the record's live reading across replays, and through the [re-chunking](materials.md#re-chunking) map. The citation forms and their resolution are in [query](query.md#citation-forms).

## Producer versions, runs and manifests

Every event says exactly which version of which producer made it.

- **Manifest**: what a producer is: name; kind (binding, derivation, outside producer, capture node, the daemon, the API); version; code commit; kinds emitted and read; parent manifest.
- **Run**: one execution of a manifest, with host, times, status, and its effective configuration and that configuration's hash. A run never spans two versions.
- **Every event names its run, so its producer version is exact.** Queries expose `producer_version` as a field of every event.

Manifests, runs and [operations](lifecycle.md#operations-and-actors) are three distinct records. An operation that ingests data also has a run.

### Parser versions

A new parser version applies to newly staged material. Records that the new version may now read and that have **no live reading** (record failures, materials staged without a parser) are read again automatically; nothing is archived by that, since nothing was live. Reading again records that have live readings is a [replay](lifecycle.md#replay), an operation, compared first with `--compare`.

### Derivation versions

A new derivation version is compared with the old one on real data before it switches (`derivation.comparisons`, which writes no outputs). Each derivation declares one of:

- `recompute`: a switch replaces its outputs over the recomputed scope as a set, archiving the old ones with reason `version_change`, whole chains, whether or not keys match;
- `keep_history`: the new version applies from the switch onward.

Until a switch, the new version processes new inputs from its committed position. **Nothing recomputes history at daemon start.** Status lists derivations with mixed versions and pending switches. Execution and the switch itself are in [derivations](derivations.md#versions).

## Laws

- **one-origin**: every event has exactly one origin, enforced by the builder, the wire format, a CHECK and the parent-list type.
- **events-immutable**: no UPDATE and no DELETE of an event row succeeds for any role, except the tombstone operation clearing an archived reading's payload.
- **one-live-reading**: at most one live reading per record coordinates, kind and output index, and per keyed `(source, kind, key)`, after every commit, under concurrent batches.
- **repeat-suppressed-advances-order**: a repeat with the same key and record hash is suppressed and advances the reading order; readings 10 (A), 30 (A), late 20 (B) leave A live.
- **later-changed-reading-supersedes**: a later changed reading in one namespace supersedes the live one; an older one is suppressed; readings from two sources never meet under one key.
- **removed-at-source-is-a-reading**: removed at source is a newer `removed` reading; an older export cannot revive the record; a reappearance is a newer `present` reading.
- **keys-exclude-volatile-parts**: keys never contain content of a superseding kind, the material, the run or the producer version, checked per kind against real example inputs.
- **alignment-keeps-keys**: alignment of a rewritten history keeps surviving lines' keys at any history length, and ambiguity creates records.
- **record-hash-independent-of-privacy**: record hashes are equal before and after any privacy rule change.
- **time-never-invented**: unknown time stays unknown; a suspect time is kept and labelled; a captured time beyond the skew is refused by the CHECK.
- **producer-version-exact**: every event's producer version is exact, and a daemon start with a changed derivation version recomputes no history.
