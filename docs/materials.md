# Materials and staging

Sinex keeps the exact bytes it captured as **materials** and reads events out of them. This page owns the material, inputs, observations and revisions, how bytes are stored before any row cites them, how they are read back, bulky fields carried by reference, re-chunking, and **staging**, the one way anything enters Sinex. Records and readings are in [events](events.md), the material's lifecycle status in [the lifecycle](lifecycle.md#material-status), each source's inputs in [sources](sources.md), and the segment container in [`hashed-blobs`](https://github.com/Sinity/polylogue-sinex-common/blob/master/crates/hashed-blobs/README.md).

## The material

A material is stored, immutable, self-contained bytes. Every event read from a source can be re-derived from one of its materials, and replay reads the stored copy, never the original path.

**A path is never a material.** A file referred to by path can change or disappear, and then nothing could be read again. Ruling, 2026-03-29: "yeah and what if it's gone? THis defeats the entire point of source materials in the first place!" (cc-queued:ea2bf14e-10bb-42e9-9ff5-616882baaf1c@2026-03-29T19:12Z).

**Materials are source-shaped.** A material is a unit the source itself has: a file revision, a part of a growing file, a capture segment, an export member, a set of database rows. Ruling, 2026-03-29: "source material should be, generally, source-shaped" (cc:ea2bf14e-10bb-42e9-9ff5-616882baaf1c:bbe835e2-4b30-498e-bc5b-d257f4d82323).

- **Database rows, never database images.** A SQLite source is captured as rows; Sinex stores neither a whole database image nor periodic snapshots of one.
- **Materials are large.** A growing input rotates into closed parts at a size or time bound declared per binding (default 100 MiB or one hour). A poll never makes its own material: a SQLite poller appends its rows to the binding's open part. One material per event is a defect.
- **Imported originals are kept whole.** An export ZIP is one material, stored as received, with its members addressed inside it ([sources](sources.md)).
- **A name is not identity.** Equal bytes may be several materials sharing one stored blob. The supplied name and declared type belong to the material, not to the bytes.

### Fields

| Field | Meaning |
|---|---|
| `id` | UUIDv7 |
| input, revision | what was observed and which revision of it these bytes are |
| part index, previous part | the material's place in its input's **part sequence** |
| stored bytes | reference to the bytes in the segment log, and their length |
| manifest | what the bytes are and how to read them: where they came from, media type, the records a driver listed, the driver's details |
| parser, parser version | the parser assigned to it, or none ([staging before a parser exists](#staging-before-a-parser-exists)) |
| status | `live`, `archived` or `tombstoned` ([material status](lifecycle.md#material-status)) |

### Roles

The same bytes can play several roles, and the model keeps them apart:

- **evidence**: the bytes that most strongly support the events;
- **acquisition payload**: what was read at observation time;
- **stored copy**: what is kept for reading again;
- **reading**: the events made from them.

A source stores at least one material from which every event it emits can be re-derived.

## Inputs, observations and revisions

- **Input**: something observed repeatedly, identified by binding, kind, locator and host. It outlives its readings.
- **Observation**: one look that found a change. Unchanged looks advance a counter and the last-looked time only.
- **Revision**: an input's content as of one observation (appended, rewritten, replaced, truncated, snapshot), chained per input. A revision's bytes are an ordered list of **extents** over immutable stored bytes, so an append costs only the new bytes. Older revisions stay readable forever, so "what did this file contain on Tuesday" has an answer.
- **Events anchor into material parts,** never into a revision, so a later revision never moves an anchor.

Each question has one owner, written once, in the transaction that learns it:

| Question | Owner |
|---|---|
| what the input looked like | observations |
| which bytes Sinex holds | revisions and their extents |
| how far events cover the input | the binding's cursor |

### Metadata

An observation records every metadata item the acquisition can read, not a fixed whitelist: size; modification, change, access and birth times; inode, device, mode, owner, group, extended attributes and ACLs where the platform exposes them; host, mount and file system. An item that could not be read is recorded as unavailable with its reason, never as empty. Directory captures carry the same through PAX tar ([directory captures](sources.md#directory-captures)). Ruling, 2026-08-16: "it definitely should exhaustively preserve all available metadataof such source materials" (cc:ec60fadc-f448-407d-baab-ce3741265a67:f0605a92-4adf-42b8-be2e-6c021a74dd9a).

## Bytes before rows

Raw bytes go into the shared append-only **segment log** ([`hashed-blobs`](https://github.com/Sinity/polylogue-sinex-common/blob/master/crates/hashed-blobs/README.md)). The writer appends a group's new bytes, syncs once per group, and commits the extents in the same transaction as the rows that cite them. A crash leaves at most an uncommitted segment tail, which is truncated when the segment is opened; the full durability order and its crash states are in [architecture](architecture.md#durability-order).

- **An event cites only stored bytes.** A material row names extents already appended and synced.
- **Held material.** The newest part of a growing input is held until its final cursor is admitted, so a failed admission retries the same bytes.
- **Open parts** are flushed before any event citing their bytes commits. Each batch carries the part's synced length, and admission refuses an anchor beyond it.
- **Deduplication of bytes** checks the stored-bytes index plus the hashes appended in groups not yet committed, so two equal blobs in flight are stored once.
- **Read authority** is the encoded extent descriptor in `core.blob_extents`; `core.blob_segments` records the committed end. Producer material acknowledgments and material/replay listings carry those descriptors so shared-store readers need no database access or hash-based file lookup.
- **Owned JSON records** decode through bounded record-range readers and hash the same bytes as they stream. Borrowed text parsers retain the input their parsing contract needs; the storage reader retains a compressed frame and a decoded frame rather than the whole record.
- **Names** are `blake3:<hex>` of the uncompressed bytes. Frames are seekable zstd; a range read decompresses and verifies only the frames it needs.

Deduplication here is of stored bytes only. Whether a second copy of the same content becomes a second material and how its records are recognised is [staging's](#re-staging-identical-content) and [identity's](events.md#identity) concern.

## Reading bytes back

- **Reads verify hashes as they read.** A mismatch is an error naming the material; it is never returned silently and never replaced by reading the live path.
- `material read <id> [--range A..B] [--to PATH]` returns or writes the exact verified bytes. A partial or failed write never leaves a complete-looking file, and the command never touches the original captured file.
- **One on-demand scan** in the segment container reports mismatches and repairs nothing. There is no scheduled scrub: invariants are kept by construction, and a corrupt extent is restored from backup ([backup and restore](operations.md#backup-and-restore)).
- **Event bodies stay as admitted.** An event's expanded payload is not dropped to be rebuilt from its material later.

## Bulky fields by reference

A kind may declare fields carried by reference to the event's byte range instead of inline: screen frames, audio segments, full accessibility trees, long terminal scrollback. The event carries the typed facts and the anchor; readers fetch the bytes by range under the same output rules as any payload ([privacy](privacy.md)). The kind's declaration says which fields are carried this way ([planned kinds](reference/README.md#planned-pages)).

## Re-chunking

Small materials that predate large parts are joined into larger ones once, as one numbered data step of the segment-log upgrade ([schema evolution](admission.md#schema-evolution)), not by a standing command:

1. concatenate each input's small materials' bytes into a new material, recording each old material's byte range in it (`rechunked_from`);
2. replay the old materials' records onto the new material ([replay](lifecycle.md#replay)): new record coordinates, a recorded old-to-new map so record citations still resolve, unchanged source record keys;
3. release the old materials' bytes only after the step has checked that the new material reproduces every byte.

The step runs inside `sinex-schema apply` like any data step; a dry run of the upgrade reports its counts and writes nothing. Materials written as large parts never need it. Offsets in input-offset keys count across all chained parts, so re-chunking leaves keys unchanged ([key kinds](events.md#key-kinds)).

## Staging

Everything enters Sinex by being **staged**: a generic element ingests it from a small configuration that names the parser, possibly autodetected. History and live input take the same path, and there is no second ingestion mechanism. Ruling, 2026-10-27: "this was not some kind of a niche feature -- this was entire basis of how sinex was architected!" (cc:ee52794c-8a9e-43b6-9fbc-9749c7e78e8f:4d35819f-890c-4d5a-85cf-cd8ab46d62d7).

```
stage ──► acquire material ──► parse ──► admit
```

### Continuous bindings

A continuous binding is a staged input declared in configuration: Sinex's own NixOS module, generated from the source's one declaration ([sources](sources.md)); a deployment sets only host values there. Enabling a source is configuration, not database state.

### One-shot staging

`stage <path | directory | live database> [--parser NAME] [--export-date DATE] [--account NAME]` is the imperative form. It is an [operation](lifecycle.md#operations-and-actors) and a [run](events.md#producer-versions-runs-and-manifests). The CLI form and its output are in [the CLI](interfaces.md#cli).

| Staged | Material |
|---|---|
| a file | one material, stored as received; an archive stays whole, its members addressed inside it |
| a directory | one PAX tar material (extended attributes, ACLs, sparse files, byte file names), its files addressed as members ([directory captures](sources.md#directory-captures)) |
| a SQLite database a declared live-database source reads (Atuin, browser history) | its rows, read in one read transaction, committed WAL frames included; never an image |
| any other SQLite file | an imported original stored whole, with its rows read into a rows part |

### Staging before a parser exists

When detection finds no parser, or more than one, the bytes are stored as material with no parser and the candidates listed. Nothing is guessed and nothing is refused. A parser is assigned later through replay (`replay "materials where parser is none and …" --parser NAME`), recorded on the material and on the operation. When a new parser version arrives, records with no live reading are read automatically ([parser versions](events.md#parser-versions)).

### Stage as you go

Live streams are always saved as material parts assembled as they arrive (the spools of [streams](streams.md)), never only parsed in flight. Their bytes are in material parts before their events commit.

### Re-staging identical content

A re-stage of identical content is not imported twice, and it is still recorded. Byte-identical data staged again is a **new material**, not a replay; its records are suppressed against the live readings by their source record keys ([what admission does with a key](events.md#what-admission-does-with-a-key)). A one-shot file whose records have no identifying fields is keyed by its content digest and offset, so the re-stage is recognised whatever its path. Ruling, 2025-06-12: "no deduplication pls. just ensuring no double import … redundancy is fine" (cc:979c375c-989d-4b8a-8891-1541aaafe1d0:32f70e17-ab5f-4687-a6a3-ccff649985ae). Ruling, 2026-05-30: "it wil still be considered entirely new material -- this has nothign to do with replay" (cc:3c91049e-3702-4e99-be65-81620e1f7552:e73554d4-91ea-403d-ad9a-62f4d0bcf128).

### Progress

`stage list` shows recent staged materials and their progress: bytes parsed of the material's size, events admitted of parsed, rate, and what paces it. Materials stored without a parser show as stored, not interpreted.

## Laws

- **bytes-read-back-equal**: a material's bytes read back equal the bytes acquired, for empty, small, multi-frame and larger-than-memory inputs; a range read decompresses only the frames it needs; a corrupted frame is an error naming the material.
- **replay-reads-stored-copy**: replay after the original file is deleted reads the stored bytes and produces the same records.
- **no-material-per-poll**: no poll makes its own material; a growing input's parts rotate only at their declared bounds.
- **metadata-exhaustive**: an observation records every readable metadata item, and an unreadable one is recorded unavailable with a reason; GNU tar is the restore oracle for directory captures.
- **stage-shapes**: `stage` of a file, a directory, a live SQLite database and a SQLite export each produce the material shape in [one-shot staging](#one-shot-staging), an operation row and a run.
- **stage-without-parser**: staging with no matching parser stores the material with no parser and the candidates; a later replay with `--parser` reads it.
- **restage-two-materials-one-live-reading**: staging byte-identical content twice from two paths yields two materials and one live reading per record.
- **streams-stored-before-commit**: a live stream's bytes are in material parts before their events commit.
