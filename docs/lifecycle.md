# The lifecycle

Nothing in Sinex is deleted by default. Readings and materials are **live**, **archived** (the trash, reversible) or **tombstoned** (irreversible, from the archive only), and whole provenance chains move between those states together. This page owns the states, archive and restore, replay and its undo, the whole-chain rule, material status, tombstoning, archive reasons, how the states are stored, as-of reads, and operations with their actors. Records and readings are defined in [events](events.md); the lifecycle verbs on the CLI are in [the CLI](interfaces.md#cli); lifecycle requests on the wire are in [producers](producers.md#lifecycle-requests); the as-of query syntax is in [query](query.md); how derivations recompute after a move is in [derivations](derivations.md#derivations-and-the-lifecycle).

## States

| State | Payload | Seen by ordinary queries | Leaves to |
|---|---|---|---|
| live | full | yes | archived |
| archived (the trash) | full; any number of archived readings per record | through archive and as-of reads | live (restore), tombstoned |
| tombstoned | none; a skeleton | no | never |

Tombstoning is to the archive what archiving is to the live set: a second stage, reachable only from the first. (ruling 2026-02-03, cc:c7811684-01e8-45d9-bb81-84d8646379fb:d6e73771-976c-4671-bdf0-a8fdb2e6b32e). (ruling 2026-10-27, cc:ee52794c-8a9e-43b6-9fbc-9749c7e78e8f:85104ca6-e6d0-4cab-842a-23ffa298ff38).

**There is no automatic retention.** Nothing moves between states by age or by a schedule; material is kept until the operator or an agent archives it.

In primary mode, Polylogue's materials and the `polylogue.*` readings made from them are ordinary Sinex materials and readings and follow this lifecycle; Polylogue on its own has [no deletion](https://github.com/Sinity/polylogue/blob/master/docs/architecture.md#deletion).

## Archive

Archive (live → archived) is ordinary deletion, and there is no other way to delete. Archiving is reversible, like an operating system's trash. (ruling 2025-08-11, cc:505f40e8-3618-4857-81d3-e201f417a2ff:9833b29b-2a3a-4d64-9ee3-d04f6eb0748e). (ruling 2026-02-03, cc:7ef0e4c5-d017-412d-b3d8-42ad7d0c2819:89712340-bbdf-4b1b-9220-ce7b4fafbf20).

- **Scope**: a reference, a query (`--source terminal --before 2026-09-01`, any `events where …`), a material, or a run.
- **It moves the scope's live readings and their whole live chains** ([whole chains](#whole-chains-move-together)).
- **The answer reports the cascade it moved**: readings by kind and reason, materials whose status changed, keyed aggregates dirtied. `--compare` reports the same cascade and writes nothing.
- **It refuses if any part cannot move**, naming each reason. The checks run on every batch ([the operation is the unit](#the-operation-is-the-unit)): a refusal before the first batch commits changes nothing; a refusal after it ends the operation `stopped_early`, naming what moved. (ruling 2025-07, aistudio:Sinex core.events, audit.log refactoring:31).

### Archive is not hiding

An archived reading is in the trash and nothing more. A later replay of its material simply reads the record again, and a re-stage of identical bytes is admitted against the live set only. Nothing remembers archived records to keep them out of replay or re-import. (ruling 2026-10-27, cc:ee52794c-8a9e-43b6-9fbc-9749c7e78e8f:d5d72ba0-797a-4efb-ab36-b9c318dd00d5).

## Restore

Restore (archived → live) is the inverse of archive: it moves back the chains that left together. (ruling 2026-02-03, cc:c7811684-01e8-45d9-bb81-84d8646379fb:ccb4ca44-25d5-4cdb-9029-bcba7a2c67c0).

- **Scope**: an archive operation (undoing it), or a reference, query or material over archived readings. The chain restored with a reading is the set of readings that left the live set in the same operation as that reading, across all of that operation's batches.
- **Refused while a different reading of the same record is live.** Archive that reading first; then the restore succeeds. (ruling 2026-03-11, codex:019cda9e-1b28-7ab0-af6c-f0eb8e602dcd@2026-03-11T15:17Z).
- **Also refused** for a tombstoned member, and for a member whose parent would not be live after the move. Refusal is per batch, as for archive.
- **Keyed aggregates are not restored as rows.** Derived readings that are keyed aggregates (a day's figures, an episode) and request-made derived events (keyed `query:<hash>`) get their keys marked dirty, and the derivation produces new readings from what is live after the restore. Re-deriving from what is live is a new derivation, and the old aggregate reading stays archived.

## Replay

Replay reads stored bytes again. It is a rescan of materials, not a procedure over events, and it is generic: it never runs a per-kind procedure. (ruling 2026-03-06, codex:019cbc74-e62a-7d93-987c-4848ca636a75@2026-03-06T05:30Z).

- **Scope is any set of records**: single events (their records), derived subtrees, whole materials, a binding, kinds, a time range, or a query's result. Replaying a derived event re-runs its derivation over its live parents, so synthesis events replay as subtrees. (ruling 2026-05-30, cc:3c91049e-3702-4e99-be65-81620e1f7552:e73554d4-91ea-403d-ad9a-62f4d0bcf128).
- **The records' stored bytes are read again** through the real pipeline with the current parser, or with `--parser`, which also assigns a parser to material staged without one. Parser carry flows from stored evidence, including the state saved at the end of the previous part.
- **Each new reading gets a new ID and becomes live; the reading it replaces is archived with its whole derived chain, reason `replay`.** That archival is what replay is: there are never two live readings of one record, and replay has no notion of skipping. Provenance never points to replaced readings. Replaying n times leaves n archived readings and one live one. (ruling 2026-10-27, cc-queued:ee52794c-8a9e-43b6-9fbc-9749c7e78e8f@2026-10-27T23:04Z).
- **No bit-for-bit promise.** A replay reads with today's parser and today's live parents, so its readings may differ from the ones it replaces. (ruling 2026-03-29, cc:ea2bf14e-10bb-42e9-9ff5-616882baaf1c:bbe835e2-4b30-498e-bc5b-d257f4d82323).

### Outcomes per record

| Outcome of the new read | Effect |
|---|---|
| a reading | becomes live; the old reading and its chain are archived (`replay`) |
| a parse failure | a record failure is recorded and the **old reading stays live** |
| the parser deliberately yields no event of a kind that had a live reading | that reading is archived, counted as removed by replay |
| a material that had live readings now yields no events at all | treated as a failure for that material: the old readings stay live and the answer says so |

### Transactions and resuming

- Parsing and producer transport finish before replay takes the writer. Native replay prepares an immutable sequence in private, unlinked scratch storage, one batch at a time; an ingress replay passes its already prepared batches directly. A failed or cancelled preparation changes no readings. Admission checks the selected materials' current lifecycle before replacement, so preparation cannot undo a concurrent archive or tombstone. Sealed readings remain sealed in preparation, and the unlock is checked again before admission commits.
- Each transaction moves complete records with their chains, saves the parser carry and the operation's progress, and is bounded in duration, so live admission is served between transactions. A part is the normal unit, split only at record boundaries whose carry is saved.
- After every commit each record has exactly one live reading.
- A stopped replay **resumes** from its saved progress without redoing finished work. An indivisible record larger than the duration budget is processed whole, and its size and hold time are reported.
- **`--compare`** runs the same path with the same validation and failures, reports unchanged, changed, added and removed readings with examples, and writes nothing: no events, positions, bytes or stream messages.

**Replay is the normal path for reading data again.** A failed record's retry is a replay of exactly its bytes; there is no separate store of failed messages.

## Undo

- **Undo a replay**: archive the replay's chain (the readings it admitted, with their descendants), then restore the previous one (the readings it archived), as one operation, `undo <operation>`. It can be repeated forever, in either direction. It is refused, naming the records, if a later operation has made other readings of those records live; archiving those first makes it possible. (ruling 2026-03-06, codex:019cbc74-e62a-7d93-987c-4848ca636a75@2026-03-06T00:00Z).
- **Undo an archive or a restore**: the inverse move, with the [restore refusals](#restore).
- **Undo an import**: archive the material, which archives everything read from it.

Undo, archive, restore and replay are ordinary, everyday features; tombstoning is the rare one.

## Whole chains move together

**State order.** With live < archived < tombstoned, every event's state is at least each of its parents' states. So a live event's parents are all live, and a tombstoned event's descendants are all tombstoned. An archived output of a still-live parent, as a derivation version change produces, is allowed. (ruling 2025-07, aistudio:Sinex core.events, audit.log refactoring:44).

- **The cascade is mandatory, never a flag**, whatever the reason for archiving: an operator's or agent's archive, replay, a newer reading, a version change.
- **Every transition walks the chain** from the root down through `parents`, and for keyed aggregates through their read ranges, to the last derived event. Each batch moves the complete chains it takes in one transaction.
- **Archiving one input of an aggregate archives the aggregate.** Re-deriving from what remains live is a new derivation: keyed outputs whose inputs are otherwise unchanged are re-admitted as new readings over live parents, never kept over archived ones.
- **Outputs of an input archived meanwhile.** When the writer admits derived events, it checks in the same transaction that every parent is live and none is tombstoned. An output with an archived parent is dropped and counted as `superseded_input` (not an error), since the cascade covers it. An output naming a tombstoned parent is refused as a code defect.

### Cost

- A `core.event_children(parent, child)` table, written at admission and never compressed, serves the walk. Archive selects each complete live chain once and uses that selection for dirty keys, archive history and live membership removal. Membership checks and payload reads are driven by selected IDs; derived source counts use their live source metadata. The registered keyed derivations determine which selected kinds require payload reads. Ownership keys are dirtied for every selected member, including kinds without a current consumer. Payload lookups for consumed kinds still inspect native compressed batch metadata.
- A keyed aggregate's parents are its recorded read ranges ([hierarchy](events.md#hierarchy)), and `event_children` holds one row per range, so a day figure over millions of inputs costs one row per range, not one per input. The walk from an event finds the aggregates whose ranges contain it.
- State is recorded at the granularity the operation names: a material's status, then per-event rows only below it. A transition costs in proportion to the materials and derived events it touches, never to all events.
- Large transitions run in bounded-duration transactions (batches), each moving complete chains, with live admission served between them. Archiving one source's year of compressed data is the case the writer-hold target is measured on ([targets](architecture.md#throughput-targets)).

### The operation is the unit

An archive, restore or tombstone that spans many batches is one operation. Its saved position is its progress, not a workflow state.

- Each batch takes the next roots of the scope in event ID order after the saved position, among readings committed by the operation's start, with their whole chains. Each batch commits at exactly one new commit position, so a scope that fits one batch, such as a single event, advances the commit log by one. Its size follows how long the previous batch held the writer. An undo runs as two phases in one operation: the archive of what the target made live, then the restore of what it archived.

- Source scopes stream their immutable reading IDs once on the read connection, at the operation's fixed start position and after its saved lower bound. This selection includes initially archived readings: a concurrent restore before a reading's batch must still be observed. Immutable source and boundary claims stay bound to that same operation; the writer checks each prepared batch's current state, moves its complete chains, and commits the position. It never waits for the source stream while holding a transaction. Restarting automatic retirement continues the same unfinished operation and its saved lower bound.
- Batch sizing measures only time processing on the writer, excluding source selection and time queued behind other jobs.

- Refusal checks run on every batch, at that batch's commit. A refusal before the first batch commits changes nothing. A refusal after it ends the operation `stopped_early`, never reported as ok: the answer names what moved and what refused. Once the cause is cleared, the operation resumes like a replay from its saved position, or it is undone by its inverse.
- "The chains that left together" means the chains the same operation moved, across all its batches.
- Keys dirtied by an operation in progress recompute after its last batch (or its `stopped_early` end), never between batches, so no aggregate reading is admitted between batches only to be archived by the next.

## Material status

A material is `live`, `archived` or `tombstoned`, and its status cascades to every event read from it.

- **An archived material is not replayed**; restore it first. A new stage of identical bytes is a new, live material.
- **Tombstoning a material** (it must be archived) removes its bytes, as a hole punch over its extents in the segment log, **only when nothing else is read from those bytes**: no other material of any status shares them. Otherwise the bytes stay, and the answer says which materials keep them. (ruling 2026-10-25, cc:ee52794c-8a9e-43b6-9fbc-9749c7e78e8f:bb22ed76-059e-4bdf-9e7b-a61d5b9bb206).
- **A tombstoned material is never replayed.** A punch that was marked but not yet done is completed at the writer's next group, or when the segment opens after a crash, and never while an open backup hold covers the extent ([backup and restore](operations.md#backup-and-restore)).

## Tombstone

Tombstone (archived → tombstoned) is irreversible and allowed only on archived chains. A live member refuses the whole request.

- **It drops payloads and keeps a skeleton forever**: event ID, source, type, `ts_orig` with its quality, tombstone time, reason, operation, and the record's place (material, anchor, output index; for a derived event, its key hash, never the key's fields). The skeleton keeps the provenance structure.
- **A tombstone destroys one interpretation; it does not stop new ones.** Replay of a live material and a re-stage of identical bytes produce new readings with new IDs, admitted normally. Nothing in admission consults tombstones to refuse a record. (ruling 2026-10-27, cc:ee52794c-8a9e-43b6-9fbc-9749c7e78e8f:1b44f4a1-22ca-4107-9711-5b0311caa1fa).
- **Tombstoned lineage stays dead in the provenance sense**: nothing new takes a tombstoned event as a parent, and a tombstoned chain is never restored. (ruling 2026-03-07, codex:019cbc74-e62a-7d93-987c-4848ca636a75@2026-03-07T05:50Z).
- **To be rid of the data itself**, the operator tombstones the material ([material status](#material-status)). When the material is kept because it holds unrelated records, the tombstoned record stays recoverable by replay, and the answer and the material's status say so.
- **Every place that held the payload is cleared**, in the same transaction or by its consumer at its position: the search read model, detector hits, annotations and embeddings of the readings, and the confirmed stream, whose messages for the tombstoned events are deleted by their stream sequence and followed by a tombstone notice ([the confirmed stream](producers.md#the-confirmed-stream)). Backups made before the tombstone are outside Sinex's reach, and the answer says how many backup sets predate it.

Tombstoning is the only irreversible deletion Sinex has.

## Archive reasons

| Reason | Set by |
|---|---|
| `archive` | an operator's or agent's archive |
| `replay` | a replay replacing the reading |
| `newer_reading` | a later changed reading of the same key ([what admission does with a key](events.md#what-admission-does-with-a-key)) |
| `version_change` | a `recompute` derivation switching version ([derivation versions](events.md#derivation-versions)) |
| `recomputed` | a keyed derivation replacing its outputs |

Cascade members carry the reason and operation of the move that took them.

Every admitted reading is kept when it is archived; near-identical archived readings are not coalesced. Provisional values of an open window are not admitted readings and leave no history ([derivations](derivations.md)). Withdrawing a derived claim is an ordinary authored event ([library](library.md)), not a lifecycle state.

## Storage of the states

Archiving moves no data, so archiving years of data costs what its state rows cost.

- One `core.events` table holds live and archived readings; archived rows stay where they are.
- The live tables name the live reading of each record ([liveness](admission.md#liveness)).
- `audit.archived` holds current state (commit position, reason, operation); `audit.archive_changes` holds every change into and out of the archive at its commit position, a restore with reason `restore`.
- Tombstones are their own table, `core.event_tombstones`, with record identity: the skeleton fields above, the key as a hash, and the tombstone's and the original commit positions. Older tombstones may retain unknown positions or class. A PostgreSQL check leaves that predecessor set intact and enforces complete metadata on every later insert or update, so no later erasure can silently join the historical gap.
- Material status is a column of the material with the operation that last set it; `raw.material_changes` holds every status change at its commit position.
- An operation is a row of `core.operations`: kind, actor, scope, counts, start and end commit positions, result, refusals, and its saved progress (phase, last root moved, batches committed). The first batch writes it; a refusal before that records nothing.
- Keys an operation dirties are held (`derivation.dirty.held_by`) until it ends; the writer releases every hold when it starts, since no operation runs then.
- A replay's new readings carry the replay operation as their run, so its undo finds what it admitted.

## As-of reads

At commit position S an event is seen when `commit_seq ≤ S` and its last archive change at or before S, if any, left it live. A tombstoned reading appears in an as-of read as its skeleton. (ruling 2025-07, aistudio:Sinex - #3 grand discussion:56). The syntax is in [query](query.md).

## Operations and actors

Every user act that changes state is an **operation**: stage, archive, restore, replay, undo, tombstone, parser assignment, derivation switch, privacy detector or rule change, `same` and `distinct` decisions, backup hold and release. Manifests, runs and operations are three distinct records ([runs](events.md#producer-versions-runs-and-manifests)); an operation that ingests data also has a run. (ruling 2026-05-07, cc:5fdffc35-be4a-457f-bb6b-4caa52b152c3:fb339038-b974-4a13-919d-8b7742d88f97).

| Field | Meaning |
|---|---|
| kind | which act |
| actor | who did it, computed by the daemon |
| scope | as given |
| counts | the resolved counts |
| start and end commit positions | where it began and ended in the commit log |
| result | ok or `stopped_early` |

- **Operations have no workflow states**: no preview, approval, expiry or cancel lifecycle. A multi-batch operation's saved position is progress, not a state; it ends ok or `stopped_early`, and a stopped one resumes from its position. A `--compare` is a read and writes nothing.
- **The actor is computed by the daemon** from the authenticated caller: the operator, an agent session with its harness and session ID, an outside producer's credential, or `sinexd` itself. No command supplies it. Writes through the API keep their actor. How callers are authenticated is in [callers](interfaces.md#callers).
- **No act is refused because the caller is an agent.** Agents have the same lifecycle powers as the operator; what differs is the recorded actor.
