# The lifecycle

Nothing in Sinex is deleted by default. Readings and materials are **live**, **archived** (the trash, reversible) or **tombstoned** (irreversible, from the archive only), and whole provenance chains move between those states together. This page owns the states, archive and restore, replay and its undo, the whole-chain rule, material status, tombstoning, archive reasons, how the states are stored, as-of reads, and operations with their actors. Records and readings are defined in [events](events.md); the lifecycle verbs on the CLI are in [the CLI](interfaces.md#cli); lifecycle requests on the wire are in [producers](producers.md#lifecycle-requests); the as-of query syntax is in [query](query.md); how derivations recompute after a move is in [derivations](derivations.md#derivations-and-the-lifecycle).

## States

| State | Payload | Seen by ordinary queries | Leaves to |
|---|---|---|---|
| live | full | yes | archived |
| archived (the trash) | full; any number of archived readings per record | through archive and as-of reads | live (restore), tombstoned |
| tombstoned | none; a skeleton | no | never |

Tombstoning is to the archive what archiving is to the live set: a second stage, reachable only from the first. Ruling, 2026-02-03: "allow tombstoning but only of archived events? maybe consider tombstoes to have similar relation ton archive as archive to live eventlog?" (cc:c7811684-01e8-45d9-bb81-84d8646379fb:d6e73771-976c-4671-bdf0-a8fdb2e6b32e). Ruling, 2026-10-27: "threes basic architecture of archival and theree's also second stage of that, tombstoning" (cc:ee52794c-8a9e-43b6-9fbc-9749c7e78e8f:85104ca6-e6d0-4cab-842a-23ffa298ff38).

**There is no automatic retention.** Nothing moves between states by age or by a schedule; material is kept until the operator or an agent archives it.

In primary mode, Polylogue's materials and the `polylogue.*` readings made from them are ordinary Sinex materials and readings and follow this lifecycle; Polylogue on its own has [no deletion](https://github.com/Sinity/polylogue/blob/master/docs/architecture.md#deletion).

## Archive

Archive (live → archived) is ordinary deletion, and there is no other way to delete. Archiving is reversible, like an operating system's trash. Ruling, 2025-08-11: "deleting evennts is supposed to be IMPOSSIBLE" (cc:505f40e8-3618-4857-81d3-e201f417a2ff:9833b29b-2a3a-4d64-9ee3-d04f6eb0748e). Ruling, 2026-02-03: archiving "is _REVERSIBLE_ operation … this design is basically equivalent of OS trash (vs rm)" (cc:7ef0e4c5-d017-412d-b3d8-42ad7d0c2819:89712340-bbdf-4b1b-9220-ce7b4fafbf20).

- **Scope**: a reference, a query (`--source terminal --before 2026-09-01`, any `events where …`), a material, or a run.
- **It moves the scope's live readings and their whole live chains** ([whole chains](#whole-chains-move-together)).
- **The answer reports the cascade it moved**: readings by kind and reason, materials whose status changed, keyed aggregates dirtied. `--compare` reports the same cascade and writes nothing.
- **It refuses if any part cannot move**, naming each reason. The checks run on every batch ([the operation is the unit](#the-operation-is-the-unit)): a refusal before the first batch commits changes nothing; a refusal after it ends the operation `stopped_early`, naming what moved. Ruling, 2025-07: "application must check whether it can delete all these anyway and if it can't, it shoud not delete anything." (aistudio:Sinex core.events, audit.log refactoring:31).

### Archive is not hiding

An archived reading is in the trash and nothing more. A later replay of its material simply reads the record again, and a re-stage of identical bytes is admitted against the live set only. Nothing remembers archived records to keep them out of replay or re-import. Ruling, 2026-10-27: "archival is not 'hiding'" (cc:ee52794c-8a9e-43b6-9fbc-9749c7e78e8f:d5d72ba0-797a-4efb-ab36-b9c318dd00d5).

## Restore

Restore (archived → live) is the inverse of archive: it moves back the chains that left together. Ruling, 2026-02-03: "archived events can be brought back" (cc:c7811684-01e8-45d9-bb81-84d8646379fb:ccb4ca44-25d5-4cdb-9029-bcba7a2c67c0).

- **Scope**: an archive operation (undoing it), or a reference, query or material over archived readings. The chain restored with a reading is the set of readings that left the live set in the same operation as that reading, across all of that operation's batches.
- **Refused while a different reading of the same record is live.** Archive that reading first; then the restore succeeds. Ruling, 2026-03-11: "that should be simply illegal. I mean, one could do that only if one first archives the replay-originated events so that there's no colision. WHich is fine." (codex:019cda9e-1b28-7ab0-af6c-f0eb8e602dcd@2026-03-11T15:17Z).
- **Also refused** for a tombstoned member, and for a member whose parent would not be live after the move. Refusal is per batch, as for archive.
- **Keyed aggregates are not restored as rows.** Derived readings that are keyed aggregates (a day's figures, an episode) and request-made derived events (keyed `query:<hash>`) get their keys marked dirty, and the derivation produces new readings from what is live after the restore. Re-deriving from what is live is a new derivation, and the old aggregate reading stays archived.

## Replay

Replay reads stored bytes again. It is a rescan of materials, not a procedure over events, and it is generic: it never runs a per-kind procedure. Ruling, 2026-03-06: "always the whole chain, from source material to the last derived synthesis event, is going to move … replay shouldn't even think about events, it's a rescan" (codex:019cbc74-e62a-7d93-987c-4848ca636a75@2026-03-06T05:30Z).

- **Scope is any set of records**: single events (their records), derived subtrees, whole materials, a binding, kinds, a time range, or a query's result. Replaying a derived event re-runs its derivation over its live parents, so synthesis events replay as subtrees. Ruling, 2026-05-30: "you can replay arbitrayr events, not just entire material" (cc:3c91049e-3702-4e99-be65-81620e1f7552:e73554d4-91ea-403d-ad9a-62f4d0bcf128).
- **The records' stored bytes are read again** through the real pipeline with the current parser, or with `--parser`, which also assigns a parser to material staged without one. Parser carry flows from stored evidence, including the state saved at the end of the previous part.
- **Each new reading gets a new ID and becomes live; the reading it replaces is archived with its whole derived chain, reason `replay`.** That archival is what replay is: there are never two live readings of one record, and replay has no notion of skipping. Provenance never points to replaced readings. Replaying n times leaves n archived readings and one live one. Ruling, 2026-10-27: "replay itslef is what archives! how can it skip" (cc-queued:ee52794c-8a9e-43b6-9fbc-9749c7e78e8f@2026-10-27T23:04Z).
- **No bit-for-bit promise.** A replay reads with today's parser and today's live parents, so its readings may differ from the ones it replaces. Ruling, 2026-03-29: "There's explicitly no bit-by-bit promise in replay." (cc:ea2bf14e-10bb-42e9-9ff5-616882baaf1c:bbe835e2-4b30-498e-bc5b-d257f4d82323).

### Outcomes per record

| Outcome of the new read | Effect |
|---|---|
| a reading | becomes live; the old reading and its chain are archived (`replay`) |
| a parse failure | a record failure is recorded and the **old reading stays live** |
| the parser deliberately yields no event of a kind that had a live reading | that reading is archived, counted as removed by replay |
| a material that had live readings now yields no events at all | treated as a failure for that material: the old readings stay live and the answer says so |

### Transactions and resuming

- Each transaction moves complete records with their chains, saves the parser carry and the operation's progress, and is bounded in duration, so live admission is served between transactions. A part is the normal unit, split only at record boundaries whose carry is saved.
- After every commit each record has exactly one live reading.
- A stopped replay **resumes** from its saved progress without redoing finished work. An indivisible record larger than the duration budget is processed whole, and its size and hold time are reported.
- **`--compare`** runs the same path with the same validation and failures, reports unchanged, changed, added and removed readings with examples, and writes nothing: no events, positions, bytes or stream messages.

**Replay is the normal path for reading data again.** A failed record's retry is a replay of exactly its bytes; there is no separate store of failed messages.

## Undo

- **Undo a replay**: archive the replay's chain (the readings it admitted, with their descendants), then restore the previous one (the readings it archived), as one operation, `undo <operation>`. It can be repeated forever, in either direction. It is refused, naming the records, if a later operation has made other readings of those records live; archiving those first makes it possible. Ruling, 2026-03-06: "originals can be restored at wil from archive, and then the new one has to go to archive, and one ciould swap them around like that forever" (codex:019cbc74-e62a-7d93-987c-4848ca636a75@2026-03-06T00:00Z).
- **Undo an archive or a restore**: the inverse move, with the [restore refusals](#restore).
- **Undo an import**: archive the material, which archives everything read from it.

Undo, archive, restore and replay are ordinary, everyday features; tombstoning is the rare one.

## Whole chains move together

**State order law.** With live < archived < tombstoned, every event's state is at least each of its parents' states. So a live event's parents are all live, and a tombstoned event's descendants are all tombstoned. An archived output of a still-live parent, as a derivation version change produces, is allowed. Ruling, 2025-07: "It should be impossible … for an event in 'alive' table to have an event from a 'dead' table as its provenance … basically, alwasy move only entire subtrees back and forth" (aistudio:Sinex core.events, audit.log refactoring:44).

- **The cascade is mandatory, never a flag**, whatever the reason for archiving: an operator's or agent's archive, replay, a newer reading, a version change.
- **Every transition walks the chain** from the root down through `parents`, and for keyed aggregates through their read ranges, to the last derived event. Each batch moves the complete chains it takes in one transaction.
- **Archiving one input of an aggregate archives the aggregate.** Re-deriving from what remains live is a new derivation: keyed outputs whose inputs are otherwise unchanged are re-admitted as new readings over live parents, never kept over archived ones.
- **Outputs of an input archived meanwhile.** When the writer admits derived events, it checks in the same transaction that every parent is live and none is tombstoned. An output with an archived parent is dropped and counted as `superseded_input` (not an error), since the cascade covers it. An output naming a tombstoned parent is refused as a code defect.

### Cost

- A `core.event_children(parent, child)` table, written at admission and never compressed, serves the walk.
- A keyed aggregate's parents are its recorded read ranges ([hierarchy](events.md#hierarchy)), and `event_children` holds one row per range, so a day figure over millions of inputs costs one row per range, not one per input. The walk from an event finds the aggregates whose ranges contain it.
- State is recorded at the granularity the operation names: a material's status, then per-event rows only below it. A transition costs in proportion to the materials and derived events it touches, never to all events.
- Large transitions run in bounded-duration transactions (batches), each moving complete chains, with live admission served between them. Archiving one source's year of compressed data is the case the writer-hold target is measured on ([targets](architecture.md#throughput-targets)).

### The operation is the unit

An archive, restore or tombstone that spans many batches is one operation. Its saved position is its progress, not a workflow state.

- Refusal checks run on every batch, at that batch's commit. A refusal before the first batch commits changes nothing. A refusal after it ends the operation `stopped_early`, never reported as ok: the answer names what moved and what refused. Once the cause is cleared, the operation resumes like a replay from its saved position, or it is undone by its inverse.
- "The chains that left together" means the chains the same operation moved, across all its batches.
- Keys dirtied by an operation in progress recompute after its last batch (or its `stopped_early` end), never between batches, so no aggregate reading is admitted between batches only to be archived by the next.

## Material status

A material is `live`, `archived` or `tombstoned`, and its status cascades to every event read from it.

- **An archived material is not replayed**; restore it first. A new stage of identical bytes is a new, live material.
- **Tombstoning a material** (it must be archived) removes its bytes, as a hole punch over its extents in the segment log, **only when nothing else is read from those bytes**: no other material of any status shares them. Otherwise the bytes stay, and the answer says which materials keep them. Ruling, 2026-10-25: "it should never affect unrelated datapoints" (cc:ee52794c-8a9e-43b6-9fbc-9749c7e78e8f:bb22ed76-059e-4bdf-9e7b-a61d5b9bb206).
- **A tombstoned material is never replayed.** A punch that was marked but not yet done is completed at the writer's next group, or when the segment opens after a crash, and never while an open backup hold covers the extent ([backup and restore](operations.md#backup-and-restore)).

## Tombstone

Tombstone (archived → tombstoned) is irreversible and allowed only on archived chains. A live member refuses the whole request.

- **It drops payloads and keeps a skeleton forever**: event ID, source, type, `ts_orig` with its quality, tombstone time, reason, operation, and the record's place (material, anchor, output index; for a derived event, its key hash, never the key's fields). The skeleton keeps the provenance structure.
- **A tombstone destroys one interpretation; it does not stop new ones.** Replay of a live material and a re-stage of identical bytes produce new readings with new IDs, admitted normally. Nothing in admission consults tombstones to refuse a record. Ruling, 2026-10-27: "tombstoning is for an event, event is just an 'interpretation' of a record within a source material. why would tombstoning an interpretation also stop creating new interpretations?" (cc:ee52794c-8a9e-43b6-9fbc-9749c7e78e8f:1b44f4a1-22ca-4107-9711-5b0311caa1fa).
- **Tombstoned lineage stays dead in the provenance sense**: nothing new takes a tombstoned event as a parent, and a tombstoned chain is never restored. Ruling, 2026-03-07: "reconstruct … with provenance links to tombstoned lineage <<---- ya that's not how provenance works, at all" (codex:019cbc74-e62a-7d93-987c-4848ca636a75@2026-03-07T05:50Z).
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
- `audit.archived` holds current state (commit position, reason, operation); `audit.archive_changes` holds every change into and out of the archive at its commit position.
- Tombstones are their own table, with record identity.
- Material status is a column of the material.

## As-of reads

At commit position S an event is seen when `commit_seq ≤ S` and its last archive change at or before S, if any, left it live. A tombstoned reading appears in an as-of read as its skeleton. Ruling, 2025-07: "with audit table, we can time travel arbitrarily." (aistudio:Sinex - #3 grand discussion:56). The syntax is in [query](query.md).

## Operations and actors

Every user act that changes state is an **operation**: stage, archive, restore, replay, undo, tombstone, parser assignment, derivation switch, privacy detector or rule change, `same` and `distinct` decisions, backup hold and release. Manifests, runs and operations are three distinct records ([runs](events.md#producer-versions-runs-and-manifests)); an operation that ingests data also has a run. Ruling, 2026-05-07: "basically operations log is \"user did something in the system\". like replay, or sinexctl stage <path_to_a_file>" (cc:5fdffc35-be4a-457f-bb6b-4caa52b152c3:fb339038-b974-4a13-919d-8b7742d88f97).

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

## Laws

- **state-order**: every event's state is at least each parent's state, after every operation.
- **archive-moves-whole-chains**: archive by reference, query, material and run moves whole chains; the answer's cascade equals `--compare`'s, and `--compare` writes nothing.
- **refusal-per-batch**: an archive or restore with one unmovable member in its first batch changes nothing; with one in batch N > 1, batches 1..N−1 stay moved, the operation ends `stopped_early` naming what moved, and resuming it after the cause is cleared reaches the same final state as an uninterrupted run.
- **restore-refused-while-other-reading-live**: restore is refused while a different reading of the record is live, and succeeds after that reading is archived.
- **restore-rederives-aggregates**: restoring an input of a keyed aggregate produces a new aggregate reading while the old aggregate row stays archived; a restore of chains that left in one multi-batch operation brings back exactly that operation's chains.
- **dirty-keys-wait-for-operation**: no keyed aggregate reading is admitted over keys an operation dirtied until its last batch (or `stopped_early` end); then each such key recomputes once.
- **aggregate-children-per-range**: an aggregate over N inputs in R read ranges writes R `event_children` rows, and archiving any one input archives the aggregate.
- **replay-archives-replaced-readings**: replay of a single event, a derived subtree, a material and a query result archives exactly the replaced readings and their chains and mints new IDs; replaying twice gives two archived readings and one live per record.
- **replay-failure-keeps-old-reading**: a parse failure in replay leaves the old reading live, and an all-empty material replay leaves the old readings live.
- **replay-resumes**: a replay killed between transactions resumes to the same final live content as an uninterrupted replay, with one live reading per record at every commit.
- **undo-swaps-forever**: undo of a replay restores the previous readings, undo of the undo swaps back, and repeated N times the live set alternates while the archive holds every reading.
- **archive-material-archives-readings**: archiving a material archives everything read from it, and a new stage of the same bytes is admitted live.
- **tombstone-only-archived**: tombstone is refused for any live member; a tombstoned event keeps exactly the skeleton fields; nothing restores it.
- **tombstone-allows-new-readings**: after tombstoning a record's readings, replay of its live material and a re-stage of identical bytes both admit new live readings.
- **no-dead-parents**: no admitted event names a tombstoned parent, and an output whose parent was archived meanwhile is dropped as `superseded_input`.
- **material-tombstone-spares-shared-bytes**: tombstoning a material removes its bytes only when no other material shares them; unrelated materials' bytes and offsets are identical before and after.
- **tombstone-clears-copies**: tombstoning clears the readings' search entries, detector hits, annotations and confirmed-stream messages.
- **as-of-reads-exact**: as-of reads at every commit position of a scripted history return the live set that held at that position.
- **operations-recorded-with-actor**: every state-changing act writes an operation with the computed actor, and an API write keeps its actor.
