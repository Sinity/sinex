# Vocabulary

Each concept and its one name, with a one-line meaning. Code, SQL, wire keys,
error text, comments and docs use these names. The values of closed sets
(kinds, sources, tables, settings) are in the generated
[reference](reference/README.md); the shared mechanics (blob, segment, extent,
day setting, operation, failure kind, deadline class) are named in common's
[vocabulary](https://github.com/Sinity/polylogue-sinex-common/blob/master/docs/vocabulary.md).

## Capture

- **source**: a named kind of record Sinex reads, `<area>.<name>` (`terminal.atuin-history`), with one declaration holding its parser, its kinds and its settings ([sources](sources.md#one-declaration-per-source)).
- **source family**: a group of sources about one domain (shell, browsers, mail …) whose fidelity requirements are stated together ([source families](source-families.md)).
- **input**: something Sinex observes repeatedly: a file, a directory, a database, a socket, an account collection.
- **input shape**: how an input is read (append-only file, database rows, export archive, directory, live stream, account API, producer).
- **change model**: how a source learns that its input changed and what a change means.
- **binding**: one configured instance of a source: the source, its input and its settings.
- **stage**: to hand bytes to Sinex as material; the one way anything enters, whether once (`stage PATH`) or continuously (a binding) ([staging](materials.md#staging)).
- **observation**: the recorded facts of one look at an input (size, time, whether it changed).
- **revision**: an input's content as of an observation that found a change.
- **spool**: a live capture's local directory of segment files, written before anything is sent; binary spools are pcapng ([streams](streams.md#spools)).
- **gap**: an interval in which a capture may have missed data, recorded as a `capture.gap` event with its reason ([gaps](sources.md#gaps)).
- **exclusion**: the one per-source setting: a source or input left out, with a recorded reason.
- **source health**: whether a source is observing as its declaration expects; shown in status.

## Materials and records

- **material**: stored, immutable, source-shaped bytes that events cite: a file revision, a part of a growing file, a capture segment, an export, a set of database rows ([materials](materials.md)).
- **part**: one closed chunk of a growing input; an input's parts form its **part sequence**.
- **manifest**: what a material's bytes are and how to read them.
- **directory capture**: a directory stored as one PAX tar material.
- **record**: one datapoint inside a material (a line, a row, a file, a message).
- **record coordinates**: `(material, anchor)`, where a record starts; the **anchor** is its byte offset.
- **source record key**: the key by which a source recognises the same record seen again in another material ([identity](events.md#identity)).
- **record hash**: the hash of a record's content, independent of privacy rules.
- **record failure**: a record the parser could not read, stored with its bytes.
- **removed at source**: the source no longer has a record; stored as a newer reading saying so.

## Events

- **event**: a typed payload with one origin (a record, or parent events) and three clocks ([events](events.md)).
- **kind**: the registered name and declaration of a payload type (`command.executed`), with its fields, field hints, keying and search participation.
- **reading**: the event one read of a record produced; at most one reading of a record is **live**.
- **reading order**: which of two readings of a record is newer; assigned or checked by Sinex, never taken from a producer's clock.
- **clocks**: `ts_orig` (when it happened, or unknown), `ts_coided` (when Sinex read it, from the UUIDv7 ID), `ts_persisted` (when it was committed).
- **derived event**: an event computed by a derivation, naming its parents.
- **producer version**: the exact version of the parser or derivation that made an event.
- **run**: one execution of a producer; every event names its run.
- **citation**: a stable reference to an event, record, key or time window, readable by every surface ([citation forms](query.md#citation-forms)).

## Lifecycle

- **live**, **archived**, **tombstoned**: the three states of an event or material ([lifecycle](lifecycle.md#states)).
- **archive**: move to the trash, reversibly, with the whole derived chain; **restore** is its inverse.
- **replay**: read stored records again; each new reading becomes live and the reading it replaces is archived with its chain.
- **undo**: archive what an operation made live and restore what it archived.
- **tombstone**: the one irreversible deletion of an archived chain; a skeleton is kept.
- **archive reason**: why a reading was archived (replay, a newer reading, the operator's archive, a recompute).
- **whole chain**: an event with everything derived from it; it moves between states together.
- **operation**: something the operator or an agent did to Sinex, recorded with its computed **actor**.
- **as-of read**: a read of what was live at a given commit position.

## Admission and storage

- **admission**: the one function that stores events and source progress together ([admission](admission.md)).
- **writer**: the daemon task that runs admission, one commit group at a time.
- **commit log**: committed changes in commit order; each has a **commit position**.
- **read model**: a table maintained by admission for reading (liveness, detector hits, `duplicates`).
- **schema upgrade**: a numbered, data-preserving change to the stored schema, applied by `sinex-schema apply`.

## Producers and transport

- **producer**: anything that hands Sinex events: a binding, a derivation, an outside program.
- **outside producer**: a program outside the daemon that sends frames over the NATS ingress (`sinex-session`, `sinex-capture`, Polylogue).
- **ingress**: the NATS subjects outside producers send frames to ([producers](producers.md#the-ingress)).
- **frame**: one message from an outside producer: kinds, material, events, progress, or a request.
- **producer client**: the one implementation of the producer side of the contract.
- **refusal**: a frame Sinex will not admit; a content refusal becomes a record failure and never stalls capture.
- **confirmed stream**: the NATS stream of committed changes in commit order, the delivery log for everything outside the daemon.
- **outside consumer**: a declared program that reads the confirmed stream under its own credential; an **outside handler** also sends outputs back.
- **withheld**: a field left out of a stream message or output, fetched by event ID under the caller's grants.

## Privacy

- **field hint**: a kind's declaration that a field may hold a kind of sensitive content ([privacy](privacy.md#field-hints)).
- **detector**: operator-supplied data that recognises sensitive content; a match is a **detector hit**.
- **rule**: binds detectors or field paths to a class and an output action.
- **class**: a named outcome the operator defines, an action plus the grants that lift it; a **grant** is held by a caller's token.
- **action**: what an output does to a matched field: mask, hash, withhold or seal.
- **seal**: encrypt at rest so that reading needs the operator's key.
- **output parity**: every surface shows a caller the same fields for the same grants.

## Derived data

- **derivation**: a declared computation whose outputs are stored events; **per-event** or **keyed** ([derivations](derivations.md)).
- **dirty key**: a key whose inputs changed and whose output set will be recomputed.
- **provisional**: the value of a key whose time window is still open.
- **fusion**: joining readings of one occurrence seen by several sources, for a defined family, into a **fused record** with its **basis**.
- **judgment**: an authored `same` or `distinct` decision on an ambiguous pair in `duplicates`.
- **interval**: a state with a start and maybe an end, computed by a derivation.
- **figure**: a measure over one period, stored as an event with its coverage.
- **coverage**: which sources could observe, over which interval, in which state (observed, partial, recorded with unknown coverage, missing).
- **episode**: a stretch of work on one objective, with interruptions and its resume card ([higher layer](higher-layer.md)).
- **panel** (`analysis.panel`): the stored cells and selected source coverage for a canonical analysis question.
- **analysis request** (`analysis.request`): an authenticated question retained as source material; its canonical identity includes the operation, dates, metrics, day setting and grants.
- **finding** (`analysis.finding`): a stored typed statement with its caveats and the figures it cites; a **recipe** is a shipped analysis whose result is an **answer** (`recipe.answer`).
- **model policy** (`model.policy`): an operator-authored route, instructions and explicit operating budget, effective only with the writer's authorization mark.
- **model request** (`model.request`): retained instructions and verified typed input identities with their exact current reading references.
- **recorded model response** (`model.response`): acquired response content, its separate output hash, provider receipt and reported actual charge; receiving it does not execute a model.
- **forecast**: a probability on a stated outcome with a horizon and an information cutoff.
- **context pack**: a stored selection of evidence assembled for one purpose; a **narrative** is a written account over a period.

## Reading and asking

- **query table**: what a query reads (`events`, `materials`, `runs`, `sources`, `failures` …), with its fields ([query](query.md)).
- **saved query**: a named query with declared scalar parameters.
- **kept result**: a query's result stored as an event so it can be cited.
- **recall**: everything recorded around a time, a day, a mark or the latest shell session ([recall](recall.md)).
- **mark**: an operator-authored moment with a note.
- **brief**: the daily summary.
- **signal**: what a rule surfaces without being asked; **interrupting** signals are few and explicit.
- **status**: what Sinex holds, what it failed to read and what it has not processed.
- **follow**: a live, resumable read of new events matching a query.
- **Today**: the view Sinex opens with ([web](web.md#today)).

## Library

- **item key**: the host, resolved root and original relative path identifying an item; an unambiguous equal-byte move within the root keeps it.
- **move candidates**: missing and new equal-byte paths whose correspondence is ambiguous; curation stays separate.
- **scan record**: a file's changed facts or removal in a captured scan part; part order is independent of wall-clock order.
- **item revision**: an item's content hash; returning to the same bytes reuses the revision while preserving the transition history.
- **preview evidence**: the stored result of rendering a revision, from which the library derives a rendition.

## Development tooling

- **scenario**: one exported NixOS check selected for a VM run.
- **development stack**: a checkout's owned PostgreSQL and NATS services, daemon and explicitly selected session captures.
- **development lock**: the exclusive lock held while a checkout's development stack runs.
- **corpus**: input files replayed by fuzz tests and expanded by longer test runs.
- **region**: a source span with an execution count in an LLVM coverage report.
