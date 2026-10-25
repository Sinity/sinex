# Vocabulary

One name per concept. Code, SQL, wire keys, error text, comments and docs use
these names and no others.

## Captured data

- **input**: something Sinex observes repeatedly: a file, a database, a socket, an API collection.
- **source**: a kind of input with its parser (`zsh`, `notes`, `google-takeout`).
- **binding**: one configured instance of a source: the source, its location and options.
- **driver**: shared code that reads one input shape (append-only file, SQLite rows, export archive, directory snapshot, live stream, API cursor, media, producer).
- **observation**: one look at an input.
- **revision**: an input's content as of an observation that found a change.
- **material**: immutable stored bytes that events cite, with a manifest describing where they came from.
- **part**: one closed chunk of a growing input; each part is its own material.
- **blob**: bytes in the content store, named by their BLAKE3 hash.
- **stored bytes**: the bytes of a material as kept in the content store; replay reads them.
- **unadmitted bytes**: blobs written before the admission that will cite them. A failed admission is retried from them while the reader holds them; a file a crash leaves with no `core.blobs` row is deleted when the daemon next starts.
- **held material**: the newest material a reader keeps in memory until its final cursor is admitted, so a failed admission retries the same bytes.
- **record**: one unit a parser reads from a material (a line, a row, a file, a message).
- **occurrence**: one thing a source recorded, found by its record location or equivalence key; it has at most one live reading.
- **record location**: `(material, anchor)`, where a record starts; the **anchor** is its byte position.
- **record failure**: a record the parser could not read, stored with its bytes.
- **carry**: parser state kept between records of one input.
- **cursor**: how far a binding's input has been admitted.

## Events

- **event**: one reading of a record, or of parent events: a typed payload, an origin and three clocks.
- **reading**: the event a record produced on one read; the **live reading** is the current one, older ones are **archived**.
- **reading order**: which of two readings of a record is newer.
- **equivalence key**: the per-type key that recognizes the same thing seen again in another material.
- **clocks**: `ts_orig` (when it happened, or unknown), `ts_coided` (when Sinex read it, from the UUIDv7 ID), `ts_persisted` (when it was committed).
- **derived event**: an event computed by a derivation, naming its parents.
- **fused command**: the `command.fused` event that joins one shell command seen by several sources.
- **batch**: what a producer hands the writer: records with their events or failures, and progress.
- **run**: one execution of a producer; every event names its run.

## Storing and reading

- **admission**: the one function that stores events and source progress together.
- **writer**: the daemon task that runs admission, one transaction at a time.
- **commit log**: committed events and visibility changes in commit order; each entry has a **commit position**.
- **derivation**: keyed (recomputes a **dirty key** when its inputs change) or per-event.
- **replay**: reading stored bytes again with the current parsers.
- **product**: a stored analysis result over many events; a **finding** is a statement about one.
- **operation**: something the operator or an agent did to Sinex, attributed and logged.

## Producers and transport

- **producer**: anything that hands Sinex events: a binding, a derivation, an outside process.
- **outside producer**: `sinex-session`, `sinex-capture` or another process sending over NATS.
- **ingress**: the NATS subjects outside producers send frames to.
- **frame**: one message from an outside producer: events, a material, or progress.
- **spool**: an outside producer's local directory of frames not yet acknowledged.
- **acknowledgment**: the reply that confirms a frame was admitted.
- **publish**: send a NATS message.
- **confirmed stream**: the NATS stream of committed events in commit order.

## Removal and privacy

- **removed at source**: the source no longer has a record; stored as a newer reading.
- **hide / unhide**: take an event out of views, counts and re-imports, and put it back.
- **excision**: the operator's permanent deletion of one record's events and the material holding it, when nothing else shares that material; only an **excisable** source, one whose every stored copy of a record is a reading of it, allows it.
- **class**: a privacy class: `open`, `guarded` or `sealed`.
- **sealed**: encrypted at rest under a per-material **data key**, wrapped for the operator's **class key**; reading needs an **unlock**.
- **rule**: a privacy rule applied at admission; **masking** hides credentials on output.

## Words not used

| Not used | Say instead |
|---|---|
| retained, retain, retention | stored, kept |
| custody, reservation, pending (for bytes) | unadmitted bytes |
| publication (for bytes) | write, install |
| frozen, freeze | fixed, saved, or name the thing itself |
| canonical | fused (commands), sorted (JSON) |
| basis | name the thing itself |
| candidate | selected |
| authority, lease | access, grant, guard |
| legacy, predecessor, old formats | nothing: internal formats have one version until the first deployment |
| control, proof, witness (in tests) | test, check |
| backing | storage |
| interpretation | parsing, reading |
| retire, retirement, closure, qualify | say what happens: delete, drop, stop |
