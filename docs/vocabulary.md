# Vocabulary

One name per concept. Code, SQL, wire keys, error text, comments and docs use
these names and no others.

## Captured data

- **input**: something Sinex observes repeatedly: a file, a database, a socket, an API collection.
- **source**: a named kind of record Sinex reads, `<area>.<name>` (`terminal.zsh-history`), with its parser, its event types and its purpose. Events carry the source's name. Several declarations can share one source name: one per table it reads (`operator.steering`) or one per format that reads it (the live clipboard and its lake lane).
- **format**: how an input is laid out and which sources it feeds (`zsh`, `takeout`, `sinnix-lake`); `--read FORMAT=PATH` plans the bindings for an input in that format.
- **binding**: one configured instance of a source: the source, its input and its settings.
- **driver**: shared code that reads one input shape (append-only file, SQLite rows, export archive, directory snapshot, live stream, API cursor, media, producer, API request).
- **reader**: one binding's driver with what it remembers about its input.
- **poll**: one look by a reader at its input. It yields an acquisition.
- **acquire**, **acquisition**: obtaining an input's bytes; an acquisition (`Acquired`) is what one poll produced: the observation's facts, the revision change and any new material.
- **observation**: the recorded facts of one poll (size, time, whether it changed); unchanged polls only count up.
- **revision**: an input's content as of an observation that found a change.
- **material**: immutable stored bytes that events cite, with a manifest.
- **manifest**: what a material's bytes are and how to read them: where they came from, their media type, the records a driver listed, and the driver's details.
- **part**: one closed chunk of a growing input; each part is its own material and names the part before it, so an input's parts form a **part sequence**.
- **blob**: bytes in the content store, named by their BLAKE3 hash.
- **owned blob**: a blob an event's payload names beyond its material (a library file copy); excising the event deletes it.
- **stored bytes**: the bytes of a material as kept in the content store; replay reads them.
- **unadmitted bytes**: blobs written before the admission that will cite them. A failed admission is retried from them while the reader holds them; a file a crash leaves with no `core.blobs` row is deleted when the daemon next starts.
- **held material**: the newest material a reader keeps in memory until its final cursor is admitted, so a failed admission retries the same bytes.
- **framing**: splitting a material's bytes into records (lines, delimiters, archive members); a **framer** is a source's framing, used alike by its driver, replay and excision.
- **record**: one unit a parser reads from a material (a line, a row, a file, a message).
- **record location**: `(material, anchor)`, where a record starts; the **anchor** is its byte offset. The query language and references call it the offset.
- **output index**: which of a record's events an event is, when one record yields several.
- **record failure**: a record the parser could not read, stored with its bytes.
- **drain**: running a source's parser over a material's records, attaching record locations, hashes, carry and record failures to each batch.
- **carry**: parser state kept between records of one input.
- **cursor**: how far a binding's input has been admitted.
- **reader memory**: what a reader remembers about its input (each row's hash, each file's size, time and hash), saved with the cursor at the end of a part so a restart reads only the parts after it.
- **shared pass**: one read of a stored file for every compatible binding of it (export archives); each binding keeps its own parts and cursor.
- **backlog**: captured bytes and records beyond a spool's acknowledged cursor, together with the producer's unacknowledged frames.
- **spool**: a live capture's directory of segment files, which its binding then reads like a growing file.

## Events

- **event**: one reading of a record, or of parent events: a typed payload, an origin and three clocks.
- **event type**: the registered name of a payload type, `<thing>.<what>` (`command.executed`, `note.document`). An occurrence is named with a past participle (`window.opened`), a state or record with a noun (`journal.entry`). A **span** is a state a source observed with its end; an **interval** is one a derivation computed.
- **reading**: the event a record produced on one read; the **live reading** is the current one, older ones are **archived**, each with an archive reason (`replay`, `newer_reading`, `hide`, `recomputed`).
- **reading order**: which of two readings of a record is newer.
- **equivalence key**: the per-type key that recognizes the same thing seen again in another material.
- **clocks**: `ts_orig` (when it happened, or unknown), `ts_coided` (when Sinex read it, from the UUIDv7 ID), `ts_persisted` (when it was committed).
- **derived event**: an event computed by a derivation, naming its parents.
- **fusion**: joining what several sources observed of one thing into one **fused record**: `command.fused`, `focus.fused`, `visit.fused`, `play.fused`, `clipboard.fused`, `notification.fused`, `mail.fused`. Each fused kind has one **fusion rule**; a **fusion input** (`FusionInput`) reads one source's events for it as **fusion records** (`FusionRecord`) (an identity, a start, maybe an end, the fields it supplies). A **fused command** is a `command.fused` event.
- **shell session**: the `shell.session` derived event: a run of commands in one terminal; `recall last` reads around the latest one.
- **batch**: what a producer hands the writer: records with their events or failures, and progress.
- **run**: one execution of a producer; every event names its run.

## Storing and reading

- **admission**: the one function that stores events and source progress together. **Stage** names only its staging table.
- **writer**: the daemon task that runs admission, one transaction at a time.
- **commit log**: committed events in commit order; each entry has a **commit position**.
- **visibility change**: a reading leaving or rejoining the live set (archived, hidden, unhidden), numbered by its **change position** in `audit.archive_changes`.
- **derivation**: keyed (recomputes a **dirty key** when its inputs change; a **computed key** is one key's new output set) or per-event.
- **kind** (of what a key reads): events of one type, optionally only those whose payload fields have given values.
- **metric**: a declared count over events (events, seconds of their intervals, or a ratio), with its **dimensions** (payload fields it is broken down by) and the sources that observe it. A **tracked metric** is one the brief compares with its recent level.
- **figure**: a metric's number over one civil hour or logical day, total or for one **dimension value**, as a `metric.figure` event; a **health figure** (`health.figure`) is the same for one series of health points.
- **coverage**: the stretches in which a source could have recorded what it records, computed from its runs and records when asked for.
- **panel**: logical days by metrics, each **cell** observed, partial, recorded or missing with its reason, from the daily figures and coverage.
- **finding**: a typed statement in a brief or a recipe's answer, with its caveats and the figures it cites.
- **recipe**: a shipped analysis (correlations, trends, deviations) run over a panel when asked for.
- **derivation runner**, **binding runner**: the daemon tasks that run derivations and keep configured bindings read.
- **comparison**: what a recompute with `--compare` would change, kept in `derivation.comparisons` without writing outputs.
- **replay**: reading stored bytes again with the current parsers, one part sequence per transaction.
- **text list**: a field declared as an array of strings; a comparison matches a member, and inequality excludes every matching member.
- **query table**: what a query reads (`events`, `archive`, `materials`, `runs`, `sources`, `operations`, `failures`, `gaps`, `drift`), each with its fields.
- **reference**: how a command names one event: an event ID or `sinex:ev/<id>`, `sinex:record/<material>/<offset>[/<event type>[/<output index>]]`, `sinex:key/<source>/<event type>/<equivalence key>`, or a time citation.
- **moment mark**: an operator-authored moment, note and client-observed context (`operator.mark`); `recall mark` reads around it.
- **saved query**: a named query definition (`query.saved`) with declared scalar parameters, invoked as `query:NAME` or `query:NAME@AUTHOR`.
- **parameter**: a named scalar value supplied when running a saved query; never a field or a clause.
- **filled bucket**: a time group emitted even when it has no records, with its observation coverage.
- **time citation**: `sinex:at/TIME/TYPE`, the live spanning output containing an absolute instant; `sinex:window/FROM/TO` names a recall window.
- **operation**: something the operator or an agent did to Sinex, attributed and logged.
- **purpose**: what a source's events are for (`command fusion and shell recall`).

## Producers and transport

- **producer**: anything that hands Sinex events: a binding, a derivation, an outside process.
- **outside producer**: `sinex-session`, `sinex-capture` or another process sending over NATS.
- **integration socket**: the session socket the editor and terminal integrations write to (`session.integrations`).
- **ingress**: the NATS subjects outside producers send frames to, under one **subject root** (`sinex`).
- **frame**: one message from an outside producer: events, a material, a chunk, or a **large frame** that names its own bytes.
- **frame bytes**: the bytes an outside producer hands over for one frame, in that frame's directory under the content store (`incoming/producers`); removed once the frame is recorded.
- **unacknowledged frames**: a producer's frames sent but not yet acknowledged, kept with the position after them and sent again after a restart.
- **frame refusal** (`FrameRefusal`): a content failure stored on a material, a byte failure that rebuilds the transfer, a malformed frame reported as a binding error, or a configuration change requiring reconnect.
- **capture failure**: a spool range whose bytes could not be delivered after three identical byte refusals; the position records its range and reason and cleanup keeps its spool file.
- **producer declaration** (`ProducerHello`): the protocol version, maximum class and event types checked when an outside producer connects.
- **acknowledgment**: the reply that confirms a frame was admitted; a **recorded query** asks Sinex for the acknowledgments of given frames.
- **publish**: send a NATS message.
- **confirmed stream**: the NATS stream of committed events in commit order, with `Archived`, `Unhidden` and `Excised` messages.
- **withheld**: a stream message or view row whose payload is left out, for its class or its size.
- **capture stop**: a live capture's lifetime (`CaptureStop`): it interrupts waits, shuts sockets and stops child processes when the capture stops.

## Removal and privacy

- **removed at source**: the source no longer has a record; stored as a newer reading in state removed.
- **hide / unhide**: take an event out of views, counts and re-imports, and put it back; hidden records are kept in `core.hidden_records`.
- **excision**: the permanent deletion of one record's events and the material holding it, when nothing else shares that material, keeping only the material's digest so its bytes are refused if registered again; only an **excisable** source, one whose every stored copy of a record is a reading of it, allows it.
- **class**: a privacy class: `open`, `guarded` or `sealed`.
- **sealed**: encrypted at rest under a per-material **data key**, wrapped for the operator's **class key**; equivalence keys and record hashes become keyed hashes under a **hash key** derived from the data key; reading needs an **unlock**.
- **rule**: a privacy rule applied at admission; **masking** hides credentials on output.

## Words not used

| Not used | Say instead |
|---|---|
| retained, retain, retention | stored, kept |
| custody, reservation, pending (for bytes) | unadmitted bytes |
| publication (for bytes) | write, install |
| frozen, freeze | fixed, saved, or name the thing itself |
| canonical | fused (records), sorted (JSON) |
| basis | name the thing itself |
| candidate | selected |
| authority, lease | access, grant |
| legacy, predecessor, old formats | nothing: internal formats have one version until the first deployment |
| control, proof, witness (in tests) | test, check |
| backing | storage |
| interpretation | parsing, reading |
| retire, retirement, closure, qualify | say what happens: delete, drop, stop |
| kind (of input) | format |
| notice | visibility change, message |
| restore, restored (of visibility) | unhide, unhidden |
| chain (of parts) | part sequence |
| occurrence | record |
| natural key | equivalence key |
| vanished (at source) | removed |
| material key | data key |
| durable bytes | length |
| ordinal | output index |
| unit (of a query) | table |
| shell group | shell session |
| standing question, analysis result, product | a recipe's answer, a finding |
| summary (activity) | figure |
| envelope (of an event) | metadata |
| consumer (of a source) | purpose |
| stage (outside admission) | plan, temporary file, write |
| physical | name the thing: record ranges, archive pass |

## Reader implementation names

- **time settings** (`TimeSettings`): the time zone and local start of a logical day, also used to group local hours.
- **binding reader** (`BindingReader`): a configured binding together with its source, reader and remembered progress.
- **reader context** (`ReaderContext`): the open content store, read connection and writer used by readers and commands.
- **shared file read** (`SharedFileRead`): compatible bindings grouped for one read of their stored file.
- **shared read start** (`SharedReadStart`): whether a binding has finished a file or needs to read it from a stored file or the input.
- **stored file** (`StoredFile`): the blob, length and material named by a shared reader's cursor.
- **interruptible reader** (`InterruptibleReader`): a reader that checks for a stop request before reading.
- **validated reader** (`ValidatedReader`): a reader that runs the supplied validation before reading.
- **intake** (`Intake`): the writer, source, settings and access used to submit an acquisition.
- **submission** (`Submission`): the admission outcome and material of a submitted request.
- **authored at** (`authored_at`): the author timestamp supplied by a Git commit, distinct from its commit timestamp.
- **request record**: a forecast or library request submitted as a material and admitted through the writer.
