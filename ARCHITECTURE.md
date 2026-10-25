# Architecture

This describes the current Rust ownership boundaries. Planned work is in Beads.

## Capture and interpretation

`sinex-sources` owns source declarations, shared acquisition drivers, framing
and parsers. Drivers obtain bytes; parsers interpret one framed record without
opening files, networks or clocks. The drain attaches record locations, hashes,
carried state and record failures to the resulting batch. Provider-specific
interpretation belongs here, not in the database or daemon.

Notes vaults use the directory driver and stored member context. Parser version
3 keeps the complete UTF-8 text, file mtime and `(vault, path)` identity while
extracting a title, native string ID, sorted tags and wikilinks. Title priority is
a nonempty header title, the last segment of a string ID, then the file stem.
Header tag arrays contribute only string values; body tags exclude heading lines.
Links strip aliases and heading anchors. These body references are lexical
extractions, not a Markdown rendering.

`front_matter` distinguishes absent, parsed JSON-compatible YAML and invalid or
unsupported interpretation. Empty YAML remains parsed null; an empty mapping
remains an empty mapping. Errors carry only a kind and available line/column,
never copied YAML snippets. A closed invalid header leaves known body references
available; its header tags remain unknown. An unclosed header has no established
body boundary. Old payloads and vanished-file markers carry no interpreted
metadata. Ordinary replay of the stored material supplies the newer interpretation.

YAML syntax, duplicate detection and bounded alias handling use serde-saphyr.
A private JSON conversion refuses non-string mapping keys, unsupported tags and
plain integers outside the parser's signed/unsigned 64-bit representation,
including unsupported plain leading-zero forms. Quoted or explicitly string-tagged
numeric IDs remain strings. The header has a 1 MiB byte/scalar budget, structural
limits and bounded alias replay. Limit/refusal outcomes keep the exact note
text. Parsers do not resolve external includes or open files.

Steering commitments, activities and reviews use closed typed row variants behind their existing
`steering.record` wire shape and `(table, record_id)` identity. The table selects
the row type before decoding. Missing fields, explicit nulls and numeric values
keep their representations; unknown columns or types become record failures
with their bytes kept. Parser version 2 uses the normal replay path to
reinterpret stored rows; it does not rewrite earlier event payloads.

Sinnix lake observations keep the `capture.observed` envelope and material-location
identity. The lane and, where declared, payload kind select concrete source fields
before decoding. Unknown lanes/kinds remain explicit, while unknown extension fields
and open notification hints keep their values with computed interpretation markers.
Known-field errors become record failures with raw bytes kept. Awair interprets
only the producer's declared score and CO2 fields; other sensor fields remain extensions.
Netflow stays material-only. Parser version 2 uses ordinary replay.

Non-health Samsung account tables keep normalized columns as generic
`account.record` fallbacks with computed `uninterpreted: true`. Table names and
synthetic normalization examples do not establish provider field schemas. A known-table
interpreter requires those declarations; the generic row does not claim one. Existing
payloads without the marker remain readable. Samsung parser version 4 uses normal replay.

`sinexd` connects these pieces: source tasks, backfill and replay submit work
to the writer. A source cursor records admitted progress; storing bytes does
not mean their records were read.

On shutdown, the derivation runner completes its active pass or maintenance
step, then releases its writer handle. Idle waits observe the shared stop signal.
The writer drains queued work after its producers release their handles; the
supervisor keeps its ten-second fallback for tasks that cannot finish.

### Stored bytes and admission

A driver's bytes are written to the content store before the batch that
registers them is admitted; admission inserts the material and its
`core.blobs` row in the same transaction as its first events and the cursor.
Only excision deletes bytes. It marks a blob `deleting` once nothing refers to
it, and the writer deletes the file under that row's lock, then the row. The
writer checks that every file a batch registers is in the store just before
admitting it; deletion and admission both run in the writer, one after the
other, so a capture that stored the same bytes an excision is deleting is
refused rather than registered without its file.

A failed admission is retried from the bytes the reader still holds: append,
directory, SQLite-row and IMAP readers keep their newest material in memory
until its final cursor is admitted, then read on. After a restart nothing
unadmitted is kept: the reader reads on from the committed cursor, so a
material admitted only in part is completed by reading the source again.

### Restart

Each reader rebuilds what it remembers through the `Reader` trait: it names
the material its committed cursor points at, and the daemon offers it that
material and the parts before it, newest first, until the reader has enough.
The append reader keeps the tail of the current revision's admitted records,
for aligning a rewrite; the SQLite-row reader keeps each row's newest hash; the
directory reader keeps which files the admitted bundles hold and reads them
again on the next walk. A sealed part keeps the binding sealed after its class
is lowered. The daemon does not know which driver a binding uses.

### Drivers

SQLite row snapshots use one read transaction across schema inspection and row
selection, so SQLite supplies committed rollback-journal and WAL visibility. A
locked database is unavailable. The first poll after a start reads every row
and stores only those whose hash changed.

IMAP answers record the UIDs they hold and the server's UID window. A cursor
inside an answer resumes at the answer's first UID, so a restart fetches the
whole answer again and admission suppresses the repeats.

Directory snapshots stream changed files into an unnamed bundle. Each record
is preceded by one byte outside its source range, so empty files have distinct
anchors. The cursor confirms the reader's file facts only at the bundle's
final byte.

An export archive is stored once and read in one forward pass for every
compatible binding (same host and class); each binding keeps its own record
parts and cursor. Sealed archives use one key per binding.

### Outside producers

`sinex-session`, `sinex-capture` and Polylogue send frames over the NATS
ingress. The bytes a material or indirect frame needs go in that frame's spool
directory under the content store root (`sinex_transport::spool`), written by a
producer on the same host or sent in chunk frames by one elsewhere. Sinex
copies and checks them against their name, stores them, admits the frame,
records its acknowledgment in the same transaction, and then removes the
spool directory. A redelivered frame is answered from that record. A producer
keeps a batch's frames and the position after them until every frame is
acknowledged, and sends them again after a restart. In a store root with mode
2770, spool directories are 2770 and completed files 0640, so a producer
running as another user in the store's group can hand over bytes without
write access to the content store. Producer bytes are never stored sealed.

The outside producers use `sinex-sources::live::run` to start capture threads
and send their spools under one lifetime. Signal handlers are registered
before capture begins. Deadline, failure, interrupt and termination stop every
capture before joining their threads. `capture::Control` owns interruptible
waits, socket read shutdown and child process groups: it interrupts groups,
escalates after 250 ms and reaps their children, including adopted descendants
through Linux subreaper support.

## Persistence and derived work

`sinex-store` owns PostgreSQL access, content storage and admission. The daemon's
writer owns transaction scheduling; admission stores events and source progress
together and marks affected derivation keys dirty. Source and derivation code
do not insert event rows independently. Stored bytes are written before the
admission that refers to them (see above).

`sinex-derivations` contains computations over supplied events and configuration.
The daemon's derivation runner reads their inputs and submits replacement output
sets through admission. Analysis methods similarly live apart from their store
and application adapters. The commit reader and NATS publisher handle committed
events separately from source acquisition.

Writer positions cover both admitted Events and visibility changes. Hide and
unhide update the current archival projection and append their history in the
same transaction; unhide keeps the original Event. Commit-log markers carry no
Event. Readers advance across those markers and excision gaps using one captured
head. Historical time lookup uses stored events and visibility history, so
pruning the hot commit log does not erase its time basis. Permanent excision
still removes the exact event's historical metadata.

`sinex-schema` declares the schema in Rust at version 0. `sinex-schema apply`
(or `sinexd admin schema`) creates it in an empty database; a database at any
other version is refused, and ordinary commands never create the schema.
Ordered upgrades begin with the first deployment.

Archive-stream consumers receive `Restored` as well as `Archived` and `Excised`.


## Interfaces and shared code

`sinex-model` owns identities and payloads; `sinex-protocol` declares public
commands and wire types without IO. `sinex-transport` owns the private-mode
switch and the producer spool. The daemon,
source drivers and CLI use that small library without putting filesystem access
in wire definitions or making outside publishers depend on source parsers.
CLI, MCP and HTTP adapt the public commands. `sinex-diagnostics` supports
the separate daemon-down diagnostic path. Schema declarations and application
remain in `sinex-schema`; checked SQL compiles against that real schema.

Forecast issuance and resolution are ordinary `forecast.request` source records.
Their parser takes the caller's role and submission time from the stored
manifest, keeping the outcome time separately. Both commands write through
`sinexd/src/authored.rs`, as library requests do: one material, stored and then
admitted in one batch.
The report reads currently visible, live issues, resolutions and resolution evidence
in one repeatable-read transaction and supplies resolved probability pairs to pure
`sinex-analysis` scoring. It stores no separate forecast state or cached score.

Shared blob storage, credential masking and development tooling come from
immutable `polylogue-sinex-common` releases. Prefer these existing owners to
another local implementation. Change an interface with its callers and remove
the replaced path. Read [AGENTS.md](AGENTS.md) for test placement and focused
development commands; passing a component test does not verify a deployment.

The `shell-sessions` query alias reads the existing `shell.session` derivation;
its `start` and `observed_until` describe recorded command extent. Its observed
span includes both endpoints, including a final command with unknown duration.
It does not supply activity-session depth, project, open state or a proven end.
The broader `sessions` alias remains planned. `recall last` binds its optional
session-field filter against `shell-sessions`, selects the latest group through
the ordinary query compiler, and reads all sources in that observed envelope.
Selection and rows use the same commit position, including cursor continuations;
equal starts use the ordinary event-ID tie break. Withheld bounds refuse rather
than borrowing an older group. No matching group keeps the ordinary output
stages and carries a no-known-session account, not a no-observations claim.

Query continuations bind the actual question and caller visibility, the first
resolved clock, time-zone/day settings and event commit position. Count and
rows share one repeatable-read transaction. Each continuation compares a
fixed-size fingerprint of the visible ordered answer, including columns and
recall metadata. New events beyond the original position can leave that answer
unchanged; removals or mutable metadata changes require a fresh query when they
change it. Fingerprints detect ordinary changes and grant no authority. Every
page still uses current visible views and the final masking/unseal checks.
Changing page size is allowed; obsolete or malformed cursors are refused.

Output uses `credential-output`, including independently recognized
overlapping credentials. Credential-shaped JSON keys refuse the complete
reply because renaming could change an object's meaning. Follow streams check
the whole query page before emitting its first row; a refused page emits a
generic error without the key and leaves stored values and cursors unchanged.
Refusal reasons and follow-query errors pass through the same masker before
encoding; their typed refusal flags stay intact.

## Sealing and excision

A sealed source encrypts each material under its own data key, wrapped for the
sealed class key; its events carry their payloads encrypted under the same key,
and their equivalence keys and record hashes are keyed hashes. Capture needs only
the class key's public half. Reading sealed payloads, replaying sealed materials
and resuming a sealed archive need an operator unlock.

Excision (`excise`) is a rare operator action. It removes every reading of a
record with every derived event that descends from it, leaves a tombstone for
each, and blanks the record's bytes in a new blob; the old blob goes once nothing
refers to it. The request is recorded once in `core.operations` and applied by
the writer in one transaction while the excision holds its source's lock.
Running the same request again finds only what is left. Excision refuses sealed
material, a producer's records, and records inside a stored original archive.

A source read again can meet an excised record (a shell history still holds the
command). The excision's record keys and hashes are kept as the source's
exclusions; `sinexd/src/excision/exclusions.rs` blanks matching records before
the new bytes are stored, the way excision blanks stored ones. Admission
compares the excisions a read applied with the source's current ones, so a read
that overlapped an excision is read again.
