# Architecture

How the crates divide the work today. [docs/vocabulary.md](docs/vocabulary.md)
defines the terms; planned work is in Beads.

## Reading sources

`sinex-sources` holds the source declarations, the shared drivers, record
framing and the parsers. A driver gets bytes from an input; a parser reads one
framed record and opens no files, sockets or clocks. The drain around the
parser attaches record locations, hashes, carry and record failures to each
batch. Anything specific to one provider lives here, not in the store or the
daemon.

`sinexd` runs the sources: `backfill` reads them to the end, and the daemon's
source tasks read them continuously. Both hand batches to the writer. A cursor
records admitted progress; bytes being stored does not mean their records were
read.

### Stored bytes and admission

A driver's bytes are written to the content store before the batch that cites
them is admitted. Admission inserts the material and its `core.blobs` row in
the same transaction as its first events and the cursor. Only excision deletes
bytes: it marks a blob `deleting` once nothing cites it, and the writer deletes
the file under that row's lock, then the row. Deletion and admission both run
in the writer, and the writer checks that every file a batch cites is present
just before admitting it, so a batch never cites a file that is gone. A
process that stops between writing bytes and admitting them leaves a file with
no row; the daemon deletes such files when it starts, holding the writer, before
any source reads.

A failed admission is retried from the material the reader holds: the append,
directory, SQLite-row and IMAP readers keep their newest material in memory
until its final cursor is admitted, then read on. After a restart nothing
unadmitted is kept; the reader continues from the committed cursor, and a
material admitted only in part is completed by reading the source again. The
new part's manifest records how much of the part before it was admitted
(`previous_through`), and replay reads that part only that far.

### Restart

Each reader rebuilds what it remembers through the `Reader` trait: it names
the material its committed cursor points at, and the daemon offers it that
material and the parts before it, newest first, until the reader has enough.
The parts are read a page at a time, so a reader that stops early reads no
more of the chain.
The append reader keeps the tail of the current revision's records, to align a
rewrite; the SQLite-row reader keeps each row's newest hash; the directory
reader keeps which files the admitted bundles hold. A sealed part keeps the
binding sealed after its class is lowered. The daemon does not know which
driver a binding uses.

### Drivers

- **SQLite rows.** One read transaction covers the schema check and the row
  query, so SQLite's own locking gives a committed view. A locked database is
  unavailable for that poll. The first poll after a start reads every row and
  stores only those whose hash changed.
- **IMAP.** An answer records the UIDs it holds and the server's UID window. A
  cursor inside an answer resumes at its first UID, so a restart fetches the
  whole answer again and admission drops the repeats.
- **Directory snapshots.** Changed files stream into one unnamed bundle. Each
  file is preceded by one byte outside its source range, so empty files have
  distinct anchors. The cursor confirms the reader's file facts only at the
  bundle's last byte.
- **Export archives.** An archive is stored once and read in one forward pass
  for every compatible binding (same host and class); each binding keeps its
  own record parts and cursor. A sealed archive uses one key per binding.
  After a restart the stored archive is read again from each cursor; the
  rest of a part only partly admitted becomes a new part.

### Typed payloads

Notes keep the complete text, the file time and `(vault, path)` identity, and
add a title, a native note ID, sorted tags and wikilinks. The YAML front matter
is parsed into JSON values or reported as absent, invalid or unsupported, with
only an error kind and position; the text is kept either way.

Steering rows, Sinnix lake observations and health records are decoded into a
type chosen by their table, lane or category before their fields are read. A
malformed known field is a record failure with its bytes kept. Fields the type
does not know are kept, and the event lists their JSON pointers in
`unknown_fields`; a category with no type at all is marked `untyped: true`.
Samsung account tables other than health stay generic `account.record` rows,
always marked `untyped`.

### Outside producers

`sinex-session`, `sinex-capture` and Polylogue send frames over the NATS
ingress. The bytes a frame needs go in that frame's spool directory under the
content store root (`sinex_transport::spool`): a producer on the same host
writes them there, and one elsewhere sends them in chunk frames. Sinex copies
the bytes, checks them against their name, stores them, admits the frame and
records its acknowledgment in one transaction, then removes the spool
directory. A frame sent again is answered from that record. A producer keeps a
batch's frames and the position after them until every frame is acknowledged,
and sends them again after a restart. In a store root with mode 2770, spool
directories are 2770 and finished files 0640, so a producer running as another
user in the store's group can hand over bytes without write access to the
content store. Producer bytes are never stored sealed. A producer pages
through the materials it registered (`<root>.materials.<producer>`), so one
that keeps nothing of its own rebuilds from Sinex what it sent.

Polylogue is the producer for AI sessions. Each file an AI tool wrote is a
material of its real input (origin, path, host), a growing file a chain of
parts, named with the Polylogue parser that reads it. Polylogue parses it and
sends `polylogue.session`, `polylogue.message`, `polylogue.tool_call` and
`polylogue.turn` events anchored at their records and keyed by Polylogue's own
IDs, so the same message in two files, or read again by a changed parser, is
one occurrence whose newer reading replaces the older. Notes, settings, kept
results, deliveries and excision requests arrive as small record materials
with one event each. A type can declare the least class its events are stored
under (`class = guarded` on `polylogue.message`), whatever class the producer
asks for. Sinex has no AI-chat parser of its own.

The outside producers start their capture threads with `sinex-sources::live`,
which registers signal handlers first and stops every capture before joining
it. `capture::Control` owns interruptible waits, socket shutdown and child
process groups: it interrupts a group, escalates after 250 ms, and reaps its
children, including descendants adopted through Linux subreaper support.

## Storing and deriving

`sinex-store` owns PostgreSQL access, the content store and admission. The
daemon's writer schedules the transactions; admission stores events and source
progress together and marks the derivation keys they affect. No other code
inserts events.

`sinex-derivations` holds computations over supplied events. The daemon's
derivation runner reads their inputs and submits each key's new outputs
through admission. `sinex-analysis` holds the analysis methods as pure code;
the daemon reads their inputs and stores their results.

The commit log orders committed events and visibility changes (hide, unhide,
replacement). Hide and unhide change what is live and append to the visibility
history in one transaction. Readers move past positions an excision left
empty. Time lookups use stored events and the visibility history, so pruning
the commit log loses nothing. The publisher sends committed events on the
confirmed stream, with `Archived`, `Restored` and `Excised` notices.

On shutdown the derivation runner finishes its current pass and releases its
writer handle; the writer drains queued work once every producer has released
its handle. The supervisor gives tasks ten seconds before stopping them.

`sinex-schema` declares the schema in Rust at version 0. `sinex-schema apply`
creates it in an empty database; any other version is refused, and no other
command creates the schema. Numbered changes start with the first deployment.
Checked SQL in `sinex-store` compiles against that schema.

## Interfaces

`sinex-model` holds identities and payloads. `sinex-protocol` declares the
public commands and wire types without IO. `sinex-transport` holds the
private-mode switch and the producer spool, so the daemon, drivers and CLI
share them without the wire types touching the filesystem. CLI, MCP and HTTP
are generated from the command table. `sinex-diagnostics` works while the
daemon is down.

Forecast issues and resolutions are ordinary `forecast.request` records. Both
commands write through `sinexd/src/authored.rs`, as library requests do: one
material, stored and then admitted in one batch. The parser takes the caller's
role and the submission time from the material's manifest. The report reads
the live issues, resolutions and evidence in one transaction and scores them
with `sinex-analysis`; nothing about forecasts is cached.

The `shell-sessions` query alias reads the `shell.session` derivation; `start`
and `observed_until` cover the recorded commands, including a last command of
unknown duration. `recall last` picks the latest matching session with the
ordinary query compiler and reads every source in its span, at one commit
position. When no session matches, it says no session is known, not that
nothing was recorded.

A query continuation is bound to its question, the caller's grants, the
resolved clock and time settings, and the commit position. It also carries a
hash of the visible answer; when hiding, excision or a metadata change alters
that answer, the continuation is refused and the query must be asked again.

Every output goes through credential masking. A JSON key that looks like a
credential refuses the whole reply, since renaming the key could change the
object's meaning. Follow streams check a page before sending its first row.

Shared blob storage, credential masking and the development tooling come from
released `polylogue-sinex-common` versions.

## Sealing and excision

A sealed source encrypts each material under its own data key, wrapped for the
sealed class key; its events' payloads are encrypted under the same key, and
their equivalence keys and record hashes are keyed hashes. Capture needs only
the class key's public half. Reading sealed payloads, replaying sealed
materials and continuing a sealed archive need an operator unlock.

Excision (`excise`) is a rare operator action. It removes one record (or one
material the operator names): every reading of it, every event derived from
it, and its bytes. It works on whole materials: a record is excised only when
every material it was read from holds nothing else, and each such material's
bytes are replaced by spaces of the same length, so the part chain keeps its
offsets; the old blob goes once nothing cites it. A tombstone (identity and
time) stays for each event. The request is recorded once in
`core.operations`, planned, and applied by the writer in one transaction
after a fresh plan matches; running it again finds only what is left. Nothing
that is not the record changes: a material other records share is never
rewritten, and equal bytes elsewhere are other records and stay.

Excision refuses, before anything changes, what it cannot remove cleanly.
Only sources whose every stored copy of a record is one of its readings
declare themselves excisable (`SourceSpec::excisable`): the live streams with
records identified by their location (clipboard, file changes, Hyprland,
MPRIS, notifications, editor and terminal sessions), media capture and
forecasts. Every other source can read a record again or store another copy
of it: an append-only file is rewritten, SQLite rows and directories are read
again, an API cursor fetches again, an export archive holds the original
bytes of every record in it, and a request can repeat a record. The
operator's excision of their records is refused, as is excision of a record
that shares its material with other records, of sealed material, of a
derived event (excise what it was derived from), and of a material whose
bytes another part of the same revision shares.

An outside producer knows which of its materials hold what, so it excises
its own: a request on `<root>.excise.<producer>` names whole materials it
registered, and runs the same excision with the producer as the actor. A
material another producer or a binding registered is refused, and nothing
changes. An excised material is no longer listed on
`<root>.materials.<producer>`. The operator's excision refuses producers'
materials.
