# Architecture

How the crates divide the work today. [vocabulary.md](vocabulary.md)
defines the terms; planned work is in Beads.

## Reading sources

`sinex-source-core` holds the parser traits, drain, shared drivers and record
framing. `sinex-sources` holds source declarations, parsers and the live loop.
Its `full` feature selects archive, database, repository and network source
families; capture-only nodes leave it off. A driver gets bytes from an input; a parser reads one
framed record and opens no files, sockets or clocks. The drain around the
parser attaches record locations, hashes, carry and record failures to each
batch. Anything specific to one provider lives here, not in the store or the
daemon.

`sinexd` runs the sources: `backfill` reads them to the end, and the daemon's
binding runner reads them continuously. Both hand batches to the writer. A cursor
records admitted progress; bytes being stored does not mean their records were
read.

`sinexd/src/intake.rs` is the one acquisition submission path. It stores the
material through `drivers::prepare`, drains it, applies the effective class,
seals when required, and sends each batch through the writer with the caller's
cancellation and unlock checks. Reader polls, material storage and parsing run
as owned blocking work, with at most four workers shared across bindings and
requests. Bulk work uses at most three, leaving capacity for live requests. Each
submission takes a permit before advancing its parser: two permits cover the
committing batch and one parsed batch ahead. The next acquisition waits for
every commit answer. A failed answer closes the batch channel and joins the
parser before returning; provisional cursor and carry never advance stored
progress. Dropping a submission also cancels its queued admission check. An API
request uses one batch and refuses every record if any record fails to parse.
The caller supplies priority and sealing access. Resuming a registered material
uses its stored bytes and never registers its data key again.

Library roots are planned by their format; crawl commands supply their URL
list as binding settings. Both are read by the binding runner. A library file selection
is a scan of that file within its root; files beside it are neither read nor
reported removed. `library-add` then turns the copies into item and rendition
requests, preserving item identity across moves and revisions. This item
creation still runs in the library command adapter.

Crawl reads URLs from the `visit.fused` derivation, or from the command's
explicit URLs. Its format owns domain filtering and the caller's optional
limit. The journald service uses `sinex-session capture journald`, which builds
capture from the format registry and stops it through the shared capture lifetime.

### Stored bytes and admission

`StoredBytes` has three forms: an unnamed file of framed records, shared
in-memory records, or a stored blob. A stored blob can supply its decoded
record reader when reopened; an archive instead supplies decoded records
separately. Storing and draining use those same bytes on retries. Plain seekable inputs use
`put_seekable`: repeated content is verified without recompression. Each completed
input pass records its name before installation, including the compression pass
when a file changes after its first hash pass.

A driver's bytes are written to the content store before the batch that cites
them is admitted. Admission inserts the material and its `core.blobs` row in
the same transaction as its first events and the cursor. Only excision deletes
bytes: it marks a blob `deleting` once nothing cites it, and the writer deletes
the file under that row's lock, then the row. Deletion and admission both run
in the writer, and the writer checks that every file a batch cites is present
just before admitting it, so a batch never cites a file that is gone. A
process that stops between writing bytes and admitting them leaves a file with
no row. Before installing a blob, the content store syncs its name in
`unadmitted-blobs/`. The daemon checks those names against the database at
start, holding the writer, before any source reads. After draining the writer
at shutdown it removes the names that have rows. The first start of an existing
store checks every file once, then syncs `unadmitted-blobs-ready` so later starts
check only the recorded names.

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
more of the part sequence. The append reader keeps the tail of the current
revision's records, to align a rewrite. It frames stored parts in chunks and
keeps only the tail's range metadata, including one oversized record whole.

The SQLite-row reader of a table updated in place keeps each row's newest
hash and the directory reader each file's size, modification time and
content hash. Both save this reader
memory with the cursor at the end of a part: at their first part, once the
entries stored since the last save reach a quarter of what they remember, and
at least every 1,000 parts. `raw.reader_memory` keeps only the latest per
binding, replaced in the cursor's transaction. A restart stops the walk at the
part the memory was saved at and takes the rest from the memory, so it reads
at most 1,000 parts, and an unchanged row or file is not stored again. A directory reader that records removals also checks that every remembered
file has its stored record location. Incomplete saved memory is reported as
unavailable and needs rebuilding from stored material history before polling.

A binding is either polled alone through `Reader` or read in a pass over a
stored file shared with every compatible binding of that file (`SharedPass`,
export archives). These are the two reader interfaces. A polled driver
implements `Reader` directly, including drivers with no reader memory. Capture
is a separate operation that fills a spool, not an alternate reading path.
A shared-pass binding keeps no memory: its cursor names the
stored copy and the next record, so a restart reads on from there, and a
cursor that already covers the unchanged file reads nothing.

A sealed part keeps the binding sealed after its class is lowered. The daemon
does not know which driver a binding uses.

### Drivers

- **SQLite rows.** One read transaction covers the schema check and the row
  query, so SQLite's own locking gives a committed view. A locked database is
  unavailable for that poll. For a table whose rows are updated in place, the
  first poll after a start reads every row and stores only those whose hash
  changed. An appended table (rows only added, never changed: the machine
  telemetry collector's) is read after its cursor only, at most 50,000 rows
  per part, the next poll continuing; its reader remembers nothing but the
  cursor, so a table of hundreds of millions of rows costs no memory and a
  restart reads no history.
- **IMAP.** An answer records the UIDs it holds and the server's UID window. A
  cursor inside an answer resumes at its first UID, so a restart fetches the
  whole answer again and admission drops the repeats. The reader remembers
  where each UID it read is stored, asks the server which it still holds, and
  records an expunged message as removed at the source; it saves that memory
  like the SQLite-row and directory readers.
- **Directory snapshots.** Changed files stream into one unnamed bundle. Each
  file is preceded by one byte outside its source range, so empty files have
  distinct anchors. The cursor confirms the reader's file facts only at the
  bundle's last byte. Paths are keyed and reopened as native bytes, including
  in saved reader memory. Manifest paths keep valid UTF-8 names verbatim. Non-UTF-8 names add
  `native_path_bytes` with base64-encoded native bytes; reopening uses that field
  when present. A file that cannot be read contributes a failure record when
  its failure appears or changes and is retried on the next poll. A file that
  changes during capture is skipped and retried. Walk failures suppress removals
  only within the failed path; a walk cut by the part size records no removals.
  Maildir and notes also remember where each file was stored and record the
  removed file's reading as removed at the source.
- **Export archives.** An archive is stored once and read in one forward pass
  for every compatible binding (same host and class); each binding keeps its
  own record parts and cursor. A sealed archive uses one key per binding.
  After a restart the stored archive is read again from each cursor; the
  rest of a part only partly admitted becomes a new part.

### Typed payloads

Notes keep the complete text and the file time, and add a title, a native
note ID, sorted tags and wikilinks. A note with an ID is identified by its
vault and ID, so a rename is a newer reading of the same note; one without
by its vault and path. The capturing host and vault root scope this identity;
the folder name is for display. Notes and Maildir records include their path
in the record hash so a move updates the live reading even when bytes stay
equal. Directory removals cite stored locations and preserve sealed history
without opening parser carry. The YAML front matter
is parsed into JSON values or reported as absent, invalid or unsupported, with
only an error kind and position; the text is kept either way.

Steering rows and health records are decoded into a type chosen by their
table or category before their fields are read. A malformed known field is a
record failure with its bytes kept. Fields the type does not know are kept,
and the event lists their JSON pointers in `unknown_fields`; a category with
no type at all is marked `untyped: true`.

Sinnix lake history for clipboard, accessibility, notifications, players,
idle inhibitors, screen, audio and netflow stays `lake.observed`. The payload
and capture coordinates remain available to the stream derivations. Display,
peripheral, air, navigation and reading-stack lanes produce domain events.
Samsung calls and reminders become `call.logged` and `task.revised`;
other account tables outside health stay generic `account.row` rows.

The machine records (activations, backup checks and drills, audits, the usage
census, configuration drift, experiment runs) and the router's capture lake
lanes have one type per file or lane; fields the type does not know are kept
by name in the payload's `extra`. A router sample is one record that yields
an event per log line, lease, traffic row or Wi-Fi client.

### Code and observed tools

`code.git` reads a repository's commits with the files each changed
against its first parent, and compares its refs between polls: a ref
moved to a commit that does not descend from its old target is a rewrite,
and the update lists the commits no ref or checkout reaches any more. They
stay recorded. `code.github` reads the GitHub repository of the same
checkout with a token from a file; `work.beads` reads a Beads store's JSONL
export and `work.agentctl` AgentCTL's event log. The Beads cursor keeps
issue IDs and hashes, so deletions survive retries and restarts without
putting issue text in the cursor. GitHub lists, checks, reviews and labels
are read to their last page; malformed lists and repeated page cursors fail
the poll. Sinex never writes to
these tools. The fields that join them are recorded as they are read: a
commit's refs and checkouts, a pull request's head branch and commit, a
job's checkout and its HEAD at start and end.

Freedom's daily domain-seconds export yields one `domain.usage` per domain
and day. A civil date without its zone stays a date in the payload, with no
event instant.

### Outside producers

`sinex-session`, `sinex-capture` and Polylogue send frames over the NATS
ingress. The bytes a frame needs go in that frame's directory under the
content store root (`sinex_transport::frame_bytes`): a producer on the same host
writes them there, and one elsewhere sends them in chunk frames. Sinex copies
the bytes, checks them against their name, stores them, admits the frame and
records its acknowledgment in one transaction, then removes the frame
directory. A frame sent again is answered from that record. A producer keeps a
batch's frames and the position after them until every frame is acknowledged,
and sends them again after a restart. In a store root with mode 2770, frame
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
one record whose newer reading replaces the older. Notes, settings, kept
results, deliveries and excisions arrive as small request materials with one
event each. Polylogue registers its own payload schemas and rules before sending events.
Its `polylogue` namespace belongs to the configured producer. Sinex ships the
other bundled declarations from Rust. Each event selects an immutable kind
version and records whether its validator ran. Bundled kinds keep Rust decoding
and serialization; outside kinds keep the submitted JSON and use their saved
schema, when present. A declaration's least class applies whatever class the
producer asks for. Sinex has no AI-chat parser of its own.

The outside producers start their capture threads with `sinex-sources::live`,
which registers signal handlers first and stops every capture before joining
it. `capture::CaptureStop` owns interruptible waits, socket shutdown and child
process groups: it interrupts a group, escalates after 250 ms, and reaps its
children, including descendants adopted through Linux subreaper support.

## Kind declarations

`registry.event_types` stores exact versions, schemas, identity and time rules,
class minima and the `index_text` flag. Registering an identical definition is
idempotent; changing an existing version is refused. Outside producers register
only their own namespace, or emit an exact version already registered by its
owner. An unknown version of a known name is refused. A new permitted name may
be admitted without a schema and is labeled `unvalidated`.

Schema apply reconciles bundled declarations and views even on a current
schema, under the writer lock. The numbered upgrade gives existing events
version 1 and `unvalidated` through constant column defaults, preserving their
payloads and compressed chunks. Bundled declarations begin at version 2.
`sinexd admin schema --kind-owner polylogue=PRODUCER` records a different
Polylogue producer identity before it connects; changing a saved owner is refused.

`kinds` and `fields` expose the saved declarations with pagination through the
CLI and MCP. Ordinary payload field types come from one declaration snapshot;
continuations keep its exact hashes. Alias fields keep their declared types
and computations. Missing or incompatible scalar values read as NULL.

A local replay pins the executable bytes and bundled declaration hashes. A
resume with a different build or declaration is refused before archiving.
Materials needing an outside parser remain live and are reported as
`producer_required`; unavailable local parsers are reported per material.
Outside producers use `ReplayScopeQuery` then submit a complete `ReplaySequence`.
The writer compares its live-reading hash, archives the selected readings and
admits all replacements and the acknowledgment in one transaction. A later
invalid batch rolls the entire sequence back.
The operation keeps its authorised scope after a refused replacement; a
different parse or build needs a new operation.

`sinex-source-core::gaps` supplies the shared `capture.gap` payload, material and
parser; its caller supplies the stream's class floor. The shared sampling rule
selects continuous sample events by a versioned hash of capture UUID and
sequence. Discrete changes, gaps and records with typed outputs are always
selected. Callers parse complete raw records and update carry before selection.

## Storing and deriving

`sinex-store` owns PostgreSQL access, the content store and admission. The
daemon's writer schedules the transactions; admission stores events and source
progress together and marks the derivation keys they affect. No other code
inserts events.

`sinex-derivations` holds computations over supplied events. The daemon's
derivation runner reads their inputs and submits each key's new outputs
through admission. Derived events are the one store for derived results:
nothing a derivation or an analysis computes is kept anywhere else.

A key reads events of some kinds in a time window: a kind is an event type,
optionally only the events whose payload fields have given values (state
intervals of one kind, journal entries of one priority), so a key never
reads a whole type to use a sliver of it.

### Fusion

Several sources often see the same thing: Atuin and zsh the same command,
the compositor and ActivityWatch the same focus, a browser's history and its
Takeout export the same visit, Sinex's live capture and the Sinnix capture
lake the same clipboard change, notification or track, IMAP and an mbox the
same mail, a Reddit export and a public archive the same comment, a Messenger
export and a saved Messenger database the same message. One mechanism joins
them (`sinex-derivations/src/fusion.rs`). Each fused kind is a rule in
`fusion/rules.rs`: its inputs in preference order, each reading one
source's events as fusion records of something with an identity, a start and
perhaps an end, and the windows within which fusion records match. Fusion records of
one identity pair closest first within a short window, then within a longer
one when the pair is unambiguous. For kinds whose fusion records are spans or
repeat (focus, plays), a group whose start falls inside a group led by a more
preferred source joins it: ActivityWatch's title changes inside a focus the
compositor reported, a player's reports of a track a streaming export holds.
The fused event (`command.fused`, `focus.fused`, `visit.fused`, `play.fused`,
`clipboard.fused`, `notification.fused`, `mail.fused`, `post.fused`,
`conversation.fused`) takes its times from its most
preferred member and each field from the most preferred member that has it,
names every member as a parent and lists the sources in `observed_by`. A
record no other source saw is fused alone, so a fused kind counts each thing
once whichever sources were running. Metrics and aliases read the fused
kinds.

### Calendar instances

`calendar.instances` (`sinex-derivations/src/calendar.rs`) expands each
calendar series into its instances. Its key is one series, and it reads every
reading of it: the event with its rule and the changed instances. The rule is
expanded in local time in the series' zone, `EXDATE` starts are left out,
`RDATE` starts added, and a changed instance replaces the one whose start it
names. A rule without `COUNT` or `UNTIL` needs a requested expansion range;
until one is supplied, its recorded instance and explicit additions are
kept with `rule_not_expanded` explaining the missing range. Each
`calendar.instance` names the readings it came from.

### Metric figures

A metric (`sinex-derivations/src/figures.rs`) declares the kinds it counts,
how it counts them (events, seconds of their intervals, or a ratio of two
counts), the fields it breaks down by, and the sources whose coverage says a
day was observed. `metric.hourly` writes one `metric.figure` per civil hour
and breakdown value from a logical day's inputs; `metric.daily` adds a day's
hourly figures up, never reading the inputs. Each figure names what it
counted as parents.

`sinex-analysis` reads those figures, never the events under them. A panel
reads the daily figures through the query language (`figures`) with the
caller's grants and the coverage of each metric's sources at the same commit
position, from the store's runs and records; a day is observed, partly
observed, recorded with unknown coverage, or missing with its reason, so a
zero is only ever a measured zero. Methods (correlations, trends, change
points, deviations) and the brief run over panels when asked for. Their
answers are not stored: they depend on coverage, which is not an event, and
asking again at the same commit position gives the same answer.

The commit log orders committed events and visibility changes (hide, unhide,
replacement). Hide and unhide change what is live and append to the visibility
history in one transaction. Readers move past positions an excision left
empty. Time lookups use stored events and the visibility history, so pruning
the commit log loses nothing. The publisher sends committed events on the
confirmed stream, with `Archived`, `Unhidden` and `Excised` messages.

On shutdown the derivation runner finishes its current pass and releases its
writer handle; the writer drains queued work once every producer has released
its handle. The supervisor gives tasks ten seconds before stopping them.

`sinex-schema` declares the current schema in Rust. `sinex-schema apply`
creates it in an empty database, or upgrades a deployed database through the
numbered forward upgrades in `upgrades.rs`, in one transaction and only while
no writer holds the database; a newer version is refused, and no other command
creates or changes the schema. Each upgrade keeps every row, and the
declarations carry the same change, so a fresh database matches an upgraded
one. Checked SQL in `sinex-store` compiles against that schema.

### Volume

Every source makes one event per record it reads: a Fit point, a journal
entry, a network flow, a shell command. No parser combines records to keep
the count down; coarser figures are derivations, so the finest grain stays
queryable and the coarse views can be recomputed from it.

Admission is sized for that. A batch of up to 10,000 events is staged with one
`COPY` and classified and written by a fixed number of set-based statements,
so its cost grows with rows, not round trips. Equivalence keys are hashes,
so the live side tables are written in key order; even so, once the key index
outgrows PostgreSQL's buffers, a batch touches most of its pages, and that
and the event indexes are where admission spends its time (`xtask bench
synthetic` reports it by statement).

Health figures show the shape for a dense series. `health.hourly` reads one
civil hour of Google Fit points and writes one `health.figure` per data
source (count, sum, minimum and maximum per value), naming the points as
parents. `health.daily` reads a logical day's hourly figures, never the points;
the logical day is the operator's time settings (`TimeSettings`) (Europe/Warsaw, from 04:00),
the same one the metric figures use.

The derivation runner reads a pass's keys a page at a time: adjacent keys
share one paged query, and a key is computed as soon as the pages pass its
end, so the runner holds a page and the keys still open, not every key's
inputs. A key's own inputs are held whole, so a derivation over a dense series
keys by hour.

## Interfaces

`sinex-model` holds identities and payloads. `sinex-protocol` declares the
public commands and wire types without IO. `sinex-transport` holds the
private-mode switch and the frame directories, so the daemon, drivers and CLI
share them without the wire types touching the filesystem. CLI, MCP and HTTP
are generated from the command table. `sinex-diagnostics` is a database-free client of `sinexd admin diagnostics`.
Offline commands invoke the sibling daemon executable without starting the
daemon. The database work lives in the daemon; the CLI has no store or SQLx
dependency.

Forecast issues and resolutions are ordinary `forecast.request` records. Both
commands use `sinexd/src/intake.rs`, as library requests do: one
material, stored and then admitted in one batch. The parser takes the caller's
role and the submission time from the material's manifest. The report reads
the live issues, resolutions and evidence in one transaction and scores them
with `sinex-analysis`; nothing about forecasts is cached.

Query aliases read only kinds something produces: `commands`, `focus`,
`visits` and `plays` the fused kinds, `figures` the metric figures,
`intervals` and `now` the state intervals.

The `shell-sessions` query alias reads the `shell.session` derivation; `start`
and `observed_until` cover the recorded commands, including a last command of
unknown duration. `recall last` picks the latest matching session with the
ordinary query compiler and reads every source in its span, at one commit
position. When no session matches, it says no session is known, not that
nothing was recorded.

A query continuation is bound to its question, the caller's grants, the
resolved clock and time settings, and the commit position. It also carries the
answer's size, counted on the first page, and a hash of the rows already
handed out; when excision or a metadata change alters those rows, the
continuation is refused and the query must be asked again. A page reads the
numbered answer only up to its own last row, so it costs the rows before it,
never the whole answer.

Every output goes through credential masking. A JSON key that looks like a
credential refuses the whole reply, since renaming the key could change the
object's meaning. Follow streams check a page before sending its first row.

Shared blob storage, credential masking and the development tooling come from
released `polylogue-sinex-common` versions.

## Sealing and excision

`sinex-sealing` owns age and payload cryptography. The daemon's `sealing`
module handles material keys, stored reads and batch protection through one
material-reading interface. The store holds only the wrapped-key queries.

A sealed source encrypts each material under its own data key, wrapped for the
sealed class key; its events' payloads are encrypted under the same key, and
their equivalence keys and record hashes are keyed hashes. Capture needs only
the class key's public half. Reading sealed payloads, replaying sealed
materials and continuing a sealed archive need an operator unlock.

Excision (`excise`) is a rare operator action. It removes one record (or one
material the operator names): every reading of it, every event derived from
it, and its bytes. It works on whole materials: a record is excised only when
every material it was read from holds nothing else, and each such material
goes whole: its row, its failures, the producer frames that registered it,
and its blob once nothing else cites it; an input left with no material goes
too. Only the material's ID and digest stay (`core.excised_materials`), so
the same bytes are refused if they are registered again. A later part, or a
binding's cursor, that named a removed material names the part before it; a
material a later part continues inside is refused. A tombstone (identity and time) stays for each event. The request is
recorded once in
`core.operations`, planned, and applied by the writer in one transaction
after a fresh plan matches; running it again finds only what is left. Nothing
that is not the record changes: a material other records share is never
touched, and equal bytes elsewhere are other records and stay.

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

## Host discovery

`services.sinex.enable` starts discovery. Each input format declares its owner,
canonical locations, capture streams, capabilities and credentials. Runtime
checks use the operator's declared home/XDG roots, input roots, provider inputs
and filesystem layout. Nix evaluation serializes these facts without probing
files. Declared backfill inputs are also ordinary discovered inputs.

A nullable `exclude.<source>` entry applies to every format and binding of that
source. An exclusion requires a reason; status includes the complete exclusion
map even when no source has run. Derivation options continue to control derived
outputs. Source paths, classes, options and time zones remain configurable.

The binding runner reconciles inputs every 60 seconds. It compares aliases only
within the same source, parser, host and distinguishing suffix, and reuses saved
binding names and input spellings. Conflicting saved identities are reported;
cursors stay attached to their original names. A missing input waits, an empty
readable input has zero bindings or records, and an unreadable input reports its
error independently of the other inputs.

`sinex-session run` selects every available session stream by default, using the
same exclusions and source settings. Its state directory holds the existing
spool names and positions. Its default remains the operator's
`~/.local/state/sinex-session` input spelling. The Nix unit creates it with mode
0700 and uses umask 0077. Sinnix supplies `/realm` storage during its coordinated
state relocation; see [session-state.md](session-state.md) for the drain-then-switch procedure.
Status reports session captures as `producer_reported` until their producer supplies
status frames. The daemon does not infer their availability from its own environment.
`sinexd admin discover --sources /etc/sinex/sources.json --host HOST`
prints the read-only input plan used by the daemon and permission preparation.
