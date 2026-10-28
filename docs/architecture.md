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
reported removed. File and directory selections inside a configured root use
that root's item identities. An unreadable entry records a scan failure and the
walk continues elsewhere; earlier facts under the unreadable path stay in
place until a successful retry. A scan records only paths whose size, modification time,
device, inode or stored-byte setting changed, plus paths found missing.
Unchanged polls write no material or copy events. One completion record closes
each changed scan after its last part; it produces no copy event. Unchanged
facts reuse the previous hashes and media metadata, including after restart.
Each copy has a key made from the host, resolved root and original relative
path. The captured material identifies each scan part; its part index orders
changes even when clocks repeat or move backwards. A move within the root
keeps that key when the old path is missing and the bytes match: a unique
device and inode match takes precedence, otherwise a unique equal-content
match is required in both directions. The scan compares all possible matches
before emitting its first part. Ambiguous copies list their move candidates,
create new items and leave existing curation separate. A new file at a moved
item's original path gets a key that also names its first scan material, so
two present files cannot take the same item key. Changed bytes at the same key create a
revision named by the content hash, including replacement by another inode.
Moving or renaming a whole root starts new items until an explicit root
relocation is recorded. Root relocation recording is not yet implemented.

`library.items` derives items and original renditions from visible scan
records. Item IDs are hashes of their keys; original rendition IDs also name
the content hash. Rendering a preview records `library.preview`, which the
same derivation turns into a rendition with both the rendering and its scan
evidence as parents. Hiding a scan invalidates its derived outputs; the runner
recomputes them from the latest visible change at each path. An item and its renditions
stay visible when another scan supplies their evidence. Revisions are content
hashes, so hiding an earlier scan cannot change their identities.
Revision history follows ordered copy records, so A to B to A keeps all
three transitions while reusing A's rendition. Library commands report actual
admission counts and wait for their requested keys through the same batched
computation engine as the background runner. A shared lock serializes keyed
computations before reading inputs, so an older read cannot overwrite a
command's newer outputs. Item keys with the same input range share one paged
query over all their values, rather than reading the library once per item. Command runs end on both success and failure.

The library scan schema step adds the scan identity fields and preview evidence type.
It upgrades databases without library records. A database containing library
records is refused before any schema or data changes: its request-created
IDs and external references need an explicit migration decision.

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

The SQLite-row reader keeps each row's newest hash and the directory reader
each file's size, modification time and content hash. Both save this reader
memory with the cursor at the end of a part: at their first part, once the
entries stored since the last save reach a quarter of what they remember, and
at least every 1,000 parts. `raw.reader_memory` keeps only the latest per
binding, replaced in the cursor's transaction. A restart stops the walk at the
part the memory was saved at and takes the rest from the memory, so it reads
at most 1,000 parts, and an unchanged row or file is not stored again.

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

- **Append-only files.** An unchanged stat tuple skips the file, unless the
  last look came within 50 ms of the file's last change, when an edit in the
  same timestamp tick would not show. Otherwise the bytes before the cursor
  are hashed in full using a streaming read:
  unchanged, only the appended records are read; changed, the file is read
  again as a new revision and aligned against the previous one, so a
  same-length edit in the middle is a rewrite, never an append.
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
Samsung account tables other than health stay generic `account.row` rows,
always marked `untyped`.

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
that keeps nothing of its own rebuilds from Sinex what it sent. The ingress
stream keeps a frame until it is admitted: it has no age limit, a full
stream refuses the next publish rather than dropping an older frame, and the
daemon's consumer redelivers without a cap.

Polylogue is the producer for AI sessions. Each file an AI tool wrote is a
material of its real input (origin, path, host), a growing file a chain of
parts, named with the Polylogue parser that reads it. Polylogue parses it and
sends `polylogue.session`, `polylogue.message`, `polylogue.tool_call` and
`polylogue.turn` events anchored at their records and keyed by Polylogue's own
IDs, so the same message in two files, or read again by a changed parser, is
one occurrence whose newer reading replaces the older. Notes, settings, kept
results, deliveries and excisions arrive as small request materials with one
event each. A type can declare the least class its events are stored
under (`class = guarded` on `polylogue.message`), whatever class the producer
asks for. Sinex has no AI-chat parser of its own.

The outside producers start their capture threads with `sinex-sources::live`,
which registers signal handlers first and stops every capture before joining
it. `capture::CaptureStop` owns interruptible waits, socket shutdown and child
process groups: it interrupts a group, escalates after 250 ms, and reaps its
children, including descendants adopted through Linux subreaper support.

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
optionally only the events whose payload fields have given values (Sinnix lake
observations of one lane, journal entries of one priority), so a key never
reads a whole type to use a sliver of it.

A semantic range resolves an objective's first and last act, or the next wake,
before reading that range. Input changes also dirty existing keys whose outputs
overlap their time range. Unknown-time session metadata is read by its session
key; it never acquires an invented start time. Waiting keys stay eligible until
the evidence they need arrives. Time-dependent outputs store their next refresh
time with the key, so silence can close a window and inactivity can mark an
episode dormant without another input.

Admission records every parent edge in `core.event_children`. Archiving an input
archives every live derived descendant in the same transaction, including keyed
aggregates, and dirties their keys. A restored aggregate requests a new reading
from the live inputs. The archived reading and its exact parents remain stored.

### Fusion

Several sources often see the same thing: Atuin and zsh the same command,
the compositor and ActivityWatch the same focus, a browser's history and its
Takeout export the same visit, Sinex's live capture and the Sinnix capture
lake the same clipboard change, notification or track, IMAP and an mbox the
same mail. One mechanism joins
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
`clipboard.fused`, `notification.fused`, `mail.fused`) takes its times from its most
preferred member and each field from the most preferred member that has it,
names every member as a parent and lists the sources in `observed_by`. A
record no other source saw is fused alone, so a fused kind counts each thing
once whichever sources were running. Metrics and aliases read the fused
kinds.

### Tag rules

The opt-in `tag.applier` per-event derivation applies the `tags` list in the
sources configuration. Each rule names an event type, a tag and literal
comparisons over payload values addressed by JSON pointers, optionally restricted
to an exact metadata source. `extension` checks the final file component;
`mime` compares a media type without its parameters. Every matching
tag becomes a `tag.applied` event naming its record and matching rules, with
the input reading as its parent and its original time and class. Unknown
times stay unknown. `services.sinex.derivations."tag.applier".enable = true`
enables the configured rules. Changing the rule list reprocesses the inputs;
admission replaces each input's output set and saves progress together, even
when no rule matches any more. Removing the rules or disabling the tag applier
runs an empty rule list to remove its former assignments. Hidden or replaced inputs lose their per-event
outputs; unhiding an input computes them again.

Document text is a per-event derivation over stored library copies. It reads
the content blob for PDF, HTML, Markdown, plain text, Word (`.docx`) and
OpenDocument (`.odt`), and writes searchable `document.text` events with their
input as parent. Text is keyed by its captured copy record and part, so equal
copies retain independent lineage. PDF text and screen OCR run at idle priority.
Conversation days group a provider's messages. Mail uses recorded thread IDs;
without one, each message stays separate, since a shared subject does not
identify a thread. Rest fusion joins sleep and nap records from health providers.

### Activity

`activity.windows` joins operator evidence across gaps shorter than 300 seconds,
with civil-hour partitions and continuation flags. Physical keyboard and pointer
input counts only with recorded `extra.layer = evdev` and `extra.virtual = false`.
Human prose prompts become work acts only with a recorded main, interactive Polylogue session;
operator log entries support their own instants. Focus, commands, visits, copies
and commits alone do not establish operator activity. Unknown device scopes stay
separate from captured host and seat scopes.

`activity.sessions` joins the windows across gaps shorter than
`services.sinex.activity.break_seconds` (900 by default), closing on explicit
lock, suspend or sleep. Sessions cross logical days. Their window and short-break
members distinguish supported operator time from the session's wall extent.
Changing the break threshold changes the producer version and replaces sessions.
The runner follows connected windows to their real boundaries, without a day cutoff. Changes dirty nearby window keys and existing sessions that overlap them.

`activity.spans` sweeps observed signal boundaries into non-overlapping elementary
segments per scope and civil hour. Its agency, physical support and AFK claims
remain separate. Conflicting exact input and absence evidence is `mixed`; focus
without operator evidence is `unknown`. Playback alone is `passive`. Unknown
physical provenance never establishes operator time. Physical recorder lifetimes produce `input.coverage`; silence becomes absence
only within their recorded bounds, with matching captured scopes at both ends. Quiet gaps up to 300 seconds require physical
input on both sides in the same recorded window. `presence.intervals` projects
this same segmentation with its method, support and contradictory AFK claims.
It has no separate AFK correction algorithm. Polylogue turns provide measured
concurrent agent intervals; each span records delegated work independently of its
operator-channel agency. Prompt and command instants create no duration.
Exact terminal-to-agent joins needed to classify supervision remain unavailable.

`work.acts` reads one recorded session and stores its qualifying acts with that
session as a parent. Acts remain available to presence even without an objective. `work.episodes` groups those acts by the recorded repository
and branch, reading context only within that objective's extent.
It keeps separate objectives in the same repository separate, spans days and
marks an episode dormant after 14 days without an act. Suspensions and resumptions
cite the boundary acts; excursions shorter than 30 seconds are absorbed and
counted. `context.packs` derives a stored resume card from the episode and its
work acts. The chronological trail holds references to those acts, with the first
human prompt, last edited path and failures. Failures remain unresolved when no check
records their exact revision. Operator acts without duration contribute no
invented operator seconds. Task/job anchors, branch closure, exact focus-return
lag and external/agent interruption causes need source records not yet available.

`activity.days` provides wake-to-wake display intervals when sleep ends are
recorded, with one key and input range per wake. Elementary agency seconds are counted once across device scopes, with
agent work reported separately. The configured logical day still partitions
calculations. Unknown wake boundaries produce no invented display day.

The `foundations` schema step registers these kinds and replaces the presence
view. Its schema number is assigned by the lander after the query and library
prerequisites. The upgrade refuses a populated former telemetry table.

The daemon samples its state every 60 seconds into the telemetry spool.
`sinex.telemetry` uses ordinary admission, visibility, source counts and commit
positions, and is readable through the `telemetry` alias or `events`.
Read marks use the writer's monotonic excision counter, advanced in the same
transaction that removes events, rather than scanning tombstones on each batch.

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

A derived batch never stores an output that cites an input which stopped
being live after the derivation read it (a newer reading, a replay or an
excision in between): a key with such an output keeps its previous outputs
and is marked dirty again, and a per-event output is dropped, since the
input's replacement is read in its turn. A full recompute also reaches every
key the derivation has outputs under, so a version that keys its windows
differently archives the old keys' outputs instead of counting an input
twice.

A derived output's record hash includes its producer name and version. A new
version writes a new reading even when its payload and parents match;
recomputing unchanged inputs under the same version remains a repeat.

The commit log orders committed events and visibility changes (hide, unhide,
replacement). Hide and unhide change what is live and append to the visibility
history in one transaction. Readers move past positions an excision left
empty. Time lookups use stored events and the visibility history, so pruning
the commit log loses nothing. The publisher sends committed events on the
confirmed stream, with `Archived`, `Unhidden` and `Excised` messages; each
event message carries the stored reading order and record hash, so a
consumer can check its copy against Sinex's.

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

Activity windows and spans read earlier commands, focus intervals and absences that overlap their hour partition. Input queries declare each interval end as a timestamp field or a duration in nanoseconds; the shared store query selects those intervals.
