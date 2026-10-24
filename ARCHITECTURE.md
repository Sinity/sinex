# Architecture

This describes the current Rust ownership boundaries. The specification and
Beads also contain intended behavior and qualification that remain unfinished.

## Capture and interpretation

`sinex-sources` owns source declarations, shared acquisition drivers, framing
and parsers. Drivers obtain bytes; parsers interpret one framed record without
opening files, networks or clocks. The drain attaches record locations, hashes,
carried state and record failures to the resulting batch. Provider-specific
interpretation belongs here, not in the database or daemon.

Notes vaults use the directory driver and retained member context. Parser version
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
metadata. Normal retained-material replay supplies the newer interpretation.

YAML syntax, duplicate detection and bounded alias handling use serde-saphyr.
A private JSON conversion refuses non-string mapping keys, unsupported tags and
plain integers outside the parser's signed/unsigned 64-bit representation,
including unsupported plain leading-zero forms. Quoted or explicitly string-tagged
numeric IDs remain strings. The header has a 1 MiB byte/scalar budget, structural
limits and bounded alias replay. Limit/refusal outcomes retain the exact note
text. Parsers do not resolve external includes or open files.

Steering commitments, activities and reviews use closed typed row variants behind their existing
`steering.record` wire shape and `(table, record_id)` identity. The table selects
the row type before decoding. Missing fields, explicit nulls and numeric values
retain their representations; unknown columns or types become record failures
with their bytes retained. Parser version 2 uses the normal replay path to
reinterpret stored rows; it does not rewrite earlier event payloads.

Sinnix lake observations retain the `capture.observed` envelope and material-location
identity. The lane and, where declared, payload kind select concrete source fields
before decoding. Unknown lanes/kinds remain explicit, while unknown extension fields
and open notification hints retain their values with computed interpretation markers.
Known-field errors become record failures with raw bytes retained. Awair interprets
only the producer's declared score and CO2 fields; other sensor fields remain extensions.
Netflow stays material-only. Parser version 2 uses ordinary retained-material replay.

Non-health Samsung account tables currently retain normalized columns as generic
`account.record` fallbacks with computed `uninterpreted: true`. Table names and
synthetic normalization examples do not establish provider field schemas. A known-table
interpreter requires those declarations; the generic row does not claim one. Existing
payloads without the marker remain readable. Samsung parser version 4 uses normal replay.

`sinexd` connects these pieces: source tasks, backfill and replay submit work
to the writer. A source cursor describes admitted progress; obtaining bytes
alone does not mean their records were interpreted successfully. Archive
capture and replay have additional open acceptance criteria in Beads.

On shutdown, the derivation runner completes its active pass or maintenance
step, then releases its writer handle. Idle waits observe the shared stop signal.
The writer drains queued work after its producers release their handles; the
supervisor retains its ten-second fallback for tasks that cannot finish.

SQLite row snapshots use one read transaction across schema inspection and row
selection, so SQLite supplies committed rollback-journal and WAL visibility.
A locked source remains unavailable; stable main-file metadata cannot prove a
committed view. Changed rows stream into an unnamed staged JSONL material.
Retries retain its identity, byte ranges and reading orders until its final
admitted cursor confirms the changed-row hash cache. Partial cursors resume the
retained snapshot before observing newer source values. Restart rebuilds hashes
from admitted retained parts.

IMAP answers retain their original material and record coordinates until every
record is admitted. Restart reads the verified remaining bytes before contacting
the mailbox, even if messages were expunged or UIDVALIDITY changed. New answers
record their examined UID boundary and exact admitted byte cut. Old partial
answers resume only from one positively accounted contiguous record prefix;
without that proof they refuse rather than refetch over retained history. Since
old answers lack the original sparse-window end, their next server poll rechecks
from the highest retained UID plus one. Private retained answers require scoped
read authority; a missing body does not authorize a replacement fetch.

Native publication custody uses `raw.pending_native_materials`, with blob
references derived from its typed frozen publication. `core.blobs.status =
'reserved'` means the named bytes may still be missing; it implements the
specification's writing phase and is neither `present` nor quarantined. Reserve
these rows before installing prepared CAS bytes. First admission completes the
reservation with material and progress in one transaction. Cleanup locks each
blob row and rechecks ready and pending references while holding the lock through
unlink and row deletion. Producer custody uses `raw.pending_producer_bytes` with
references derived from its frozen declaration. A publication is identified by
its authenticated producer namespace and final frame; material-body and indirect
envelope roles stay distinct. Reservation precedes installation. Final admission
marks its owner acknowledged in the same transaction as the immutable frame ack;
private byte cleanup then retires that owner under its publication and blob locks.
Unregistered legacy files are not repaired by these reservations. Excision of a
producer's records is refused.

The native binding pipeline prepares plain or encrypted bodies privately, reserves
custody and queue capacity, then installs and verifies the retained representation.
The reservation freezes source facts, parser bytes, removal requests and cursor
bounds. Failed first admission keeps that exact version for restart; provider edits
do not substitute a new view. Restart opens sealed pending bodies only with an
operator unlock. The source guard stays with queued
first admission, and live cancellation/authority checks run again before commit.
Ready material keys are never republished by cached readers.

The application `publication` owner also retains authored/library requests and
staged scan/preview media. Its version 2 pending format freezes cited blob names
and decoded sizes alongside the body; body-only version 1 remains readable.
Readers reject unknown versions and citation/projection mismatches. First
admission validates the exact citations on their actual admitted or resolved
readings before replacing pending custody with ready event references. An
unrelated record or source owning the same hash cannot substitute for that
reading's citation. Library request manifests record prepared rendition
citation facts, so replay reproduces ownership without changing the rendition
payload or identity. Older requests without those facts acquire no inferred
media ownership. If a fresh request resolves to an old reading without a ready
media owner, first admission refuses and keeps its pending custody; it never
silently discards the prepared bytes or fabricates old ownership.

An authored retry first recovers its frozen prior publication as that prior
request, then submits the fresh request independently. Replies report recovered
material IDs separately; this supplies no client idempotency guarantee. Current
authored adapters are stateless plain/guarded request or scan streams with one
recorded reading order. A request that matches an excised record is refused;
unrelated requests are not. Excision records the stored media its events cited,
by source, and later requests citing the same media for that source are refused
whatever size they claim; other sources keep their own media. These authored
adapters refuse sealed publication. Each authored body commits in one transaction.
Library scans retain their existing per-poll media-byte budget (including one
larger file), but metadata/event count and that transaction's size are not a
new bounded-memory guarantee. A scan cursor now records its exact admitted byte
cut; an absent legacy cut remains unknown.

Directory snapshots stream changed files into an unnamed staged bundle. The
version 2 bundle prefixes each record with one structural byte outside its source
range. Even empty files then have distinct byte anchors and advancing cursor
ends; source record hashes exclude the prefix. Notes and Maildir parser version 2
use this framing. Retained unambiguous older ranges remain readable; ambiguous
older byte anchors require an explicit mapping from retained member evidence.
Uniquely mapped legacy records sharing a byte end are admitted together, so a
new checkpoint cannot omit an empty tail; whole-material retry preserves their
locations and deduplication. An older checkpoint alone does not prove that tail
was admitted. Directory validation bounds a same-end group to the preceding
nonempty record and one empty tail; ordinary batch limits remain targets for
that indivisible group.

The existing drain reads declared source ranges, preserving hashes and carried
state. Memory holds the current file record and batch plus directory/member metadata;
a single oversized note or message still requires that whole decoded record.
File-read failures stop draining without advancing the failed range. The committed
cursor confirms file-cache changes only at the bundle's final byte; earlier
retries keep its captured bytes and material identity. Restart restores known
member paths from explicit consumed ranges and exact retained event/error
locations. An isolated later outcome confirms only its own member. Present
files are reread; remembered membership supplies no fingerprint shortcut.
Before observing new source bytes, an incomplete current bundle resumes from
its verified retained body with its original identity, coordinates and scan
time. New manifests retain constant-size cursor/observation resume metadata.
Older ignored suffixes without progress evidence remain a historical replay
limit; raw registration alone does not prove their interpretation completed.
All material sinks implement file storage explicitly; production sinks stream it.

Known event classes remain a floor for history-derived capture, including
when an operator lowers the current source setting. Failure records establish
no class by themselves. Completed sealed directory membership can be restored without a private key
when the effective binding floor remains sealed. A lowered floor requires
scoped authority, and history-derived capture retains the sealed floor. An
unfinished sealed body additionally needs authenticated access under that
authority, with no plaintext staging. A new directory bundle resets parser
carry because each file is independent; resuming the same bundle retains its
committed carry and original material key.

Retained replay streams verified plain bodies into a scoped unnamed file and
reads each declared record through the drain. Sealed replay authenticates the
stream and uses the existing encrypted backing and cached seek reader; it never
stages plaintext. Source framing declares both its existing slice behavior and
its reader strategy. Legacy ranges preserve original byte offsets, including
selected JSON arrays and append records with undecided tails. Malformed final
JSON documents retain the prior whole-record failure shape. Excision streams
same-length blanking into the blob writer, preserving untouched bytes and only
the selected records' closing newlines. Replay still commits one material chain
atomically, including carry and failure accounting.

Record-range metadata and the shared blob frame index grow with their counts.
Memory also holds the current decoded record and ordinary drain batches; one
whole-document record or undecided append prefix can still be large. Legacy
delta decoding remains a whole-body cost in the shared reader. Unsupported
compressed export containers retain their explicit replay refusal.

New blob containers in a protected 2770 store root use mode 0640, subject to the
writer's umask (the daemon declares 0007). A distinct group client reads plain
bytes through the read-only shared reader. Private roots retain 0600 files;
deduplication never changes existing private files or directories. This does not
repair inaccessible historical blobs. Encrypted containers still require their
data keys; group access does not grant decryption.

Outside producers freeze private bytes and exact declarations before sending a
compact reservation. A local shared spool uses the configured store group: an
exact 2770 root creates managed 2770 directories and completed 0640 sender files.
The receiver copies bytes into its own private snapshot before installation;
sender files stay unwritable by the receiver. Private roots and remote uploads
retain 0600 files. Existing incompatible private handoff paths refuse without
changing saved bytes or permissions and require explicit restaging. The NixOS
module declares the shared parents and session-user group membership; activation
and the running users' group memberships remain operational requirements.
Separately rooted remote upload does not need shared-filesystem access.
Material metadata is committed with recursively sorted JSON
objects, independent of map insertion order. Indirect frame encoding preserves
payload object order and existing reading hashes; the protocol explicitly enables
`serde_json/preserve_order` for its Rust consumers. A large Material frame reserves
both its body and its private JSON envelope under the same final outer frame ID.
The inner frame name may differ and remains bound by that envelope's bytes.

Local producers share only the private spool. Remote producers upload one role at
a time using a 512 KiB buffer and bounded encoded messages. Prefixes are synced
before acknowledgment; overlapping resends must match. Final material handling
copies and verifies producer-writable bytes before the existing CAS installer runs
outside Writer. The owned source/publication guard follows detached byte work and
queued admission through cancellation. Missing reserved bytes stay unavailable,
and retries cannot substitute edited provider content. Empty roles send one empty
chunk. A reservation can return the original final acknowledgment without reopening
roles already cleaned up. Sender identity metadata remains frozen until the sender
observes that acknowledgment. New unreserved material/chunk frames refuse; schema4
writers cannot write schema5 archives. Sealed producer storage is unsupported and
refuses before creating a new local plaintext spool when that class is known.

Transport buffers are bounded; snapshot copies and whole-name verification still
read each complete role. A driver record, material manifest, frozen typed frame and
indirect JSON decode retain their existing one-value memory costs. The shared
transport spool iterator reads one sender artifact at a time and reports malformed/unreadable
artifacts explicitly; it supplies no queue, cursor or resend controller.

The outside producers use `sinex-sources::live::run` to start capture threads
and publish their spools under one lifetime. Signal handlers are registered
before capture begins. Deadline, production failure, interrupt and termination
stop every capture before joining their threads. `capture::Control` owns
interruptible waits, socket read shutdown and child process groups. It interrupts
groups, escalates after 250 ms, and reaps their children, including adopted
descendants through Linux subreaper support; error and panic
unwinding also kill and reap an owned child. Capture spawns and the final group drain are serialized; process identities
remain registered until reaping, so a delayed stop cannot signal a reused PID. The direct
`sinex-session capture` command uses the same lifetime.

## Persistence and derived work

`sinex-store` owns PostgreSQL access, content storage and admission. The daemon's
writer owns transaction scheduling; admission stores events and source progress
together and marks affected derivation keys dirty. Source and derivation code
do not insert event rows independently. Blob installation and database commit
are different operations; a content hash does not prove semantic completeness.

`sinex-derivations` contains computations over supplied events and configuration.
The daemon's derivation runner reads their inputs and submits replacement output
sets through admission. Analysis methods similarly live apart from their store
and application adapters. The commit reader and NATS publisher handle committed
events separately from source acquisition.

Writer positions cover both admitted Events and visibility changes. Hide and
unhide update the current archival projection and append their history in the
same transaction; unhide keeps the original Event. Commit-log markers carry no
Event. Readers advance across those markers and excision gaps using one captured
head. Historical time lookup uses retained Events and visibility history, so
pruning the hot commit log does not erase its time basis. Permanent excision
still removes the exact event's historical metadata.

`sinex-schema` declares the schema in Rust at version 0. `sinex-schema apply`
(or `sinexd admin schema`) creates it in an empty database; a database at any
other version is refused, and ordinary commands never create the schema.
Ordered upgrades begin with the first deployment.

Archive-stream consumers receive `Restored` as well as `Archived` and `Excised`.


## Interfaces and shared code

`sinex-model` owns identities and payloads; `sinex-protocol` declares public
commands, wire types and pure encoding without IO. `sinex-transport` owns the
shared filesystem private-mode switch and frozen producer spool. The daemon,
source drivers and CLI use that small library without putting filesystem access
in wire definitions or making outside publishers depend on source parsers.
CLI, MCP and HTTP adapt the public commands. `sinex-diagnostics` supports
the separate daemon-down diagnostic path. Schema declarations and application
remain in `sinex-schema`; checked SQL compiles against that real schema.

Forecast issuance and resolution are ordinary `forecast.request` source records.
Their parser takes transport-role attribution and submission time from the retained
manifest, preserving outcome time separately. Both command adapters use the shared
application publication owner; forecast code neither installs blobs nor writes
archive rows. Schema4 adds only their registry entries and typed reader views.
The report reads currently visible, live issues, resolutions and resolution evidence
in one repeatable-read transaction and supplies resolved probability pairs to pure
`sinex-analysis` scoring. It stores no separate forecast state or cached score.

Shared blob storage, credential masking and development tooling come from
immutable `polylogue-sinex-common` releases. Prefer these existing owners to
another local implementation. Change an interface with its callers and remove
the replaced path. Read [AGENTS.md](AGENTS.md) for test placement and focused
development commands; passing a component test does not qualify a deployment.

The `shell-sessions` query alias reads the existing `shell.session` derivation;
its `start` and `observed_until` describe recorded command extent. Its observed
span includes both endpoints, including a final command with unknown duration.
It does not supply activity-session depth, project, open state or a proven end.
The broader `sessions` alias remains planned. `recall last` binds its optional
session-field filter against `shell-sessions`, selects the latest group through
the ordinary query compiler, and reads all sources in that observed envelope.
Selection and rows use the same commit position, including cursor continuations;
equal starts use the ordinary event-ID tie break. Withheld bounds refuse rather
than borrowing an older group. No matching group retains the ordinary output
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

Output uses `credential-output-v2`, including independently recognized
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
