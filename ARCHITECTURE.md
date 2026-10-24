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
from admitted retained parts. Image-enabled readers restore a partial material's
complete retained suffix and resume it before acquiring newer source values.

IMAP answers retain their original material and record coordinates until every
record is admitted. Restart reads the verified remaining bytes before contacting
the mailbox, even if messages were expunged or UIDVALIDITY changed. New answers
record their examined UID boundary and exact admitted byte cut. Old partial
answers resume only from one positively accounted contiguous record prefix;
without that proof they refuse rather than refetch over retained history. Since
old answers lack the original sparse-window end, their next server poll rechecks
from the highest retained UID plus one. Private retained answers require scoped
read authority; missing or erased bodies do not authorize a replacement fetch.

Optional `settings.options.sqlite_images` requires explicit `scope:
"whole_database"`, `class` and positive `interval_secs`; `turn_pages` defaults to
256 and `rebase_threshold` to 1.0. This grants retention of all database pages,
including unselected tables, freelists and deleted cells. Configured source and
field classes for that database are lower bounds. No configuration is enabled
implicitly. A due image and its selected rows share one pinned read transaction;
typed pending turns yield between page scans and cancellation discards unfinished
scans without progress. A row between image captures remains logical evidence and
cites no older image as its exact source. Image-enabled row envelopes preserve
native fields separately from framework row IDs and optional `_from` references.

Plain/guarded image materials hold full or changed-page payloads followed by a
streamed dense 32-byte map entry per page (about 0.78% at 4 KiB before compression).
Logical database length, evidence blob length and map offset/count remain distinct.
Ordinary revision extents reconstruct database bytes only; publication verifies
each reconstructed page and map entry against the pinned view. Each due image
still reads every page, and extent verification/storage may reread those pages.
Changed-page indices and extent plans also consume memory proportional to changed
pages/spans. Rebase occurs after more than half of pages change, accumulated
compressed patch cost exceeds the configured base multiple, or unique referenced
compressed bytes exceed twice the compressed base. This footprint is not a
measurement of decoder I/O. The shared v0.5.16 assembler retains at most four
verified plain opens and their seek tables, with one decoded frame shared across
them. Returning to a retained base reuses whole-name verification; alternating
sources can still decode their selected frames repeatedly, and evicted sources
are verified again. Referenced footprint and actual read/decode work therefore
remain separate. Sealed images use fresh encrypted full bases at cadence with
keyed digests and no cross-image page reuse. Provider
page readers feed encryption directly; sealed retained input is never staged as
plaintext or opened with borrowed operator authority.

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
Unregistered legacy files are not repaired by these reservations. Destructive
producer excision remains guarded until exclusion-aware acquisition is supported.

The native binding pipeline prepares plain or encrypted bodies privately, reserves
custody and queue capacity, then installs and verifies the retained representation.
The reservation freezes source facts, parser bytes, removal requests and cursor
bounds. Failed first admission keeps that exact version for restart; provider edits
do not substitute a new view. Image costs stay unresolved until the installed CAS
representation passes page/map verification. Restart opens sealed pending bodies
and images only with scoped class authority. The source guard stays with queued
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
recorded reading order. Existing acquisition policy refuses matching excluded
bodies before installation while allowing unrelated requests. Purge freezes
source-bound cited media names and decoded sizes in its immutable operation
scope before removing event references. Matching names remain excluded even
under a fresh request or a different claimed size; other sources keep their
own ownership. A complete empty list permits unrelated media, while missing
historical facts leave citation-bearing acquisition unavailable. Sealed naming
facts stay under their class boundary; these authored adapters currently refuse
sealed publication. Each authored body commits in one transaction.
Library scans retain their existing per-poll media-byte budget (including one
larger file), but metadata/event count and that transaction's size are not a
new bounded-memory guarantee. A scan cursor now records its exact admitted byte
cut; an absent legacy cut remains unknown.

Companion image material, immutable revision/extents, wrapped keys, row material
and first-batch progress share the writer transaction. Later chunks name that
fixed image revision. Independent bindings may retain immutable branches from
the same compatible predecessor; capture order stays distinct from late commit
order. Generic replay skips image bodies and reinterprets retained logical rows.
Excision currently refuses every row/image target in a retained database lineage
across bindings before any event/blob/key mutation, including logical rows with
no exact image pointer and images held by pending native reservations. Native
image reservation and admission also refuse a database lineage with permanent
exclusions from another recorded binding. Image-aware purge remains an open criterion. Physical
images do not complete deletion-key evidence, open-part rotation or the broader
SQLite source requirements.

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

Schema upgrades run explicitly through `sinex-schema apply`, with the writer
stopped, using one ordered Rust change list. Direct admission refuses an older
version. The 0→1 upgrade preserves existing rows and archival notice IDs and
seeds current visibility at a recorded cutover. Earlier visibility cannot be
reconstructed after legacy unhide deleted its evidence; historical queries
before that cutover refuse. Archive rows retain original archival metadata and
expose `baseline` and `known_from` for this distinction.

The 1→2 upgrade updates only the persisted `note.document` payload schema and
appends its structured metadata columns to `event_types.note_document`. The
view keeps its identity, dependencies and existing grants. Old payloads keep
their original values; absent additive fields read as SQL NULL. Replaying
retained note material creates new interpretations with the current parser.
A current-version apply does not refresh registry metadata or reinterpret notes.

Archive-stream consumers receive `Restored` as well as `Archived` and `Excised`.
Consumers decoding that subject must understand the added variant before an
upgrade; event-type-only subscribers do not receive it. Existing notice IDs and
saved consumer positions keep legacy pending notices retryable.


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

## Superseded producer rows and source bytes

Polylogue requests physical row-batch retirement on its own NATS retirement
subject, naming a confirmed archive, excision owner and immutable manifest
digest. `sinexd/row_retirement.rs` interprets that producer's descriptor history;
`sinex-store/retirement.rs` stages selection in temporary PostgreSQL tables and
changes material references under the existing writer. The complete unit prefix
and replacement coverage are checked before mutation. Original batches and older
replacement batches and their source dependencies are candidates. Each candidate
is resolved through its producer-owned acknowledged material frame. Missing
source-frame evidence for an original confirmed descriptor refuses retirement;
unpublished intermediate replacements remain optional candidates. Selected
batches and source references across all producer/archive namespaces stay;
retirement folds their confirmed metadata in bounded pages on every request.
A dependency still selected by the same unit is not an obsolete cleanup
candidate; sharing by another unit or archive is reported as retained backing.
Selected batch bytes are read back and checked against their hashes before
retirement changes backing references. Raw source references receive a presence
check; this operation does not rehash the source corpus.
Already completed requests resolve their descriptors and report cleanup without
rereading live batch contents. A request that retries an owned deletion verifies
again before cleanup.

Retirement repoints eligible material/extent references to same-length blank
bytes and preserves descriptor events and commit-log rows. Same-revision aliases,
other event readings, sealed materials and descendant parts retain their backing.
Other material, extent or event-blob aliases retain shared physical bytes. No
original provider file is selected for removal. Existing durable
`core.blobs` deletion recovery owns unlink retries; every request retries it and
reports remaining owned files separately from retained shared files. The request
proof also names an idempotent entry in the existing operations journal.

A retired global source frame keeps its immutable acknowledgment in the database.
Content-address interpretation requires the declared Polylogue material role;
generic producer frame names retain their ordinary acknowledgment behavior.
Have, Reserve and delivery retries return an unavailable refusal when that
material no longer backs the advertised hash. New row and replacement source
references are checked inside the writer transaction against producer-owned
registered backing. An old acknowledgment cannot resurrect deleted bytes or
authorize a later archive to cite them. These checks track registered backing,
not a fresh physical verification of every source file.

This producer grant performs cleanup of already confirmed excision metadata; it
does not grant general material excision. Operator excision retains its existing
access gate. Older servers have no retirement responder, allowing callers to
report unsupported physical retirement immediately.

Sealed material keys contain authenticated, versioned capability roles inside
class-wrapped age envelopes. Exact 32-byte predecessor envelopes decode as data
keys. Data-and-record-naming envelopes can decrypt content; record-naming-only
envelopes cannot enter blob or payload decryption, including through legacy
unwrap callers. Record naming and blob naming remain separate domains. Scoped
class holders with a naming capability can test candidate equivalence keys and
record hashes, but cannot decrypt old content with that capability. Replay and
restart restore the retained naming capability rather than derive identities
from a replacement data key.

Explicit operator unlock and `excise --sealed` prepare supported logical record
removal through the existing source drain. Native equivalence keys match retained
versions across encryption epochs. Partial removal streams same-length blanking
into fresh encrypted backing, preserving unrelated bytes and anchors. The Writer
atomically revalidates the complete source candidate set, dependent ciphertext,
carry, old envelopes and replacements before retiring old data capabilities.
Unrelated readings retain their values, identities and observation facts.

Whole removal makes the original body unavailable. The storage-owned excision
operation authenticates that state independently of key role; provider manifests
cannot declare it. Ordinary whole removal retains naming-only capability. When
unrelated protected values survive, a fresh data capability retains those values
while replay uses authenticated minimal transitions without reopening the body.
Format-owned carry erasure removes selected private state. A later material that
owns such carry can be rekeyed with its body unchanged. Replay and current-carry
metadata have separate class-encrypted purposes and scopes.

Preparation holds the current decoded record/value, carry and encrypted frame
buffers. Candidate/range metadata grows with retained records and materials.
Shared parser predecessors are reread for each branch rather than retaining a
private carry for every material, so long shared prefixes add I/O. Mixed plain
and sealed epochs, unsupported parser versions/state, original containers and
physical SQLite image lineages refuse before mutation. The current carry-erasure
adapters cover zsh, ActivityWatch and stateless Atuin. Matching permanent sealed
exclusions during later capture requires scoped authority; locked excluded-source
continuity remains operational scope, separate from fresh public-recipient capture.
