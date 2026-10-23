# Architecture

This describes the current Rust ownership boundaries. The specification and
Beads also contain intended behavior and qualification that remain unfinished.

## Capture and interpretation

`sinex-sources` owns source declarations, shared acquisition drivers, framing
and parsers. Drivers obtain bytes; parsers interpret one framed record without
opening files, networks or clocks. The drain attaches record locations, hashes,
carried state and record failures to the resulting batch. Provider-specific
interpretation belongs here, not in the database or daemon.

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

SQLite row snapshots use one read transaction across schema inspection and row
selection, so SQLite supplies committed rollback-journal and WAL visibility.
A locked source remains unavailable; stable main-file metadata cannot prove a
committed view. Changed rows stream into an unnamed staged JSONL material.
Retries retain its identity, byte ranges and reading orders until its final
admitted cursor confirms the changed-row hash cache. Partial cursors resume the
retained snapshot before observing newer source values. Restart rebuilds hashes
from admitted retained parts. Image-enabled readers restore a partial material's
complete retained suffix and resume it before acquiring newer source values.

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

Companion image material, immutable revision/extents, wrapped keys, row material
and first-batch progress share the writer transaction. Later chunks name that
fixed image revision. Independent bindings may retain immutable branches from
the same compatible predecessor; capture order stays distinct from late commit
order. Generic replay skips image bodies and reinterprets retained logical rows.
Excision currently refuses every row/image target in a retained database lineage
across bindings before any event/blob/key mutation, including logical rows with
no exact image pointer. Image-aware purge remains an open criterion. Physical
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

Outside producers install material files through the streaming blob writer and
upload them from one verified open representation using one 512 KiB plaintext
buffer, plus the bounded encoded outgoing frame and the shared blob reader's
frame metadata and decoder. Empty materials
still send one empty chunk; acknowledgment and pending-batch retry ordering are
unchanged. This bounds producer transport memory, not a driver's decoded records.

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

## Interfaces and shared code

`sinex-model` owns identities and payloads; `sinex-protocol` declares public
commands. CLI, MCP and HTTP adapt those commands. `sinex-diagnostics` supports
the separate daemon-down diagnostic path. Schema declarations and application
remain in `sinex-schema`; checked SQL compiles against that real schema.

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

Output uses `credential-output-v2`, including independently recognized
overlapping credentials. Credential-shaped JSON keys refuse the complete
reply because renaming could change an object's meaning. Follow streams check
the whole query page before emitting its first row; a refused page emits a
generic error without the key and leaves stored values and cursors unchanged.
Refusal reasons and follow-query errors pass through the same masker before
encoding; their typed refusal flags stay intact.

## Superseded producer row batches

Polylogue requests physical row-batch retirement on its own NATS retirement
subject, naming a confirmed archive, excision owner and immutable manifest
digest. `sinexd/row_retirement.rs` interprets that producer's descriptor history;
`sinex-store/retirement.rs` stages selection in temporary PostgreSQL tables and
changes material references under the existing writer. The complete unit prefix
and replacement coverage are checked before mutation. Original batches and older
replacement batches are candidates; selected batches and source references stay.
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
source blob or original provider file is selected for removal. Existing durable
`core.blobs` deletion recovery owns unlink retries; every request retries it and
reports remaining owned files separately from retained shared files. The request
proof also names an idempotent entry in the existing operations journal.

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
from a replacement data key. Partial sealed excision still refuses until its
body, dependent values and format-owned carry redaction are implemented together.
