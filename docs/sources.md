# Sources

How Sinex captures: what a source is, how it is declared, which inputs it reads and which materials it stores, how capture survives everything downstream of it, how gaps, input states and coverage make completeness visible, and the rules every source's records follow. The material model and staging as a concept are in [materials](materials.md); records, readings and identity in [events](events.md); the base-layer streams and their spools in [streams](streams.md); each source family and the catalog of sources in [source families](source-families.md); the producer wire contract in [producers](producers.md); privacy in [privacy](privacy.md); capture from other hosts and devices in [hosts](hosts.md); status presentation in [interfaces](interfaces.md#status).

## Purpose

Sinex keeps the operator's digital exhaust as source-shaped material, at the finest record grain the input carries, so the operator and agents can recover what happened and derive better interpretations later. A source captures every field and every record its input carries. Volume is handled by readers, admission and storage, never by choosing a smaller product. Missing data is visible as missing, never shown as "nothing happened".

Scope is every input that is not an AI session: shell and terminal, desktop state, browsers, account archives, mail, calendars and contacts, finance, health, wearables and phone, location, social and chat, media and reading, code and work records, notes, documents and files, filesystem, audit and network, journal and machine records, audio and screen, and the operator's own acts and self-reports. [Source families](source-families.md) state each at full fidelity. Polylogue acquires and parses AI sessions, including AI members inside account archives, and sends its materials and `polylogue.*` events through the ingress ([producers](producers.md)); Sinex has no AI-session parser.

## Capture by default

`services.sinex.enable = true` captures every source whose canonical input exists on the host. There is no per-source enable option and no opt-in feature: a source that is built is on. (ruling 2025-06-27, cc:57547e40-5ef5-4e62-b089-a09820967865:e0902901-9329-4f3f-a320-461eca10109c). (ruling 2026-05-24, cc-queued:67eb2417-f1d1-4623-a652-ceb0e4c653e3@2026-05-24T10:09:42Z).

**Discovery** finds applicable inputs at canonical locations: the host layout registry, XDG locations, each tool's own configuration and the configured accounts. A location names a root (home, XDG data or configuration, the runtime directory, an application's environment variable, a named root the host declares, the spool) and a path relative to it; where a source reads is never a per-source setting. It runs at start and during operation, so a new browser profile, mail folder, device, journal namespace or repository is captured when it appears, without a restart, and a passive notice names the new input. Existing roots expand by their provider's own rules (browser profiles, ledger includes, note attachments, archive members, git worktrees, device channels). There is no recursive search of the whole host; inputs are found where their programs put them.

Discovery gives every input one availability before any record is read: **capturing** (found and read, or captured into a spool), **unavailable** with its cause (not present, permission denied, awaiting credentials, not applicable, failed), **excluded** with the operator's reason, **on demand** (records written only by request, so there is no input to find), or reported by a session producer. Whether a capturing input is current, backlogged, quiet or stalled comes later, from its records and its declared expectation.

**Discovery dry run**: `sinexctl sources-discover` (the `sources-discover` command on every surface) prints every input with its availability and reason, and the **unimplemented census**: catalog sources with no implementation whose provider's data exists on this host (a Firefox profile, a nushell history), found by checking only their canonical locations. The catalog's floor becomes measurable, and source work can be ordered by what the host actually has.

**Exclusion is the only per-source setting:** `exclude.<source> = { reason = "…"; }`, optionally narrowed to one input. Status lists every exclusion with its reason. No answer means everything; a subset needs a recorded reason. Content that should not be shown is handled by output rules in [privacy](privacy.md), which never limit capture.

**Unavailable is a state, never zero.** An unavailable credential, an unreadable path, a missing device or an inapplicable provider is shown as "awaiting credentials", "permission denied" (with the grant it needs), "not present" or "not applicable". None of these is out of scope, and none reads as an empty source.

**What waits for the operator.** Only three things: a paid external API, a credential the host lacks, and capturing third parties in a way not done before. Until the operator answers, such a source spends nothing and shows "awaiting credentials" (default; open operator question). Capture breadth is otherwise never a question. (ruling 2026-10-27, cc:ee52794c-8a9e-43b6-9fbc-9749c7e78e8f:f1f39c29-7c7d-42e0-99ec-0c7a1b10b751).

**Volume is performance work.** No source has a daily cap on records, bytes, file size, history age, fields, attachments or text length. A stream that a host truly cannot admit may declare deterministic sampling of events only, with its raw capture kept whole ([streams](streams.md#sampling)).

## Staging is the entry

Everything enters Sinex by being staged ([materials](materials.md#staging) defines staging). For sources this means:

- **Continuous bindings** are staged inputs generated from the source declarations; there is no second ingestion mechanism for them. `stage <path | directory | live database> [--parser NAME]` is the same act done once, as an operation and a run.
- **Historical import and live sensing are one path.** A live source that restarts after a gap catches up by reading what accumulated.
- **Staging without a parser keeps the bytes.** When no parser matches, or more than one does, the bytes are stored as material with no parser and the candidates are listed; nothing is guessed and nothing is refused. Status shows such material as stored, not interpreted, and [replay](lifecycle.md#replay) with `--parser NAME` reads it later.
- **Live streams are staged as they arrive**, as material chunks in their spools, never only parsed in flight ([streams](streams.md)).
- **No double import.** Re-staging identical content is suppressed by its content key. Re-importing byte-identical data as a deliberate new act creates a new material, not a replay.
- **Replay is how stored data gets a new interpretation.** A changed parser replays the materials it reads; wiping the store is never the route for a parser change.

## One declaration per source

Each source has one declaration in Rust, with its parser and event kinds beside it. The NixOS options (exclusions and real settings), session capture's stream list and tooling are generated from the declarations or checked against them at build time. Registration rejects duplicate names and bindings with no implementation, so a build fails on either.

| Field | Meaning |
|---|---|
| name | `<area>.<name>`, from the vocabulary |
| origin | an input of one of the [input shapes](#input-shapes), or requests through the API (records the operator or an agent writes, stored as the request's bytes); a producer is a transport, not an origin |
| change model | one of the [change models](#change-models), carried by the shape's driver settings; a shape's typed change-model settings (for example the SQLite `deletion_check` cadence) are source options |
| canonical locations and discovery rule | where the input lives and how its roots expand ([capture by default](#capture-by-default)) |
| source record key and reading-order rule | per event kind; the key kinds are defined in [events](events.md#identity) |
| time quality | per event kind: which clock each time comes from, and its precision |
| field hints | per payload field, from the [hint vocabulary](privacy.md#field-hints) |
| correlation identifiers | window, process, session, request and other identifiers the source records; derivations join only on declared identifiers or on time |
| `expect` | when the source should have something: `continuous { silent_after }` (a live stream whose silence past `silent_after` is a stall), `cadence { every }` (a poll), `export { products }` (an export-only provider, whose coverage is the span each export covers; an export is never "stale"), or `on_demand`. A fact about the provider, so it is declared, never set per host; declarations that share a source name declare one |
| `expires_after` | where the provider expires its own records (Chromium history: 90 days; recently-played windows; CI logs); drives the [expiry race](#completeness) |
| `coverage_note` | which periods, accounts or products a material can and cannot contain, beyond `expect` |
| `required_fields`, `probe`, `runaway_bytes_per_day` | fields every record must carry; an optional probe that checks the input is reachable; the volume above which a day is reported as a runaway |
| search | `none`, `words` or `words_and_trigram` per event kind; a kind is kept out of the text index by declaring `search: none`, never by matching its name |
| settings | a typed struct of what a deployment actually varies: accounts, capture devices and regions, poll intervals; never where the input is |
| acceptance query | at least one question the source answers, written in the [query language](query.md); it is the source's acceptance test |

Decided target (2026-11-01): `expect` and `expires_after` replace the separate health-expectation and coverage rows, because both answer one question, when this source should have something. The current `SourceSpec` fields `excisable` (excision is gone; the [lifecycle](lifecycle.md) is the one way to remove data) and the per-source privacy `class` (privacy is field hints plus the operator's output rules) are removed. Nothing in a declaration subsets capture: no filter, cap, debounce, MIME choice or field subset.

- **Settings are what a deployment varies.** Per-process plumbing (resource limits, instance counts, batch sizes, bind mounts) is not a setting; it is fixed in the code or the module. Source options are typed and reject unknown fields at start.
- **Per-user facts are configuration; vendor facts are declaration.** Account names and personal paths come from the deployment's configuration, never from source code. How a vendor writes its data belongs to the declaration: Wykop's timestamps are Warsaw local time because that is how Wykop writes them. Machine-independent setup (units, the access bridge, spool layout, journal capture, device rules) lives in Sinex's own module ([operations](operations.md#deployment)); a deployment supplies only host values.
- **A bad source never takes the daemon down.** A binding that fails its start-up check is disabled with its reason shown in status, and the rest of `sinexd` starts.
- **Event kinds are data.** Rust kinds generate their declarations; outside producers register versioned kinds over the ingress ([producers](producers.md)); a kind without a schema is admitted and marked unvalidated, never refused.
- **Per-field use is declared, never elided.** Each kind's declaration records which fields are copied into the payload and which are read on demand from the raw bytes ([materials](materials.md)). A field read by reference is still captured.
- **Parsers are pure.** A driver gets bytes from an input; a parser reads framed records and opens no files, sockets or clocks. Anything specific to one provider lives with its source, never in admission, storage or the daemon.

### External formats

A source reads data as an external service or program produces it: the vendor's export, the program's database, the protocol's bytes. A format the operator once invented to hold data in the meantime is not a source format; exceptions exist but are the exception. Sinex therefore never ingests a stand-in tool's database, CSVs or merged products, temporary capture envelopes as a permanent format, merged sleep, hand-filtered piles, title classifications or merged web-history ledgers; their purposes are served by derivations over the primary inputs. (ruling 2026-10-28, cc:ee52794c-8a9e-43b6-9fbc-9749c7e78e8f:3ae3257e-e70a-433a-be97-d1222670c284). (ruling 2026-10-11, cc:e3e37f29-2490-47ae-89ef-fd40b6242e9a:d716993a-f5d6-4422-aa5e-97d91e430d93).

Journal JSON exports from `journalctl -o json --all` are staged with
`stage --parser journal-json PATH`, or discovered under the `journal-json`
directory of an input root. Each line is a complete vendor entry; a final
line without a newline is read at EOF. The export retains its original
bytes and journal cursor and produces the same journal and systemd state
readings as live capture. Live capture continues to read its timestamped
spool through the separate `journald` format. The finite export source is
`machine.journald-export`; the continuous `machine.journald` source retains
its live freshness expectation.

### Window managers and terminals

A window manager and a terminal are ordinary data sources, captured like any other (Hyprland and Kitty among them). Each sits behind an interface for its role, so the window, workspace and terminal-session kinds that derivations and views read do not depend on which program produced them, and adding another window manager or terminal is a new source behind the same interface. (ruling 2026-10-28, cc:ee52794c-8a9e-43b6-9fbc-9749c7e78e8f:3ae3257e-e70a-433a-be97-d1222670c284).

## Inputs and materials

**Materials are source-shaped and large.** A growing input rotates into immutable parts at a size or time bound (default 100 MiB or one hour, configurable per binding). The bounds are targets, not ceilings: an oversized record is stored whole. One material per event or per poll is a defect. An open part is written outside the content store, flushed before any event citing it is committed, and carried by its durable length; after a crash it is truncated to that length ([materials](materials.md) defines parts and revisions).

**Bytes, never paths.** A path reference is never enough. Every byte a record depends on (an audio segment, an attachment, a referenced file) is held as material.

### Input shapes

One driver per shape, shared by every source of that shape:

| Shape | How change is detected | Material |
|---|---|---|
| append-only file | the change classifier: `statx` key, head hash, sampled windows and the racy-clean check; a full read only at start and after a watcher overflow, never on a schedule | appended bytes up to the last complete record as a new part; a rewritten, replaced or truncated file as a fresh revision, earlier material kept |
| SQLite database | one read-only read transaction per poll, committed WAL frames included; rows after the cursor plus open keys; deletions by the declared deletion check | the rows read, one line per row with its table and rowid, in a rows part |
| directory capture | per file, the change classifier; a path missing from a completed scan is recorded apart from any move | each changed file, or the whole selection, as a [PAX tar material](#directory-captures) |
| journal | the journal cursor | journal export-format bytes in rotating parts |
| live IPC stream | the stream itself | captured bytes with capture timestamps in the stream's spool ([streams](streams.md)) |
| live media | the capture process | encoded segments in the media spool; a segment and its sidecar are synced before they get their final names, so a listed segment is always whole |
| API cursor | the provider's cursor or page token | each response body as received, with its request metadata |
| export archive | the archive's hash | the archive stored as received; records anchored in its member stream; one forward pass serves every binding that reads the archive, each with its own parts and cursor |
| file drop | new files | each file as received |

### SQLite as rows

A database Sinex polls is captured as rows read in one committed read transaction, never as a page, file or WAL image, and there is no snapshot stream beside the row stream. (ruling 2026-10-24, chatgpt:6adb720d-d2fc-83e9-8eac-c9e1d7b39cbf:e9c41cc7-3782-4bb4-ba40-011b951b35f0).

- The database is opened read-only, never with the create flag, never with `immutable=1`, and never by copying the main file, WAL or `-shm`. A locked database reports `busy` for that poll and is retried without advancing the cursor.
- Row values keep SQLite's value types: BLOBs stay bytes, integers keep their precision, and null differs from a missing column. Unknown columns and new tables are captured.
- Mutable tables are read as open keys plus a periodic full pass. Reading a table as append-only needs a documented provider guarantee and a generation or reset check.
- Rows deleted in the source are found by comparing keys and row hashes, because a cursor cannot see a deletion. This is the shape's declared change model, run at the source's `deletion_check` cadence (default daily); it is part of reading the source, not a repair pass.

### Imports kept as received

A file, archive or package the operator hands to Sinex (`stage`, `import`, a file drop) is stored byte-for-byte as received. (ruling 2026-10-24, chatgpt:6adb720d-d2fc-83e9-8eac-c9e1d7b39cbf:e9c41cc7-3782-4bb4-ba40-011b951b35f0).

- An account archive is stored as the archive, not as extracted files.
- A SQLite file staged once (an export, a browser's history file) is stored as received, and its rows are also read in one read transaction into a rows part; events anchor in the rows part.
- A vendor package with a SQLite member is stored whole, and the member's rows are read into a rows part the same way.
- Rows-not-images applies to databases Sinex polls live. It never removes an original input the operator supplied.

### Change models

Each source declares one change model: append-only; rewritten and trimmed by its writer; updated rows; soft delete; hard delete; provider-reported delete; snapshot. A change in the source always produces a newer reading while earlier material is kept.

- **Soft and hard deletes** found by a full pass become a newer reading in state "removed at source" ([events](events.md#removed-at-source)), anchored to the bytes that show it (the key list of the pass). Sinex's own data is never deleted by it.
- **Provider expiry.** A record the provider expires itself (older than the declaration's `expires_after`) is expiry, not deletion: nothing is written and the earlier readings stay live.
- **Provider-reported deletes** (IMAP `EXPUNGE` or `VANISHED`, a Gmail delete) are deletion events. A deletion is never inferred from absence in a sparse or incomplete response.
- **Histories that rewrite themselves.** Records with their own start time are keyed by `(start, command bytes, n)` with `n` persistent across revisions; histories without timestamps are aligned against the writer's rewrite model, and ambiguity creates records rather than dropping them ([events](events.md#alignment-of-rewritten-histories)).

## Directory captures

A directory capture (a notes vault selection, a document root, a device's file tree, an application's state directory that is not a database) is stored as a POSIX PAX tar material, because tar is a standard container whose extended headers carry every file property a restore needs and whose members other tools can read.

- Members carry raw byte file names (no UTF-8 conversion), mode, owner, nanosecond times, extended attributes, POSIX ACLs and sparse-file maps in PAX headers.
- Records anchor at member offsets in the tar stream; the [`hashed-blobs` crate](https://github.com/Sinity/polylogue-sinex-common/blob/master/crates/hashed-blobs/README.md) provides member access.
- An unreadable entry is recorded as an entry failure with its error, never omitted and never read as removed.
- GNU tar is the independent restore oracle in tests: extracting the material with GNU tar reproduces names, bytes, extended attributes, ACLs and sparse layout.

Restart restoration also reads retained predecessor framed-directory materials by their immutable manifest format, preserving their IDs, bytes and record anchors. Their modification time remains a historical modification time; absent change time is unknown. Before treating an unchanged predecessor file as current, the reader hashes a stable current file observation and establishes its current change-time baseline only when the bytes match. Equal bytes create no new material. The cache uses one tagged current checkpoint encoding, including its exact admitted snapshot position; untagged saved memory requires its immutable anchor material to determine what its timestamps meant. Cache upgrades commit through ordinary admission even when no content changed, retaining the existing material and admitted byte boundary. A failed or lost acknowledgement leaves the upgrade pending until the checkpoint's committed cursor is acknowledged. Newer admitted records after a saved snapshot override its older facts. Metadata traversal may continue behind a predecessor snapshot to recover original removal locations for its saved files, without resurrecting removed files. Each part is bounded by the current cursor or the newer part's retained `previous_through`; missing or contradictory evidence is diagnosed. New directory capture continues to write PAX tar.

## Capture survival

Capture survives everything downstream of it: the broker, the database, admission and upgrades.

The session producer uses one dedicated async scheduler worker so transport tasks keep running while the main loop reads spools synchronously. Captures run on their own threads, and source startup and health snapshots use the blocking pool. The async worker count is independent of the host CPU count.

- **Spool first.** Capture starts before any NATS or database connection and keeps spooling while either is down. Only a local fault (a failed spool write) ends capture, and that becomes a health error.
- **Sending follows the producer client.** Capture programs outside the daemon send through the [producer client](producers.md#the-producer-client): saved before sent, retried forever, material acknowledged before the events that cite it, and [refusals](producers.md#refusals) that advance the binding instead of stalling it. Only acknowledged, closed segments behind the committed cursor are removed from a spool.
- **Fairness.** Each pass publishes or reads a bounded number of materials per binding, in turn, in the session producer and in the daemon's binding reader alike, so no binding drains completely while others wait. Backlog is counted incrementally from cursor offsets.
- **Full disks.** Nothing is dropped silently and nothing is deleted to make room. Producers pause on a full filesystem; a live stream's overflow becomes a counted [gap](#gaps), and a disk failure a visible fault.
- **Cursors.** A cursor records admitted progress: it never moves past data that has not been read, an empty fetch is not "caught up", and positions are saved only with committed rows. Bytes being stored does not mean their records were read.
- **Restarts** resume from committed progress and stored material, including CSV headers, JSON context and partially read archive parts. A reader holds an acquired material while admission retries.
- **Child processes** run with interruptible lifetimes and a recorded exit status; a nonzero exit is a failed attempt, never an empty reading.
- **Upgrades** may pause capture briefly; the spool covers the pause. A deploy that drops nothing is welcome but not required, and is not a reason for process topology.

## Gaps

Every interval in which a stream may have missed data is one `capture.gap` event: stream, start, end, reason, and a known or unknown missing count. The spool driver writes it ([streams](streams.md)), and every other source uses the same event.

| Reason | Meaning |
|---|---|
| `overflow` | a local queue or kernel buffer overflowed |
| `disconnected` | the stream's source or broker connection dropped |
| `encoder` | an encoder dropped frames or samples |
| `provider` | an expired sync token, a failed or partial page, a history 404 |
| `sequence` | a gap in the source's own sequence numbers |
| `stalled` | a continuous source silent past its `silent_after` (decided target) |

- Sequence numbers are assigned before enqueueing, so omissions stay countable. `bus.connection` events hold connection start and end only.
- Numbered lake envelopes compare valid records of the same host, lane and declared boot or session generation. A proven gap is an ordinary `capture.gap` event with reason `sequence`, anchored at the later source record. `sequence.first_missing` and `sequence.last_missing` are inclusive; their difference plus one is the missing count. A parse failure makes the interval through the next valid record unknown. An unqualified decrease leaves the generation unknown; an explicit generation change starts a new baseline. Committed parser carry preserves comparisons across restarts of the binding. A new rotated binding begins with unknown continuity rather than inferring loss before its first record.
- **A gap is recorded, never filled by invention.** Where a provider signals lost history (an expired Gmail history ID, an expired Health Connect token, an IMAP UIDVALIDITY reset, an inotify overflow), one full read of that input runs and its result is a newer reading; it cannot reconstruct the lost intermediate changes.
- **Continuity per interval.** Each source over each interval has a coverage state ([completeness](#completeness)). An empty result over an unknown interval is never "nothing happened".

## Source health

Status keeps these facts per source; [interfaces](interfaces.md#status) presents them as one `sources` relation, with gaps, inputs and shape history as further relations:

- the input state ([input states](#input-states));
- parser version; last attempt and its outcome; last record captured and last admitted; cursor; oldest backlog;
- raw records and bytes against admitted events; failures and the refusal rate; sampled counts and exemptions; gaps;
- "ran, only repeats" distinct from "did not run".

A registered command or a live process does not establish capture; only admitted records do.

- **Read outcomes are one current coverage reading per binding.** Each binding on each host, and each input discovery finds unreadable, has one live `source.coverage` reading: in the current build observed, partial, recorded with unknown coverage, or missing with the error; as decided target, a coverage state of the shared vocabulary. A read admits a newer reading only when the state or its reason differs from the current one, so an unchanged binding read every few seconds writes nothing, and the time of the current reading is when that state began. Earlier readings stay in history as archived readings. A restarted daemon continues from retained readings of the current `source.coverage` kind version; client output masking does not hide its own binding state from the reader. Older kind versions remain historical evidence and do not supply the current binding state; a malformed reading of the current version is reported as an error. An explicit one-time read (`backfill`, `stage`) records its own outcome. These readings are inputs of the [coverage read model](#completeness), which states coverage in the shared vocabulary.
- **Not present yet is not missing.** An input that does not exist yet (an export never staged, a device never attached) is `unavailable(not_present)`, waiting for input: status shows it as such, and it writes no coverage reading. An input that exists but cannot be read is missing, with the reason; once it can be looked at again, a newer reading replaces the missing one.
- **One run per source while it is read.** The daemon holds one source run per source, parser version and host for as long as it reads that source, and one run for the coverage readings, ending them when it stops; a read round starts no runs.
- **Expectations as data.** The daemon writes every source's declaration to `registry.sources` at start: its shapes, purpose, kinds, expectation, `silent_after`, `every` and `expires_after`. The `sources` query unit reads them, so a quiet source and a dead one are told apart from rows, not from code.
- **Runaways.** A day's volume above `runaway_bytes_per_day` is reported as a runaway, so noise is found and fixed at its source.
- **Disk runway.** Status always shows free space and the days remaining at the measured write rate, with a warning below 14 days. Nothing is refused for space; capture pauses only on a truly full filesystem.

### Input states

Decided target (2026-11-01). An input has exactly one state at any time, and that state is the vocabulary of status:

| State | Meaning |
|---|---|
| `capturing` | reading and keeping up |
| `backlogged` | reading, with records waiting behind the cursor |
| `quiet` | nothing new, within its expectation |
| `stalled` | a continuous source silent past `silent_after`, or a reader that fails on every attempt while its process lives |
| `held` | a remote producer that was capturing has not reported past a point while out of contact ([hosts](hosts.md#contact-held-data-and-coverage)) |
| `unavailable(cause)` | `not_present`, `permission_denied(grant)`, `awaiting_credentials` or `not_applicable` |
| `excluded(reason)` | the operator's exclusion |
| `finished` | a one-shot stage that read everything |

The state is computed from facts already stored (cursor, last record, expectation, last attempt and its outcome, the producer's capture report); nothing stores the state itself, so it cannot drift from them.

## Completeness

Decided target (2026-11-01). Three questions always have an exact answer: what does Sinex hold, how is it known to be complete, and how did each fact get here. A quiet source and a dead one look different everywhere, and missing data is visible as missing in every answer, not only on a status page.

**Coverage** is, for each `(source, input, interval)`, one of `complete`, `gap(reason)`, `unknown`, `outside_span`, `not_yet` (its next opportunity has not come), `excluded`, `unavailable(cause)`, or Sinex's own `held` ([hosts](hosts.md)). The other seven states are the vocabulary shared with Polylogue and with `fill` ([query-language](https://github.com/Sinity/polylogue-sinex-common/blob/master/crates/query-language/README.md)). It is computed from expectations, cursors, material time spans, export spans, gaps and producers' capture reports.

- **The coverage read model** is maintained from the commit log with a consumer position, like search, at hour grain per input, so a day grid or a minute lookup is an index read. It is never a second source of truth: resetting its position rebuilds it from events. Source health, recall's coverage header, fill, Lynchpin-retirement parity and the Sources grid all read it, so they agree.
- **A coverage facet on every timed answer.** A query over events with a time span returns, beside its rows, `coverage: [{source, complete_fraction, gaps, unknown}]` for the sources whose kinds it read. A count over a span with any coverage that is not complete is marked `lower bound`. An empty answer over an unobserved interval never reads as "nothing happened".
- **Accounting identity.** Per material and per input: records framed = admitted + repeats + revisions + removed at source + failures + accounted headers. Every number on the inputs board is a sum over this identity, and a mismatch is a defect, not a status.
- **Silence has a deadline.** A continuous source silent past `silent_after` becomes `stalled`, records a `capture.gap` with reason `stalled`, and raises one interrupting notice per stall, with its evidence, the last success and a next action: "Shell history (Atuin) stopped: no record since Tue 3 Mar 09:14 although zsh history recorded 212 commands since; the reader reports `database busy` on every poll." Recovery clears it, and a later catch-up read never deletes the gap. An unchanged successful poll is never a stall, and low counts alone never raise one.
- **Expiry race.** For a source with `expires_after`, status shows the margin between its last capture and the provider's expiry, and a notice fires before data is lost at the provider (interrupting when the margin is below one poll period). Placing such pollers on an always-on host is the structural fix ([placement](hosts.md#placement)).
- **Export freshness.** For export-only providers, status shows the newest covered day per product and how old the newest export is, so "take a new export" is a prompt with a reason.
- **Signals.** Stall (interrupting), expiry race, runaway volume, refusal-rate spike, disk runway below 14 days, new input, parser arrival and stage finished (passive) are shipped rules ([signals](interfaces.md#signals)).

## Retention and redundancy

**Nothing is deleted by time.** Not materials, spools, self-observation, gaps or the confirmed stream. There is no garbage collection of source materials; only crash-left unreferenced files are removed. Removing data is the operator's act through the [lifecycle](lifecycle.md), and disk space is the operator's concern. (ruling 2026-05-03, cc-queued:71f182b0-73fb-4faf-81e3-31d6b40d2bb2@2026-05-03T22:00:23Z). (ruling 2026-09-02, cc-queued:0704fd51-a688-4dc7-8b55-8e389e0703c5@2026-09-02T18:55:04Z). (ruling 2026-10-11, cc:e3e37f29-2490-47ae-89ef-fd40b6242e9a:d716993a-f5d6-4422-aa5e-97d91e430d93).

**Noise is fixed where it starts.** Log spam and self-capture loops are fixed at the noisy unit or the feedback path, never by retention caps or capture filters.

**Redundancy is by design.** The same fact seen by several sources stays several records; [fusion](derivations.md#fusion) relates them where a family is defined. Only double import of the same content is prevented. (ruling 2025-06-12, cc:979c375c-989d-4b8a-8891-1541aaafe1d0:32f70e17-ab5f-4687-a6a3-ccff649985ae).

**Sinex observes itself as a source.** Its own telemetry is captured as ordinary `sinex.telemetry` events backed by materials, through the same admission, commit log, visibility, replay and query path as every source ([storage](admission.md#storage-representation)). Activity derivations select their declared kinds and never treat these samples as operator activity. One snapshot of several gauges is one record. The write rate is bounded by declared sampling of current gauges, and nothing Sinex logs about its own capture is captured back into a loop.

## Common record rules

Every source follows these.

- **Every natural record yields an event** (generic or typed) or a record failure citing its bytes. A native list item or time-series sample is a record even when the vendor wraps many in one JSON array, bin or file. Media segments are records of a continuous signal whose frames and samples stay addressable by relative time. Administrative headers are accounted as metadata, not activity. No parser combines records to keep the count down; coarser figures are derivations.
- **Nothing plausible hides a gap.** Unknown categories stay queryable raw records marked untyped; a malformed known field becomes a record failure with its bytes kept. No parser returns a plausible zero, empty string or current timestamp in place of missing evidence.
- **Identity.** Provider IDs scoped to the actual account, device and database generation; never a display name, mutable title or file position when a provider ID exists. Repeated equal commands, messages, plays and samples stay distinct unless evidence shows they are the same record. File offsets are byte offsets, never array indices or line numbers. The key model itself is in [events](events.md#identity).
- **Time.** Event time, acquisition time and processing time are separate. Source precision, zone text, uncertainty, boot and monotonic clocks and device clock anchors are kept. Civil dates stay dates, not midnight UTC. Ambiguous daylight-saving times stay ambiguous unless the source settles them. A vendor's daily total is a legitimate native record, distinct from a Sinex daily derivation. The time model is in [events](events.md#time).
- **Change and partial reads.** Source changes create newer readings while earlier material is kept. Missing paths, refused APIs and incomplete scans are unavailable or partial, never completed and empty. An oversized valid record is processed whole.
- **Credentials** come from runtime files and never enter material, cursors, diagnostic arguments or logs. Encrypted source values (cookies, password-manager exports, end-to-end chat stores) stay encrypted as captured unless the normal authorized client supplies plaintext; Sinex adds no new key access.
- **Capture is read-only.** No source mutates a provider, sends mail, edits a ledger, submits a form, replays a POST or changes a device setting.

## Performance of capture

Targets are stated per event and per byte so any host can check them: raw records per second, raw bytes per day per source, admitted events per second, database and index bytes per event, WAL per event, peak memory, oldest backlog and derivation lag. Absolute rates are reported together with the host that produced them. The admission throughput target is in [admission](admission.md#performance); capture's own targets are:

- **Freshness.** Discrete live records are visible within 5 s at p95; media within one closed segment plus 5 s; polling sources within their declared interval plus 5 s. A catch-up drains at a net positive rate while live capture continues.
- **Admission on real shapes.** Throughput is verified on dense, real-shaped records (fitness points, audit records, telemetry, journal entries), not only on a synthetic count.
- **Memory** scales with read buffers, parser carry and the largest record being parsed, never with the input or its history. File, archive and media bytes stream into storage; no whole-file byte buffer is required.
- **Coverage reads.** The Sources grid over 7 days and all sources answers in ≤ 100 ms from the coverage read model (one row per input-hour); the coverage facet adds ≤ 10 ms to a query; stall detection follows `silent_after` within one evaluator tick (≤ 60 s).
- **Upstream page limits** are paged to exhaustion, and a limit the provider imposes is recorded as its coverage.
- Performance is measured as paired benchmark runs ([testing](testing.md)); no test asserts elapsed time.
