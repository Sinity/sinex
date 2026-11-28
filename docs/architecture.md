# Architecture

Sinex records a person's digital activity as source materials and citable
readings. This page connects the product boundaries, data flow, durability
and configuration. Each linked area owns its behaviour. [AGENTS.md](../AGENTS.md)
maps the crates, and [vocabulary.md](vocabulary.md) defines the terms.

## Purpose

Sinex is the substrate for everything the operator's life produces: shell history, browsers, the desktop session, mail, chat, exports from online services, files, devices, audio, and the AI sessions Polylogue sends. Its core takes a firehose, survives interruption, and says what it holds, what it failed to read and what it has not processed yet. Above the stored readings it builds derived events in layers (fused records, intervals, presence, activity, figures) and answers questions about all of it through one query language, recall, the CLI, MCP, HTTP and the reader web.

## Command surface

The [decided target](interfaces.md#cli) is a selection-first `sinex` client and
three nested MCP tools. A selection is resolved once, typed verbs run in order,
and the existing request/admission owner records writes and retries. The client
rename and command port are implementation work; generated help and the command
table describe the current build. The daemon remains `sinexd`, with maintenance
under `sinexd admin`. The chosen task bounds each delivery while the domain pages
retain the full future design.

## Scope

- **Sinex's concepts are its own.** Polylogue is about AI work: sessions, their lineage, and the accumulated understanding of them (Pages, annotations, judgments). Sinex is the substrate. Similar words do not make a shared concept, and neither product bends a concept to fit the other. (ruling 2026-10-27, cc:ee52794c-8a9e-43b6-9fbc-9749c7e78e8f:a5b1490b-01e2-4dd1-9c85-8370ccc4bfee).
- **Polylogue runs alone or on Sinex.** On Sinex (primary mode) Polylogue is an outside producer and consumer like any other: it sends its materials and readings through the ingress and reads the confirmed stream. Sinex has no AI-session parser of its own. What Polylogue sends is described in [Polylogue's primary mode](https://github.com/Sinity/polylogue/blob/master/docs/primary-mode.md); the generic contract is [producers.md](producers.md).
- **Media curation and personal analysis are separate applications.** They own
  their questions, rules, views and user workflows. Sinex owns source capture,
  material and event storage, lifecycle, access, queries and generic admission.
  Applications read through the public client and submit authored requests or
  producer outputs through public contracts. The current daemon still has
  feature-specific handlers, derivations and writer work. These are the
  implementations to move out. See [library.md](library.md),
  [evidence.md](evidence.md) and [the producer contract](producers.md#application-writes).
- **Code is shared only where the mechanics are identical:** the segment container ([hashed-blobs](https://github.com/Sinity/polylogue-sinex-common/blob/master/crates/hashed-blobs/README.md)), the query grammar and the logical day ([query-language](https://github.com/Sinity/polylogue-sinex-common/blob/master/crates/query-language/README.md)), the operation table and failure kinds ([command-surface](https://github.com/Sinity/polylogue-sinex-common/blob/master/crates/command-surface/README.md)), the web frame ([web-shell](https://github.com/Sinity/polylogue-sinex-common/blob/master/crates/web-shell/README.md)), the signal rules ([signals](https://github.com/Sinity/polylogue-sinex-common/blob/master/crates/signals/README.md)) and the test tooling ([devkit](https://github.com/Sinity/polylogue-sinex-common/blob/master/crates/devkit/README.md)).

## Principles

**Raw bytes are truth; events are readings.** Each source reading cites a
record in retained material. When the input is location-only, the reading cites
the retained listing, and the original contents remain with their external
owner. Derived readings cite their inputs. A stored digest identifies bytes,
a material identifies an acquisition, and a record key identifies what can
have a newer reading. See [materials.md](materials.md) and [events.md](events.md).

**Native inputs share staging.** The same acquisition, parsing and admission
path serves live bindings and accepted historical input. Discovery follows new
activity automatically and lists existing history for explicit acceptance.
Outside producers may acquire and parse in their own program. Both routes
commit through the same admission owner. See [materials.md](materials.md#staging)
and [producers.md](producers.md).

**Nothing is deleted by default; deletion is a lifecycle.** Readings are live, then archived (reversible), then tombstoned (irreversible, archived only), and whole chains move together. There is no automatic retention: material is kept until the operator archives it. See [lifecycle.md](lifecycle.md).

**Invariants by construction, not repair.** A crash leaves only states that the normal open path handles (see [durability](#durability-order)). There is no repair, reconcile or verify-and-fix command and no scheduled check, because a state that needs repairing is a defect in the code that produced it.

**No arbitrary limits.** Batch, turn and page sizes schedule work; nothing truncates or refuses valid input, and paged answers continue. See [query.md](query.md) and [interfaces.md](interfaces.md).

**Capture scope and disclosure are separate.** Output rules change what a
caller sees without changing the stored reading. Sealing preserves the evidence
in protected form. [Privacy](privacy.md) defines the target and the access policy
during the transition.

**Agents are primary users.** People and agents use the same operation
definitions. Credentials authorize operations and grants govern disclosure.
Sinex computes authorship separately. See [callers](interfaces.md#callers).

**Throughput is a product requirement.** Sources emit one event per record they read, and coarser views are derivations, so the finest grain stays queryable; the write path is built to carry that. (ruling 2026-05-01, chatgpt:69f4b37f-ebc8-83eb-a356-f21885b03f05:5c7da4ca-2e26-40cd-b1f3-25132fe1121b). See [targets](#throughput-targets).

**Sources read external formats.** A source reads data as an outside service or program produces it (an export archive, a database another program writes, a protocol stream), not a format the operator once invented to hold data; an exception is allowed but is the exception ([external formats](sources.md#external-formats)).

**Window managers and terminals are sources like any other.** Hyprland and Kitty are data sources, captured behind interfaces that make adding another compositor or terminal a new source declaration rather than a change to the core ([sources](sources.md#window-managers-and-terminals)).

**No part of Sinex depends on the operator's host.** No path, zone, day start, account name or host routing is written into the code or the repository: each is configuration, and vendor facts (the zone a service writes its timestamps in) are source declarations. Anyone with Nix builds, tests and deploys Sinex with plain commands. See [operations.md](operations.md#configuration).

## Data flow

```
source ──► stage ──► material (segment log) ──► parse ──► admission ──► PostgreSQL
                                                              │            │
                                       outside producers ─► NATS ingress   ├─► read models (search, detector hits)
                                                                           ├─► in-daemon derivations ─► admission
                                                                           ├─► confirmed stream ─► outside consumers
                                                                           └─► query, recall, CLI, MCP, HTTP, web
```

1. **Acquisition.** A source declaration names the inputs and how they change.
   Its driver acquires a byte stream, a set of database rows or a file listing.
   Live streams are spooled as they arrive. Admission retains material before
   committing any reading that cites it. See [sources.md](sources.md),
   [streams.md](streams.md) and [materials.md](materials.md).
2. **Parsing.** A parser reads one framed record at a time from stored bytes and opens no files, sockets or clocks; a record it cannot read becomes a record failure tied to its bytes. See [admission.md](admission.md#failures-and-progress).
3. **Admission.** One writer commits readings with their cursors, failures,
   dirty keys and operation. Native sources and core derivations submit inside
   the daemon. Public client writes go through typed operations, and outside
   producers offer material and readings through [NATS ingress](producers.md).
   All reach the same writer. See [admission.md](admission.md).
4. **Identity and liveness.** Admission classifies each reading by record key.
   A record has at most one live reading in its ordering namespace. A repeat
   is suppressed, and a later changed reading supersedes. See [events.md](events.md)
   and [admission.md](admission.md#liveness).
5. **Commit order.** Every admitted event and lifecycle change takes a commit position. Readers in the daemon follow the commit log; every committed change is published in commit order on the [confirmed stream](producers.md) for consumers outside it.
6. **Derivation.** Derivations inside the daemon read PostgreSQL in commit order and admit their outputs directly; derivations outside are subscribers of the confirmed stream, each with its own durable consumer, and send their outputs back through the ingress. Derived events are the one store of derived results. See [derivations.md](derivations.md) and [higher-layer.md](higher-layer.md).
7. **Reading.** Queries, recall, the CLI, MCP, HTTP and the reader web read the live set (or an as-of set) through the output rules of [privacy.md](privacy.md). See [query.md](query.md), [recall.md](recall.md), [interfaces.md](interfaces.md) and [web.md](web.md).
8. **Lifecycle.** Archive, restore, replay, undo and tombstone are operations through the same admission path, moving whole chains. See [lifecycle.md](lifecycle.md).

## Components

**The daemon, `sinexd`,** holds the one writer and the admission function, the binding reader that runs continuous sources, the publisher that feeds the confirmed stream, replay, the derivation runner, the read-model consumers, the API (Unix socket and HTTP), the reader web and status. Database work lives only in the daemon.

**The stores.** PostgreSQL with TimescaleDB holds the catalog and Sinex's
committed progress. Material bytes live in the append-only segment log. NATS
with JetStream carries the ingress queue and confirmed references. Before
admission, producers retain unacknowledged input and depend on their spool and
retry state. After admission, PostgreSQL is authoritative. Outside consumers
commit their own progress and use a fresh snapshot if the broker can no longer
resume at their completion marker. See [admission.md](admission.md#storage-representation)
and [producers.md](producers.md#the-confirmed-stream).

**Clients and producers.** `sinexctl` is the CLI and the MCP server, a client of the daemon with no store dependency; offline diagnostics invoke the sibling `sinexd admin diagnostics` command while the daemon is stopped. `sinex-session` captures the desktop session as the operator's user, and
`sinex-capture` runs capture outside the daemon. The current binaries can
parse and submit at the edge. The target is original-byte spooling, read by
the daemon locally or transferred from a remote capture node. This gives
Sinex's own sources one parser and cursor owner while outside producers such
as Polylogue keep their parsing responsibilities. `sinex-schema` creates and upgrades the schema. CLI, MCP and HTTP are generated from one operation table. See [interfaces.md](interfaces.md) and [producers.md](producers.md).

**The query language, `sinex-query`,** parses, binds and completes queries against the model declarations. Stored query definitions remain in `sinex-model`; the store compiles bound queries to SQL.

**Declarations.** Kinds (with field hints and search participation), sources, derivations, consumers and streams are each declared once in Rust; The NixOS module already uses the generated catalog.

## Durability order

Every write follows one order, so a crash at any point leaves a state the normal code resumes from:

1. Bytes are appended to the segment log and synced.
2. The extents and the rows that cite them commit in one transaction.
3. Core derivations and database read models save what they applied with their
   progress. The publisher saves its progress after publishing complete
   positions to JetStream and verifies it against the broker on restart.
4. Source cursors and ingress receipts advance only for committed input. An
   outside consumer saves completed progress with its own applied results.

**The writer's use of the segment log.** The single writer appends a group's new blobs, issues one sync per group on a dedicated thread overlapped with the next group, and commits the extents in the same transaction as the rows that cite them. Deduplication checks the hash index plus the hashes appended in groups not yet committed. A segment opens at its last committed extent and truncates any tail; a tombstoned extent that is marked but not yet punched is punched on open. The container format (seekable zstd frames, plain or sealed, member access, hash checks) is [hashed-blobs](https://github.com/Sinity/polylogue-sinex-common/blob/master/crates/hashed-blobs/README.md).

**Crash states.** A crash leaves at most one of the following, and the normal code resumes each:

| State left | Resumed by |
|---|---|
| an uncommitted segment tail | truncation when the segment opens |
| an unpunched tombstoned extent | the punch when the segment opens, or at the writer's next group |
| a derived store behind its saved position | the consumer reading on from its position |
| a source position behind its committed rows | re-reading from the committed cursor; admission suppresses the repeats |
| a confirmed stream missing its newest messages while the publisher's saved position covers them | the publisher's check before each pass |

**The confirmed stream is a store with its own sync.** JetStream acknowledges a publish before it syncs its files (every 120 s by default), so after a host crash the stream can lack its newest messages while PostgreSQL keeps the publisher's position that covered them. The stream keeps its name and creation time. Before each pass the publisher reads back from the stream's newest message to the completion marker at or below its saved position. If the marker is older than the saved position, the position moves back to the marker, so the lost positions go out again; consumers skip positions they already applied. Syncing every message instead would stall the stream whenever the disk is busy.

**Files appear whole.** A capture file that another step reads by its name (a media segment and its sidecar, a resume cursor) is synced before it is renamed to that name, and its directory is synced after.

How the positions themselves are stored is in [admission.md](admission.md#commit-log-and-positions); backups record the same boundary ([operations.md](operations.md#backup-and-restore)).

## Pipeline

No stage waits on another that has nothing to do with its data, and the one serial stage only appends and commits:

- **acquisition** of bytes from inputs, live inputs before bulk;
- **parsing** on blocking workers with permits, at most one batch parsed ahead of the batch awaiting its answer, memory bounded by waiting;
- **one writer**: one classification snapshot, one text `COPY` and one commit per batch;
- **bulk transactions bounded by duration**, with live batches served between them;
- **consumers** (derivations, read models, publisher) each with their own position.

The writer's priorities and transaction are in [admission.md](admission.md#admission-and-the-writer); capture-side costs are in [sources.md](sources.md).

## Throughput targets

Targets are hardware-independent: every figure is reported as bytes per event, bytes kept per event, WAL per event, syncs per group and stage utilisation, together with the absolute rate on whichever host ran it. Host tuning is deployment configuration. (ruling 2026-10-27, cc:ee52794c-8a9e-43b6-9fbc-9749c7e78e8f:453f3ef1-80cc-4269-8de5-13764daa556b).

| Target | Value |
|---|---|
| admitted, searchable and acknowledged source events | at least 15,000/s on a busy host, 40,000/s on a quiet one |
| live latency, submission to commit, during backfill or replay | p99 at most 1 s |
| idle stages | none while a queue waits for them |

Write-path targets (lifecycle operations on compressed chunks, segment throughput) are in [admission.md](admission.md#performance); query and command latency budgets in [interfaces.md](interfaces.md); how figures are measured in [testing.md](testing.md).

## Configuration and clocks

**A setting is read once, typed, and passed explicitly.** Each binary's `main` loads one typed configuration (the deployment's configuration file, then flags, then environment variables where meaningful) and validates it there: the zone resolved to a named zone (refusing to start otherwise), the day start checked, paths absolute. That value and a `Clock` are passed to every component. Libraries never read the environment or the system zone, and no component has a default constructor for a setting, so no setting can be re-read with a different default where it is used.

**One home per setting.**
- **Deployment configuration** holds what the operator sets for a machine; what it contains and where it comes from is in [operations.md](operations.md#configuration).
- **Changeable settings** (quiet hours, the interrupt budget, enabled variants, briefing switches) come from a declared catalog with a type, default and scope per key. Writes are validated and unknown keys refused; the daemon holds a typed snapshot, replaces it on change and passes it on.
- **Source options are typed** and reject unknown fields; **vendor facts are source declarations** (a service that writes local times in one zone says so in its declaration).
- **Each default is defined once;** integrations in other languages (Kitty, Neovim) read one variable the deployment sets.

**One logical day.** One type, `DaySetting` (a named zone and a start), with one implementation in [query-language](https://github.com/Sinity/polylogue-sinex-common/blob/master/crates/query-language/README.md), used by queries, SQL generation, derivations, analysis, recall and rendering. A zone travels with its name, so SQL never falls back to UTC. The setting and its default are in [operations.md](operations.md#logical-day).

**Outputs record their settings.** Derived outputs and cursors record the settings they were made under, and a changed setting re-derives them through the version mechanism of [derivations.md](derivations.md).

**One instant type, one clock, one error enum.** Sinex has one instant type, one `Clock` passed in (tests drive it), and one error enum whose kind names are stable on the wire; conversions happen only at the storage boundary.
