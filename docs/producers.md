# Producers and outside consumers

This page is the public contract for any program outside the Sinex daemon that writes to Sinex or reads its committed changes, in any language: the NATS ingress, the producer client, how refusals are handled without stalling capture, the confirmed stream, outside consumers and handlers with their credentials, kind registration, and lifecycle and replay requests on the wire. The one admission function behind the ingress, the commit log and storage are in [admission](admission.md); the meaning of archive, restore, replay, undo and tombstone is in [the lifecycle](lifecycle.md); which fields a reader may see is in [privacy](privacy.md); spools and capture survival for Sinex's own capture programs are in [sources](sources.md). What Polylogue sends and how it applies the stream is in Polylogue's [primary mode](https://github.com/Sinity/polylogue/blob/master/docs/primary-mode.md).

## The ingress

**One admission owner serves in-daemon writes and producer frames.** Native
sources, core derivations and [typed client requests](interfaces.md) submit
through the daemon. Outside producers use NATS to deliver their own materials
and readings and receive durable admission receipts. The daemon publishes
committed changes in order on [the confirmed stream](#the-confirmed-stream).
NATS provides the producer queue and the outside change feed. PostgreSQL records
Sinex's committed progress, and outside consumers record the progress they
have applied.

The daemon binds its public API socket after NATS acknowledges all producer request subscriptions on that connection. This startup boundary lets a client issue its first producer RPC immediately after the new socket becomes available, including after a daemon restart. Subscription startup failures propagate through daemon supervision.

### Frames

A producer sends typed frames on its own ingress subject and receives typed replies.

| Frame | Carries | Sinex does |
|---|---|---|
| `hello` and kind registration | protocol version, producer identity, the producer's kind declarations and versions (each with its [recall class](recall.md#recall-classes)); decided target: a clock sample `{wall, monotonic}` | refuses mismatches here; validates the kinds against the registry ([kind registration](#kind-registration)); replies with its receive and send times, which give the [clock offset](hosts.md#time-across-hosts) |
| material | material bytes and their description (input, observation, digest), in as many frames as the material needs | checks the bytes against their digest, stores them and records the material; acknowledges |
| events | readings located at records of acknowledged materials, with producer version and keys | admits them through the one admission function; replies with each event's outcome |
| progress | the producer's position in its own inputs; decided target: a capture report per input (`captured_through`, held records and bytes, free spool space, capture state) and the same clock timestamps as `hello` | records it with the work it covers; the capture report feeds [contact and held coverage](hosts.md#contact-held-data-and-coverage) |
| gap | a `capture.gap` interval the producer knows it missed | admits it as a gap of that input |
| `have` | a list of frame identities | replies which of them are recorded, with their outcomes |
| producer replay | a replay scope and the new readings for it | see [producer replay](#producer-replay) |
| lifecycle request | archive, restore, undo or tombstone over a scope | see [lifecycle requests](#lifecycle-requests) |

**Material before events.** A producer sends a material's bytes and waits for their acknowledgment before it sends events that cite them. Admission therefore never waits for material, and a refused events frame never loses bytes (see [refusals](#refusals)). How a material is described (inputs, observations, revisions, record offsets) is in [materials](materials.md); how a reading is identified (record coordinates, source record keys, reading order, record hash) is in [events](events.md#identity).

Decided target (2026-11-01): encoded custody and extent reads (the next three paragraphs) go. Producers fetch materials by ID through the producer client, and one framing for spooled frames is enough; exposing the segment layout to producers couples them to Sinex's storage.

**Encoded custody.** Producers prepare material bytes, oversized frame JSON and saved fact journals through the shared framed codec. Names and lengths still identify the decoded source bytes. A location-free descriptor binds the codec, frame index, encoded digest and decoded identity. Chunk offsets and totals count encoded bytes. The daemon checks every identity before append and checks the encoded digest again while appending; plaintext never needs a staging file. A local hardlink of the producer's frozen inode is an optimization, and cross-filesystem custody copies only encoded bytes. A server request never names a client-selected path. A material frame may list its bytes as content-defined chunks (`chunks`, each a name and length); the frame directory then holds the encodings of the chunks Sinex does not hold, and the acknowledgment carries one extent descriptor per chunk, in order ([materials](materials.md#bytes-before-rows)). Parser snapshots copy encoded bytes and expose a bounded decoded reader.

Producer positions and unacknowledged journals use the same encoding and one synced atomic file replacement. A position is rewritten only when it changes: a poll that reads nothing new leaves the file as it is, so an idle binding costs no sync or rename. An upgrade must drain or explicitly convert older-format state before changing its format; the new runtime does not interpret a plaintext journal as an empty position. The explicit synthetic `sinex-transport` example `frame_permissions` qualifies distinct numeric-UID filesystem custody when run as root; unprivileged permission tests remain in the default suite. Local frame custody errors preserve the original filesystem I/O error, including its kind and OS code. Invalid names, mismatched bytes and configured group or mode violations remain distinct validation errors, so a producer can classify local refusal and transient I/O without inspecting error text or repeating the filesystem operation.

**Extent reads.** A committed material acknowledgment carries its encoded segment extents. Replay listings return the same descriptors with the decoded length; a shared-store reader opens those extents directly. Material fetches expose bytes rather than segment locations. A content digest identifies bytes and does not supply their storage location.

**Material byte pages.** An authenticated `FetchMaterial` names the material, an inclusive decoded-byte `offset`, an optional exclusive `end`, and a positive `byte_budget`. The reply carries the material metadata, page offset, decoded bytes and `next_offset` when the requested range continues. An omitted end means the material's length; an empty range returns an empty final page. Each request checks current family, kind, class, privacy and sealed-byte access. Page budgets bound each transfer without limiting material size. Shared-store extent reads are an internal client optimization. Same-host producers hand off frozen encoded files locally and send only the frame; only a configured remote upload root transfers chunk messages.

**Event outcomes** are the admission function's: admitted, repeated, superseded, older, removed, failed, superseded-in-batch, and dropped as `superseded_input`, with the committed position and the event IDs Sinex assigned. A producer is told success only after commit.

**Acknowledgments are typed.** Each names the frame, the order Sinex recorded it in and the hash of the frame body recorded under that name, and carries exactly one result: `admitted` (the registered material and its extents, the admitted event IDs, the counts above and the last commit position), `kinds_registered` (each declaration with its definition hash), `replayed` (the replay's operation, its admitted event IDs and last position) or `refused` (the refusal kind and reason). A conflict refusal carries the hash of the body already recorded, never the offered one. `core.ingress_frames.outcome` stores the same result, tagged by `result`, beside the frame's columns. The connect reply (`welcome` or `refused`), the consumer reply (`provisioned`, `snapshot_required`, `gap` or `refused`), the snapshot reply (`page`, `restart` or `unavailable`) and the fetch replies (`event`, `erased` or `refused`; `material` or `refused`) are typed the same way, so a reply that is two things at once cannot be written or read.

### Write identity

- **Each frame has one saved identity and immutable contents.** A producer prepares a write, saves it, and only then sends it. A retry reuses the identity and the contents; a later write gets a new identity even when its contents are equal.
- **A redelivered or resent frame gets its recorded outcome** and is never admitted twice. Sinex records each frame's outcome in the transaction that admits it, so the answer to a resend after any crash or outage is the original answer.
- **Material digests deduplicate bytes**: the same bytes sent twice are stored once; whether they make one material or two follows [materials](materials.md).
- **Reading order** for a stable key is assigned or checked by Sinex and never taken from a producer's clock ([reading order](events.md#identity)). A frame may offer `reading_order: null` for writer assignment. In the admission transaction, each `(source, kind, record key)` advances an order counter; retrying the same acknowledged frame returns its saved result without advancing it. An explicit integer keeps the source's reading order and raises the counter to it when higher. Each counter must cover the history of its own key, including archived readings. With no counter for an offered key, Sinex must establish that key's prior order before treating it as new. The current implementation seeds only keys offered in the first counter-bearing frame of each source and kind. Completing historical order for later offered keys is implementation work.

### Producer identity and credentials

Each producer identity has its own credential and its own frame namespace. A product running on several hosts is several producer identities (one per host) under one kind family; convergence through stable source record keys holds whichever host sends a reading. Decided target (2026-11-01): an identity is the pair (component, host), its credential is an NATS nkey whose private half never leaves the host and whose public half is in configuration, and a frame's host is its credential's host; a host named inside a payload is a claim ([hosts](hosts.md#producer-identity-credentials-and-sending)). This replaces per-producer passwords and the separate attested-host option. A producer credential may publish to its own ingress subject and read its own acknowledgment subject and inboxes; it cannot read another producer's subjects.

### The ingress stream

- **A work queue with no age limit and no delivery cap.** A frame stays until it is answered, through an outage of any length.
- **A transient failure** (database unavailable, writer busy) is answered with a delayed negative acknowledgment, and the frame is delivered again. It is never reported as a refusal or as an empty success.
- **A full stream discards nothing.** It refuses new frames, so producers pause or keep spooling locally until there is room; no queue drops its oldest messages.
- **Protocol version and configuration mismatches are refused at connect**, with a typed reply naming both sides' versions. Every reply is typed, and the protocol version is explicit in the connection.

### Producer replay

A producer that re-parsed its own materials (a parser upgrade, a corrected kind) declares the replay scope and sends the new readings. Sinex archives the old readings of that scope and admits the new ones under the [replay](lifecycle.md#replay) rules, in the same transactions, and the replay can be undone like any other. A parse that fails or yields nothing leaves the old readings live.

## The producer client

The producer client is the Rust implementation of this contract, in the `sinex-transport` crate. Sinex's own capture programs and Polylogue use it; a producer in another language implements the same behaviour against the frames above.

- **Save before send.** Bytes and frames are written to the producer's local spool before they are sent, so nothing is lost when NATS or Sinex is down. Capture starts before any connection exists and continues while either is unavailable (capture survival for Sinex's own programs is in [sources](sources.md)).
- **Retry forever.** Publishing retries with interruptible backoff. There is no attempt count and no age limit.
- **Resend on reconnect.** Unacknowledged frames are sent again, under their original identities; Sinex answers repeats from its record. A producer may send `have` to learn which frames are already recorded.
- **Remove only what is acknowledged.** Spooled bytes and frames are removed only once acknowledged and behind the producer's committed position.
- **Positions follow commits.** A producer's position in its own inputs is saved only for rows Sinex has committed; an empty fetch is not "caught up".
- **Fetch by ID.** The client fetches material bytes and ranges, and events in pages by ID (`FetchEvents`), each under the caller's grants ([privacy](privacy.md#output-parity)). A page answers each ID in request order: the complete event, its tombstone skeleton, or a typed refusal (`denied`, `missing`, `locked`, `unavailable`) for that event alone; a caller with no read grant, or a malformed request, is refused as a whole.
- **Transfer classes and connect on demand** for producers on metered or battery-powered hosts ([hosts](hosts.md#producer-identity-credentials-and-sending)).

**One client, one conformance kit** (decided target, 2026-11-01). The client lives in `sinex-transport` with an API of wire types only; Polylogue uses it and keeps no producer client of its own. The contract's test is a conformance kit in devkit: any producer in any language runs it against a test Sinex, and it covers resend, crash at every step, refusal, outage and `have`. Sinex's own producers, Polylogue and any third-language producer (a phone app through a generated binding) pass it.

## Refusals

**A refusal never stalls capture.** Because material bytes are acknowledged before the events that cite them, every refusal has a stored material to point at, and the producer's binding can move on.

| Refusal | What happens |
|---|---|
| content refusal of an events frame (a reading the registry or admission refuses) | becomes a record failure on that material, tied to its bytes; the binding advances past it |
| byte mismatch (bytes that do not match their digest) | resent from the producer's spool; a repeated mismatch is recorded as a failure |
| protocol version or configuration mismatch | refused at connect, before any frame is accepted |
| malformed frame | a code defect: refused and reported as an error |

- A fixed parser or kind version re-reads the materials that carry refusals by replay; nothing is held back waiting for the fix.
- The refusal rate per source and kind is a status figure ([status](interfaces.md#status)).
- **There is no dead-letter store.** A refused reading already has its bytes stored and its failure recorded against them, so a second place for it would only hold a copy.

## The confirmed stream

**Every committed change after the stream start position is published in commit order.** A stream instance that starts at a current-state position gives each new consumer a snapshot of the current state, followed by complete deltas. It is the delivery log for everything outside the daemon.

### Content

- **Changes**: every committed event, and every lifecycle change: archived, restored, tombstoned, and material status changes ([the lifecycle](lifecycle.md)).
- **Each data message carries its global commit position**, the event ID and the change. Archive change IDs identify rows; they are never consumer positions. Events precede lifecycle changes within one position, with lifecycle rows ordered by their stable change IDs.
- **Completion is explicit.** `PositionComplete { commit_seq: P }` says every change through P has been published. It is cumulative and includes empty positions. Consumers stage partial positions, expose them and advance their position only on this marker. Transfer pages can divide any position; page budgets never cap its changes. An empty store emits position zero to signal readiness.
- **Events travel as references.** `Event { commit_seq, event, event_type, class }` carries the commit position, the event ID, the kind and the stored class, and never a payload, for every class. A consumer fetches the events of each delivered batch in one page by ID under its own grants ([fetch by ID](#the-producer-client)); a class outside its grants returns a typed refusal for that event.
- **Why references.** A reference holds only what a tombstone skeleton keeps forever, so nothing on the stream ever needs erasing, and every payload a consumer reads is read under the deletion state and rules in force at that moment. A payload copy on the stream would stay outside later tombstones and privacy rules unless Sinex recorded every copy's place to erase it. The stream would also copy every payload a second time, while rebuilds already read PostgreSQL in snapshot pages and a page fetch serves the rest. The stream therefore carries references and stays durable, in commit order and without an age limit.
- **Routing uses the stored class.** An event routes by its class and a lifecycle notice by the class recorded with the change.
- **Subjects express the configured scope.** Events use `confirmed.event.<class>.<kind>`, lifecycle notices use `confirmed.lifecycle.<class>.<kind>`, and global completion and gap controls use `confirmed.control`, under the configured root. The daemon provisions each durable consumer's class and kind filters.
- **Consumer refusals carry a kind.** A producer with no configured confirmed-read grant receives `denied`. A malformed request receives `invalid`. A database or broker failure receives `unavailable`, so a reader can distinguish an outage from withdrawal of its access.
- **One stream for every consumer.** There is no per-consumer stream: each consumer's grants apply when it fetches events.

### Snapshots

A stream instance created over a database that already holds history starts at a current-state position H. A consumer that has not followed this instance asks for a durable consumer and is answered `snapshot_required` (stream start position, current position, scope, stream instance). It then reads the snapshot page by page with `ReplayQuery::Snapshot { cursor }`:

- The first page fixes the snapshot: the current commit position B, the producer's scope and installed catalog, the stream instance, and the last stream message before the database read (its sequence and fingerprint, read from the broker). Publication follows commit, so B covers every event that message or any earlier one delivered.
- Each page carries up to 256 readings of the catalog's kinds in the consumer's classes, as of B, in commit order: whole for classes its grants lift, as references for sealed readings, each with its archived state and last lifecycle position at B. Original admission positions are kept.
- The cursor is plain data. On every page Sinex recomputes the scope and catalog from the producer's current grants and the stream instance from the broker, checks that B is within history and that no reading of the selected kinds at or below B was tombstoned since, and checks that the catalog's kind definitions are still registered. Any change answers `restart` with the reason, and the consumer discards its partial snapshot. A database or broker failure answers `unavailable`, and the consumer asks again with the same cursor.
- The last page carries `SnapshotComplete { position: B, scope, stream_instance, last }`. The consumer applies the whole snapshot atomically and saves it with position B; a partial snapshot is never an applied prefix. Its next consumer request names `start: { stream_instance, last }`, and Sinex reads that message back from the broker and starts delivery after it (from the first sequence when there was none). After its first completion marker the consumer resumes with that marker instead.

### Retention

**The confirmed stream keeps every delivery after its start position**: no age limit, no compaction, no discard. Stream size is a storage cost, measured and reported in status.

**A tombstone erases nothing from the stream.** A published reference holds only skeleton fields, so it stays. The tombstone's notice carries the newly allocated tombstone position and the original admission position, and a fetch of the event returns its skeleton. A stream built after the tombstone leaves the original position an empty slot. Payload-free archive history and lineage remain available for reconstruction.

### Rebuild and outages

- A new stream instance records a serialized PostgreSQL start position H and a store identity derived from the database's secret in its metadata. If the broker store is lost, its replacement gets a new start position and consumers take a new snapshot. Restoring a database below H or belonging to another store refuses the stream. Missing tombstone metadata from older history stays unknown and unchanged; the schema keeps that closed set while rejecting every new incomplete tombstone. The full-history reader still reports those unknown positions as a named gap.
- The stream records its contract version (6: events as references) and start position in its metadata, independently of the producer protocol version. A stream with another contract is refused and kept; deployment deletes it or selects a fresh namespace, and consumers take a new snapshot from the new instance.
- Restarting the same broker store resumes its saved PostgreSQL checkpoint. Data items have stable IDs, so replay before a completion marker is idempotent; handlers also deduplicate staged items after the broker's duplicate window expires.
- A NATS outage pauses the publisher, which resumes from its saved position; derivation inside the daemon continues.
- **Identical sequences after the start position.** For any filter and positions after H, the in-daemon commit-order reader and the confirmed stream deliver identical `(commit position, event ID, change)` sequences, and a consumer that fetches each batch's events in a page applies the same payloads. They differ only in an event tombstoned after it was published: its reference stays, its fetch returns the skeleton, and its tombstone notice follows.
- Publication reads only the routing fields of one page's events, in one batch under the page's snapshot; it reads no payload. Batch lookup order never changes feed order or completion markers.
- **A store that lost its newest messages.** JetStream acknowledges a publish before it syncs its files, so after a host crash the stream can lack messages the publisher's saved position covers. Before each pass the publisher reads back from the stream's newest message to the completion marker at or below its saved position; if that marker is older, the position moves back to it and the lost positions are published again. Consumers skip positions they already applied.

**Resuming.** A consumer request carries the completion marker it applied last: stream instance, sequence and a fingerprint of the stored message (sequence, original broker timestamp, subject and body). Sinex reads that message from the broker and resumes delivery at it. A changed instance or an erased or changed marker answers `snapshot_required`; a broker failure refuses for retry.

## Outside consumers and handlers

**An outside consumer is declared in configuration.** Each one (Polylogue, an outside derivation, an agent runner or any other handler) has an identity and a credential that may request its daemon-provisioned, grant-scoped durable consumer and publish to its own ingress subject. Its token's grants decide what it sees, under the same output rules as every other caller ([grants](privacy.md#rules-classes-grants-and-actions)). A producer's consumer normally holds the grants for its own kinds, so it fetches its own payloads complete.

An outside handler declares:

| Declaration | Meaning |
|---|---|
| identity | its producer and consumer identity |
| version | the version of its logic; outputs carry it |
| input filter | which kinds and sources it reads |
| start position | a snapshot position followed by all later complete deltas |

It then behaves as follows:

- **It sees only committed facts**, under its grants, in commit order, plus lifecycle messages.
- **Its completed position and the completion marker it applied** commit with the corresponding local result. A reconnect names that marker; Sinex reoffers it, then later data, and does not redeliver the original history. Broker acknowledgments and a start sequence alone do not show completion. A separate staged cursor can acknowledge saved partial pages without moving the completed boundary. A late old marker cannot move that boundary past unapplied data.
- **Its outputs return through the ingress** with source, version, a stable output identity and parent citations, and follow the [derivation](derivations.md) rules, including whole-chain archival: when an input is archived, the outputs that cite it are archived with it.
- **Delivery is at least once.** Duplicates never duplicate conclusions, because outputs have stable identities; external side effects are the handler's to make idempotent.
- **A filter or version change** requires an explicit start decision, with the resulting lag visible in status.
- **An irrecoverable position** (one whose history is unavailable) stops the handler with a named gap; it never silently starts from now.
- **Lag and failures** of every outside consumer appear in [status](interfaces.md#status).

### Application writes

The current ingress registers kinds and materials, admits frames of readings
with parent citations, and records frame receipts. Frame retry by itself cannot
replace a complete keyed result or prove that a read used to plan a write is
still current. Core derivations use a separate, in-daemon batch for that
purpose. Its availability in the daemon does not make it a public capability.

The target public contract uses the existing admission and operation owners.
An application reads a fixed context and submits its input references and
selected scope, the expected current records or slots, and an operation
identity. Preconditions can express absence as well as an expected revision.
The read boundary and scope must cover new qualifying inputs as well as
changes to the records returned. A
keyed replacement also supplies its producer namespace, version, key and
complete output set, including an empty set. Sinex checks preconditions and
input changes relevant to the offered scope at admission. When accepted, it
commits outputs, retires superseded memberships, and records the receipt with
the write. If it applies a very large replacement in batches, it keeps the
key pending through intermediate commits and reports only actual progress.
A change outside the relevant scope leaves the submission valid. A stale
submission receives a conflict and can read again. An identical retry gets
its original receipt, and different content using the same operation identity
is refused.

Application code decides its domain rules and evidence. Sinex owns authority,
provenance, atomic precondition checks and retry. This lets an application
publish a changed result even when its new computation yields no outputs,
and keeps concurrent curation requests from overwriting one another. These
public writes are target work. Their implementation belongs in the generic
operation and producer interfaces, without application-specific writer jobs,
dispatch handlers or daemon-loaded application code.

## Kind registration

**The operator installs each producer's catalog.** Sinex compiles in no outside producer's kinds. The installed catalog is immutable from startup and binds a `SourceRef` (name and version), its family, and the full emitted kind definitions. A credential has separate allowed source prefixes, kind prefixes and classes. Registration accepts only definitions present in that installed catalog; conflicting definitions under one source or kind version are refused.

A kind declaration has the same shape for an outside producer as for Sinex's own kinds: name, version, payload fields, [field hints](privacy.md#field-hints) per field, keying, and search participation (`none`, `words` or `words_and_trigram`). Admission checks every event against its registered kind, so an outside kind gets the same checks, read models and output rules as a built-in one. The per-kind typed views are generated from the registry over live readings.

**Polylogue registers the `polylogue.*` kinds**, declared in its `hello` with each kind's recall class: `polylogue.session`, `polylogue.prompt`, `polylogue.tool_call`, `polylogue.live_interval`, `polylogue.commit_link`, `polylogue.delivery` and `polylogue.assertion`. Sinex knows none of them by name; recall, agency and episodes read them through their declarations. Their declarations live in Polylogue's own code. A change to a `polylogue.*` kind ships with Polylogue's generated catalog; the operator installs that catalog before its producers register the new definitions.

## Lifecycle requests

**Outside producers only add; deletion is the lifecycle.** A producer sends materials, events and progress. To take something out of the live set it sends a lifecycle request, exactly as a CLI or MCP caller would; there is no producer-specific deletion route and no ingress subject that removes materials or events.

- **Requests**: archive, restore, undo and tombstone over a scope (references, a query, a material, a run), with the producer as actor, on `<root>.lifecycle.<producer>`. Tombstoning is reserved to the operator ([one command set](interfaces.md#one-command-set)), so a producer's tombstone request is answered only as a comparison.
- **Same answers and refusals as the CLI.** The semantics, the whole-chain cascade and the refusal rules are [the lifecycle's](lifecycle.md#archive); the wire carries the same operation.
- **Preview first.** A request may ask for the comparison only: Sinex returns the cascade the operation would apply and writes nothing.
- **A refusal names the part that cannot move.**
- **Every operation is recorded** with its computed actor ([operations and actors](lifecycle.md)).
- **`stopped_early` is never reported as ok.** A multi-batch operation that stops after moving some batches says what moved and can be resumed.

Producer replay frames are described under [the ingress](#producer-replay).
