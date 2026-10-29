# Producers and outside consumers

This page is the public contract for any program outside the Sinex daemon that writes to Sinex or reads its committed changes, in any language: the NATS ingress, the producer client, how refusals are handled without stalling capture, the confirmed stream, outside consumers and handlers with their credentials, kind registration, and lifecycle and replay requests on the wire. The one admission function behind the ingress, the commit log and storage are in [admission](admission.md); the meaning of archive, restore, replay, undo and tombstone is in [the lifecycle](lifecycle.md); which fields a reader may see is in [privacy](privacy.md); spools and capture survival for Sinex's own capture programs are in [sources](sources.md). What Polylogue sends and how it applies the stream is in Polylogue's [primary mode](https://github.com/Sinity/polylogue/blob/master/docs/primary-mode.md).

## The ingress

**Writes inside the daemon go straight to admission; writes from outside come through NATS.** The daemon's own sources, derivations, replay, lifecycle operations and API requests call the one admission function directly. Everything else (Polylogue, `sinex-session`, `sinex-capture`, an outside derivation, any program in any language) sends frames to the NATS ingress, and Sinex admits them through the same function. Every committed change is then published, in commit order, to [the confirmed stream](#the-confirmed-stream). Ruling, 2026-10-27: "stuff internal to sinex daemon can go directly into db, extrernal stuff must go thorugh nats" (cc:ee52794c-8a9e-43b6-9fbc-9749c7e78e8f:bb282e5a-2cb6-49e9-abf8-62afd8f19287).

**NATS is integral.** Without it there are no outside producers, no frame acknowledgments, no confirmed stream and no language-neutral way to derive from Sinex. PostgreSQL stays the store of record, every position included; NATS is the delivery path to and from everything outside the daemon. Ruling, 2026-10-18: "I'm almost certain NATS is integral to the architecture … sinex architecture is quite deliberate" (cc:ee52794c-8a9e-43b6-9fbc-9749c7e78e8f:be2768ea-de74-48a0-8c8d-26a318191069).

### Frames

A producer sends typed frames on its own ingress subject and receives typed replies.

| Frame | Carries | Sinex does |
|---|---|---|
| kind registration | the producer's kind declarations | validates them against the kind registry; see [kind registration](#kind-registration) |
| material | material bytes and their description (input, observation, digest), in as many frames as the material needs | checks the bytes against their digest, stores them and records the material; acknowledges |
| events | readings anchored at records of acknowledged materials, with producer version and keys | admits them through the one admission function; replies with each event's outcome |
| progress | the producer's position in its own inputs | records it with the work it covers |
| `have` | a list of frame identities | replies which of them are recorded, with their outcomes |
| producer replay | a replay scope and the new readings for it | see [producer replay](#producer-replay) |
| lifecycle request | archive, restore, undo or tombstone over a scope | see [lifecycle requests](#lifecycle-requests) |

**Material before events.** A producer sends a material's bytes and waits for their acknowledgment before it sends events that cite them. Admission therefore never waits for material, and a refused events frame never loses bytes (see [refusals](#refusals)). How a material is described (inputs, observations, revisions, record anchors) is in [materials](materials.md); how a reading is identified (record coordinates, source record keys, reading order, record hash) is in [events](events.md#identity).

**Encoded custody.** Producers prepare material bytes, oversized frame JSON and durable fact journals through the shared framed codec. Logical names and lengths still identify the decoded source bytes. A location-free descriptor binds the codec, frame index, encoded digest and decoded identity. Chunk offsets and totals count encoded bytes. The owner checks every identity before append and checks the encoded digest again while appending; plaintext never needs a staging file. A local hardlink of an owned frozen inode is an optimization, and cross-filesystem custody copies only encoded bytes. A server request never names a client-selected path. Parser snapshots copy encoded bytes and expose a bounded decoded reader.

Producer positions and unacknowledged journals use the same encoding and one synced atomic file replacement. An upgrade must drain or explicitly convert predecessor state before changing its format; the new runtime does not interpret a plaintext journal as an empty position. The explicit synthetic `sinex-transport` example `frame_permissions` qualifies distinct numeric-UID filesystem custody when run as root; ordinary permission tests remain in the default suite.

**Extent read authority.** A committed material acknowledgment carries its encoded segment extents. Material and replay listings return the same descriptors with the decoded length; a shared-store reader opens those extents directly. A content digest identifies bytes and does not supply their physical location. This contract is negotiated as producer protocol version 2.

**Event outcomes** are the admission function's: admitted, repeated, superseded, older, removed, failed, superseded-in-batch, and dropped as `superseded_input`, with the committed position and the event IDs Sinex minted. A producer is told success only after commit.

### Write identity

- **Each frame has a durable identity and immutable contents.** A producer prepares a write, saves it, and only then sends it. A retry reuses the identity and the contents; a later write gets a new identity even when its contents are equal.
- **A redelivered or resent frame gets its recorded outcome** and is never admitted twice. Sinex records each frame's outcome in the transaction that admits it, so the answer to a resend after any crash or outage is the original answer.
- **Material digests deduplicate bytes**: the same bytes sent twice are stored once; whether they make one material or two follows [materials](materials.md).
- **Reading order** for a stable key is assigned or checked by Sinex and never taken from a producer's clock ([reading order](events.md#identity)). Protocol version 2 accepts `reading_order: null` for writer assignment. In the admission transaction, each `(source, kind, record key)` advances a durable order counter; retrying the same acknowledged frame returns its saved result without advancing it. An explicit integer keeps the source's reading order and updates the counter's high-water mark.

### Producer identity and credentials

Each producer identity has its own credential and its own frame namespace. A product running on several hosts is several producer identities (one per host) under one kind family; convergence through stable source record keys holds whichever host sends a reading. A producer credential may publish to its own ingress subject and read its own acknowledgment subject and inboxes; it cannot read another producer's subjects.

### The ingress stream

- **A work queue with no age limit and no delivery cap.** A frame stays until it is answered, through an outage of any length.
- **A transient failure** (database unavailable, writer busy) is answered with a delayed negative acknowledgment, and the frame is delivered again. It is never reported as a refusal or as an empty success.
- **A full stream discards nothing.** It refuses new frames, so producers pause or keep spooling locally until there is room; no queue drops its oldest messages.
- **Protocol version and configuration mismatches are refused at connect**, with a typed reply naming both sides' versions. Every reply is typed, and the protocol version is explicit in the connection.

### Producer replay

A producer that re-parsed its own materials (a parser upgrade, a corrected kind) declares the replay scope and sends the new readings. Sinex archives the old readings of that scope and admits the new ones under the ordinary [replay](lifecycle.md#replay) rules, in the same transactions, and the replay can be undone like any other. A parse that fails or yields nothing leaves the old readings live.

## The producer client

The producer client is the Rust implementation of this contract, in the `sinex-transport` crate. Sinex's own capture programs and Polylogue use it; a producer in another language implements the same behaviour against the frames above.

- **Save before send.** Bytes and frames are written to the producer's local spool before they are sent, so nothing is lost when NATS or Sinex is down. Capture starts before any connection exists and continues while either is unavailable (capture survival for Sinex's own programs is in [sources](sources.md)).
- **Retry forever.** Publishing retries with interruptible backoff. There is no attempt count and no age limit.
- **Resend on reconnect.** Unacknowledged frames are sent again, under their original identities; Sinex answers repeats from its record. A producer may send `have` to learn which frames are already recorded.
- **Remove only what is settled.** Spooled bytes and frames are removed only once acknowledged and behind the producer's committed position.
- **Positions follow commits.** A producer's position in its own inputs is saved only for rows Sinex has committed; an empty fetch is not "caught up".
- **Fetch by reference and by event ID.** The client fetches material bytes and ranges, fields carried as references, and fields marked withheld on the confirmed stream, each under the caller's grants ([privacy](privacy.md#output-parity)).

## Refusals

**A refusal never stalls capture.** Because material bytes are acknowledged before the events that cite them, every refusal has a stored material to point at, and the producer's binding can move on.

| Refusal | What happens |
|---|---|
| content refusal of an events frame (a reading the registry or admission refuses) | becomes a record failure on that material, tied to its bytes; the binding advances past it |
| byte mismatch (bytes that do not match their digest) | resent from the producer's spool; a repeated mismatch is recorded as a failure |
| protocol version or configuration mismatch | refused at connect, before any frame is accepted |
| malformed frame | a code defect: refused and reported as an error |

- A fixed parser or kind version re-reads the materials that carry refusals by ordinary replay; nothing is held back waiting for the fix.
- The refusal rate per source and kind is a status figure ([status](interfaces.md#status)).
- **There is no dead-letter store.** A refused reading already has its bytes stored and its failure recorded against them, so a second place for it would only hold a copy.

## The confirmed stream

**Every committed change is published to the confirmed stream, in commit order.** It is the delivery log for everything outside the daemon.

### Content

- **Changes**: every committed event, and every lifecycle change: archived, restored, tombstoned, and material status changes ([the lifecycle](lifecycle.md)).
- **Each data message carries its global commit position**, the event ID and the change. Archive change IDs identify rows; they are never consumer watermarks. Events precede lifecycle changes within one position, with lifecycle rows ordered by their stable change IDs.
- **Completion is explicit.** `PositionComplete { commit_seq: P }` says every change through P has been acknowledged. It is cumulative and includes empty positions. Consumers durably stage partial positions, expose them and advance their scalar watermark only on this barrier. Transfer pages can divide any position; page budgets never cap its changes. An empty store emits position zero to signal readiness.
- **Every event travels by reference.** `EventReference { commit_seq, event }` contains only the event ID and its original commit position. Payloads, source keys and other metadata are fetched under the consumer's current policy and grants ([privacy](privacy.md#output-parity)). The shared stream retains no inline event metadata or payload, including for the open class. A large stored payload or parent list uses the same bounded fetch path as a small event.
- **Routing uses the active transformed class.** A pending policy projection makes publication unavailable; raw admission classes and source keys are never substituted. Fetch keeps the canonical source identity separate from the returned projected payload identity.
- **Subjects express the configured scope.** Events use `confirmed.event.<class>.<kind>`, lifecycle notices use `confirmed.lifecycle.<class>.<kind>`, and global completion and gap controls use `confirmed.control`, under the configured root. The daemon provisions each durable's class and kind filters.
- **One stream for every consumer.** There is no per-consumer stream: each consumer's grants apply when it fetches withheld fields.

### Retention

**The confirmed stream keeps everything**: no age limit, no compaction, no discard. A new outside derivation reads from the beginning without falling back to PostgreSQL, so there is only one delivery path. Its size is a storage cost, measured and reported in status.

**Tombstone reaches the stream.** The publisher records every acknowledged copy's sequence together with the native stream incarnation (name and creation time). Excision deletes those exact copies. Its notice carries the newly allocated excision position and the exact original admission position. The original position remains an empty slot; rebuild never inserts an extra data tuple there. Payload-free archive history and lineage remain available for reconstruction.

### Rebuild and outages

- If the NATS store is lost, the publisher detects the new native stream incarnation and republishes from PostgreSQL with the same commit positions. Tombstoned events produce skeleton notices at their actual excision positions. Pruned commit-log rows are reconstructed from retained events and lifecycle metadata. Unknown historical positions or class metadata produce a named `Gap`; no completion barrier crosses it.
- Restarting the same broker store resumes its saved PostgreSQL checkpoint. Data items have stable logical IDs, so replay before a completion barrier is idempotent; handlers also deduplicate durable staged items after the broker's duplicate window expires.
- The stream carries reference-only contract version 3 metadata, independently of the producer protocol version. A nonempty older inline-metadata stream is refused and preserved. Conversion or a parallel stream requires an explicit operational decision.
- A NATS outage pauses the publisher, which resumes from its saved position; derivation inside the daemon continues.
- **Identical sequences.** For any filter, the in-daemon commit-order reader and the confirmed stream deliver identical `(commit position, event ID, change)` sequences. They differ only in fields withheld or carried by reference under the consumer's grants.

## Outside consumers and handlers

**An outside consumer is declared in configuration.** Each one (Polylogue, an outside derivation, an agent runner or any other handler) has an identity and a credential that may request its daemon-provisioned, grant-scoped durable consumer and publish to its own ingress subject. Its token's grants decide what it sees, under the same output rules as every other caller ([grants](privacy.md#rules-classes-grants-and-actions)). A producer's consumer normally holds the grants for its own kinds, so it fetches its own payloads complete.

An outside handler declares:

| Declaration | Meaning |
|---|---|
| identity | its producer and consumer identity |
| version | the exact version of its logic; outputs carry it |
| input filter | which kinds and sources it reads |
| start position | an explicit commit position to start from (the beginning is allowed, since the stream keeps history) |

It then behaves as follows:

- **It sees only committed facts**, under its grants, in commit order, plus lifecycle messages.
- **Its position** is acknowledged in NATS and saved durably as a commit position, with a deduplication key per message kind. It acknowledges a message only after its own durable local state and its outputs to Sinex are acknowledged, and resumes after its last acknowledged message.
- **Its outputs return through the ingress** with source, version, a stable output identity and parent citations, and follow the [derivation](derivations.md) rules, including whole-chain archival: when an input is archived, the outputs that cite it are archived with it.
- **Delivery is at least once.** Duplicates never duplicate conclusions, because outputs have stable identities; external side effects are the handler's to make idempotent.
- **A filter or version change** requires an explicit start decision, with the resulting lag visible in status.
- **An irrecoverable position** (one whose history is unavailable) stops the handler with a named gap; it never silently starts from now.
- **Lag and failures** of every outside consumer appear in [status](interfaces.md#status).

## Kind registration

**Sinex compiles in no producer's kinds.** A producer owns its kind list and registers it on the ingress; Sinex validates each declaration against the kind registry and refuses it when it conflicts with a kind already registered (a name owned by another producer, or a different declaration under a version already registered).

A kind declaration has the same shape for an outside producer as for Sinex's own kinds: name, version, payload fields, [field hints](privacy.md#field-hints) per field, keying, and search participation (`none`, `words` or `words_and_trigram`). Admission checks every event against its registered kind, so an outside kind gets the same checks, read models and output rules as a built-in one. The per-kind typed views are generated from the registry over live readings.

**Polylogue registers the `polylogue.*` kinds.** Their declarations live in Polylogue's own code and will be listed in Polylogue's [planned Sinex kinds reference](https://github.com/Sinity/polylogue/blob/master/docs/reference/README.md#planned-pages); Sinex's [planned kinds reference](reference/README.md#planned-pages) will list only Sinex's own kinds. A change to a `polylogue.*` kind ships with Polylogue and registers at its next connect.

## Lifecycle requests

**Outside producers only add; deletion is the lifecycle.** A producer sends materials, events and progress. To take something out of the live set it sends a lifecycle request, exactly as a CLI or MCP caller would; there is no producer-specific deletion route and no ingress subject that removes materials or events.

- **Requests**: archive, restore, undo and tombstone over a scope (references, a query, a material, a run), with the producer as actor.
- **Same answers and refusals as the CLI.** The semantics, the whole-chain cascade and the refusal rules are [the lifecycle's](lifecycle.md#archive); the wire carries the same operation.
- **Preview first.** A request may ask for the comparison only: Sinex returns the cascade the operation would apply and writes nothing.
- **A refusal names the part that cannot move.**
- **Every operation is recorded** with its computed actor ([operations and actors](lifecycle.md)).
- **`stopped_early` is never reported as ok.** A multi-batch operation that stops after moving some batches says what moved and can be resumed.

Producer replay frames are described under [the ingress](#producer-replay).

## Laws

- **resent-frame-gets-recorded-outcome**: a redelivered or resent frame gets its recorded outcome and admits nothing twice; the ingress keeps a frame through an outage of any length.
- **content-refusal-advances-binding**: a content-refused events frame becomes a record failure on its material, and later frames of the binding are admitted.
- **events-wait-for-material-acknowledgment**: no admitted event from the ingress cites material bytes that were not acknowledged before it.
- **mismatch-refused-at-connect**: a producer with a mismatched protocol version or configuration is refused at connect with a typed reply, and no frame of it is admitted.
- **reader-and-stream-sequences-identical**: for any filter, the in-daemon commit-order reader and the confirmed stream deliver identical `(commit position, event ID, change)` sequences.
- **consumer-credential-scope**: a configured reader pulls only its provisioned kind-and-class-scoped durable and global position controls; it cannot subscribe to unfiltered confirmed subjects, pull a foreign or revoked-scope consumer, or read another identity's reply inbox.
- **rebuilt-stream-keeps-positions**: a stream rebuilt from PostgreSQL carries the same commit positions, tombstoned events as skeleton notices, and a consumer resumes from its saved position on it.
- **stream-withholds-and-fetch-matches**: the shared stream carries only references, and the fetch by event ID applies the same current policy and grants as every other output.
- **handler-one-effective-output**: an outside handler crashed before processing, after its output was admitted, and after acknowledgment yields one effective output per input and version; an archived input archives the handler's outputs.
- **kind-conflict-refused**: a kind registration that conflicts with the registry is refused and changes nothing.
- **lifecycle-request-matches-cli**: a lifecycle request on the wire gives the same cascade, outcome and refusals as the same operation through the CLI, and records the producer as actor.
- **no-producer-deletion-route**: the ingress has no subject or frame that removes materials or events outside the lifecycle.

### Confirmed position boundaries and authenticated fetches

`EventReference { commit_seq, event }` carries every event on the same scoped topic; fetching it verifies both the event ID and position. `PositionComplete { commit_seq }` closes a global commit position after its events and lifecycle changes have been published. Consumers advance their durable scalar position only on this control message. `Gap { reason }` reports historical evidence that cannot establish an exact position and never advances it.

Successful event fetches return a mandatory payload and `ProjectionIdentity { policy_epoch, payload_hash }`. The payload hash uses SJSON1: BLAKE3 over compact JSON serialization, recursively sorting object keys by UTF-8 order, with the finite-number and string encoding defined by the protocol serializer and its golden fixtures. It identifies the returned current-policy value. `record_hash` remains the canonical source-reading hash and does not claim to hash that projected value. Pending or unavailable output projections return a typed refusal. Confirmed stream contract 3 carries only immutable event ID and original-position references; a retained inline stream requires the explicit reviewed protocol boundary before use.

Producer read grants are operator-configured kind prefixes and classes, with no access granted by registration alone. Fetch subjects use the authenticated producer identity. Event fetches apply the public visibility and redaction rules and return exact kind, version, record hash and commit position. Event and material RPC replies use the shared framed JSON transport, including oversized payloads and metadata. Material fetches return authoritative encoded segment descriptors after checking the same grants and producer-family ownership. The server authorizes one consistent response, serializes it once into bounded ordered chunks and finishes with its total length and incremental BLAKE3 hash. Sealed bytes require an explicit unlocked access path.

A fetch for an event already excised returns `ErasedEvent { event, commit_seq, original_commit_seq }` when the retained tombstone proves both positions and the current caller has its kind and class grants. It carries no payload and is mutually exclusive with a retained event. A consumer holding the old message checks the event ID and original position, records the content as unavailable and leaves the original slot payload-free. Only the confirmed position-complete barrier advances its scalar; the actual excision remains a later lifecycle notice. Unknown historical class or positions produce an explicit unavailable gap, and a missing event without erasure evidence remains missing.

### Provisioned consumers

A configured reader requests its native durable on its own `confirmed_consumer.<producer>` subject with `{after, cache, status_only: false}`. `cache` identifies the persistent local index. The daemon binds the consumer identity and NATS capability to a canonical hash of its configured scope. Narrowing grants therefore removes permission to pull the previous broader durable. It filters event and lifecycle subjects by configured kind prefixes and classes; global position-complete and gap controls remain visible. The reply names the stream, consumer and scope hash. A changed scope is explicit in that hash.

Clients may pull, acknowledge and inspect only their exact durable. They cannot create or delete consumers or fetch raw stream messages. Authenticated RPC inboxes are restricted to `_INBOX.<producer>.>`. Shared-stream payloads require an authenticated fetch even for open events.

A native durable resumes only when its exact acknowledged position-complete barrier proves the requested local scalar and its cache and scope still match. An older restored index, a new cache, an initial position or missing proof replays retained scoped history from its beginning. Requests beyond durable commit history return a named gap. `status_only: true` reads the current owner commit position without provisioning or resetting a consumer; the optional `position` field never substitutes an invented zero for unavailable evidence.

### Explicit write families

An authenticated credential has no write authority until the operator configures its logical family, write kind prefixes and offered classes. Read grants confer no registration or write rights. Kind declarations name the configured family, so two hosts can register the same immutable definition idempotently; conflicting same-version definitions refuse. The welcome returns the authoritative family and whether the credential is the configured canonical publisher.

Canonical summary prefixes belong to one explicitly configured publisher credential in each family. Other hosts may capture their allowed raw facts and register identical declarations, but cannot publish those summaries. No election is performed. Host and frame identity remain in the ingress history and run; material input hosts, part chains and record locations retain source copies when `Event.source` uses the logical family. Raw reading keys include their source-copy identity; canonical tool-call and turn keys converge under the configured publisher.

New ingress frames retain their configured family and capture kind prefixes in the admission transaction. Historical frames without that evidence remain unknown and require an explicit mapping decision before family-based replay or material access. Current credential configuration does not retroactively assign their ownership.

### Framed RPC replies and receipt hints

Producer RPC replies use the authenticated caller's own `_INBOX.<producer>.<nonce>` subject. Ordered data frames carry their stream ID, sequence and byte offset; the final marker carries the total length and BLAKE3 hash. Each transfer unit fits the server's actual message budget. Stop-and-wait flow acknowledgments use an ephemeral inbox below the same principal prefix. No durable transfer cache or ledger is created, and a progressing reply has no total size limit.

The client exposes a decoded response only after verifying the final marker and JSON end. Disconnects, missing or corrupt frames and idle cancellation discard the incomplete response; the caller retries its original idempotent request. The transport holds bounded serialized chunks. Existing SQL JSONB, catalog graph metadata and largest-record DTO allocations remain backend units; framing does not claim that every query is a streaming database read.

Unsolicited ack broadcasts contain only an immutable receipt hint: the native frame catalog number and the hash of its logical frame name. They never wait for a subscriber or establish producer progress. The producer retrieves the complete receipt through the authenticated `recorded` RPC and advances only after verification. Recorded replies, material listings and replay scopes have typed continuation pages; page size is a transfer unit, and traversal continues until the frontier is absent.

### Public temporal and attention context

An authenticated producer requests `PublicContext` on its own `public_context` subject through the framed reply transport. The deployment grants this capability explicitly with `public_context: true`; registration, write authority and event read grants do not grant it. The request contains no producer or host identity. Unknown request fields are refused, and NATS permissions restrict the subject and reply inbox to the authenticated identity.

An available snapshot carries the shared `DaySettingWire`, a settings identity, the actual committed host-policy revision, quiet hours, the interrupt budget's configured limit and observed use, and its observation time. The identity covers the configured logical day, physical host and public attention settings. Private intervals, commands, paths, notification state and keys are excluded. An unconfigured host returns `unavailable`; an uninitialized policy or a checkpoint under different settings returns `pending`. A client retains these states and resolves the named zone explicitly at its operation boundary. One resolved snapshot serves query, rendering and signals for that operation; a cached older snapshot is identified as cached rather than substituted for current availability.

The snapshot's opaque owner token authenticates the exact public snapshot for that producer and host. It grants no event access or write capability. The narrowly configured shipped Sinex-blocked rule may cite this token when reporting an actual local emergency notification after an outage; recovery validates its separate capability and records the actual receipt without notifying again. Authored rules cannot use that exception.

An optional coverage request names the consumer cache and its completed position. Coverage is separate from settings availability: incomplete input coverage still permits an available public settings snapshot, but cannot authorize a canonical complete-set replacement. A complete proof binds the owner-required input kinds and versions, classes, authenticated producer and family, native consumer incarnation and completed barrier, active privacy epoch and exact settings revision. The owner validates that proof again in the transaction for a new canonical frame; an exact recorded frame retry returns its recorded result first.
