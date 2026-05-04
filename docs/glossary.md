# Glossary

## A

### anchor_byte
The byte offset within a source material file that identifies where an event's
data originates. Combined with `source_material_id`, it forms a stable
real-world occurrence identifier. Stored on `core.events`.

### automaton (pl. automata)
A derived node that processes existing events and emits synthesis-provenance
events. Three processing models: **Transducer** (1:1 stateless transform),
**Windowed** (accumulate-then-emit), **ScopeReconciler** (per-scope state
reconciliation). All managed by `DerivedNodeAdapter`.

### archived event
An event that has been moved from `core.events` to `audit.archived_events`
as part of replay or lifecycle management. Archived events are immutable and
serve as the replay target.

## B

### BLAKE3
The cryptographic hash function used for local content-addressed storage (CAS).
Every blob stored in the local CAS is identified by its BLAKE3 digest. Faster
than SHA-256 with comparable security properties.

## C

### CAS (Content-Addressed Storage)
Storage where content is addressed by its cryptographic hash (BLAKE3 digest)
rather than by a user-assigned name. Sinex uses a local CAS under
`<stateRoot>/blobs/sinex-cas/XX/YY/<hash>` with the `SINEXBLAKE3` backend key.

### cascade (replay)
The process of finding and archiving all events derived from a set of material
events during replay. The cascade analyzer walks the synthesis DAG up to 100
levels deep to find every event that must be archived before the source
material can be re-processed.

### checkpoint
A durable position marker in a JetStream consumer. Ingestors and automata
persist checkpoints to NATS KV (and local files) so they can resume from the
last processed event after a restart. Checkpoint interval: 1000 events.

### confirmation
A published message on the `sinex.events.confirmed.>` NATS subject signaling
that an event has been durably persisted to PostgreSQL. Automata consume
confirmations to trigger derived event processing.

### content key
A string identifying a blob in the content store using the format
`<backend>-s<size>--<hash>`. Example: `SINEXBLAKE3-s1024--abc123...def`.
Parsed into `ContentStoreKey` (backend, size, digest).

### continuous aggregate (CA)
A TimescaleDB feature that automatically maintains pre-computed rollups of
time-series data. Sinex currently has no continuous aggregates — all rollups
are ordinary views or materialized views. CAs bucket on `ts_coided` (derived
from UUIDv7 `id`), which means historical imports are invisible until a manual
refresh.

### COPY protocol
PostgreSQL's high-performance bulk insert mechanism. Sinex uses tab-delimited
SIMD-escaped COPY for material-provenance batches of 50 or more events,
avoiding the per-row overhead of `INSERT ... VALUES`.

## D

### dead-letter queue (DLQ)
A NATS JetStream stream (`sinex.events.dlq.>`) where events that fail
validation, parsing, or FK constraints are routed for operator inspection and
manual recovery.

### derived node
See **automaton**.

## E

### event
The fundamental unit of observation in sinex. An event has exactly one
provenance type (material or synthesis), a UUIDv7 `id`, `ts_orig` (when it
happened), and a typed JSON payload. Stored in the TimescaleDB hypertable
`core.events`.

### event payload
The typed JSON body of an event, defined by an `EventPayload` trait
implementation. Each payload has compile-time `EventSource` and `EventType`
constants. Payload schemas are registered in `sinex_schemas.event_payload_schemas`.

## G

### gateway
The `sinex-gateway` service providing the external API surface: JSON-RPC for
queries and commands, SSE for real-time event subscriptions, and native
messaging for browser extensions. Auth via stateless token-suffix RBAC.

## I

### ingestd
The `sinex-ingestd` service that consumes event batches from NATS JetStream,
validates them, and persists them to PostgreSQL. Routes batches through COPY
(>= 50 material events) or QueryBuilder (< 50 events). Synthesis events always
use QueryBuilder with REPEATABLE READ transactions.

### ingestor
A node that captures raw data from an external source (filesystem, terminal,
desktop, system, browser, documents), registers source materials, and emits
material-provenance events. Ingestors implement the `IngestorNode` trait with
three scan modes: snapshot, historical, and continuous.

## M

### material assembly
The process by which an ingestor registers source material in
`raw.source_material_registry`, splits it into frames/slices, and publishes
them to NATS for ingestd to persist. Managed by `AcquisitionManager`.

### material provenance
One of the two provenance types: `source_material_id` is set, `source_event_ids`
is NULL. Means "I interpreted this byte range of this source file." Created by
ingestors. Can be replayed by re-reading the source material.

## P

### privacy engine
A synchronous, per-event processor that runs in the ingestor process before
NATS publish. Applies redaction, encryption, hashing, and suppression rules
based on `ProcessingContext` (Command, Clipboard, WindowTitle, Metadata, etc.).

### provenance
The origin of an event. Two mutually exclusive types: **material** (from source
data) and **synthesis** (derived from other events). Enforced by XOR CHECK
constraint at the DB level and `EventBuilder` typestate at compile time.

## R

### replay
The process of archiving old events and re-processing source material through
the normal pipeline. Replay is not a special mode — after archiving, the system
runs normally: NATS scan command triggers the ingestor to re-read the source
material, fresh events flow through the standard pipeline. Replay applies
current privacy rules and schema validation, not the original ones.

## S

### source material
A file or data source registered in `raw.source_material_registry` that serves
as the provenance root for material events. Every material event references
exactly one source material via `source_material_id` and an `anchor_byte` offset.

### synthesis provenance
One of the two provenance types: `source_material_id` is NULL, `source_event_ids`
is set. Means "I derived this conclusion from these parent events." Created by
automata. Can be replayed by re-running the automaton on the (unchanged) parent
events.

## T

### ts_coided
Timestamp derived from the UUIDv7 `id` field — when sinex first observed
the event. Always "now" at creation time. Continuous aggregates bucket on
`ts_coided`.

### ts_orig
The real-world timestamp of the observed occurrence. May differ significantly
from `ts_coided` for historical imports. Query by `ts_orig` for "what happened
when?", by `ts_coided` for "what did sinex know when?".

### ts_persisted
Timestamp set by a DB trigger when the row was written to disk. Used for
auditing write latency but not for business-logic queries.

## U

### UUIDv7
Time-ordered UUID (RFC 9562) used as the primary key for `core.events`. The
timestamp component is extracted as `ts_coided`. UUIDv7 monotonicity guarantees
that new event IDs are always greater than all previously persisted IDs,
eliminating the need for cycle detection during synthesis insertion.
