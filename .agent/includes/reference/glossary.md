# Glossary

## A

### anchor_byte
The byte offset within a source material file that identifies where an event's
data originates. Combined with `source_material_id`, it forms a stable
real-world occurrence identifier. Stored on `core.events`.

### automaton (pl. automata)
A derived node that processes existing events and emits derived-provenance
events. Three processing models: **Transducer** (1:1 stateless transform),
**Windowed** (accumulate-then-emit), **ScopeReconciler** (per-scope state
reconciliation). All managed by `AutomatonRuntime`.

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
events during replay. The cascade analyzer walks the derived DAG up to 100
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
time-series data. Sinex uses CAs for telemetry and activity rollups such as
event-engine batch stats, API stats, node stats, stream stats, command
frequency, file activity, window focus, and system state. Historical imports
may require explicit refresh depending on the relation and policy.

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
provenance type (material or derived), a UUIDv7 `id`, `ts_orig` (when it
happened), and a typed JSON payload. Stored in the TimescaleDB hypertable
`core.events`.

### event payload
The typed JSON body of an event, defined by an `EventPayload` trait
implementation. Each payload has compile-time `EventSource` and `EventType`
constants. Payload schemas are registered in `sinex_schemas.event_payload_schemas`.

## G

### gateway
The `sinexd::api` module providing the external API surface: JSON-RPC for
queries and commands, SSE for real-time event subscriptions, and native
messaging for browser extensions. Auth uses stateless token-suffix RBAC.

## I

### ingestd
The `sinexd::event_engine` module that consumes event batches from NATS
JetStream, validates them, and persists them to PostgreSQL. Routes batches
through COPY (>= 50 material events) or QueryBuilder (< 50 events). Derived
events always use QueryBuilder with REPEATABLE READ transactions.

### ingestor
A node that captures raw data from an external source (filesystem, terminal,
desktop, system, browser, documents), registers source materials, and emits
material-provenance events. Ingestors implement the `SourceUnit` trait with
three scan modes: snapshot, historical, and continuous.

## M

### material assembly
The process by which an ingestor registers source material in
`raw.source_material_registry`, splits it into frames/slices, and emits
material-provenance events for the event engine to persist.

### material provenance
One of the two provenance types: `source_material_id` is set, `source_event_ids`
is NULL. Means "I interpreted this byte range of this source file." Created by
ingestors. Can be replayed by re-reading the source material.

## P

### privacy engine
The DB/user policy admission engine owned by the event engine. It applies
redaction, encryption, hashing, and suppression rules from `privacy.*` policy
tables to source and derived event payloads before persistence. The primitive
rule compiler/catalog is implementation and seed material, not a parallel
source-unit, parser, or automaton policy authority.

### provenance
The origin of an event. Two mutually exclusive types: **material** (from source
data) and **derived** (derived from other events). Enforced by XOR CHECK
constraint at the DB level and `EventBuilder` typestate at compile time.

## R

### replay
The process of archiving old events and re-processing source material through
the normal pipeline. Replay is not a special mode — after archiving, the system
runs normally: NATS scan command triggers the ingestor to re-read the source
material, fresh events flow through the standard pipeline. Replay applies
current privacy rules and schema validation, not the original ones.

## S

### source binding
An entry in the source-unit catalog (`SourceUnitBinding`, registered via
`register_source_unit_binding!`) describing how a specific source unit is
wired at deployment time: implementation crate, adapter, output event type,
privacy context, material policy, checkpoint policy, runtime shape, and
package impact. Bindings are the durable contract between a Rust source-unit
implementation and its NixOS-side runtime configuration. They are compiled
into the source-unit registry and exercised through normal Rust/NixOS
verification; the old generated catalog and source-worker drift gate no longer
exist.

### source material
A file or data source registered in `raw.source_material_registry` that serves
as the provenance root for material events. Every material event references
exactly one source material via `source_material_id` and an `anchor_byte` offset.

### source record
The unit yielded by an `InputShapeAdapter` and consumed by a `MaterialParser`:
anchored bytes (`bytes`, `anchor: MaterialAnchor`) carved out of a source
material, plus optional metadata. The adapter owns enumeration and cursor
advancement; the parser owns semantic interpretation that produces a
`ParsedEventIntent`.

### source unit
The stable identity (`SourceUnitId`, e.g. `terminal.atuin-history`,
`browser.history`, `system.journald`) that groups a parser, its emitted event
types, and its binding configuration. Source units are data declared via
`register_source_unit!` against a `SourceUnitDescriptor`; runtime dispatch
resolves source units by inventory lookup against this registry — no match
arms. A source unit is NOT a process or deployment identity; multiple
source-unit instances of the same kind can co-exist under the same `sinexd`
deployment. Post-Wave-B fold (#1081), the per-domain ingestor crates and
standalone source-worker binary were deleted; source-unit hosting is now part
of `sinexd`.

### derived provenance
One of the two provenance types: `source_material_id` is NULL, `source_event_ids`
is set. Means "I derived this conclusion from these parent events." Created by
automata. Can be replayed by re-running the automaton on the (unchanged) parent
events.

## T

### ts_coided
A pure function of the UUIDv7 `id` (`GENERATED ALWAYS AS uuid_extract_timestamp(id)`,
not an independent column) — when sinex created *this interpretation*. The `id` is a
random UUIDv7 minted at creation, so `ts_coided` is "now" at creation and **differs
across replay**: a replayed event is a new interpretation with a new `id`, hence a new
`ts_coided`, even though its `ts_orig` is unchanged. Continuous aggregates bucket on
`ts_coided`.

### ts_orig
The real-world timestamp of the observed occurrence — intended to be quality-ranked
from `raw.temporal_ledger` evidence (the live quality derivation is planned, not yet
wired; see #1570). **Stable across replay** (re-derived from the same material). May
differ significantly from `ts_coided` for historical imports. Query by `ts_orig` for
"what happened when?", by `ts_coided` for "what did sinex interpret it?".

### ts_persisted
Timestamp set by a DB trigger when the row was written to disk. Used for
auditing write latency but not for business-logic queries.

## U

### UUIDv7
Time-ordered UUID (RFC 9562) used as the primary key for `core.events`. The
timestamp component is extracted as `ts_coided`. UUIDv7 monotonicity guarantees
that new event IDs are always greater than all previously persisted IDs,
eliminating the need for cycle detection during derived insertion.
