# Ingestd Service Orchestrator

The `IngestService` is the central orchestrator for the `sinex-ingestd` daemon. it coordinates high-throughput event ingestion from NATS into `PostgreSQL` and assembles source materials for storage in the SDK content store.

## Service Architecture

The service follows a task-based architecture where critical and non-critical operations are isolated into separate asynchronous tasks:

| Task | Type | Purpose |
|------|------|---------|
| **`JetStream` Consumer** | Critical | NATS -> DB event pipeline |
| **Material Assembler** | Critical | NATS -> content-store material assembly |
| **Stats Logger** | Non-Critical | Periodic metrics logging & self-observation |
| **Schema Reloader** | Non-Critical | Syncing schema cache with database every 5 min |

## Initialization Sequence

The service implements a strict fail-fast initialization policy:

1. **Database Connection**: Establishes connection pool to `PostgreSQL`.
2. **NATS Connection**: Connects to the NATS cluster and initializes `JetStream`.
3. **Migration Lock**: Acquires a `PostgreSQL` advisory lock (`ingestd.migrations`) to ensure only one instance performs schema synchronization at a time.
4. **Schema Synchronization**: Synchronizes `EventPayload` types from the codebase to the `sinex_schemas.event_payload_schemas` table.
5. **Validator Init**: Loads active schemas into the `EventValidator` cache.
6. **Schema Broadcasting**: Publishes schema metadata to `JetStream` and full schema JSON to NATS KV for node-side validation.
7. **Service Construction**: Completes the `IngestService` struct and releases the migration lock.

## Shutdown & Lifecycle

Graceful shutdown is managed via a shared `AtomicBool` flag and cooperative cancellation:

- **Signal Handling**: External signals (SIGTERM/SIGINT) trigger the `shutdown()` method.
- **Flag Propagation**: The shutdown flag is set, which is observed by all worker tasks in their respective event loops.
- **Task Quiescence**: The orchestrator waits up to 5 seconds for non-critical tasks to finish before closing resources.
- **Resource Cleanup**: Closes the database pool explicitly. NATS resources are dropped with task shutdown.

### Known Limitations

- **Shutdown Atomicity**: If cancelled mid-batch, the `JetStream` consumer may produce duplicate events on restart (mitigated by database unique constraints).
- **Schema Contention**: The 5-minute schema reload task acquires a write lock on the validator, which briefly pauses event processing.
