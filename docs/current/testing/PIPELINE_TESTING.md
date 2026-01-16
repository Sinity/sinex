# Pipeline-First Test Recipe

Pipeline tests exercise the same flow that production uses: nodes publish to NATS JetStream,
sinex-ingestd consumes, and the database observes the persisted events. Follow this checklist when
you need end-to-end coverage.

Git-annex is mandatory for pipeline runs; the harness expects it to be available.

## 1. Acquire a TestContext With Shared NATS

```rust
#[sinex_test]
async fn pipeline_flow(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_shared_nats().await?;
    // …
    Ok(())
}
```

`with_shared_nats()` reuses the process-wide EphemeralNats instance so the expensive server startup
does not repeat for every pipeline test. PipelineScope requires shared NATS and enforces
namespacing so parallel tests stay isolated. Use `with_nats()` only for non-pipeline suites that
do not use PipelineScope.

## 2. Provision Streams and Consumers (When Manual JetStream Setup Is Needed)

PipelineScope provisions the ingestd streams automatically. If you need additional JetStream
streams or custom consumers, derive every name from the per-test namespace:

```rust
use async_nats::jetstream;

let namespace = ctx.pipeline_namespace();
let js = ctx.jetstream().await?;

let events_stream = namespace.stream("SINEX_TEST_EVENTS");
let events_subject = namespace.subject("events.raw.>");

js.get_or_create_stream(jetstream::stream::Config {
    name: events_stream,
    subjects: vec![events_subject],
    ..Default::default()
}).await?;
```

Do not call `env.nats_stream_name(...)` or build stream names by hand; the namespace helper is the
only safe way to share NATS across tests.

## 3. Start ingestd via PipelineScope

```rust
let scope = ctx.pipeline_scope().await?;
scope
    .publish("fs-watcher", "file.created", json!({"path": "/tmp/demo"}))
    .await?;
scope.wait_for_event_count(1).await?;
// … run assertions …
```

PipelineScope wraps `PipelineHarness`: it embeds ingestd in-process using the current test's
database URL and NATS context, waits for persistence, and cleans up on drop. The harness reuses a
per-database work directory under `/tmp/sinex-ingestd-shared/` so git-annex and assembler state no
longer need to be bootstrapped from scratch for every run.

### PipelineScope Owns the Slot Reset

PipelineScope calls `ctx.reset_database_slot()` on creation and fails fast if the slot is not
clean. Always use the pipeline-first approach (`ctx.publish_json_event()` or `scope.publish()`).
If you must seed via direct repository access in rare cases, call
`ctx.reset_database_slot().await?` once before seeding and never repair state after assertions.

## 4. Publish Through the Real node APIs

`TestnodePublisher` wraps the node SDK with sane defaults. It publishes slices, payloads,
and confirmations just like a running node would, and it accepts an explicit namespace:

```rust
let namespace = ctx.pipeline_namespace().prefix().to_string();
let publisher = TestnodePublisher::with_namespace(
    ctx.nats_client(),
    "fs-watcher",
    Some(namespace),
);
publisher.publish_event("file.created", json!({"path": "/tmp/demo"})).await?;
```

## 5. Run ingestd In-Process

PipelineScope embeds ingestd via the same runtime that powers `TestnodePublisher`. If you need
low-level control (custom config, multi-tenant ingestd), use `PipelineHarness` directly, but keep
namespace-derived stream names to preserve shared NATS isolation.

## 6. Use Deterministic Seed Clocks

Pipeline seeding helpers (`seed_events_via_pipeline`, `seed_events_via_scope`) enforce
`SeedClock::fixed()` so pipeline suites stay reproducible. If you need custom timestamps, use
explicit `EventOverrides` or `TimestampSpec` while keeping the fixed seed clock.

## 7. Assert on the Database State

After the pipeline runs, use the regular repository APIs (`ctx.pool.events()` etc.) to assert on the
persisted events. Avoid repairing missing events—if something did not flow through ingestd, fail the
test and investigate using the failure snapshots `sinex_test_utils` captures automatically.

## 8. Cleaning Up

`TestContext` tears down NATS, ingestd, and the allocated database slot on drop. If a test panics,
the failure snapshot machinery emits artifacts under `target/test-artifacts/` with JetStream slot
stats, captured logs, and background task metadata.

## Quick Reference

| Step | Helper |
| --- | --- |
| Start NATS | `ctx.with_shared_nats().await` (required for PipelineScope) |
| JetStream context | `ctx.jetstream().await` |
| Namespace | `ctx.pipeline_namespace()` |
| Stream provisioning | `namespace.stream(..)` / `namespace.subject(..)` + JetStream API |
| node publisher | `scope.publish(...)` / `TestnodePublisher::with_namespace(...)` |
| Pipeline + ingestd | `ctx.pipeline_scope().await?` |
| ingestd | implicit (spun up by PipelineScope) |
