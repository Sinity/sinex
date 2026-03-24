## Known Issues & Design Tensions

### Confirmed Bugs (Critical)

| Issue | Location | Notes |
|-------|----------|-------|
| `block_on()` in async runtime | `blob_manager.rs:503` | `futures::executor::block_on()` in sync method — will panic or deadlock from tokio context |
| Gateway ingest produces provenance-less events | `handlers/ingest.rs:50-58` | `events.ingest` RPC lacks provenance → always fails XOR CHECK → DLQ. **Smoke test is broken.** |
| `blocking_write()` on tokio RwLock from OS thread | `rpc_server.rs:279` | In file-watcher callback on std::thread::spawn |
| MaterialReadySet unbounded memory | `material_ready_set.rs` | No eviction logic |

### Confirmed Bugs (High)

| Issue | Location | Notes |
|-------|----------|-------|
| Replay cascade restore silently discarded | `replay_control.rs:1561` | `let _ = restore_cascade(...)` — data loss on restore failure |
| Events persisted but invisible on confirmation failure | `jetstream_consumer.rs:779-806` | DB commit succeeds but NAKs entire batch on any confirmation failure |
| DLQ bypass when no transport | `derived_node/adapter.rs:322-324` | Events dropped if runtime is None |
| Advisory lock on pooled connection | `state_machine.rs:1003` | Lock/unlock may hit different sessions |

### Confirmed Bugs (Medium)

| Issue | Location | Notes |
|-------|----------|-------|
| Privacy engine unused by automata | `derived_node/adapter.rs` | Zero privacy imports. Derived events inherit ingestor leaks |
| `hard_delete_by_source` bypasses audit trigger | `persistence.rs:1647` | DELETE without operation_id |
| Provenance corruption via default UUID | `adapter.rs:262` | `event.id` None → zero UUID as source_event_id |

### Confirmed Bugs (Low)

| Issue | Location | Notes |
|-------|----------|-------|
| `core.node_runs` table never populated | Schema | Zero INSERT calls |
| TimeSeries COALESCE misleading | `composable_query.rs:306` | ts_orig NOT NULL, COALESCE never fires; real issue is ingestd fills now() |

### Recently Fixed (verified 2026-03-23)

| Issue | Fix |
|-------|-----|
| Replay state machine lacks FOR UPDATE | All transitions now use `SELECT ... FOR UPDATE` |
| DashMap stale assembly entries | Cleanup task + remove on finalize |
| std::sync::Mutex no poison recovery | `unwrap_or_else(poisoned.into_inner())` |

### Design Tensions (Both Sides Are Correct)

**Thin ingestors vs terminal ingestor complexity:** Terminal ingestor has 10K-entry dedup hash ring for file rotation. Justified: prevents doubling every command in event log. The "thin ingestor" principle is about semantic scope (don't correlate across sources), not code size.

**One query surface vs two query paths:** Composable engine (events.query) and continuous aggregates serve different needs. CAs = fast for dashboards; composable = flexible for investigation. No code currently JOINs both. Missing: SumBy/AvgBy aggregation mode for duration analytics.

**Replay determinism vs privacy evolution:** Replay re-runs privacy engine with CURRENT rules, not original. Correct by design (privacy improvements should apply retroactively) but violates intuition that "replay produces same output."

**Health endpoint 200 vs accurate status:** Gateway `/health` always returns 200 (stays in load balancer rotation for DB-backed RPCs). Status-code-only probes see false-green. 503 on NATS failure would remove gateway even though it can still serve queries.

**Leadership without fencing tokens:** Frozen leader + TTL expiry creates brief dual-leader window. Acceptable for single-host, problematic if ever multi-host.

### Architectural Fragilities (Things That Work But Barely)

| Fragility | Impact if hit | Mitigation exists? |
|-----------|---------------|-------------------|
| NatsPublisher 100-permit semaphore is per-publisher (`nats_publisher.rs:21`) | Starvation risk depends on publisher sharing | Per-publisher work already done |
| COPY batch: one bad row kills entire batch | Up to 1000 events retried via NAK | HistoricalImporter has bisect-retry but ingestd doesn't use it |
| Checkpoint save failure is silent (warn log only) | Crash -> re-process from stale position -> duplicates | DLQ should catch, but no e2e test (BLK-4) |
| Advisory lock on pooled connections | Lock acquired on conn A, released when pool recycles A | Use dedicated non-pooled connection |
| CAs invisible after historical import | 3-hour lookback misses imported data | Must manually `CALL refresh_continuous_aggregate()` |
| Git-annex process spawn per blob | 100 events/sec = 100 processes/sec for small blobs | No mitigation |

### Clean Codebase Signals

- Zero `todo!()`, `unimplemented!()`, `FIXME`, `HACK`, `XXX` in non-test code
- deny lints fully effective
- Only infallible const `unwrap()` calls survive in non-test code
- 29 VM tests, 65 self-validation exercises across 4 tiers
