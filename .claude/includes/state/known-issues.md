## Current Issue Summary

Canonical current-work backlog lives in:

- `.claude/scratch/040-maximalist-remaining-plan.md`

Canonical deferred deployment and horizon backlog lives in:

- `.claude/scratch/041-advanced-horizon-plan.md`

This include keeps only the compressed memory surface for AGENTS consumers.

### Open Issues Still Worth Remembering

| Issue | Location | Notes |
|-------|----------|-------|
| Payload-to-material correspondence is still weak | event pipeline | Still the most important unresolved provenance-integrity gap. |
| Browser/webhistory historical capture is still missing | source capture surface | Still a real product/runtime gap, but the implementation plan now lives in `040`. |
| Checkpoint save failure is still warn-only | node-sdk checkpoint persistence | Still a trust gap during crash/restart scenarios. |
| Full original pull-batch atomicity is not the consumer contract | `jetstream_consumer.rs` | The remaining task is to either keep defending this model or change it intentionally. |

### Recently Fixed (verified 2026-04-03)

| Issue | Fix |
|-------|-----|
| Replay state machine lacks FOR UPDATE | All transitions now use `SELECT ... FOR UPDATE` |
| DashMap stale assembly entries | Cleanup task + remove on finalize |
| std::sync::Mutex no poison recovery | `unwrap_or_else(poisoned.into_inner())` |
| Gateway ingest smoke path was broken | `events.ingest` now registers source material and emits provenance-valid envelopes |
| Token watcher startup blocked the runtime | `rpc_server.rs` now uses async readiness handoff; the old `blocking_write()` callback path is gone |
| `blob_manager.rs` contained sync `block_on()` in runtime-sensitive code | Re-checked and retired; no sync `block_on()` path remains there |
| `MaterialReadySet` had no bounded-maintenance proof | Maintenance/eviction behavior is now covered by ingestd tests |
| Replay restore silently dropped compensating invalidation | `replay_control.rs` now republishes the compensating invalidation path |
| Derived-node DLQ fallback could fail open without transport | `derived_node/adapter.rs` now fails honest when DLQ transport is unavailable |
| `hard_delete_by_source` audit-trigger bypass was only a suspicion | Delete-by-source archiving is now regression-locked through the audit trigger |
| `core.node_runs` looked like dead schema | runtime state now inserts and updates real `node_runs` rows |
| Automata privacy propagation had no proof | privacy filtering is now regression-locked on the invalidation output path |
| History DB schema-version read failures could take down `xtask jobs list --json` | `HistoryDb::open()` now recreates unreadable junk-schema history DBs instead of failing the jobs surface |

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
| Advisory lock on pooled connections | Lock acquired on conn A, released when pool recycles A | Use dedicated non-pooled connection |
| CAs invisible after historical import | 3-hour lookback misses imported data | Must manually `CALL refresh_continuous_aggregate()` |
| Git-annex process spawn per blob | 100 events/sec = 100 processes/sec for small blobs | No mitigation |

### Clean Codebase Signals

- Zero `todo!()`, `unimplemented!()`, `FIXME`, `HACK`, `XXX` in non-test code
- deny lints fully effective
- Only infallible const `unwrap()` calls survive in non-test code
- 29 VM tests, 65 self-validation exercises across 4 tiers
