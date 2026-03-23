## Known Issues & Design Tensions

### Confirmed Bugs

| Issue | Component | Severity | Notes |
|-------|-----------|----------|-------|
| `core.node_runs` table never populated | node SDK | Low | Schema exists, column on events exists, INSERT never called. `node_run_id` always NULL |
| TimeSeries NULL ts_orig fallback | gateway query | Low | NULL ts_orig silently uses ts_coided (import time, not event time) |
| Privacy engine unused by automata | all automata | Medium | Derived events inherit any ingestor privacy leaks. No per-automaton ProcessingContext |

### Design Tensions (Both Sides Are Correct)

**Thin ingestors vs terminal ingestor complexity:** Terminal ingestor has 10K-entry dedup hash ring for file rotation. Justified: prevents doubling every command in event log. The "thin ingestor" principle is about semantic scope (don't correlate across sources), not code size.

**One query surface vs two query paths:** Composable engine (events.query) and continuous aggregates serve different needs. CAs = fast for dashboards; composable = flexible for investigation. No code currently JOINs both. Missing: SumBy/AvgBy aggregation mode for duration analytics.

**Replay determinism vs privacy evolution:** Replay re-runs privacy engine with CURRENT rules, not original. Correct by design (privacy improvements should apply retroactively) but violates intuition that "replay produces same output."

**Health endpoint 200 vs accurate status:** Gateway `/health` always returns 200 (stays in load balancer rotation for DB-backed RPCs). Status-code-only probes see false-green. 503 on NATS failure would remove gateway even though it can still serve queries.

**Leadership without fencing tokens:** Frozen leader + TTL expiry creates brief dual-leader window. Acceptable for single-host, problematic if ever multi-host.

### Architectural Fragilities (Things That Work But Barely)

| Fragility | Impact if hit | Mitigation exists? |
|-----------|---------------|-------------------|
| NatsPublisher 100-permit semaphore is per-process | Flood from one source starves all others | Partial (per-publisher work started) |
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
