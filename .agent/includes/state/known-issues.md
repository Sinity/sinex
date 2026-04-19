## Current Issue Summary

Canonical current-work backlog lives in:

- `.agent/scratch/040-maximalist-remaining-plan.md`

Canonical deferred deployment and horizon backlog lives in:

- `.agent/scratch/041-advanced-horizon-plan.md`

This include keeps only the compressed memory surface for AGENTS consumers.

### Open Issues Still Worth Remembering

| Issue | Location | Notes |
|-------|----------|-------|
| Gateway smoke unproven (token was empty) | deployment surface | 562K events in prod via ingestor path. Gateway dead until rebuild with re-encrypted token. After rebuild: prove `sinexctl gateway ingest → sinexctl query`. |
| FS ingestor OOM (pending rebuild) | deployment surface | NixOS module `defaultMemory` raised 256M → 1G. OOM-kills until `nixos-rebuild switch`. |
| Operator telemetry read models need rollout | self-observation | The six `_1h` operator surfaces were modeled as Timescale continuous aggregates, but `core.events` is partitioned by UUIDv7 `id`, so Timescale refuses that definition. Repo now uses hourly views over `ts_coided`; deploy the updated schema apply on host. |
| Browser/webhistory historical capture still lacks the right source-shaped ingestion path | source capture surface | The direct CLI shortcut is gone. The remaining work is native browser-DB ingestion plus historical import support for real browser export formats (`json`, `jsonl`, `csv`). |

### Recently Fixed (verified through 2026-04-16)

| Issue | Fix |
|-------|-----|
| Payload-to-material correspondence was weak | `total_bytes` column on `raw.source_material_registry` + ingestd `anchor_byte >= 0` validation |
| Text-search pagination cursor drift | `TRUNC(ts_rank_cd, 6)` in projection + matching Rust cursor truncation |
| Nested TextSearch lost snippet/relevance semantics | COALESCE fallback + documented limitation for combined terms |
| Numeric PathOp aborted on non-numeric strings | CASE WHEN jsonb_typeof guard on numeric cast |
| CountBy/SourceStats ties non-deterministic | Deterministic tiebreaker ORDER BY on all aggregate queries |
| Checkpoint save failure was warn-only | Consecutive failure counter → hard error after 3 in DerivedNodeAdapter + StreamNode |
| Pull-batch atomicity contract undocumented | Explicit module-level contract documentation in jetstream_consumer.rs |
| DB/session/lock residuals | Advisory lock already hardened; no remaining issues found |
| Trust boundary: ts_orig + anchor_byte unvalidated | ingestd validates future ts_orig (warn) + negative anchor_byte (DLQ route) |
| Duplicated EnvGuard/ScopedEnvGuard in tests | Shared EnvGuard with `with_keys()`, `set_single()` + test file dedup |
| Duplicated env-parse patterns in nodes | 5 nodes → `sinex_primitives::env` shared helpers |
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
| `xtask status` treated any stray `sinex-gateway` process as "up" | Gateway status now uses readiness probing instead of `pgrep` truthiness |
| `xtask` history opens made `status`/analytics take multiple seconds | Quick-check + lazy tracing + shared DB open path cut repeated history-open cost sharply |
| Local pipeline had no real end-to-end proof | Clean dev-stack proof now exists for gateway + terminal + filesystem through `NATS -> ingestd -> Postgres -> sinexctl query` |
| Host activation was still theoretical | `sinnix-prime` is switched live with healthy gateway readiness, materialized admin token, and active managed services |
| Terminal/desktop target-user bridges were unproven | Host ACL/runtime bridge fix now yields live `.zsh_history`, Atuin, and Hyprland source-material emission under systemd |
| DLQ stream name mismatch | gateway and node-sdk used `EVENTS_DLQ`; ingestd creates `SINEX_RAW_EVENTS_DLQ`. Fixed in 5 files (2026-04-16). |
| Desktop ingestor hard-error on unknown window address | `activewindowv2` handler panicked on startup race with unknown address. Now falls back to `hyprctl clients -j` (2026-04-16). |
| Automata processed 0 events during normal operation | `DerivedNodeAdapter` used invalidation-only loop (`manages_own_continuous_loop: true`). Changed to `false`, implemented `process_event_batch` for confirmation stream bridge (2026-04-16). |
| FS ingestor OOM (256M NixOS limit) | NixOS module `defaultMemory` raised to 1G. Needs rebuild to take effect (2026-04-16). |
| Gateway admin token empty | agenix secret was encrypted with empty plaintext. Re-encrypted with real token in sinnix repo. Needs rebuild (2026-04-16). |
| Operator telemetry `_1h` surfaces were impossible continuous aggregates | `core.events` is hypertabled on UUIDv7 `id`, so Timescale would not create operator CAs over derived `ts_coided`. Replaced the six operator CAs with hourly views over `ts_coided` and added regression coverage (2026-04-19). |

### Design Tensions (Both Sides Are Correct)

**Thin ingestors vs terminal ingestor complexity:** Terminal ingestor has 10K-entry dedup hash ring for file rotation. Justified: prevents doubling every command in event log. The "thin ingestor" principle is about semantic scope (don't correlate across sources), not code size.

**One query surface vs two query paths:** Composable engine (events.query) and telemetry read models serve different needs. The operator `_1h` surfaces now aggregate via hourly views over `ts_coided`; the user-facing activity surfaces stay as event-time views over `ts_orig`. No code currently JOINs both. Missing: SumBy/AvgBy aggregation mode for duration analytics.

**Replay determinism vs privacy evolution:** Replay re-runs privacy engine with CURRENT rules, not original. Correct by design (privacy improvements should apply retroactively) but violates intuition that "replay produces same output."

**Health endpoint 200 vs accurate status:** Gateway `/health` always returns 200 (stays in load balancer rotation for DB-backed RPCs). Status-code-only probes see false-green. 503 on NATS failure would remove gateway even though it can still serve queries.

**Leadership without fencing tokens:** Frozen leader + TTL expiry creates brief dual-leader window. Acceptable for single-host, problematic if ever multi-host.

### Architectural Fragilities (Things That Work But Barely)

| Fragility | Impact if hit | Mitigation exists? |
|-----------|---------------|-------------------|
| NatsPublisher 100-permit semaphore is per-publisher (`nats_publisher.rs:21`) | Starvation risk depends on publisher sharing | Per-publisher work already done |
| COPY batch: one bad row kills entire batch | Up to 1000 events retried via NAK | HistoricalImporter has bisect-retry but ingestd doesn't use it |
| Advisory lock on pooled connections | Lock acquired on conn A, released when pool recycles A | Use dedicated non-pooled connection |
| Operator telemetry views aggregate at query time | Wide Grafana lookbacks can become expensive if event volume climbs sharply | Monitor query cost and add dedicated persisted rollups only when measured |
| Git-annex process spawn per blob | 100 events/sec = 100 processes/sec for small blobs | No mitigation |

### Clean Codebase Signals

- Zero `todo!()`, `unimplemented!()`, `FIXME`, `HACK`, `XXX` in non-test code
- deny lints fully effective
- Only infallible const `unwrap()` calls survive in non-test code
- 29 VM tests, 65 self-validation exercises across 4 tiers
