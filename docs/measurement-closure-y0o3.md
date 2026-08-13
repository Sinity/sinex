# Measurement closure for sinex-y0o3.2, y0o3.10, and y0o3.11

Recorded on 2026-08-13 from commit `b1b127268`. The committed measurement harnesses are present in `f6acb34ad`. This report records executed commands and does not promote synthetic measurements to production-scale estimates.

## Environment

`xtask doctor` passed against the checkout-local development database. PostgreSQL, TimescaleDB, NATS, and the checkout TLS fixtures were available. The reported development database was `sinex_dev` under the checkout cache, not the production database.

## Real representative fixture inventory

The selected ActivityWatch fixture was the complete January 2025 daily slice. It contains 31 daily NDJSON files, 26,323,805 bytes, and 97,417 newline-delimited records. The fixture inventory command was:

```bash
set -o pipefail
month_dir=/realm/data/captures/activitywatch/events_by_day
files=$(for day in $(seq -w 1 31); do path="$month_dir/2025-01-${day}.ndjson"; test -f "$path" && printf '%s\n' "$path"; done)
printf 'files=%s\n' "$(printf '%s\n' "$files" | wc -l)"
printf 'bytes=%s\n' "$(printf '%s\n' "$files" | xargs -r wc -c | awk 'END {print $1}')"
printf 'records=%s\n' "$(printf '%s\n' "$files" | xargs -r wc -l | awk 'END {print $1}')"
printf 'first=%s\n' "$(printf '%s\n' "$files" | sed -n '1p')"
printf 'last=%s\n' "$(printf '%s\n' "$files" | sed -n '$p')"
df -B1 --output=avail /realm | sed -n '2p'
```

Observed output: `files=31`, `bytes=26323805`, `records=97417`, first file `2025-01-01.ndjson`, last file `2025-01-31.ndjson`, and 1,136,416,567,296 available bytes on `/realm`. This is raw fixture evidence only. No staged ActivityWatch import was run, so no event, CAS, staging, NATS, or COPY-rate delta is attributed to this fixture.

## Executed harnesses

### Projection and replay calculators

```bash
xtask test -p xtask -E 'test(manifest_projection_keeps_duplicate_storage_explicit) | test(replay_report_marks_hour_scale_cost_and_compression_ratio)' --impact-mode=off
```

Result: 2 exact tests passed. These tests validate the calculator contracts. They do not measure an import or replay.

### Compressed scoped replay probe

```bash
xtask test -p sinex-db -E 'test(compressed_chunk_scoped_replay_cost_is_measurable)' --impact-mode=off
```

The bounded `sinex-y0o3.10.1` harness allocates a fresh development database per test. It changes that database's `core.events` chunk interval to one millisecond, inserts eight natural UUIDv7-timed bursts for each sample, and therefore creates multiple chunks without inventing historical UUIDv7 values or touching production.

The bounded samples are 256, 2048, and 8192 material-root events. Each sample records wall time and WAL observability for fixture insertion, compression, scoped replay archive, and recompression. The emitted `y0o3_10_1_compressed_scoped_replay_measurement` JSON also includes `pg_current_wal_lsn` snapshots and byte delta when available, `pg_stat_wal.wal_bytes` delta when available, live/archive row counts, direct-root/cascade/archive counts, and full chunk states before compression, after compression, after archive, and after recompression.

The replay phase uses `ReplayScope` filtering plus the production cascade-session helpers to select roots, expand the cascade, and call the archive trigger within one transaction. Assertions require a multi-chunk fixture, all chunks compressed before archive and after recompression, exact root/cascade/archive counts, zero matching live rows after archive, and exact archived-row count. Missing WAL observability is represented as `null`; it does not invalidate the replay correctness measurement.

The focused run on 2026-08-13 used test database `sinex_test_pool_37` and passed in 9.577 seconds. All three samples selected, cascaded, archived, and verified exactly their requested row count, leaving zero matching live rows and fully compressed chunks after recompression.

| Rows | Insert ms | Compress ms | Archive ms | Recompress ms | Archive LSN delta | Archive `pg_stat_wal` delta |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 256 | 137 | 51 | 66 | 4 | 0 | 0 |
| 2,048 | 229 | 53 | 335 | 26 | 5,301,200 | 11,121,134 |
| 8,192 | 537 | 80 | 1,557 | 44 | 21,820,304 | 33,427,070 |

The structured output recorded 8, 16, and 26 compressed chunk snapshots after the three sequential samples. The latter two include already-compressed chunks from earlier samples in the same otherwise fresh database. The per-sample archive assertions remain material-filtered and exact.

Residual scope: this is a checkout-local synthetic measurement, not a historical seven-day chunk-size model or a production replay. It cannot safely exercise the daemon-only source re-scan, confirmed publish/ack, or operation invalidation marker. `pg_stat_wal` is cluster-global, so its delta can include concurrent development-database activity. The harness does exercise the production database route used by the replay controller: scoped root selection, cascade session preparation and expansion, archive trigger/cascade, and recompression.

### MaterialReadySet scale probe

```bash
xtask test -p sinexd -E 'test(scale_probe_reports_bounded_cardinality_and_purge_cost)' --impact-mode=off
```

Result: 1 exact test passed. The probe is synthetic and reported these body measurements:

| Cardinality | Insert wall | Purge wall | Peak entries | Purged entries |
| ---: | ---: | ---: | ---: | ---: |
| 10,000 | 38.590 ms | 4.648 ms | 10,000 | 10,000 |
| 50,000 | 243.221 ms | 42.480 ms | 50,000 | 50,000 |
| 100,000 | 488.051 ms | 50.425 ms | 100,000 | 100,000 |

These are process-local synthetic timings. They do not include staged-import admission, database FK deferrals, NAK latency, or source material cardinality from the real fixture.

### Current development storage snapshot

```bash
xtask analytics storage-growth --projection-days 365 --limit 20 --json
```

Result: the current checkout database contained 46 registered materials, 11,062 parsed events, and a `core.events` relation estimate of 34,177,024 total bytes. It reported one uncompressed chunk and zero compressed chunks while compression was configured. The top source was `git-commit-history`, not ActivityWatch. This snapshot is retained as environment evidence and is not used for the ActivityWatch projection.

## Acceptance-criteria dispositions

| Bead | Disposition | Evidence and remaining gap |
| --- | --- | --- |
| `sinex-y0o3.2` | Partial, not closure-ready | The real January 2025 ActivityWatch fixture has measured bytes and records. The required GB projection against `/realm` and hours-scale COPY import ETA are not measured because the staged ActivityWatch path was not run and the required core event, manifest, CAS, staging, NATS, and COPY-rate deltas are absent. No production-scale estimate is made. |
| `sinex-y0o3.10` | Partial, with bounded y0o3.10.1 measurement | The committed harness measures 256, 2048, and 8192 synthetic multi-chunk scoped replay archives and recompression through the production database route. It does not yet establish historical seven-day chunk cost, source re-scan, confirmed publish/ack, or a production-scale replay estimate. |
| `sinex-y0o3.11` | Partial, not closure-ready | The committed ReadySet curve passed at 10,000, 50,000, and 100,000 synthetic entries. It does not measure the real staged-import cardinality, admission throughput degradation, eviction pause distribution in the import route, or FK-deferral rate against the 150-second budget. No `dpof` promotion is justified by synthetic-only timings. |
