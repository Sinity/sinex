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

### Compressed archive probe

```bash
xtask test -p sinex-db -E 'test(compressed_chunk_archive_cost_is_measurable)' --impact-mode=off
```

Result: 1 exact test passed. The probe inserted 256 synthetic events, compressed the `core.events` chunk, archived all 256 rows, and verified 256 rows in `audit.archived_events`. It reported `archive_wall_ms=41`, `compressed_bytes_before=245760`, `compressed_bytes_after=499712`, `uncompressed_bytes_before=0`, `uncompressed_bytes_after=0`, and `wal_bytes=0.0`. The zero WAL delta and zero uncompressed-size fields are observed output limitations, not evidence of zero physical work. The test does not execute a scoped replay or recompress the post-archive chunk.

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
| `sinex-y0o3.10` | Partial, not closure-ready | The compressed archive probe measured and verified a 256-event archive on a real checkout TimescaleDB. The required scoped replay, replay wall time, usable WAL measurement, and post-replay recompression result are absent. No pathological-cost escalation is justified by this run. |
| `sinex-y0o3.11` | Partial, not closure-ready | The committed ReadySet curve passed at 10,000, 50,000, and 100,000 synthetic entries. It does not measure the real staged-import cardinality, admission throughput degradation, eviction pause distribution in the import route, or FK-deferral rate against the 150-second budget. No `dpof` promotion is justified by synthetic-only timings. |

The committed code remains unchanged in this chunk because the measurements identify missing execution coverage rather than a production defect. The next closure run needs a real staged ActivityWatch import delta and a replay route that records operation wall time, WAL LSN delta, chunk state before and after, and archive/replay counts in one isolated test.
