# Recall Query Latency Evidence, 2026-07-03

This note records the live dev-store query plans behind bead `sinex-g3k`.
The store had about 69.3M `core.events` rows, including about 57.8M
`webhistory` rows.

## Decision

Choose Branch A: fix the event-card SQL shape. Do not add a projection table in
this slice.

The existing `(source, ts_orig)` and `ts_orig` btree indexes are sufficient for
interactive recall windows when the generated listing query orders by
occurrence time and emits singleton equality predicates. The slow path was not
missing storage; it was `ORDER BY id` plus `source = ANY($single_value_array)`,
which made PostgreSQL scan UUIDv7/id order and filter large row ranges.

## Before

Exact source-filtered event-card shape for recent browser history:

```sql
WHERE source = ANY(ARRAY['webhistory'])
  AND ts_orig >= now() - interval '48 hours'
ORDER BY id DESC
LIMIT 201
```

Observed plan shape:

- used the hypertable primary-key/id scan
- removed 880,681 rows by filter
- buffers: 193,876 hit, 107,747 read
- execution time: 18,647.982 ms

Exact recall-family shape with seven sources, a 48h `ts_orig` window, and
`ORDER BY id DESC LIMIT 201`:

- used pkey/id-ordered chunk append
- removed 38,911 rows by filter
- buffers: 10,070 hit, 7,125 read
- execution time: 1,341.830 ms

## Branch A Proof

Singleton source plus occurrence order:

```sql
WHERE source = 'webhistory'
  AND ts_orig >= now() - interval '48 hours'
ORDER BY ts_orig DESC, id DESC
LIMIT 201
```

Observed plan shape:

- used `_hyper_*_ix_events_source_ts_orig`
- full event-card projection
- buffers: 59 hit on warm run
- execution time: 1.156 ms

Broad occurrence-window event-card shape:

```sql
WHERE ts_orig >= now() - interval '48 hours'
ORDER BY ts_orig DESC, id DESC
LIMIT 201
```

Observed plan shape:

- used `_hyper_*_ix_events_ts_orig`
- full event-card projection
- buffers: 58 hit, 46 read
- planning time: 22.982 ms
- execution time: 9.923 ms

Point lookup for `show <ref>` by event id:

- used the pkey index
- buffers: 5 read
- execution time: 1.881 ms

## Implementation Shape

For non-text event listings with a `time_range`, the composable query path now
orders and paginates by `(ts_orig, id)`. Cursor anchors carry `ts_orig` so page
2 continues from the same ordering key instead of falling back to id-only
pagination.

For singleton `source`, `event_type`, and `host` filters, the filter builder now
emits `column = $1` instead of `column = ANY($1)`. Multi-value filters still use
`ANY`.

## Remaining Risk

`core.events` is still partitioned by UUIDv7 id, so `ts_orig` predicates do not
exclude old chunks. The index path is nevertheless fast for interactive recent
windows on the current dev-scale store. If future historical recall windows need
full-archive occurrence-time scans, use a rebuildable read projection keyed or
partitioned by `ts_orig`; do not repartition `core.events`.
