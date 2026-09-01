# Interactive performance contracts

`xtask test bench --contracts` runs the benchmark scenarios listed by the
`interactive` tier in `xtask/config/perf-contracts.toml`. The tier names the
budgets used by interactive recall and operator surfaces:

- recall window render: p95 ≤ 200 ms
- event-card listing: p95 ≤ 200 ms
- MCP query round trip: p95 ≤ 500 ms
- `sinexctl` cold start: p95 ≤ 1,000 ms
- ingest to queryable: p95 ≤ 5,000 ms

The benchmark run is regression-gated against the latest matching history
point and also applies the absolute scenario caps. The named budgets are kept
with the contract manifest so adding a surface requires an explicit budget and
scenario association. A deliberate p95 overrun is covered by the xtask
verification test and fails the gate.

## Conditional escalation

The current recall path stays on the indexed `core.events` queries measured in
`crate/sinex-db/docs/recall_query_latency_2026_07_03.md`. If an occurrence-time
window workload (especially a rollup or aggregate) exceeds its interactive
budget on the live-size development store, first consider a bounded result
cache keyed by cursor and epoch. If that does not meet the budget, add the
rebuildable `recall.timeline` read model keyed and partitioned by `ts_orig`.

The read model is a cache, not an authority: cards hydrate from
`core.events`, and replay, archive, or redaction changes rebuild or invalidate
it. Do not repartition `core.events` or derive UUIDv7 timestamps from
`ts_orig`; the event table remains partitioned by interpretation identity.
