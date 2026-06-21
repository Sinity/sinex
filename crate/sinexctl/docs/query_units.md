# Sinex Query Units

`sinexctl query` executes descriptor-backed selections over existing Sinex
read surfaces. The command is not a SQL escape hatch: it parses a compact
Sinex grammar into typed `SinexQuery` descriptors, validates fields and
operators before execution, and renders finite `ViewEnvelope` output.

Implemented units:

| Unit | Existing execution path |
| --- | --- |
| `events` | `events.cards` via `EventQuery` |
| `source-drivers` | `sources.status.view` / source coverage rows |
| `source-materials` | `sources.list` summaries |
| `debt` | unified debt providers: DLQ, source coverage, derivation specs |
| `operations` | `ops.list` / `OperationView` rows |
| `runtime-health` | `runtime.health` summary |

Examples:

```bash
sinexctl query 'events where source = "terminal.fish-history" and event_type = "terminal.command" limit 100'
sinexctl query 'source-drivers where readiness != "ready" limit 50'
sinexctl query 'source-materials where status = "completed" limit 25'
sinexctl query 'debt where kind = "admission" or kind = "projection" limit 50'
sinexctl query 'operations where status = "failed" limit 25'
sinexctl query 'runtime-health where state != "healthy" limit 1'
```

Grammar:

```text
<unit> [where <predicate> (and|or <predicate>)*] [limit <n>] [offset <n>]
<predicate> = <field> <operator> <value>
<predicate> = <field> exists
```

Operators are declared by each query-unit descriptor. Unsupported units,
fields, operators, enum values, and value types fail before an RPC is issued.
Event queries currently expose the executable `EventQuery` filter fields:
`source`, `event_type`, `host`, `scope_key`, and `equivalence_key`, each with
exact-match predicates. Contract/schema/time predicates should not appear in
docs or completions until they have an executable lowering path.
Runtime-health queries execute against the bounded runtime summary row exposed
by `runtime.health`; predicates filter the summary fields declared by the
descriptor (`module`, `role`, `state`, and `stale_after`).

`json` and `yaml` return the full `ViewEnvelope<SinexQueryResultListView>`.
`ndjson` emits one `SinexQueryResultRow` per line. `table` prints a compact
human view with the unit, public ref/title, and summary.
