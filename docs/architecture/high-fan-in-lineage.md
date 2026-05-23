# High-Fan-In Lineage

Status: design record for #1112.

Small synthesis events should keep using `source_event_ids[]`. Large summaries,
clusters, context packs, and moment/evidence outputs need a compact lineage
representation that preserves synthesis provenance without stuffing thousands of
parent IDs into one event row.

This is not a third provenance class. A high-fan-in output is still synthesis:
it is derived from events. The parent set is represented by a derivation scope
instead of only an inline UUID array.

## Representation

Use side tables for large fan-in. Do not add a second material provenance path
and do not store huge parent lists in event payload JSON.

```sql
create table core.derivation_scopes (
  id uuid primary key,
  producer_id text not null,
  semantics_version text not null,
  scope_kind text not null,
  scope jsonb not null,
  input_query_hash text not null,
  input_count bigint not null,
  input_set_hash text not null,
  time_range tstzrange,
  created_at timestamptz not null default now(),
  operation_id uuid references core.operations_log(id)
);

create table core.derivation_scope_members (
  scope_id uuid not null references core.derivation_scopes(id) on delete cascade,
  event_id uuid not null references core.events(id),
  role text not null default 'input',
  weight double precision not null default 1.0,
  metadata jsonb not null default '{}'::jsonb,
  primary key (scope_id, event_id, role)
);

create table core.event_derivation_scopes (
  event_id uuid not null references core.events(id) on delete cascade,
  scope_id uuid not null references core.derivation_scopes(id),
  relationship text not null check (
    relationship in ('input_scope', 'evidence_scope', 'comparison_scope')
  ),
  primary key (event_id, scope_id, relationship)
);
```

The `core.events` XOR provenance constraint remains conceptually unchanged:
material events cite source material; synthesis events cite parent events. For
large fan-in, the synthesis parentage is discovered through
`event_derivation_scopes` and `derivation_scope_members`.

## Exact Membership

Exact membership rows are required when:

- replay must cascade precisely through individual input events;
- trace output must explain specific evidence items;
- context packs or moment candidates expose weighted evidence;
- settlement or late-arrival invalidation needs event-level affected-scope
  detection.

Query-defined lineage without member rows is acceptable only when:

- the source query is deterministic and versioned;
- `input_query_hash`, `input_count`, and `input_set_hash` are stored;
- replay can reconstruct the set or explicitly report that exact membership was
  compacted and cannot be expanded without rerunning the query;
- the output is advisory/read-model evidence rather than a deletion/replay
  cascade authority.

## Trace Semantics

`sinexctl trace` should render:

```text
derived event
  └─ derivation scope: daily_summary/2026-05-16
       ├─ producer: daily-summarizer@semver
       ├─ input_count: 18423
       ├─ input_set_hash: blake3:...
       └─ members: expandable on demand
```

Default trace output should show the scope node and summary fields. Expansion
should be explicit and paginated.

## Replay Preview

Replay preview for scoped derivations:

1. Recompute the scope query under the target semantics version.
2. Compare old and new `input_count`.
3. Compare old and new `input_set_hash`.
4. If hashes match, descendants may be left untouched unless producer semantics
   changed.
5. If hashes differ, show added/removed sample members and affected derived
   outputs.
6. Recompute by creating new immutable derived events and superseding or
   invalidating old outputs through normal replay/operation records.

Late-arrival settlement should use the same scope hashes to distinguish “new
evidence arrived” from “producer semantics changed.”

## First Producer

Daily summarizer is the first good producer:

- parent count is naturally high;
- output is already windowed and replayable by time range;
- trace can show a daily scope without expanding thousands of events;
- late-arrival behavior can later connect to the settlement model.

Context packs and moment-query evidence packs should reuse the same scope model
once their artifact contracts exist.

## Archive Cascade

Archive/replay cascade must traverse both inline `source_event_ids[]` and
derivation-scope membership when exact members exist. If a scope is query-defined
without exact members, cascade should mark the derived output as scope-affected
and require replay preview to recompute membership before destructive archive.

## Guardrails

- Keep `source_event_ids[]` for small fan-in derivations.
- Do not introduce material provenance for event-derived summaries.
- Do not mutate aggregate event payloads in place.
- Do not hide large parent lists inside JSON payloads.
- Require pagination for member expansion.
