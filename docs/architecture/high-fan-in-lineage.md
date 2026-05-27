# High-Fan-In Lineage

Status: design record for #1112.

## Resolution: Hierarchical Aggregation Is The Dominant Shape

The original concern — that day summaries or weekly rollups would produce
`source_event_ids[]` arrays with thousands of entries, blowing past PostgreSQL
TOAST thresholds and stressing cascade walks — was framed against a flat
aggregation model. That shape does not actually arise in practice.

Aggregations are expressed **hierarchically**:

- minute aggregations parent up to 60 raw events;
- hourly aggregations parent up to ~60 minute aggregations;
- day summaries parent up to 24 hourly aggregations;
- weekly rollups parent 7 day summaries; and so on.

Each layer fans in to at most ~60–100 children. `source_event_ids[]` stays
small at every level, TOAST is a non-issue, and the existing UUID-array
provenance keeps working unmodified through arbitrarily deep aggregation
stacks.

Producers writing aggregation chains MUST express them this way. The
substrate guidance is: never aggregate flat from raw events to a long-window
summary — always pass through the natural intermediate windows. This keeps
cascade walks shallow at each step, makes partial invalidation tractable, and
preserves the per-layer trace story.

Previously considered: a `Provenance::Query { query_hash, scope }` form with
side tables (`derivation_scopes`, `derivation_scope_members`,
`event_derivation_scopes`) was sketched as an answer to the
thousands-of-parents shape. That sketch is preserved below for cases where it
still applies, but it is no longer the primary answer for aggregation
lineage.

## Remaining Case: Query-Based / Non-Aggregation High Fan-In

Hierarchical aggregation does not cover every conceivable high-fan-in
derived. Two residual shapes can still legitimately need a representation
other than an inline UUID array:

1. **Cross-cutting derived.** A single event genuinely derived from many
   disparate parents that do not share a natural windowing dimension — for
   example, a context pack that pulls evidence from semantically related but
   temporally scattered events, or a moment-evidence bundle assembled from a
   hand-picked set.
2. **Query-defined derivation.** An output whose parentage is most honestly
   described as "all events matching this versioned query at this time" — the
   set may be large but its definition is the query, not an enumeration.

For these residual shapes, the side-table model below remains a reasonable
representation. It is not required for hierarchical aggregation, and it is
not a third provenance class: the output is still derived, with the parent
set expressed via a derivation scope instead of an inline array.

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

The `core.events` XOR provenance constraint stays conceptually unchanged:
material events cite source material; derived events cite parent events.
For the residual high-fan-in cases, the derived parentage is discovered
through `event_derivation_scopes` and `derivation_scope_members` rather than
inline arrays.

## Exact Membership vs Query-Defined Lineage

Where the side-table model is used, exact membership rows are required when:

- replay must cascade precisely through individual input events;
- trace output must explain specific evidence items;
- context packs or moment candidates expose weighted evidence;
- settlement or late-arrival invalidation needs event-level affected-scope
  detection.

Query-defined lineage without member rows is acceptable only when:

- the source query is deterministic and versioned;
- `input_query_hash`, `input_count`, and `input_set_hash` are stored;
- replay can reconstruct the set or explicitly report that exact membership
  was compacted and cannot be expanded without rerunning the query;
- the output is advisory/read-model evidence rather than a deletion/replay
  cascade authority.

## Trace Semantics

`sinexctl trace` for hierarchical aggregation should render the parent stack
naturally — each layer shows up as a normal derived node with its small
parent array, and trace traversal walks layer by layer.

For scope-backed derived, trace should render:

```text
derived event
  └─ derivation scope: context_pack/2026-05-16-velocity
       ├─ producer: context-pack-builder@semver
       ├─ input_count: 312
       ├─ input_set_hash: blake3:...
       └─ members: expandable on demand
```

Default trace output should show the scope node and summary fields.
Expansion should be explicit and paginated.

## Replay Preview

Hierarchical aggregation replays one layer at a time using the existing
per-event cascade — no new machinery required.

For scope-backed derived:

1. Recompute the scope query under the target semantics version.
2. Compare old and new `input_count`.
3. Compare old and new `input_set_hash`.
4. If hashes match, descendants may be left untouched unless producer
   semantics changed.
5. If hashes differ, show added/removed sample members and affected derived
   outputs.
6. Recompute by creating new immutable derived events and superseding or
   invalidating old outputs through normal replay/operation records.

Late-arrival settlement should use the same scope hashes to distinguish "new
evidence arrived" from "producer semantics changed."

## Archive Cascade

Archive/replay cascade must traverse both inline `source_event_ids[]` and
derivation-scope membership when exact members exist. For hierarchical
aggregation this is just the normal cascade walk through the layered
arrays. If a scope is query-defined without exact members, cascade should
mark the derived output as scope-affected and require replay preview to
recompute membership before destructive archive.

## Guardrails

- Express aggregations hierarchically by default. Never aggregate flat from
  raw events to a long-window summary.
- Keep `source_event_ids[]` for ordinary derivations, including each layer of
  a hierarchical aggregation chain.
- Reach for the side-table scope model only for genuinely cross-cutting or
  query-defined derived where hierarchical decomposition is unnatural.
- Do not introduce material provenance for event-derived summaries.
- Do not mutate aggregate event payloads in place.
- Do not hide large parent lists inside JSON payloads.
- Require pagination for member expansion when scopes are used.
