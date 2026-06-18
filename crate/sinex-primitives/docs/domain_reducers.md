# Domain Reducers

Status: partially implemented. The shared metadata vocabulary lives in
`sinex_primitives::domain_reducer`, and the task domain exports
`TASK_REDUCER_SPEC` for `tasks.current` v1. Issue #1120 remains open for the
generic projection/runtime substrate: registration, invalidation, replay
scheduling, trace shape, and object-event indexes.

Event-native domains keep events canonical. Current state is a projection,
owned by a reducer spec, and must stay traceable back to the events that built
it.

This record defines the shared reducer contract for lifecycle-shaped domains:
tasks, notes, projects, health observations, finance objects, and other domains
where users naturally ask for "the current object" even though the durable truth
is an event stream.

## Contract

A domain reducer declares one projection family:

```rust
pub struct DomainProjectionSpec {
    pub domain_id: &'static str,
    pub semantics_version: &'static str,
    pub object_kind: &'static str,
    pub input_event_types: &'static [&'static str],
    pub object_key_policy: &'static str,
    pub ordering_policy: ProjectionOrderingPolicy,
    pub settlement_policy: ProjectionSettlementPolicy,
    pub conflict_policy: ProjectionConflictPolicy,
    pub output_kind: OutputKind,
    pub output_shape: ProjectionOutputShape,
}
```

The Rust vocabulary for this contract lives in
`sinex_primitives::domain_reducer`. Domain reducers should export a typed
`DomainProjectionSpec` next to their reducer implementation; for example the
task domain exports `TASK_REDUCER_SPEC` for `tasks.current` v1 from
`sinex_primitives::task_domain`.

`output_kind` must be `OutputKind::ProjectionRow` for reducer current-state
outputs. Reducers consume canonical events but never make their current state
canonical over the event spine.

The corresponding reducer implementation must provide:

| Rule | Meaning |
| --- | --- |
| `object_key(event)` | Maps an input event to zero or one projected object. |
| `apply(state, event, ctx)` | Applies an ordered event and records whether state is final, provisional, stale, or conflicted. |
| `finalize(state)` | Emits the current projection plus trace metadata. |
| `teardown(object_key)` | Lists the events, proposals, adapters, and derived scopes that explain the object. |

Today, only the metadata vocabulary and task reducer spec are implemented.
Shared infrastructure will own registration, invalidation, replay scheduling,
trace shape, and generic object-event indexes when #1120 lands. Per-domain code
owns domain semantics: task status transitions, account balance rules, health
hypothesis state, project membership rules, and typed state validation.

## Projection Records

The planned shared schema target is a reducer registry plus a current-object
index:

```sql
create table domain.projection_specs (
  domain_id text not null,
  semantics_version text not null,
  object_kind text not null,
  input_event_types jsonb not null,
  object_key_policy text not null,
  ordering_policy jsonb not null default '{}'::jsonb,
  settlement_policy jsonb not null default '{}'::jsonb,
  conflict_policy jsonb not null default '{}'::jsonb,
  output_shape jsonb not null default '{}'::jsonb,
  status text not null check (status in ('draft', 'active', 'shadow', 'retired')),
  created_at timestamptz not null default now(),
  primary key (domain_id, semantics_version, object_kind)
);

create table domain.current_objects (
  domain_id text not null,
  semantics_version text not null,
  object_kind text not null,
  object_key text not null,
  state jsonb not null,
  state_hash text not null,
  last_event_id uuid references core.events(id),
  derivation_scope_id uuid,
  settlement_status text not null default 'final',
  conflict_status text not null default 'none',
  updated_at timestamptz not null default now(),
  primary key (domain_id, semantics_version, object_kind, object_key)
);

create table domain.object_event_index (
  domain_id text not null,
  semantics_version text not null,
  object_kind text not null,
  object_key text not null,
  event_id uuid not null references core.events(id),
  role text not null default 'lifecycle',
  ordering_key text,
  primary key (domain_id, semantics_version, object_kind, object_key, event_id, role)
);
```

`state jsonb` is an envelope, not a mandate that every domain must stay
schemaless. Domains with stronger invariants can add typed tables keyed by the
same `(domain_id, semantics_version, object_kind, object_key)` tuple. The shared
record remains the trace and invalidation rendezvous.

## Replay And Settlement

Reducers consume immutable events and produce rebuildable projections. The
planned replay behavior therefore invalidates affected object keys rather than
mutating historical state in place.

1. An archived or replayed event maps to affected object keys through
   `object_event_index`.
2. The reducer rebuilds those keys from the surviving input set, ordered by its
   declared ordering policy.
3. Late-arriving events follow the reducer's settlement policy from issue
   #1111: they can leave state
   provisional, trigger reconciliation, or mark a conflict.
4. High-fan-in outputs use a derivation scope from
   `crate/sinexd/docs/automata/high_fan_in_lineage.md` when the exact event set is too large for inline
   `source_event_ids[]`.

The event stream is still canonical. `domain.current_objects` is disposable
derived state; losing it should be recoverable from events plus reducer specs.

## Trace

When the generic substrate exists, `sinexctl trace` should treat a current
object as a projection node:

```text
domain_object:tasks/task:inbox:42
  reducer: tasks.current v1
  state: open
  settlement: final
  events:
    - declaration.recorded task.created
    - task.updated
    - proposal.judged
  adapters:
    - taskwarrior export mirror (optional)
  derivation_scope:
    - exact membership when high fan-in applies
```

Trace output must distinguish:

| Input | Canonicality |
| --- | --- |
| User declaration event | Canonical once admitted with material provenance. |
| Model proposal | Non-canonical until accepted through the proposal/judgment/finalizer path. |
| External adapter mirror | Canonical only when the adapter authority category says so. |
| Current projection row | Never canonical over the event stream. |

## Proposal And Adapter Boundaries

Reducers do not decide whether a proposed change becomes truth. They only fold
events that are already admitted to the domain stream.

Model or assistant-generated changes enter through the proposal/judgment/finalizer
substrate. External systems enter through the authority categories in
`crate/sinexd/docs/sources/integration_authority.md`:

| Adapter category | Reducer treatment |
| --- | --- |
| `EventNativeCanonical` | Fold emitted lifecycle events directly. |
| `FederatedCanonicalMirror` | Fold mirrored events and retain adapter authority in trace. |
| `ProjectionExport` | Do not fold exported state back unless a separate import adapter admits events. |
| `TransitionalReference` | Treat as comparison evidence, not domain truth. |
| `BidirectionalAdapter` | Requires explicit conflict policy before activation. |

## First Target: Tasks

Tasks are the first implementation target because the lifecycle is useful and
bounded:

| Event | Reducer effect |
| --- | --- |
| `declaration.recorded` with `task.created` assertion | Creates task object with material-provenance declaration as root. |
| `task.updated` | Applies title, project, tag, priority, or schedule changes. |
| `task.completed` | Moves state to completed and records completion time. |
| `task.deferred` | Records a future activation window. |
| `task.canceled` | Marks terminal canceled state. |
| `proposal.finalized` | Applies an accepted proposal as a normal lifecycle event. |

The task reducer should use typed state once the schema lands. A JSON envelope is
acceptable for the shared projection index, but task-specific invariants such as
terminal-state transitions belong in domain code.

Finance is second. It needs the same reducer substrate, but double-entry balance
rules, account authority, and import parity make it a poor first proof.

## Shared Versus Domain-Owned

Shared reducer infrastructure owns:

- reducer spec registration and semantics-version status;
- object-event indexing;
- replay invalidation by object key;
- current-object storage and refresh orchestration;
- generic trace shape;
- generic settlement/conflict flags;
- optional shadow-mode comparison between semantics versions.

Per-domain reducer code owns:

- object key derivation;
- accepted event type semantics;
- typed state validation;
- domain-specific conflict rules;
- domain-specific export shape;
- fixtures proving lifecycle behavior.

This boundary prevents each domain from inventing its own projection machinery
without making the projection row the new source of truth.
