# SQL Derivation Engine

Some derived surfaces are SQL-shaped. They need versioned specs, run metadata,
lineage, settlement, and replay semantics, not bespoke Rust automata and not
anonymous views.

## Contract

A SQL derivation is a registered query spec:

```rust
pub struct SqlDerivationSpec {
    pub derivation_id: String,
    pub semantics_version: String,
    pub output_kind: SqlDerivationOutputKind,
    pub input_contract: InputContract,
    pub query_hash: String,
    pub query_sql_ref: String,
    pub schedule: DerivationSchedule,
    pub settlement_policy: SettlementPolicyRef,
    pub provenance_policy: ProvenancePolicyRef,
    pub resource_policy: ResourcePolicy,
}

pub enum SqlDerivationOutputKind {
    ProjectionTable { table: String },
    CandidateQueue { proposal_kind: String },
    DerivedEvent { event_type: String },
}
```

Use SQL derivations for deterministic set transforms, rollups, feature tables,
readiness/coverage summaries, and simple projection maintenance. Use Rust
automata for complex state machines, external calls, model calls, parser logic,
or logic that needs rich procedural validation.

## Schema Shape

```sql
create table core.sql_derivation_specs (
  derivation_id text not null,
  semantics_version text not null,
  output_kind text not null,
  output_target text not null,
  input_contract jsonb not null,
  query_hash text not null,
  query_sql_ref text not null,
  schedule jsonb not null,
  settlement_policy jsonb not null,
  provenance_policy jsonb not null,
  resource_policy jsonb not null default '{}'::jsonb,
  status text not null check (status in ('draft', 'active', 'shadow', 'retired')),
  created_at timestamptz not null default now(),
  primary key (derivation_id, semantics_version)
);

create table core.sql_derivation_runs (
  id uuid primary key,
  derivation_id text not null,
  semantics_version text not null,
  operation_id uuid references core.operations_log(id),
  scope jsonb not null,
  input_count bigint,
  input_set_hash text,
  output_count bigint,
  status text not null,
  started_at timestamptz not null default now(),
  completed_at timestamptz,
  failure jsonb
);
```

Specs can point at checked-in SQL files by `query_sql_ref`; the hash pins the
exact query body used for replay. Runtime RPC users do not submit arbitrary SQL.

## Output Rules

| Output kind | Use | Provenance |
| --- | --- | --- |
| Projection table | Read models, coverage/readiness, moment features. | Run metadata plus input contract; no event emitted by default. |
| Candidate queue | Proposed assertions requiring review. | Proposal path; not canonical until judged/finalized. |
| Derived event | Durable semantic event with event-stream semantics. | Derived provenance; high fan-in uses derivation scopes. |

Emit derived events only when downstream consumers need event semantics:
immutability, event-time querying, trace as event, replay/archive participation,
or automata subscriptions. Otherwise prefer projection tables.

## Runtime Algorithm

1. Load active specs from checked-in or registered catalog.
2. Resolve scope from schedule: event trigger, time window, replay operation, or
   manual run.
3. Check settlement/watermark policy before final output.
4. Execute the SQL in a bounded read snapshot.
5. Write outputs through approved output writers, not arbitrary SQL mutation.
6. Record run metadata: input count, input-set hash, output count, query hash,
   operation id, and status.
7. For high-fan-in derived events, create a derivation scope and compact
   lineage.
8. For semantic experiments, write to the selected shadow lane.
9. Publish invalidation/readiness signals after successful run.

## Validation And Safety

SQL derivations must pass validation before activation:

- query body comes from reviewed repo content or an operator-approved registry;
- read set is limited to declared input schemas/tables;
- write set is limited to approved output target;
- no DDL, unsafe functions, network/file access, or dynamic SQL;
- resource policy declares timeout, row limit, memory expectation, and schedule;
- output events have schemas and payload validation;
- query hash and semantics version are recorded in every run;
- replay compares input-set hash and query hash to distinguish input changes
  from semantics changes.

This engine is not a user SQL console.

## Replay And Settlement

Integrations:

| Design | Integration |
| --- | --- |
| High-fan-in lineage | Derived events with large inputs use derivation scopes, input counts, and input-set hashes. |
| Late-arrival settlement | Watermarks decide final/caveated output and replay reason. |
| Semantic epochs | Shadow derivations can target a lane instead of canonical projections. |
| Audited renames | Query refs and output targets update through alias/projection rebuild operations when label-only. |

Replay has two modes:

| Mode | Trigger |
| --- | --- |
| input replay | Input set changed while query hash and semantics version stayed stable. |
| semantic replay | Query hash, semantics version, or registered spec changed. |

Both modes produce run records. Only semantic replay implies the derivation logic
changed.

## First Slice

First implementation target: source readiness/continuity projection.

Reasons:

- low semantic risk;
- no model calls;
- output is a projection table, not canonical event mutation;
- consumes source material/acquisition/parser/private-mode coverage data;
- directly supports readiness/caveat surfaces for moment queries and context
  packs.

Second target: moment-query scoring feature tables.

## Fixture

Source readiness derivation:

1. Seed source material rows, acquisition run records, and a private-mode gap.
2. Run `source_readiness.v1` SQL derivation for a day.
3. Verify a projection row reports covered ranges, gaps, caveats, input count,
   and input-set hash.
4. Add a late source material row inside the day.
5. Re-run and verify the new run is classified as input replay, not semantic
   replay.

## Boundaries

- Do not replace Rust automata for complex logic.
- Do not let anonymous SQL views become canonical semantics.
- Do not allow model calls from SQL.
- Do not bypass proposal/judgment/finalizer for canonical semantic assertions.
- Do not accept arbitrary mutation SQL over RPC.
