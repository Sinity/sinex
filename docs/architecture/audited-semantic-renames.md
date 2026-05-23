# Audited Semantic Renames

Some taxonomy changes are label corrections. Others are true reinterpretations
that require replay. Sinex needs a cheap path for the first class without
weakening event immutability or hiding semantic changes.

## Decision

Use read-time aliases and projection rebuilds for label-only renames. Do not
mutate `core.events` rows in place. Use archive/replay or scope recomputation
when the underlying interpretation changes.

Chosen mechanisms:

| Mechanism | Use |
| --- | --- |
| Alias catalog | Event type/source/source-unit/schema-label rename where the observed fact and payload semantics did not change. |
| Projection rebuild | Read models, search indexes, generated docs, and UI labels that can be regenerated under the alias catalog. |
| Archive/replay/recompute | Parser logic, privacy output, timestamp derivation, occurrence identity, payload content, or source coverage changed. |

Rejected mechanisms:

| Alternative | Rejected because |
| --- | --- |
| Direct metadata UPDATE on `core.events` | Narrows the no-mutation invariant in the highest-risk table. |
| Always archive-and-rewrite | Creates new event ids and derived churn for label-only changes. |
| Ad hoc client aliases | Every consumer drifts and rename history becomes invisible. |

## Alias Catalog

Candidate schema:

```sql
create table sinex_schemas.event_type_aliases (
  old_source text,
  old_event_type text not null,
  new_source text,
  new_event_type text not null,
  effective_at timestamptz not null default now(),
  reason text not null,
  operation_id uuid not null references core.operations_log(id),
  primary key (old_source, old_event_type)
);

create table sinex_schemas.payload_field_aliases (
  source text not null,
  event_type text not null,
  old_field_path text not null,
  new_field_path text not null,
  effective_at timestamptz not null default now(),
  reason text not null,
  operation_id uuid not null references core.operations_log(id),
  primary key (source, event_type, old_field_path)
);

create table sinex_schemas.source_unit_aliases (
  old_source_unit_id text primary key,
  new_source_unit_id text not null,
  effective_at timestamptz not null default now(),
  reason text not null,
  operation_id uuid not null references core.operations_log(id)
);
```

Consumers that expose canonical names must use shared canonicalization helpers.
Low-level historical queries can still ask for stored names.

## Preview

Every rename operation starts with a preview:

```text
sinexctl events rename-type --from command.executed --to shell.command.executed --dry-run
sinexctl schema alias-field --event-type browser.page.visited --from url_title --to title --dry-run
sinexctl events rename-preview --operation rename.json
```

Preview output must include:

- affected stored source/event type pairs;
- row counts;
- time range covered by affected rows;
- source units and parser semantics versions involved;
- payload schema ids involved;
- generated docs/proof surfaces affected;
- derived projection rebuild impact;
- classification: alias-only, projection-only, replay-required, or rejected.

## Replay-Required Changes

Route to replay/recompute when any of these change:

| Change | Reason |
| --- | --- |
| Parser emits different events | The interpretation of source material changed. |
| Payload content changes | Stored fact changed, not only its label. |
| Privacy/redaction policy changes payload | Replay must apply current privacy rules. |
| Timestamp derivation changes | `ts_orig` semantics changed. |
| Occurrence identity/anchor changes | Real-world identity mapping changed. |
| Source rows previously skipped | Coverage changed. |
| Schema validation changes accepted/rejected payloads | Persistence semantics changed. |

The preview should reject alias mode for these and point to replay tooling.

## Audit

Aliases are operation-owned:

1. create `core.operations_log` entry with operator, reason, dry-run report hash,
   and requested changes;
2. insert alias rows in one transaction;
3. rebuild affected projections/indexes/docs through named follow-up jobs;
4. trace and query surfaces include operation id when canonical names differ
   from stored names.

Rollback means adding a reversing alias operation or retiring an alias through a
new operation. Do not silently delete history.

## Taxonomy Migration Use

Global taxonomy migrations use this mechanism for label-only transitions:

1. publish taxonomy design/source-of-truth update;
2. add alias rows for old names;
3. update new producers to emit new names;
4. rebuild generated schema/proof/docs surfaces;
5. update query helpers to canonicalize old and new names;
6. only consider event replay if payload semantics changed.

This gives #1082-style taxonomy work a bridge without pretending historical
rows were originally emitted under the new names.

## Verification Plan

Label-only rename test:

- seed events under `command.executed`;
- add alias to `shell.command.executed`;
- query through canonical helper and verify both old stored rows and new rows
  match the canonical name;
- verify raw historical query can still see stored names;
- verify operation id appears in alias metadata.

Parser-semantics rejection test:

- request a rename operation that changes timestamp policy or payload field
  meaning;
- preview classifies it as `replay_required`;
- no alias row is inserted;
- suggested verification points to replay/material interpretation tooling.

## Boundaries

- Do not mutate arbitrary payloads.
- Do not use aliases to avoid privacy, timestamp, occurrence, or parser replay.
- Do not allow aliases without operation/audit evidence.
- Do not require every low-level forensic query to hide stored names.
- Do not conflate relational schema column renames with event taxonomy aliases.
