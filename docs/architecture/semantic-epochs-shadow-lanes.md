# Semantic Epochs And Shadow Lanes

Semantic layers evolve differently from source parsers. Entity extraction,
relation extraction, chunking, embedding, prompt routing, and living-document
reasoners can all change interpretation without changing the underlying source
material. Sinex needs replay-safe experimentation for those layers before a new
interpretation becomes canonical.

This record defines semantic epochs, shadow lanes, comparison reports, and
promotion/discard semantics.

## Concepts

```rust
pub struct SemanticEpoch {
    pub epoch_id: Uuid,
    pub name: String,
    pub scope: SemanticScope,
    pub code_ref: Option<String>,
    pub config_hash: String,
    pub components: Vec<SemanticComponentVersion>,
    pub prompt_set_hash: Option<String>,
    pub model_config_hash: Option<String>,
    pub created_by: ActorRef,
    pub operation_id: Uuid,
}

pub struct ShadowLane {
    pub lane_id: Uuid,
    pub name: String,
    pub kind: LaneKind,
    pub base_epoch_id: Option<Uuid>,
    pub candidate_epoch_id: Uuid,
    pub scope: SemanticScope,
    pub status: ShadowStatus,
    pub purpose: String,
}
```

| Concept | Meaning |
| --- | --- |
| Semantic epoch | Versioned semantic configuration: code ref, config, prompt/router/model policy ids, and component versions. |
| Canonical lane | Current promoted interpretation for a scope. |
| Shadow lane | Candidate interpretation over a fixed scope, isolated from canonical projections. |
| Experiment lane | Disposable or time-limited lane used for local comparison. |
| Comparison report | Machine-readable diff between baseline and candidate lanes. |

An epoch is not only a code version. It is the named set of semantic inputs that
can affect interpretation.

## Storage Shape

```sql
create schema if not exists semantic;

create table semantic.epochs (
  id uuid primary key,
  name text not null,
  scope jsonb not null,
  code_ref text,
  config_hash text not null,
  components jsonb not null,
  prompt_set_hash text,
  model_config_hash text,
  created_by text not null,
  operation_id uuid references core.operations_log(id),
  created_at timestamptz not null default now(),
  supersedes_epoch_id uuid references semantic.epochs(id),
  unique (scope, config_hash)
);

create table semantic.lanes (
  id uuid primary key,
  name text not null,
  kind text not null check (kind in ('canonical', 'shadow', 'experiment')),
  base_epoch_id uuid references semantic.epochs(id),
  candidate_epoch_id uuid not null references semantic.epochs(id),
  scope jsonb not null,
  status text not null check (status in ('planned', 'running', 'completed', 'compared', 'promoted', 'discarded', 'expired')),
  purpose text,
  operation_id uuid references core.operations_log(id),
  created_at timestamptz not null default now(),
  completed_at timestamptz,
  expires_at timestamptz
);

create table semantic.lane_outputs (
  lane_id uuid not null references semantic.lanes(id) on delete cascade,
  output_kind text not null,
  output_key text not null,
  source_event_id uuid references core.events(id),
  source_material_id uuid references raw.source_material_registry(id),
  source_anchor jsonb,
  output_hash text not null,
  payload jsonb not null,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  primary key (lane_id, output_kind, output_key)
);

create table semantic.lane_diffs (
  id uuid primary key,
  baseline_lane_id uuid not null references semantic.lanes(id),
  candidate_lane_id uuid not null references semantic.lanes(id),
  diff_kind text not null,
  counts jsonb not null,
  examples jsonb not null default '[]'::jsonb,
  report_hash text not null,
  created_at timestamptz not null default now(),
  unique (baseline_lane_id, candidate_lane_id, diff_kind, report_hash)
);
```

Shadow outputs are not canonical events or projections. They are experiment
artifacts. If storage pressure or privacy requires deletion, keep compact diff
reports and delete raw lane outputs through an operation record.

## Runtime Algorithm

1. Resolve lane scope to a fixed input set before running. Use source material,
   event ids, document chunks, or derivation scopes so baseline and candidate
   compare the same evidence.
2. Create a semantic epoch for the candidate config.
3. Create a shadow lane referencing the baseline and candidate epochs.
4. Run candidate automata/parsers in lane mode, or import a deterministic
   lane-output artifact from an external runner, and write outputs to
   `semantic.lane_outputs`.
5. Compute deterministic diffs by output kind.
6. Present comparison reports for review.
7. Promote only through a judgment/finalizer or explicit operation.
8. Discard or expire lanes through operation records.

The hot ingestion path should not depend on this runtime. Shadow lanes are
operator-initiated semantic experiments.

The current first slice implements the registry, isolated output storage,
explicit output writes, deterministic entity/relation diff recording, CLI/RPC
inspection, and read-only MCP inspection. It intentionally does not yet ship an
autonomous lane runner that invokes entity/relation automata in lane mode.
Until that runner exists, `sinexctl semantics lane write-outputs` is the
operator/import boundary for candidate outputs produced by a controlled
external run.

## Comparison Reports

Reports should be machine-readable:

| Output kind | Diff examples |
| --- | --- |
| Entities | new, missing, split, merge, category changed, confidence changed. |
| Relations | edge added, edge removed, weight changed, predicate changed. |
| Chunks | boundary moved, anchor changed, chunk split/merged. |
| Embeddings | missing vector, model changed, ranking delta for fixed queries. |
| Proposals | new proposal, withdrawn proposal, judgment debt changed. |
| Context packs | answer/evidence changes for fixed fixtures. |

Every report includes counts plus representative examples. The report should
also include input-set hash, baseline epoch, candidate epoch, and scope.

## Model Effects And Judgments

Model-effect replay policy is external to the lane mechanism:

- deterministic semantic lanes can rerun freely;
- lanes that would call an LLM must use recorded model effects unless the
  model-effect policy explicitly permits new calls;
- prompt/router/model policy versions are referenced by stable ids or hashes,
  not copied wholesale into the epoch row;
- budget/canary policy belongs to the prompt/router infrastructure.

User judgments are durable authority records. Regenerating candidates in a
shadow lane must not erase or overwrite previous judgments. A lane comparison
may report judgment debt, such as "12 previously accepted entities disappear
under candidate config", but promotion must route through the judgment/finalizer
boundary when canonical state changes.

## Promotion And Discard

Promotion means a candidate epoch becomes canonical for its declared scope. The
lane output itself remains historical evidence; it does not silently become
canonical state.

Promotion requires:

1. comparison report exists;
2. authority record exists: user judgment, operator operation, or approved PR
   policy, depending on domain;
3. finalizer writes canonical events/projections or schedules replay under the
   promoted epoch;
4. old canonical epoch is retained as superseded;
5. trace can explain which epoch produced the current object.

Discard means the lane will never be promoted. Discarded lanes can keep reports
while raw outputs expire.

## First Slice

The first implementation target is entity/relation shadow comparison over a
fixed source-material or document-chunk scope.

Why this target:

- entity/relation churn is visible and easy to count;
- current automata already carry `semantics_version`;
- promotion can stay out of canonical tables initially;
- it does not depend on embedding infrastructure or live model calls;
- it gives the knowledge-graph activation work a reviewable safety rail.

Embedding-model comparison is second. It needs recorded model-effect policy and
ranking fixtures before it can be honest.

## Operator Surface

```text
sinexctl semantics epoch create --name entity-v2 --scope scope.json --component entity-extractor=2.0
sinexctl semantics epoch list
sinexctl semantics lane create --kind shadow --candidate-epoch-id <epoch-id> --scope scope.json
sinexctl semantics lane list --status planned
sinexctl semantics lane status <lane-id> --status running
sinexctl semantics lane write-outputs <lane-id> --outputs-file lane-outputs.json
sinexctl semantics lane outputs <lane-id> --format json
sinexctl semantics lane compare --baseline-lane-id <baseline> --candidate-lane-id <candidate>
sinexctl semantics lane diffs <lane-id> --format json
sinexctl semantics lane promote <lane-id> --via-judgment <judgment-id>
sinexctl semantics lane discard <lane-id> --reason "merge churn too high"
```

`promote` is a future surface, not an implemented command. Promotion remains
behind the proposal/judgment/finalizer authority boundary. The implemented
discard command records a status transition and explicit discard reason; raw
output deletion must stay an explicit operation.

Read-only MCP exposes the same inspection vocabulary for agent use:

- `sinex.semantic_epochs`
- `sinex.semantic_lanes`
- `sinex.semantic_lane_outputs`
- `sinex.semantic_lane_diffs`

## Boundaries

- Do not replace source-material/parser replay. Semantic lanes are for
  interpretation changes over fixed inputs.
- Do not let shadow outputs appear in ordinary query results unless the query
  explicitly asks for that lane.
- Do not promote model-generated changes without the proposal/judgment/finalizer
  authority boundary.
- Do not rerun model calls during deterministic replay unless model-effect
  policy permits it.
- Do not make every automaton epoch-aware immediately; add lane mode to concrete
  semantic producers as needed.
