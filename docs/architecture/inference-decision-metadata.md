# Inference Decision Metadata

Derived outputs sometimes include a decision: thresholded entity matching,
duplicate resolution, relation scoring, heuristic session boundaries,
sampling/selection, or external-state-dependent classification. These decisions
need replayable metadata without turning confidence into truth and without
duplicating model-effect records.

## Contract

```rust
pub struct InferenceDecisionMetadata {
    pub producer_id: String,
    pub semantics_version: String,
    pub decision_kind: String,
    pub confidence: Option<f32>,
    pub threshold: Option<f32>,
    pub seed_material: Option<SeedMaterial>,
    pub deterministic_seed: Option<u64>,
    pub basis: Vec<EvidenceRef>,
    pub parameters_hash: String,
    pub external_state_refs: Vec<ExternalStateRef>,
    pub replay_policy: InferenceReplayPolicy,
}

pub enum InferenceReplayPolicy {
    Deterministic,
    RecomputeWithCapturedExternalState,
    RecomputeOnlyInShadowLane,
    RequiresJudgment,
}
```

Store this metadata only when a producer made a non-obvious decision. Do not
force every deterministic 1:1 transform to write verbose metadata.

## Side Table

Use a side table when decisions need query, trace, or cross-producer analysis:

```sql
create table core.inference_decisions (
  id uuid primary key,
  event_id uuid references core.events(id) on delete cascade,
  proposal_id uuid,
  producer_id text not null,
  semantics_version text not null,
  decision_kind text not null,
  confidence double precision,
  threshold double precision,
  seed_material jsonb,
  deterministic_seed bigint,
  basis jsonb not null default '[]'::jsonb,
  parameters_hash text not null,
  external_state_refs jsonb not null default '[]'::jsonb,
  replay_policy text not null,
  created_at timestamptz not null default now()
);
```

Embed metadata in payload only when it is part of the domain contract, such as a
public entity-extraction confidence. Put operational/replay metadata in the side
table.

## Model Effects Boundary

LLM calls, embedding calls, and other model/provider invocations use model
effect records. Inference decision metadata can reference those effects, but it
does not replace them.

| Case | Record |
| --- | --- |
| Deterministic entity matcher scored candidates | Inference decision metadata. |
| LLM extracted candidate task from prose | Model effect plus proposal metadata; optional inference decision metadata for final threshold/gating. |
| Embedding nearest-neighbor ranking | Model effect for vector generation; inference decision metadata for thresholded acceptance/rerank decision. |
| Pure SQL count rollup | No inference decision metadata unless threshold/gating occurs. |

## External State

External mutable state is forbidden in deterministic replay unless captured.

Allowed patterns:

- capture external state as source material before deciding;
- record a model/effect or API-effect record with stable request/response
  metadata;
- store an external-state reference sufficient for audit and mark replay as
  `RecomputeWithCapturedExternalState`;
- refuse deterministic replay and require shadow-lane reevaluation.

Hidden reads of mutable files, web APIs, clocks, environment variables, or
runtime state must not influence a decision without a recorded reference.

## Deterministic Seeds

If randomness or tie-breaking is needed, derive a seed from stable inputs:

```text
seed = blake3(
  producer_id ||
  semantics_version ||
  decision_kind ||
  input_set_hash ||
  scope_key ||
  explicit_salt
)
```

Store either the seed or the seed material. Changing the salt or parameters hash
is a semantic change and belongs in a shadow lane or replay plan.

## Proposals

Weak inference should usually produce proposals, not canonical events.

Proposal records can reference inference decision metadata to explain:

- why the candidate was proposed;
- confidence and threshold at proposal time;
- evidence basis;
- deterministic seed/tie-breaker;
- whether replay can reproduce the candidate.

Judgment/finalizer decides whether the candidate becomes canonical. Confidence
alone is not authority.

## Fixtures

### Entity Match Confidence

Input: two entity mentions with similar normalized names and overlapping
evidence.

Expected metadata:

- `decision_kind = entity.match`;
- confidence and threshold recorded;
- basis references both mention events/material anchors;
- replay policy deterministic;
- weak match below threshold becomes proposal or no-op, not canonical merge.

### Deterministic Selection

Input: ten candidate tags, UI needs three representative examples.

Expected metadata:

- seed material includes input-set hash, producer id, semantics version, and
  salt;
- selected examples reproduce across replay;
- changing salt is a semantic change.

### Threshold Change In Shadow Lane

Input: relation extractor threshold changes from `0.72` to `0.66`.

Expected metadata:

- canonical lane decisions remain unchanged;
- shadow lane records new parameters hash and threshold;
- comparison report shows added/removed relation proposals;
- previous user judgments are preserved.

## Boundaries

- Do not make confidence a universal truth score.
- Do not store verbose inference metadata for every simple transform.
- Do not replace model-effect records.
- Do not allow hidden external state in deterministic replay.
- Do not let confidence bypass proposal/judgment/finalizer for canonical weak
  assertions.
