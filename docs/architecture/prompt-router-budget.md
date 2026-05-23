# Prompt Router And Budget Ledger

Model-effect recording is necessary but not sufficient. Sinex also needs shared
prompt registry, model routing, rollout/canary, privacy eligibility, and budget
accounting before model calls are attempted.

This record defines that policy layer. It sits above recorded model effects and
below proposal/judgment/finalizer authority.

## Contracts

```rust
pub struct ModelTaskRequest {
    pub task_kind: String,
    pub prompt_id: String,
    pub input_hash: String,
    pub privacy_context: PrivacyContext,
    pub operation_id: Uuid,
    pub bucket_key: Option<String>,
}

pub trait ModelRouter {
    fn decide(&self, request: &ModelTaskRequest) -> Result<RoutingDecision, RoutingError>;
}

pub struct RoutingDecision {
    pub policy_id: String,
    pub prompt_id: String,
    pub prompt_version: String,
    pub provider: String,
    pub model: String,
    pub experiment_id: Option<String>,
    pub bucket_key: Option<String>,
    pub decision_reason: String,
}
```

Model callers must ask the router. They should not hardcode prompt bodies,
provider strings, model names, fallback behavior, or canary percentages in node
logic.

## Schema Shape

```sql
create schema if not exists llm;

create table llm.prompt_templates (
  prompt_id text not null,
  version text not null,
  purpose text not null,
  template_hash text not null,
  schema_hash text,
  privacy_class text not null,
  owner text not null,
  status text not null check (status in ('draft', 'active', 'shadow', 'retired')),
  body_storage_ref text,
  created_at timestamptz not null default now(),
  primary key (prompt_id, version)
);

create table llm.routing_policies (
  policy_id text primary key,
  task_kind text not null,
  allowed_models jsonb not null,
  fallback_order jsonb not null,
  replay_policy text not null,
  privacy_policy_ref text,
  rollout jsonb not null default '{}'::jsonb,
  active boolean not null default true,
  updated_at timestamptz not null default now()
);

create table llm.routing_decisions (
  id uuid primary key,
  operation_id uuid references core.operations_log(id),
  task_kind text not null,
  policy_id text not null references llm.routing_policies(policy_id),
  prompt_id text not null,
  prompt_version text not null,
  provider text not null,
  model text not null,
  experiment_id text,
  bucket_key text,
  decision_reason text not null,
  created_at timestamptz not null default now()
);

create table llm.budget_ledger (
  id uuid primary key,
  operation_id uuid references core.operations_log(id),
  routing_decision_id uuid references llm.routing_decisions(id),
  caller text not null,
  provider text not null,
  model text not null,
  prompt_tokens bigint,
  completion_tokens bigint,
  cost_estimate_microusd bigint,
  runtime_ms bigint,
  status text not null,
  failure_class text,
  created_at timestamptz not null default now()
);
```

Prompt bodies can live outside the table when privacy or repo policy requires
it. The table stores durable hashes and storage references.

## Routing

Routing inputs:

- task kind;
- prompt id;
- active routing policy;
- privacy context and data class;
- model/provider availability;
- budget remaining;
- deterministic bucket key for rollout/canary;
- operator override when authorized.

Routing output is a recorded decision. Model effects must link to the routing
decision that selected prompt/model/provider.

Deterministic A/B and canary routing derives buckets from:

```text
bucket = hash(policy_id || experiment_id || bucket_key || task_kind)
```

Given the same policy and bucket key, the route is reproducible.

## Budget Ledger

Budget ledger entries are written for successes and failures. They record:

- caller and task kind;
- routing decision;
- provider/model;
- prompt/completion tokens where available;
- runtime;
- estimated cost;
- failure class;
- operation id.

Status/reporting surfaces should answer:

```text
sinexctl llm prompts list
sinexctl llm budget report --since 7d
sinexctl llm routing explain --task entity-extraction --input-hash ...
```

## Privacy

Routing consults privacy policy before sending text to a model.

| Privacy result | Router behavior |
| --- | --- |
| remote allowed | choose eligible remote/local model by policy. |
| force local | route only to local models or fail closed. |
| redact required | require caller to provide redacted input hash/body. |
| disallowed | reject before model-effect attempt and record budget failure/decision. |

Sensitive prompt bodies should not be stored plaintext unless policy permits it.

## Relation To Other Systems

| System | Relationship |
| --- | --- |
| Model effects | Actual calls and replay policy; linked to routing decision. |
| Semantic epochs | Reference prompt/router/model policy hashes for shadow comparison. |
| Proposal/judgment/finalizer | Owns canonical promotion; prompts only propose. |
| Embeddings | Use same router/budget policy when model/provider selection matters. |
| MCP/context server | Read-only context can feed prompt inputs, but action/promotion still goes through authority boundaries. |

## First Slice

First implementation target:

1. prompt template registry with id/version/hash/status;
2. routing policy registry for one task kind;
3. deterministic router decision with bucket key;
4. budget ledger row for success/failure;
5. model-effect record links to routing decision.

Do not implement all LLM-backed automata in the first slice.

## Boundaries

- Do not choose a permanent provider/model here.
- Do not bypass model-effect recording.
- Do not let prompt output become canonical without proposal/judgment/finalizer.
- Do not store sensitive prompt bodies in plaintext by default.
- Do not allow model callers to bypass the router with hardcoded provider/model
  strings.
