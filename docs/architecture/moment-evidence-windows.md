# Moment Evidence Windows

Many useful questions are not row searches. "Commands I ran while discussing X"
starts from text or semantic hits, turns those hits into candidate time windows,
then gathers shell, chat, window, document, git, and coverage evidence around
those windows.

This record defines the shared moment-query model so context packs, semantic
search, readiness caveats, and future SQL derivations do not invent separate
temporal join rules.

## Query Model

```rust
pub struct MomentQuery {
    pub text: Option<String>,
    pub filters: Vec<EventFilter>,
    pub seed_sources: Vec<EventTypePattern>,
    pub evidence_sources: Vec<EventTypePattern>,
    pub window: MomentWindowPolicy,
    pub time_range: Option<TimeRange>,
    pub readiness_policy: ReadinessPolicy,
    pub scoring_profile: MomentScoringProfile,
}
```

| Field | Meaning |
| --- | --- |
| `text` | Keyword or semantic query. V1 can use keyword search only. |
| `filters` | Ordinary event filters applied to seed search. |
| `seed_sources` | Source families that can create candidate windows. |
| `evidence_sources` | Source families collected after a window exists. |
| `window` | How seed hits expand to intervals. |
| `time_range` | Outer bound for seed and evidence collection. |
| `readiness_policy` | Whether missing source coverage is allowed, warned, or fatal. |
| `scoring_profile` | Weights for relevance, density, diversity, alignment, and caveats. |

The query returns moment candidates:

```rust
pub struct MomentCandidate {
    pub id: Uuid,
    pub time_range: TimeRange,
    pub seed_evidence: Vec<EvidenceRef>,
    pub supporting_evidence: Vec<EvidenceRef>,
    pub caveats: Vec<CaveatRef>,
    pub scores: MomentScores,
}

pub struct EvidenceRef {
    pub event_id: Option<Uuid>,
    pub source_material_id: Option<Uuid>,
    pub anchor: Option<MaterialAnchor>,
    pub event_type: String,
    pub observed_range: TimeRange,
    pub role: EvidenceRole,
    pub weight: f32,
}
```

Evidence roles are `seed`, `support`, `contradiction`, and `caveat`. Temporal
proximity alone never proves causality; it only justifies inclusion in an
inspectable evidence window.

## Window Policies

Moment windows must be explicit:

| Policy | Use |
| --- | --- |
| `exact_event` | Use the event's own interval when the event has start/end semantics. |
| `fixed_padding` | Expand each seed hit by configured before/after durations. |
| `session_boundary` | Snap to a detected session window when session evidence exists. |
| `source_horizon` | Use source-specific horizons, such as a chat thread segment or document section. |
| `merged_cluster` | Merge nearby seed hits into one candidate when their windows overlap. |

The query must report which policy produced each candidate. Consumers should not
silently widen windows later.

## Query Phases

1. Seed search: keyword search, vector search, event filters, or document
   retrieval produce seed hits.
2. Temporal expansion: seed hits become candidate windows through the declared
   window policy.
3. Evidence collection: configured source families are gathered inside each
   window.
4. Coverage join: continuity gaps, private-mode suppression, source readiness,
   and timing-quality caveats attach to each candidate.
5. Scoring: candidates are ranked by seed relevance, evidence density, source
   diversity, temporal alignment, and caveat penalties.
6. Context projection: selected candidates can materialize an inspectable
   context pack.

## Caveats

Caveats are first-class evidence:

```rust
pub struct CaveatRef {
    pub caveat_kind: CaveatKind,
    pub source: String,
    pub time_range: TimeRange,
    pub severity: CaveatSeverity,
    pub explanation: String,
    pub evidence_ref: Option<EvidenceRef>,
}
```

Examples:

| Caveat | Meaning |
| --- | --- |
| `source_gap` | A source family has no coverage over part of the window. |
| `private_mode` | Capture was intentionally suppressed; details may be deniable. |
| `source_not_ready` | The source unit is not deployed or lacks required permissions. |
| `timing_quality_low` | Evidence timestamp is staged, inferred, atemporal, or coarse. |
| `replay_pending` | A relevant source material has pending replay or parser work. |

Coverage/readiness systems remain the authority for gap facts. Moment query only
joins them onto candidates.

## Scoring

Scores are explanatory, not hidden ranking magic:

```rust
pub struct MomentScores {
    pub seed_relevance: f32,
    pub evidence_density: f32,
    pub source_diversity: f32,
    pub temporal_alignment: f32,
    pub caveat_penalty: f32,
    pub final_score: f32,
}
```

The candidate response should include enough per-score detail for users to see
why a moment ranked highly. V1 can use deterministic keyword relevance and count
features; embeddings are optional accelerators, not required semantics.

## Durable Storage

Moment results are ephemeral by default. Persist only when the operator saves a
run, materializes a context pack, or uses a moment as evidence for another
derived artifact.

Optional durable tables:

```sql
create schema if not exists query;

create table query.moment_query_runs (
  id uuid primary key,
  query jsonb not null,
  query_hash text not null,
  created_by text not null,
  created_at timestamptz not null default now()
);

create table query.moment_candidates (
  run_id uuid not null references query.moment_query_runs(id) on delete cascade,
  candidate_id uuid not null,
  ts_start timestamptz not null,
  ts_end timestamptz not null,
  scores jsonb not null,
  caveats jsonb not null default '[]'::jsonb,
  primary key (run_id, candidate_id)
);

create table query.moment_evidence (
  run_id uuid not null,
  candidate_id uuid not null,
  event_id uuid references core.events(id),
  source_material_id uuid references raw.source_material_registry(id),
  role text not null,
  weight double precision not null default 1.0,
  metadata jsonb not null default '{}'::jsonb
);
```

Saved runs are query artifacts. They are not canonical events unless a separate
domain process admits them as evidence through normal provenance.

## Search Composition

Moment query composes with other retrieval systems:

| System | Role |
| --- | --- |
| Keyword search | Produces seed hits from payload text, document chunks, titles, command strings, and chat text. |
| Embeddings | Optional seed-search/rerank input; not required for v1. |
| Document retrieval | Produces chunk hits with material anchors. |
| SQL derivations | Provide reusable feature tables or projections under a declared derivation spec. |
| Context packs | Consume selected candidates and expose the full evidence/caveat bundle. |

SQL derivations can implement reusable features, but moment-window semantics
belong here. Avoid anonymous views that encode scoring or expansion rules
without a spec/version.

## Fixture Query

Question: "commands I ran while discussing async runtime debugging".

```text
sinexctl query moments \
  --text "async runtime debugging" \
  --seed chat,message,document \
  --evidence shell,window,git,document \
  --window session \
  --format json
```

Expected shape:

1. Chat/document hits seed one or more candidate windows.
2. The session policy snaps hits to detected work sessions when available.
3. Shell commands, window focus, git changes, and nearby documents attach as
   supporting evidence.
4. Source gaps and private-mode intervals attach as caveats.
5. The response can be turned into a context pack without re-running separate
   temporal joins.

Partial results are valid when caveated. A missing browser or document source
does not make shell/chat evidence unusable; it lowers readiness/confidence and
must be visible in the candidate.
