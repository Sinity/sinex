# Semantic Trace Teardown

`sinexctl trace` already needs simple provenance ancestry. Higher semantic
layers also need a teardown view: what parts of this object came from observed
material, imported structure, declarations, deterministic inference, model
effects, judgments, omissions, and projections?

This record defines trace perspectives and the teardown output shape. It does
not replace ancestry tracing; it adds shared adapters so projections do not each
invent bespoke trace logic.

## Perspectives

Trace perspectives are named graph/query modes:

| Perspective | Question |
| --- | --- |
| `ancestry` | Which material or parent events directly explain this object? |
| `siblings` | What else came from the same source material, anchor, or derivation scope? |
| `supersession` | Which previous interpretations, semantic renames, or replacements does this object supersede? |
| `temporal_neighborhood` | What nearby events/materials may explain context without implying causality? |
| `candidates` | Which proposed, rejected, or modified candidates are relevant? |
| `teardown` | What is the target's epistemic composition and caveat set? |

CLI shape:

```text
sinexctl trace <id> --perspective ancestry
sinexctl trace <id> --perspective siblings
sinexctl trace <id> --perspective supersession
sinexctl trace <id> --perspective temporal-neighborhood
sinexctl trace <id> --perspective candidates
sinexctl trace <id> --teardown
sinexctl trace <id> --format dot --perspective teardown
```

Gateway shape:

| Method | Role |
| --- | --- |
| `trace.get` | Existing ancestry-compatible trace. |
| `trace.perspective` | Perspective-specific graph/query. |
| `trace.teardown` | Typed semantic composition summary. |
| `trace.expand_scope` | Lazy expansion for high-fan-in derivation scopes. |

## Teardown Output

```rust
pub struct SemanticTeardown {
    pub target: SubjectRef,
    pub target_kind: String,
    pub composition: TeardownComposition,
    pub provenance_roots: Vec<ProvenanceRootSummary>,
    pub derivation_scopes: Vec<DerivationScopeRef>,
    pub declarations: Vec<DeclarationRef>,
    pub judgments: Vec<JudgmentRef>,
    pub model_effects: Vec<ModelEffectRef>,
    pub semantic_epoch: Option<SemanticEpochRef>,
    pub caveats: Vec<CaveatRef>,
}

pub struct TeardownComposition {
    pub material_observation_count: u64,
    pub imported_structure_count: u64,
    pub user_declaration_count: u64,
    pub deterministic_inference_count: u64,
    pub model_effect_count: u64,
    pub judgment_count: u64,
    pub omission_caveat_count: u64,
    pub unknown_or_compacted_count: u64,
}
```

Composition counts are audit summaries over known links. They are not
percentages of epistemic certainty.

## Shared Trace Adapters

Shared adapters should cover common relationship classes:

| Adapter | Inputs |
| --- | --- |
| Event ancestry | `core.events.source_material_id`, `core.events.source_event_ids`. |
| Material siblings | Same source material plus nearby anchors. |
| Derivation scope | High-fan-in scope membership and summaries. |
| Declaration index | Declaration subjects and conceptual-time assertions. |
| Proposal/judgment | Proposal, judgment, finalizer records. |
| Semantic rename/supersession | Old/new interpretation links and replacement records. |
| Moment/context evidence | Saved context-pack or moment evidence refs. |
| Continuity caveats | Source gaps, private-mode omission records, source readiness. |

Projection tables can implement a small target resolver:

```rust
pub trait TraceTargetAdapter {
    fn resolve_target(&self, target: SubjectRef) -> Option<TraceTarget>;
    fn primary_events(&self, target: &TraceTarget) -> Vec<EventId>;
    fn related_subjects(&self, target: &TraceTarget, perspective: TracePerspective) -> Vec<SubjectRef>;
}
```

Most trace logic should live in shared adapters. Per-domain projections provide
target resolution and domain-specific relation labels, not full provenance
walkers.

## Algorithm

1. Resolve target to an event, source material, domain object, entity, note,
   context pack, moment candidate, or report.
2. Walk direct material/derived provenance to configured depth.
3. Expand high-fan-in derivation scopes lazily. Default output shows the scope
   summary, input count, hash, and representative samples rather than every
   member.
4. Join declaration, proposal/judgment, model-effect, semantic-epoch,
   supersession, and continuity-caveat adapters.
5. Build the requested perspective graph.
6. Return stable JSON, and optionally DOT or a terse narrative rendering.

The teardown perspective can reuse ancestry data, but it is not only ancestry:
it summarizes relationship classes that may sit beside the parent chain.

## Lazy Scope Expansion

For compacted lineage, default trace output should render:

```text
derived_scope:daily-summary/2026-05-16
  input_count: 18427
  input_set_hash: blake3:...
  representative_inputs: 20
  expand: trace.expand_scope(scope_id, limit, cursor)
```

`trace.expand_scope` pages through members. It must not expand every member by
default, and the teardown's `unknown_or_compacted_count` should include any
unexpanded compacted membership.

## Fixtures

### Task From Note With Judgment

```text
note material
  -> extracted task proposal
  -> user judgment modifies title
  -> finalizer emits declaration.recorded task.created
  -> task reducer projects current task
```

Expected teardown:

- one material observation root for the note;
- one proposal and one judgment;
- one declaration consumed by the task reducer;
- deterministic projection for current task state;
- no model-effect count if extraction is rule-based.

### Context Pack With Compacted Lineage

```text
moment query run
  -> candidate window
  -> context pack
  -> derivation scope with thousands of evidence events
```

Expected teardown:

- compacted derivation scope rendered without full expansion;
- evidence roles split into seed/support/caveat;
- caveats include source gaps and private-mode intervals when present;
- `trace.expand_scope` can page evidence members.

## Boundaries

- Do not make rejected candidates canonical. They appear only in the
  `candidates` perspective and teardown references.
- Do not infer causality from temporal neighborhood links.
- Do not collapse model effects and judgments: prompts can propose, judgments
  authorize, finalizers deterministically apply.
- Do not require every projection table to implement bespoke trace logic.
- Do not expose deniable private-mode detail through omission caveats.
