# Proposal Judgment Finalizer

Status: architecture contract for #1086. Implementation first slice is tracked
in #1352.

Sinex needs model and heuristic outputs to be useful without becoming
unaccountable authority. The proposal/judgment/finalizer substrate is that
boundary: producers can suggest, authorized actors judge, and deterministic
finalizers apply accepted changes with traceable provenance.

## Roles

| Record | Authority | Purpose |
|--------|-----------|---------|
| Proposal | Producer-owned | Candidate canonical assertion or operation, with target, payload, evidence, confidence, and rationale. |
| Judgment | User/operator/policy-owned | Accept, reject, or modify a proposal with actor, timestamp, comment, and optional corrected payload. |
| Finalization | System-owned deterministic step | Applies an accepted judgment and emits/records canonical output with provenance to proposal and judgment. |

Models, prompts, embeddings, SQL heuristics, parser workbenches, and coding
agents may produce proposals. They do not authorize promotion by themselves.

## Proposal Shape

A proposal should record:

- stable proposal id;
- proposal kind, such as `entity.merge`, `task.create`, `relation.add`,
  `parser.mapping`, `instruction.request`;
- target reference, if the proposal modifies an existing object;
- candidate payload or operation;
- evidence chain: event ids, source material ids, model-effect ids, inference
  decision ids, source readiness/continuity caveats;
- confidence and scoring metadata when applicable;
- producer id/version;
- rationale summary safe for display;
- status: pending, superseded, judged, finalized, expired.

The proposal target is the candidate canonical output, not a domain-specific
fake lifecycle event. For example, an inferred task creates a proposal whose
candidate payload is `task.created`; it does not create `task.proposed` as a
task lifecycle fact.

## Judgment Shape

A judgment should record:

- judgment id;
- proposal id;
- actor kind: user, operator, deterministic policy, test fixture;
- actor id or policy id;
- decision: accept, reject, modify, defer;
- corrected payload for modify;
- comment/reason;
- timestamp;
- authorization context.

LLM prompt output is not a valid actor kind for promotion. If a model critiques
another model, that critique is another proposal or score, not a judgment.

## Finalizer Semantics

Finalizers are deterministic and idempotent. Given the same accepted proposal,
judgment, and current canonical state, they either produce the same output or a
typed conflict/caveat.

Finalizers must:

- verify the referenced proposal and judgment exist;
- verify the judgment accepts or modifies the proposal;
- apply the corrected payload when present;
- emit or record canonical output through existing event/projection paths;
- link output provenance to the proposal and judgment;
- make no-op/idempotent repeats explicit;
- preserve prior user judgments when proposal-generating automata replay.

Final outputs still use the ordinary material or derived provenance model.
Proposal/judgment records do not create a third event provenance class.

## Authority Consumers

| Consumer | Boundary |
|----------|----------|
| MCP tools | Read-only tools bypass this; write-like tools must create proposals. |
| Instruction/actuator loops | Model/agent instructions become proposals until judged or policy-approved. |
| Task and health extraction | Ambiguous or inferred facts become proposals; accepted outputs become canonical domain events. |
| Entity/relation extraction | Weak matches and merges become proposals; finalizer updates canonical graph/projections. |
| Parser/workbench output | Generated mappings and source-unit changes become proposals, not direct repo/runtime mutation. |
| Semantic shadow lanes | Promotion requires judgment/finalizer or explicit operator operation. |

## Replay Behavior

Proposal producers are replayable. Judgments are authority records and must not
be erased just because a proposal producer regenerates candidates.

Replay rules:

- regenerated identical proposal: link to or supersede the previous candidate
  without dropping judgments;
- regenerated changed proposal: create a new proposal or revision and preserve
  old judgments as historical authority;
- accepted proposal disappears under a new model/config: report judgment debt in
  the comparison surface;
- rejected proposal reappears: preserve rejection evidence and avoid repeated
  operator prompts unless the evidence materially changed.

## First Implementation Slice

The first slice should avoid LLM dependency. Use a fixture producer that emits a
candidate tag/relation/task assertion from deterministic input.

Minimum proof:

1. record one pending proposal with target, evidence, rationale, confidence, and
   producer metadata;
2. record a user/test-fixture judgment that accepts, rejects, and modifies in
   separate tests;
3. finalizer applies an accepted or modified proposal deterministically;
4. replaying the proposal producer preserves the previous judgment;
5. gateway/RPC and CLI list pending proposals and record a judgment as stable
   JSON.

## Non-Goals

- Do not build a full curation TUI in the substrate slice.
- Do not let proposals mutate code, Nix, source units, or external state
  directly.
- Do not make model confidence equivalent to authority.
- Do not encode every domain ontology detail in the proposal core.
