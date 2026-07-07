---
created: 2026-06-29
purpose: Stable, quotable standing goal for the Sinex dogfood dev-loop effort
status: active
project: sinex
---

# Standing goal (Sinex)

Make Sinex fundamentally better, fast — and make it earn its keep by using it.
Treat the live dev loop as a real, ingesting feedback engine: reach a populated
steady-state dev event store (terminal, fs, git, journald, and more flowing for
real) and develop against it, dogfooding Sinex on my own work — including
reconstructing prior-session state through Sinex itself (the "what was I doing
around T" / agent-brief lens over my own dev activity), so work continues
indefinitely without context-reset loss.

Always the architecturally right shape, never a bandaid: capabilities must emerge
from honest, general substrate — clean constructs, the provenance model honored
(source-material vs event, material vs derived), shared projections and a real
relation/evidence/coverage algebra — never one-off report silos or misleading
inferred "insights." Getting it running right now is not the point; getting it
into the shape we want is. Every thread yields a real artifact on real data.
Move fast by batching work and running threads in parallel, never idling on
compiles, tests, or ingests.

## Doctrine source
`/realm/inbox/download/autonomous_project_improvement_methodology.md` (2026-06-28)
is the authoritative working doctrine. Key sections for Sinex:
- §2 operating doctrine (prod ≠ dev loop; issues are a parts bin; demos must be
  thin lenses over general primitives; fix broken tooling, don't record "lessons").
- §4 epistemic taxonomy (raw evidence / deterministic measurement / projection /
  outcome event / candidate / judgment / narrative / external truth). Stop using
  "insight" as a bucket. Absence of a marker must not become a positive class.
- §6 Sinex keystone: the **EvidenceWindow(anchor, scope, sources, relation_policy,
  coverage_policy) → ContextPack(markdown+json)** primitive. context / recall /
  incident / agent-brief should all collapse onto it, with explicit coverage and
  absence semantics.

## Project-specific shape
- source-material vs event; material vs derived provenance; event-log vs
  projection; stable relation vs contextual evidence edge; artifact vs canonical.
- Coverage/absence must be explicit — a missing source must not read as zero activity.
