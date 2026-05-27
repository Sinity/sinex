# Automaton Chaining

Sinex automata are not a flat field of independent reactors. They form chains:
one automaton's output becomes another's trigger, and intermediate outputs are
themselves canonical events with provenance. This record fixes how those
chains compose, how failures are isolated, and how authority moves through
them.

It does not redefine model-call policy (that belongs to
[issue #1116](https://github.com/Sinity/sinex/issues/1116)) or
promotion authority (that belongs to
`proposal-judgment-finalizer.md`). It defines the shape *between* automata,
including the circuit-breaker contract that lets degraded chains keep
producing useful output without silently corrupting downstream consumers.

## Why Chains Are A First-Class Concept

The temptation is to treat each automaton as a self-contained pipeline. That
breaks down quickly:

- Intermediate outputs (an enriched command, a closed session, a tagged
  entity) are themselves valuable events; downstream consumers other than the
  next chain step also need them.
- A failure in step N of a chain must not silently kill step N+1; in many
  cases the chain should degrade rather than stop.
- Re-runs and replays of one stage must not silently invalidate downstream
  authority records like judgments or finalized canonical assertions.
- Privacy and routing decisions taken at one stage must be carried forward;
  the next automaton in the chain does not get to re-decide privacy class
  for free.

Treating chains explicitly lets us reason about authority, failure mode, and
replay at the chain boundary instead of per-automaton.

## Chain Shapes

Three chain shapes recur. They are illustrative; real deployments wire
additional fan-out edges and feed projections.

**Document → knowledge graph.** Document ingestion produces parsed content;
a decomposer emits per-note events; entity extraction, embedding, tagging,
and knowledge-graph builders fan out from there.

**Activity → behavioral intelligence.** Desktop, terminal, and filesystem
ingestors feed command enrichment, session detection, cross-source
correlation, intent inference, summarization, and pattern detection.
Intermediate stages emit canonical session and correlation events; final
stages emit proposals (tags, patterns, summaries) per the
proposal/judgment/finalizer contract.

**Daily derived.** A clock-driven automaton queries the day's events and
emits a structured day summary, which then drives semantic annotation and
embedding indexing.

The exact wiring of each chain is owned by the corresponding automaton
issues and crate docs; this record only fixes the contract those chains must
hold.

## Chain Authority Rules

1. **Intermediate events are canonical, not internal.** A chain stage's
   output is an ordinary event with provenance. It is not a private
   intermediate; other consumers may subscribe to it.
2. **Promotion to canonical assertion still requires judgment.** When a
   chain produces what looks like a new canonical fact (tag, relation, task,
   merge), that step emits a proposal, not a direct mutation, per
   `proposal-judgment-finalizer.md`. The chain does not bypass the
   proposal/judgment/finalizer spine simply because there are several
   automata involved.
3. **Routing and privacy carry forward.** When a chain stage runs a model,
   it uses the prompt router ([issue #1116](https://github.com/Sinity/sinex/issues/1116)) and records the
   decision. Downstream stages inherit the privacy class of the input; they
   do not silently re-route a redacted input to a remote model.
4. **Replay preserves authority records.** Re-running an earlier stage may
   produce updated proposals. It must not erase user/operator judgments on
   prior proposals.
5. **Backpressure stays per-stage.** Each stage owns its own consumer lag
   and rate limit. A slow stage does not silently drop upstream events; it
   exerts backpressure through the ordinary delivery substrate.

## Circuit Breaker Contract

LLM-backed (and other failure-prone) stages share a circuit-breaker
abstraction. The exact implementation lives in `sinex-node-sdk`; what this
record fixes is the contract the breaker must hold.

States: `closed`, `open`, `half-open`.

- `closed` — calls run normally; failures increment a counter.
- `open` — calls bypass to the stage's declared fallback; the breaker waits
  a reset interval before allowing a probe.
- `half-open` — a single probe call decides whether to re-close or re-open.

Required behavior:

1. **Declared fallback per stage.** Every breaker-protected stage declares
   its fallback at config time. Allowed fallback shapes:
   - `structured_only` — emit the canonical payload without LLM-derived
     fields (e.g., `narrative: null`).
   - `skip` — emit nothing; downstream consumers see no output for this
     input.
   - `rule_based` — fall back to a deterministic heuristic that is itself
     versioned and recorded.
2. **Open state is observable.** When a breaker opens, that fact is itself
   a recorded event or status signal. Operators can see which chains are
   degraded without grepping logs.
3. **Open state is auditable downstream.** Outputs produced under an open
   breaker carry a marker that distinguishes them from full outputs.
   Consumers (and especially proposal/judgment surfaces) must be able to
   tell that a payload is a fallback shape, not a confident model output.
4. **No silent promotion under fallback.** A fallback that produces a
   proposal must carry the breaker state so that judgment review can take
   it into account. A fallback never promotes to canonical assertion
   without the ordinary judgment/finalizer path.
5. **Reset is timer-based, not retry-storm.** Reset uses a wall-clock
   timer, not call-rate counting; otherwise breakers can flap under bursty
   load.

## Failure Isolation

- A failure in stage N does not stop chain N+1 from running on other inputs.
  Stage N+1 simply does not see outputs for the inputs that failed.
- A persistent failure in stage N opens its breaker and switches to its
  declared fallback. Downstream stages see the fallback shape and behave
  accordingly.
- DLQ semantics belong to the ordinary delivery substrate. The chain
  contract does not invent a new dead-letter shape; it ensures the existing
  substrate sees the failure with attribution.
- Restart and replay of a single stage uses the ordinary replay surface.
  Authority records (judgments, finalizations) are preserved across replay
  per the replay rules in `proposal-judgment-finalizer.md`.

## Relation To Other Records

| Record | Relationship |
| --- | --- |
| [issue #1116](https://github.com/Sinity/sinex/issues/1116) (prompt router + budget ledger) | Owns prompt registry, model selection, and per-call budget; chain stages call into it for every model call. |
| `proposal-judgment-finalizer.md` | Owns promotion authority; chain stages emit proposals rather than mutating canonical state. |
| `semantic-epochs-shadow-lanes.md` | Owns shadow-lane comparison when a chain stage runs in a new model/config. |
| [issue #1118](https://github.com/Sinity/sinex/issues/1118) (inference decision metadata) | Owns the inference decision record shape that chain stages link from their outputs. |
| `runtime-qos.md` | Owns per-stage backpressure, rate limits, and lag accounting. |
| `runtime-private-mode.md` | Owns the privacy classification that chain stages must inherit and not bypass. |

## First Slice

A first chain implementation should be intentionally small:

1. One ingestor-fed chain with two stages and one fan-out.
2. Each stage emits canonical intermediate events with provenance.
3. The model-backed stage uses the prompt router and records a routing
   decision and a budget-ledger entry.
4. The model-backed stage has a configured breaker with one declared
   fallback shape; tests prove the fallback path is taken when the model
   fails repeatedly and that fallback outputs are distinguishable from
   confident outputs.
5. A replay of the first stage produces an updated proposal for the second
   stage without erasing any prior judgment.

## Boundaries

- Do not redefine event-delivery semantics here; the ordinary delivery
  substrate owns them.
- Do not let a chain promote canonical state without proposal/judgment/
  finalizer.
- Do not let breaker fallbacks silently re-route privacy-restricted inputs
  to a different model class.
- Do not bury intermediate events as private to the chain; they remain
  canonical and observable.
- Do not encode specific chain topologies here; they live in the owning
  automaton issues and crate docs.
