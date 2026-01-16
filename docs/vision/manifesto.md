# Sinex Vision & Principles
> **Purpose:** Capture the long-term philosophical north star and design principles; pair with `docs/current/architecture/Core_Architecture.md` for execution details.

*Last updated: 2025-10-23*

Sinex is a personal cognitive infrastructure project: a system that captures
what happens on the machine, understands it, and puts that knowledge back in
service of the person who generated it. This document consolidates the
philosophical intent, the architectural pattern language, and the research
trajectory that previously lived across dozens of exploratory essays.

For the authoritative ingestion and operational checklist, see `docs/current/architecture/provenance.md` and `nixos/README.md`.

## 1. Purpose & Philosophy

### 1.1 Cognitive Sovereignty
- The human remains the ultimate authority. Every automation stage must be
  explainable, inspectable, and reversible.
- Local-first operation is non‑negotiable: all core capabilities must function
  without external services, and every byte of captured data remains under the
  operator’s direct control.

### 1.2 Deep Oneness – Dissolving Arbitrary Boundaries
- **Single event stream:** `raw` and `synthesis` share one append-only log; the
  presence (or absence) of `source_event_ids` conveys provenance.
- **Senses vs. Automata:** every processor is a first-class "node" that
  either interprets external material (ingestors) or reasons over internal
  events (automata). There is no second-class scripting tier.
- **Using == Extending:** configuration, data exploration, and extension should
  converge. Long term, authoring a new behaviour should feel like capturing a
  new kind of event or adding a declarative flow.

### 1.3 Declarative Core
- Prefer declarative flows (`.sql`/future `.flow.yaml`) for deterministic
  synthesis; keep imperative Rust agents for tasks that are inherently
  speculative, non-deterministic, or outward-facing.
- The flow engine is expected to evolve from "SQL-as-automaton" into a richer
  dataflow runtime, but the guiding rule remains: **logic lives as data.**

### 1.4 Human-in-the-Loop Metacognition
- The system records not only facts but *how* it derived them. Provenance chains
  (`source_event_ids`) and the operations log (`core.operations_log`) are the
  audit trail of its thinking.
- When heuristics cannot decide, the system must surface conflicts through the
  `exo` tooling and wait for human judgment.

## 2. Living Architecture

```
[External World] -(Senses)-> [Short-Term Memory] -(Perception)-> [Cognition]
         ^                                                      |
         |                                                      |
         +-----------------------(Curation / Consciousness)-----+
```

| Stage                | Responsibility | Implementation Notes |
|----------------------|----------------|----------------------|
| **Senses**           | Acquire raw material | Declarative staging agents describe *what* to watch; reusable sensor libraries handle sockets, files, APIs, or subprocesses. |
| **Short-Term Memory**| Register material | `raw.source_material_registry` issues "birth certificates" (ULID, checksum, offsets) for every blob staged into git-annex. |
| **Perception**       | Interpret streams | Ingestors obey the **Stage-as-you-go** pattern: create an in-flight source-material record, emit events immediately, periodically finalize the blob. |
| **Cognition**        | Synthesize knowledge | Declarative flows and specialised automata transform events into knowledge graph state (`core.entities`, `core.entity_relations`, `km.*`). |
| **Action**           | Influence the world | Instructional events (`command.*`) share the same bus as observational events, enabling bidirectional nodes (e.g., Hyprland ingestor/actuator). |
| **Consciousness**    | Keep humans in control | `exo explore`, curation tooling, and diagnostics expose state, provenance, and outstanding reconciliations. |

### 2.1 Provenance Fundamentals
- **Normalized pointers:** events store offsets into source material (`offset_start`, `offset_end`, `anchor_byte`) instead of embedding raw bytes.
- **Timing categories:** ingestors classify material as `intrinsic`, `external_wrapper`, or `inferred` to determine trustworthy timestamps (`ts_orig`).
- **Checkpoint expectations:** continuous ingestors re-scan on startup before
  resuming live capture, ensuring no gaps during restarts.

### 2.2 Processor Taxonomy
- **Declarative automaton:** stateless SQL/flow definitions executed by the
  flow engine.
- **Stateful agent:** imperative Rust processor reserved for advanced heuristics
  (e.g., deduplication) or LLM-backed reasoning.
- **Bidirectional node:** sensor + actuator combined, subscribing to
  observational and instructional events alike.

## 3. Strategic Trajectory

1. **Operational Maturity (now – 6 months)**
   - Harden the existing stack: authentication/authorization, encryption at
     rest (pgsodium), TLS for service RPC, automated recovery playbooks.
   - Finish the JetStream migration; retire Redis-centric assumptions in docs
     and code.
2. **Declarative Extensibility (6 – 12 months)**
   - Graduate the SQL-as-automaton flow engine into a richer runtime with
     incremental state, windowed analysis, and reusable patterns.
   - Deliver an authoring experience where new automata are defined using flows
     plus optional inline scripts, not bespoke Rust crates.
3. **Agency & Actuation (12 – 18 months)**
   - Flesh out actuator nodes, closing observation/instruction loops for
     desktop automation, knowledge capture, and remediation.
   - Introduce permissioned instructional event handling and audit dashboards.
4. **Research Horizons (parallel track)**
   - Entity resolution, embedding pipelines, and temporal analytics that make
     the event log queryable as "lived experience" without leaning on mysticism.
   - Long-term experiments are clearly labelled as research and do not gate
     production maturity.

## 4. Appendices

### 4.1 Glossary
- **Cognitive Sovereignty:** the guarantee that the operator remains in control
  of their data, automations, and narrative.
- **Stage-as-you-go:** ingestion pattern where live streams are captured into an
  in-flight source blob while events reference it immediately.
- **Instructional Event:** an event in the `command.*` namespace, treated as an
  actionable order by actuators.

### 4.2 Research Themes (Archived Context)
- **Knowledge Graph Replayability:** every synthesis table must be rebuildable
  by re-running automata over the event stream.
- **Temporal Analytics:** modelling perceived time (session gaps, focus
  contours) is promising but remains an experiment until proven useful.
- **Augmented Agency:** LLM-orchestrated agents and external device meshes are
  aspirational; they stay in sandboxed prototypes until reliability matches the
  principles above.

---

This document is the stable touchstone. Treat it as the north star when
assessing new features, architectural trade-offs, and research bets.
