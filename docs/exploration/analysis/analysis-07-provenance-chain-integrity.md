# Provenance Chain Integrity

Scope
- Determine if events can exist without provenance and how provenance invariants are enforced.

Method
- Inspect EventBuilder, Provenance types, and validation logic.

Enforcement points
- EventBuilder::build rejects missing provenance, enforcing Material or Synthesis at construction (crate/lib/sinex-core/src/db/models/event_builder.rs:173-176).
- Provenance::Synthesis uses NonEmptyVec, so a synthesis event always has at least one parent (crate/lib/sinex-core/src/db/models/event_builder.rs:204-218).
- Validator checks for duplicate parent ids in synthesis provenance (crate/lib/sinex-core/src/db/validation.rs:310-325).

Type implications
- EventId used in provenance is Id<Event<JsonValue>>, even for typed events, so provenance is already type-erased (crate/lib/sinex-core/src/db/models/event_builder.rs:10-12).

Observations
- The core model makes provenance mandatory, but direct construction of Event (public fields) can bypass EventBuilder if callers are careless.
- Some automata fall back to a hardcoded bootstrap event id when recent_events is empty; this ensures provenance but can hide missing lineage (e.g., health automaton) (crate/nodes/sinex-health-automaton/src/lib.rs:540-553).

Follow-ups
- Consider making Event fields private to force EventBuilder usage, or provide lint checks for direct Event { .. } literals outside tests.
- Track bootstrap provenance usage as a metric; it may indicate gaps in upstream event ingestion.
