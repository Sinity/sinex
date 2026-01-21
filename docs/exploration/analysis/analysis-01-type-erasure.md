# Type Erasure Boundary Mapping

Scope
- Identify where typed Event<T> becomes Event<JsonValue>, and what information is lost.
- Focus on core event model, storage, and macro-generated conversions.

Method
- rg "Event<" and targeted reads of event model, persistence, and macro code.

Key boundaries
- Core erasure: Event::to_json_event drops typed payload and resets id (crate/lib/sinex-core/src/db/models/event.rs:230-268).
- Storage boundary: EventRepository::insert converts Event<T> to JSON and assigns id if missing (crate/lib/sinex-core/src/db/repositories/events/persistence.rs:330-355).
- Macro boundary: typed_event_envelope auto-converts typed variants via to_json_event, and even manufactures placeholder events for unit or complex variants (crate/lib/sinex-macros/src/typed_event_envelope.rs:80-145).

Observations
- Provenance already uses JSON event IDs even for typed events via the EventId alias (crate/lib/sinex-core/src/db/models/event_builder.rs:10-12).
- Type recovery (to_typed) always produces a fresh Event<T> with id = None; this discards persisted identity even when payload round-trips (crate/lib/sinex-core/src/db/models/event.rs:249-267).
- Conversion preserves payload_schema_id and provenance but not type-level invariants; the only guard is schema validation downstream.

Impact
- ID identity is not stable across type erasure and recovery. Any workflow that expects persisted ids on typed events must re-attach ids explicitly after conversion.
- If payload schema evolution diverges from Rust type definitions, errors will surface only at validation or deserialization boundaries.

Follow-ups
- Consider preserving ids across to_json_event / to_typed when safe, or attaching a typed payload marker to improve diagnostics.
- Document where typed events are expected to be converted (SDK emitters vs DB insert) to reduce ambiguity.
