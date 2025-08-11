# Sinex Provenance Requirements

## CRITICAL: All Events MUST Have Provenance

According to the canonical architecture (docs/TARGET_canonical.md), **EVERY** event in the system MUST have provenance. There is NO valid state where an event has neither external nor internal provenance.

## The Two Provenance Types (XOR)

### 1. External Provenance (First-Order Events)
Events derived from Source Material captured by sensd:
- `source_material_id`: Reference to the Source Material
- `anchor_byte`: Specific byte offset in the material
- `offset_start/offset_end`: Range within the material

**Examples:**
- File system events parsed from inotify logs
- Terminal commands extracted from shell history
- Browser events from WebExtension streams

### 2. Internal Provenance (Synthesized Events)
Events derived from other events by automata:
- `source_event_ids`: Array of parent event IDs

**Examples:**
- Canonical events synthesized from multiple sources
- Aggregations and summaries
- Inference and analysis results

## Common Misconceptions

### ❌ WRONG: "Raw observation events have no provenance"
This is incorrect. What people call "raw observations" must FIRST be captured as Source Material by sensd, THEN converted to events with external provenance.

### ❌ WRONG: "System events like heartbeats don't need Source Material"
This is incorrect. Even system monitoring events must reference Source Material - perhaps from a metrics collection stream or system state snapshot.

### ❌ WRONG: "provenance: None means it's a raw event"
This is incorrect and violates the architecture. The term "raw event" should mean "first-order event with external provenance", not "event without provenance".

## Architectural Principle

From the canonical architecture:
> "Source Material is Ground Truth: The raw bytes captured from the external world are the immutable evidence. Events are interpretations of that evidence."

This means:
1. ALL external observations must be captured as Source Material first
2. Events are ALWAYS interpretations of either Source Material or other events
3. There is no such thing as an event that exists independently

## Implementation Requirements

### For Satellites/Ingestors
1. NEVER create events without provenance
2. If processing external data, ensure sensd has captured it as Source Material first
3. Use the Material provenance type with proper offsets

### For Automata
1. ALWAYS populate source_event_ids
2. Track the complete lineage of synthesis

### For Direct System Observations
If a service needs to emit events about system state:
1. First register a sensor job with sensd
2. sensd captures the observation as Source Material
3. Then create events with external provenance to that material

## Code Patterns to Avoid

```rust
// ❌ NEVER DO THIS
let event = RawEvent::new(source, event_type, payload);
// This creates an event with no provenance!

// ✅ DO THIS INSTEAD (for external events)
let event = RawEvent::new(source, event_type, payload)
    .with_provenance(Provenance::from_material(material_id, start, end))
    .with_anchor_byte(Some(anchor));

// ✅ OR THIS (for synthesized events)
let event = RawEvent::new(source, event_type, payload)
    .with_provenance(Provenance::from_events(parent_ids));
```

## Database Constraint

The database enforces this with a CHECK constraint:
```sql
CHECK (
    (material_id IS NOT NULL AND source_event_ids IS NULL)
    OR
    (material_id IS NULL AND source_event_ids IS NOT NULL)
)
```

Any attempt to insert an event without provenance will fail at the database level.

## Migration Path

For existing code that creates events without provenance:
1. Identify all such code paths
2. Determine if the event should have external or internal provenance
3. For external: Ensure sensd captures the source first
4. For internal: Track the parent events properly
5. Update the code to always set provenance

Remember: **There is no valid (NULL, NULL) provenance state**. Every event must have exactly one type of provenance.