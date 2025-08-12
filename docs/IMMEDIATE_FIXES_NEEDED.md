# Immediate Fixes Needed

## Critical Compilation Issues

### 1. EventRecord Missing Fields

**Problem**: The code in `repositories/events.rs` references fields that don't exist in EventRecord:
- Line 48: `self.source_material_offset_start` 
- Line 49: `self.source_material_offset_end`
- Line 68: `self.anchor_byte`
- Line 62: `self.host`
- Line 65: `self.ingestor_version`

**Quick Fix**: Add these fields to EventRecord
```rust
// crate/lib/sinex-migrations/src/schema/records/event.rs
#[derive(Debug, Clone, FromRow)]
pub struct EventRecord {
    // ... existing fields ...
    pub host: String,  // ADD THIS
    pub source_material_offset_start: Option<i64>,  // ADD THIS
    pub source_material_offset_end: Option<i64>,    // ADD THIS
    pub anchor_byte: Option<i64>,                   // ADD THIS
    pub ingestor_version: Option<String>,           // ADD THIS
}
```

### 2. Database Schema Mismatch

**Problem**: The schema definition doesn't include these columns

**Quick Fix**: Add migration
```sql
ALTER TABLE core.events 
ADD COLUMN IF NOT EXISTS host TEXT NOT NULL DEFAULT 'unknown',
ADD COLUMN IF NOT EXISTS source_material_offset_start BIGINT,
ADD COLUMN IF NOT EXISTS source_material_offset_end BIGINT,
ADD COLUMN IF NOT EXISTS anchor_byte BIGINT,
ADD COLUMN IF NOT EXISTS ingestor_version TEXT;
```

## Design Improvements (Non-Breaking)

### 1. Add Factory Methods with Required Provenance

```rust
impl RawEvent {
    /// Create event with material provenance (all required fields)
    pub fn material(
        source: impl Into<EventSource>,
        event_type: impl Into<EventType>,
        payload: JsonValue,
        ts_orig: Timestamp,
        material_id: Id<SourceMaterial>,
        anchor_byte: i64,
    ) -> Self {
        Self {
            id: None,
            source: source.into(),
            event_type: event_type.into(),
            payload,
            ts_orig: Some(ts_orig),
            host: get_hostname(),
            provenance: Some(Provenance::Material {
                id: material_id,
                offset_start: None,
                offset_end: None,
            }),
            anchor_byte: Some(anchor_byte),
            ingestor_version: get_ingestor_version(),
            payload_schema_id: None,
            associated_blob_ids: None,
        }
    }
    
    /// Create event with synthesis provenance
    pub fn synthesis(
        source: impl Into<EventSource>,
        event_type: impl Into<EventType>,
        payload: JsonValue,
        ts_orig: Timestamp,
        parent_ids: Vec<Id<RawEvent>>,
    ) -> Self {
        assert!(!parent_ids.is_empty(), "Synthesis events must have at least one parent");
        
        Self {
            id: None,
            source: source.into(),
            event_type: event_type.into(),
            payload,
            ts_orig: Some(ts_orig),
            host: get_hostname(),
            provenance: Some(Provenance::Events(parent_ids)),
            anchor_byte: None,
            ingestor_version: get_ingestor_version(),
            payload_schema_id: None,
            associated_blob_ids: None,
        }
    }
}
```

### 2. Add Validation Helper

```rust
impl RawEvent {
    /// Check if event is valid according to architecture
    pub fn validate(&self) -> Result<(), ValidationError> {
        // Must have provenance
        if self.provenance.is_none() {
            return Err(ValidationError::MissingProvenance);
        }
        
        // Must have timestamp
        if self.ts_orig.is_none() {
            return Err(ValidationError::MissingTimestamp);
        }
        
        // XOR validation
        match &self.provenance {
            Some(Provenance::Material { .. }) => {
                if self.anchor_byte.is_none() {
                    return Err(ValidationError::MaterialMissingAnchor);
                }
            }
            Some(Provenance::Events(ids)) => {
                if ids.is_empty() {
                    return Err(ValidationError::SynthesisNoParents);
                }
                if self.anchor_byte.is_some() {
                    return Err(ValidationError::SynthesisHasAnchor);
                }
            }
            None => return Err(ValidationError::MissingProvenance),
        }
        
        Ok(())
    }
}
```

### 3. Update Documentation

Add clear warnings to existing methods:
```rust
impl RawEvent {
    /// Creates an INVALID event without provenance.
    /// 
    /// WARNING: This violates the architecture! Use `material()` or `synthesis()` instead.
    #[deprecated(since = "0.2.0", note = "Creates invalid events. Use material() or synthesis()")]
    pub fn new(/* ... */) -> Self {
        // ...
    }
}
```

## Testing Fixes

### Add Validation Tests
```rust
#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn rejects_events_without_provenance() {
        let event = RawEvent {
            provenance: None,
            // ...
        };
        assert!(event.validate().is_err());
    }
    
    #[test]
    fn accepts_material_events() {
        let event = RawEvent::material(
            "test",
            "test.event",
            json!({}),
            Utc::now(),
            Id::new(),
            0,
        );
        assert!(event.validate().is_ok());
    }
    
    #[test]
    fn accepts_synthesis_events() {
        let event = RawEvent::synthesis(
            "test",
            "test.event",
            json!({}),
            Utc::now(),
            vec![Id::new()],
        );
        assert!(event.validate().is_ok());
    }
}
```

## Priority Order

1. **URGENT**: Fix EventRecord to match repository code (compilation error)
2. **HIGH**: Add database migration for missing columns
3. **MEDIUM**: Add new factory methods with better ergonomics
4. **LOW**: Deprecate old constructors
5. **FUTURE**: Full refactoring to make provenance non-optional

## Commands to Run

```bash
# 1. Fix EventRecord struct
$EDITOR crate/lib/sinex-migrations/src/schema/records/event.rs

# 2. Create migration
just migrate-create add_missing_event_columns

# 3. Run migration
just migrate

# 4. Check compilation
cargo check --workspace

# 5. Run tests
just test
```