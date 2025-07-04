# EventRegistry Autogeneration Implementation

## Overview

This document describes the implementation of EventRegistry autogeneration that eliminates the need for manual maintenance of event type registrations in Sinex.

## Problem Statement

Previously, the EventRegistry required manual maintenance of:
- Static arrays of event type names
- Manual schema definitions for each event type
- Source-to-event mappings
- Keeping these in sync with actual EventType implementations

This led to:
- Events being forgotten in the registry (demonstrated: 6 filesystem events vs 3 in manual registry)
- Schema drift between payload types and registry schemas
- Development friction when adding new event types

## Solution Architecture

### Core Infrastructure

#### EventRegistryBuilder Pattern
```rust
pub struct EventRegistryBuilder {
    event_types: Vec<&'static str>,
    event_to_source: Vec<(&'static str, &'static str)>,
    schema_generators: HashMap<&'static str, fn() -> RootSchema>,
}
```

**Key Features:**
- Dynamic registry construction at runtime
- Automatic deduplication of event types while preserving source mappings
- Schema generation from payload types using `schemars`

#### Event Crate Registration Pattern
Each event crate implements a `register_events()` function:

```rust
/// Register all filesystem events with the registry
pub fn register_events(builder: &mut EventRegistryBuilder) {
    use sinex_core::EventType;
    
    builder.add_event_type(
        FileCreated::EVENT_NAME,
        FileCreated::SOURCE_NAME,
        || {
            let gen = schemars::gen::SchemaGenerator::default();
            gen.into_root_schema_for::<FileCreatedPayload>()
        }
    );
    // ... other events
}
```

**Benefits:**
- Single source of truth: EventType trait constants
- Automatic schema generation from payload types
- Type-safe registration using trait implementations

### Implementation Details

#### Directory Structure
```
crate/sinex-events-fs/src/
├── lib.rs                  # Contains register_events() function
├── filesystem.rs           # Contains EventType implementations
```

#### Auto-Registration Flow
1. Event crate defines EventType implementations with constants
2. Event crate provides `register_events()` function using those constants
3. Collector calls all event crate registration functions
4. EventRegistryBuilder builds final registry with deduplication

#### Circular Dependency Resolution
- Auto-registration happens at collector level, not in sinex-core
- Avoids circular dependency between sinex-core and event crates
- Event crates depend on sinex-core, collector depends on both

## Implementation Status

### ✅ Completed
- **EventRegistryBuilder infrastructure** in sinex-core
- **sinex-events-fs auto-registration** with 6 filesystem events
- **Comprehensive test suite** demonstrating functionality
- **Documentation and examples**

### ⏳ Remaining Work
- **sinex-events-desktop**: Add `register_events()` function
- **sinex-events-terminal**: Add `register_events()` function  
- **sinex-events-system**: Add `register_events()` function
- **Migration**: Switch collector to use auto-registration by default

## Test Results

The implementation includes comprehensive tests that validate:

### Test: Auto-Registration Pattern
```rust
#[test]
fn test_auto_registration_pattern() -> TestResult {
    let mut builder = EventRegistryBuilder::new();
    sinex_events_fs::register_events(&mut builder);
    let registry = builder.build();
    
    // Verifies all 6 filesystem events are registered
    assert!(registry.has_event("file.created"));
    assert!(registry.has_event("dir.created"));
    // ... etc
}
```

### Test: Manual vs Auto Comparison
Demonstrates the value proposition:
- **Auto-registered**: 6 filesystem events
- **Manual registry**: 3 filesystem events (missing `file.moved`, `dir.created`, `dir.deleted`)

### Test: Deduplication Behavior
Validates that multiple sources can emit the same event type without duplication in the registry.

## API Reference

### For Event Crate Authors

```rust
/// Standard pattern for event crate registration
pub fn register_events(builder: &mut sinex_core::unified_collector::EventRegistryBuilder) {
    use sinex_core::EventType;
    
    builder.add_event_type(
        YourEventType::EVENT_NAME,
        YourEventType::SOURCE_NAME,
        || {
            let gen = schemars::gen::SchemaGenerator::default();
            gen.into_root_schema_for::<YourPayloadType>()
        }
    );
}
```

### For Collector Integration

```rust
/// Current approach (manual)
let registry = create_registry();

/// New approach (auto-registration)
let registry = create_registry_with_auto_registration();
```

## Migration Strategy

### Phase 1: Parallel Implementation ✅
- Implement builder pattern alongside manual registry
- Demonstrate auto-registration for filesystem events
- Validate approach with comprehensive tests

### Phase 2: Event Crate Migration
- Add `register_events()` to remaining event crates
- Follow pattern established by sinex-events-fs
- Test each crate's registration

### Phase 3: Collector Integration
- Switch collector to use auto-registration by default
- Keep manual registry as fallback during transition
- Update all call sites

### Phase 4: Cleanup
- Remove manual registry code
- Update documentation
- Remove TODO comments

## Key Benefits Achieved

1. **Eliminates Manual Maintenance**
   - No more static arrays to update
   - No risk of forgetting new event types
   - Automatic schema consistency

2. **Type Safety & Consistency**
   - Uses EventType trait constants as source of truth
   - Schema generation from actual payload types
   - Compile-time validation

3. **Deduplication Support** 
   - Multiple sources can emit same event types
   - Single schema per event type maintained
   - All source mappings preserved

4. **Developer Experience**
   - Clear pattern for new event crates
   - Self-documenting approach
   - Reduced cognitive load

## Files Modified

### Core Infrastructure
- `crate/sinex-core/src/unified_collector.rs`: EventRegistryBuilder implementation

### Event Crates
- `crate/sinex-events-fs/src/lib.rs`: Auto-registration function

### Collector Integration
- `crate/sinex-collector/src/collector.rs`: Demo auto-registration function

### Tests & Documentation
- `test/unit/core/event_registry_auto_registration_test.rs`: Comprehensive test suite
- `Cargo.toml`: Updated dependencies for tests

## Validation

The implementation has been validated through:
- All existing tests continue to pass
- New auto-registration tests pass
- Demonstrates concrete improvement (6 vs 3 events)
- No breaking changes to existing APIs
- Clean architecture without circular dependencies

This implementation provides a clear path forward for eliminating the manual EventRegistry maintenance burden while maintaining full backward compatibility.