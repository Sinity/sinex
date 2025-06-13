# Architecture Simplification Proposal for Sinex

## Current State
With the removal of individual ingestors in favor of a unified collector, several architectural abstractions have become over-engineered for our needs.

## Proposed Simplifications

### 1. Remove EventType/EventSource Traits
**Current Issues:**
- Complex compile-time event discovery that's manually populated anyway
- Confusing separation between EventType and EventSource
- Not actually used by the unified collector implementation
- Adds cognitive overhead without clear benefits

**Proposed Solution:**
- Use simple string constants for event types (already have this)
- Direct implementation of event sources without trait abstraction
- Configuration-driven event filtering

### 2. Merge IngestorApp and IngestorRuntime
**Current Issues:**
- Two overlapping layers handling lifecycle management
- Both initialize event sinks and manage configuration
- Unnecessary indirection

**Proposed Solution:**
- Single `CollectorRuntime` that combines the best of both:
  - CLI parsing and configuration loading
  - Event sink management based on mode (dry-run, file, database)
  - Lifecycle management (heartbeats, shutdown, error handling)
  - Direct integration with the unified collector

### 3. Simplify Unified Collector
**Current Issues:**
- Tries to use complex registry pattern but doesn't fully implement it
- Manual mapping of configuration to source implementations
- Clone implementation just for spawning tasks

**Proposed Solution:**
```rust
pub struct UnifiedCollector {
    config: UnifiedConfig,
    enabled_events: HashSet<String>,
}

impl UnifiedCollector {
    // Direct implementation without trait requirements
    pub async fn run(&self, event_tx: mpsc::Sender<RawEvent>) -> Result<()> {
        // Start sources based on enabled events
    }
}
```

### 4. Keep What Works Well
- **EventSink abstraction**: Clean, testable, supports multiple modes
- **Event constants**: Simple string constants work fine
- **Configuration system**: TOML-based config is straightforward
- **Database layer**: ULID-based immutable events work well
- **DLQ and error handling**: Robust failure management

## Benefits of Simplification

1. **Reduced Cognitive Load**: Fewer abstractions to understand
2. **Easier Maintenance**: Less code to maintain and debug
3. **Better Performance**: Fewer layers of indirection
4. **Clearer Architecture**: Direct path from configuration to execution
5. **Easier Testing**: Simpler components are easier to test

## Implementation Plan

1. **Phase 1**: Remove EventType/EventSource traits and registry
2. **Phase 2**: Merge IngestorApp functionality into a simplified runtime
3. **Phase 3**: Simplify unified collector to work directly with the runtime
4. **Phase 4**: Update tests and documentation

## Code Size Reduction Estimate
- Remove ~500 lines of trait definitions and registry code
- Consolidate ~300 lines of overlapping framework code
- Net reduction: ~800 lines of code
- Clearer, more maintainable architecture