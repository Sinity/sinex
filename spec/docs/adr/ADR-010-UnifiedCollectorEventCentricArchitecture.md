# ADR-010: Unified Collector Event-Centric Architecture

* **Status:** Implemented
* **Date:** 2025-01-10
* **Implementation Date:** 2025-07-17

## Context

Current architecture has multiple ingestor binaries (filesystem, kitty, hyprland) creating:
- Process overhead (3x memory, CPU, startup)
- Configuration fragmentation
- Deployment complexity
- Conceptual confusion - sources/ingestors overshadow events

## Decision

Implement unified collector with event-centric architecture where:
- Events are primary entities
- Sources are implementation details events happen to share
- Single binary, single configuration

## Architecture

### Core Traits
- **EventType**: Defines event payload structure and source mapping
- **EventSource**: Handles event stream generation from sources
- **StatefulStreamProcessor**: Unified processor interface for all satellites

### Key Patterns
- **Single/Multiple Sources**: Events can originate from one or multiple sources
- **Hierarchical Configuration**: Config inheritance with event-specific overrides
- **Compile-time Registry**: Automatic discovery of all event types
- **Unified Collection**: Single binary replaces multiple ingestor processes

### Implementation Components
- **Event Registry**: Compile-time discovery and registration system
- **Event Output**: Configurable routing (database, logs, debug files)
- **Schema Generation**: Automatic JsonSchema derivation from event types
- **Unified Collector**: Single process replacing multiple ingestors

## Consequences

**Positive:**
- Single process, single config
- Events as first-class citizens
- Reduced cognitive overhead
- Cross-event correlation possible

**Negative:**
- Migration complexity
- Larger single binary
- Need to maintain backward compatibility

## Implementation Status

**FULLY IMPLEMENTED** - The unified architecture has been successfully deployed:

1. **StatefulStreamProcessor Trait** - All satellites migrated from EventSource pattern
2. **Event-Centric Design** - Events are primary entities with sources as implementation details
3. **Unified Configuration** - Single configuration system across all event types
4. **Registry System** - Compile-time discovery and registration operational
5. **Cross-Event Correlation** - Enabled through unified event processing pipeline

**Current Architecture**:
- All satellites implement `StatefulStreamProcessor` trait
- Event types defined with `EventType` trait pattern
- Unified configuration with hierarchical merging
- Single ingestd process handles all event ingestion
- Redis Streams enable cross-event correlation and processing