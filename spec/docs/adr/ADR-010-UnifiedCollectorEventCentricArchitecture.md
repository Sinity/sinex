# ADR-010: Unified Collector Event-Centric Architecture

* **Status:** Accepted
* **Date:** 2025-01-10

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

### EventType Trait
```rust
trait EventType {
    type Payload: Serialize + DeserializeOwned + JsonSchema;
    type SourceImpl: EventSource; // Can be tuple for multiple sources
    
    const EVENT_NAME: &'static str; // "file.created"
    const SOURCE_NAME: &'static str = <Self::SourceImpl as EventSource>::SOURCE_NAME;
}
```

### EventSource Trait  
```rust
#[async_trait]
trait EventSource {
    type Config: Clone + Serialize + DeserializeOwned;
    const SOURCE_NAME: &'static str;
    
    async fn stream_events(&mut self, tx: mpsc::Sender<RawEvent>) -> Result<()>;
}
```

### Multiple Sources Support
```rust
// Single source
impl EventType for FileCreated {
    type SourceImpl = FilesystemWatcher;
}

// Multiple sources via tuple
impl EventType for CommandExecuted {
    type SourceImpl = (KittySocketListener, BashHistoryWatcher);
}
```

### Configuration
```toml
[events]
enabled = ["file.created", "file.modified", "command.executed"]

# Hierarchical merging
[event.files]
watch_patterns = ["**/*.rs", "**/*.toml"]

[event.file_created]  # Merges with event.files
debounce_ms = 50
```

### Registry
Compile-time discovery of all EventType implementations, generating const arrays.

### Event Output
```rust
struct EventOutput {
    write_to_db: bool,
    log_events: bool,
    debug_file: Option<PathBuf>,
}
```

## Implementation Plan

1. Core traits (EventType, EventSource)
2. Registry with compile-time discovery
3. Event implementations (file.created, command.executed, etc.)
4. EventSource implementations wrapping existing logic
5. UnifiedCollector using SimpleIngestor pattern
6. Schema generation from JsonSchema derive

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