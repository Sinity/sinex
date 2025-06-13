# Unified Collector Vision - Architectural Evolution

**Date**: 2025-01-10
**Context**: Evolution from decentralized database-centric to hybrid centralized-app approach

## Vision Shift

Originally, Sinex was conceived as a highly decentralized system where everything revolved around the database as the central coordination point. While this remains valuable for durability and inter-process communication, there's now recognition that a powerful central application (the unified collector) could provide significant additional value.

## New Unified Collector Capabilities

### 1. Interactive TUI/CLI Interface
- **Live monitoring** of event streams, rates, errors
- **Interactive configuration** without restart
- **Self-documenting** config with inline help
- **Config persistence** - changes made in TUI saved back to TOML
- **Enter/exit interface** without stopping collection (detachable UI)

Implementation approach:
- Use `ratatui` for TUI
- Background event collection continues while UI active
- UI connects to collector via internal channels
- Changes applied live through config hot-reload system

### 2. Built-in Analytics
- **Event rate monitoring** per source/type
- **Error tracking** with categorization
- **Resource usage** (memory, CPU, connections)
- **Event lag tracking** (time from occurrence to capture)
- **Pattern detection** in event streams

### 3. Observability Dashboard
- **Prometheus metrics** exposed on HTTP endpoint
- **Health checks** for each event source
- **DLQ status** and recovery tools
- **Performance profiling** hooks

### 4. Configuration Management
- **Hot-reload** from file changes
- **Interactive editing** via TUI
- **Validation** with helpful error messages
- **Config generation** from current state
- **Multi-source** config (env vars, files, CLI)

### 5. Developer Experience
- **Event explorer** - browse recent events interactively
- **Schema browser** - see registered schemas
- **Test event generation** for debugging
- **Performance tuning** recommendations
- **Source health diagnostics**

## Architectural Implications

### Keep Modular, Not Monolithic
Rather than inlining everything into a sprawling main.rs, maintain clean modules:

```rust
sinex-collector/
├── src/
│   ├── main.rs           // Thin orchestration layer
│   ├── collector.rs      // Core collection logic
│   ├── config/           // Config management (from shared)
│   ├── error/            // Error handling (from shared)
│   ├── observability/    // Metrics and monitoring
│   ├── tui/              // Terminal UI components
│   │   ├── app.rs        // TUI application state
│   │   ├── views/        // Different screens
│   │   └── widgets/      // Custom widgets
│   └── analytics/        // Event analytics engine
```

### Integration Points
1. **Config hot-reload** triggers:
   - Event source reconfiguration
   - Filter updates
   - Performance tuning

2. **Observability** feeds:
   - TUI dashboards
   - Prometheus endpoints
   - Health check APIs

3. **Error handling** enables:
   - Smart retry policies
   - DLQ management UI
   - Error analytics

## Migration Strategy

### Phase 1: Consolidate and Organize
1. Move useful modules from `ingestor/shared/` to `sinex-collector/src/`
2. Keep them as separate modules, not inline everything
3. Remove truly unused code

### Phase 2: Add TUI Foundation
1. Basic TUI with event stream view
2. Detachable interface (collector continues running)
3. Simple metrics display

### Phase 3: Interactive Features
1. Config editing in TUI
2. Hot-reload integration
3. Error exploration

### Phase 4: Analytics and Intelligence
1. Pattern detection
2. Performance recommendations
3. Anomaly alerts

## Benefits of This Approach

1. **Single powerful tool** instead of scattered utilities
2. **Live visibility** into system behavior
3. **Rapid iteration** on configuration
4. **Built-in debugging** capabilities
5. **Production-ready monitoring**

## What This Doesn't Change

- Database remains source of truth for events
- Workers still process from promotion queue
- Event immutability preserved
- Can still run headless (TUI optional)

## Open Questions

1. Should TUI be in same binary or separate?
   - Same binary with `--tui` flag seems simpler
   - Could also be `sinex-collector-tui` connecting to running collector

2. How to handle remote collectors?
   - TUI could connect over network to remote collectors
   - Would need API/RPC endpoint

3. Config synchronization?
   - Changes via TUI saved immediately?
   - Or explicit "save config" action?

## Next Steps

1. Keep config management, error handling, observability modules
2. Organize them properly in collector crate
3. Start simple TUI experiment
4. Iterate based on actual usage