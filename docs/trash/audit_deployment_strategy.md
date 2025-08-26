# Sinex Codebase Audit Deployment Strategy

## Overview
This document maps 10 specialized audit prompts across 30 agent instances to comprehensively analyze the Sinex codebase (264 Rust files across 22 crates).

## Deployment Distribution

### Prompt 1: Rust Idiom and Ergonomics Hunter (4 agents)
Focus: Core libraries with heavy Rust patterns

**Agent 1.1** - Core Types & Utilities
```
TARGET: 
crate/lib/sinex-core/src/types/
crate/lib/sinex-core/src/db/models/
```

**Agent 1.2** - Satellite SDK  
```
TARGET:
crate/lib/sinex-satellite-sdk/src/stream_processor.rs
crate/lib/sinex-satellite-sdk/src/cli.rs
crate/lib/sinex-satellite-sdk/src/coordination.rs
```

**Agent 1.3** - Test Utilities
```
TARGET:
crate/lib/sinex-test-utils/src/fixtures.rs
crate/lib/sinex-test-utils/src/database_pool.rs
crate/lib/sinex-test-utils/src/lib.rs
```

**Agent 1.4** - Macros & Code Generation
```
TARGET:
crate/lib/sinex-macros/src/
```

---

### Prompt 2: Error Handling Archaeology (3 agents)
Focus: Error-prone boundaries and service interfaces

**Agent 2.1** - Core Services
```
TARGET:
crate/core/sinex-ingestd/src/
crate/core/sinex-gateway/src/
```

**Agent 2.2** - Repository Layer
```
TARGET:
crate/lib/sinex-core/src/db/repositories/
crate/lib/sinex-core/src/types/error.rs
```

**Agent 2.3** - Satellites Error Handling
```
TARGET:
crate/satellites/sinex-fs-watcher/src/
crate/satellites/sinex-terminal-satellite/src/
crate/satellites/sinex-desktop-satellite/src/
```

---

### Prompt 3: Async Hygiene Inspector (3 agents)
Focus: Async-heavy components

**Agent 3.1** - Core Async Services
```
TARGET:
crate/core/sinex-ingestd/src/service.rs
crate/core/sinex-sensd/src/
crate/core/sinex-rpc-dispatcher/src/
```

**Agent 3.2** - Satellite Async Processing
```
TARGET:
crate/satellites/sinex-terminal-satellite/src/unified_processor.rs
crate/satellites/sinex-fs-watcher/src/unified_processor.rs
crate/satellites/sinex-desktop-satellite/src/unified_processor.rs
```

**Agent 3.3** - Automata Async Workers
```
TARGET:
crate/satellites/sinex-analytics-automaton/src/
crate/satellites/sinex-content-automaton/src/
crate/satellites/sinex-health-aggregator/src/
```

---

### Prompt 4: Type System Sophistication Scanner (3 agents)
Focus: Type-heavy interfaces and domain models

**Agent 4.1** - Domain Models
```
TARGET:
crate/lib/sinex-core/src/types/domain.rs
crate/lib/sinex-core/src/db/models/event.rs
crate/lib/sinex-core/src/types/events/
```

**Agent 4.2** - Schema & Validation
```
TARGET:
crate/lib/sinex-schema/src/
crate/lib/sinex-core/src/types/validation/
```

**Agent 4.3** - Service Interfaces
```
TARGET:
crate/lib/sinex-services/src/
crate/lib/sinex-satellite-sdk/src/stream_processor.rs
```

---

### Prompt 5: Dead Code and Entropy Detector (2 agents)
Focus: Largest crates with potential cruft

**Agent 5.1** - Core Libraries
```
TARGET:
crate/lib/sinex-core/
crate/lib/sinex-satellite-sdk/
```

**Agent 5.2** - All Satellites
```
TARGET:
crate/satellites/
```

---

### Prompt 6: SQL and Database Pattern Auditor (3 agents)
Focus: Database interaction layers

**Agent 6.1** - Repository Implementations
```
TARGET:
crate/lib/sinex-core/src/db/repositories/events.rs
crate/lib/sinex-core/src/db/repositories/state.rs
crate/lib/sinex-core/src/db/repositories/knowledge_graph.rs
```

**Agent 6.2** - Schema & Migrations
```
TARGET:
crate/lib/sinex-schema/src/schema/
crate/lib/sinex-migrations/migrations/
```

**Agent 6.3** - Query Helpers & Sanitization
```
TARGET:
crate/lib/sinex-core/src/db/query_helpers.rs
crate/lib/sinex-core/src/db/sanitization.rs
crate/lib/sinex-services/src/search.rs
```

---

### Prompt 7: Documentation Debt Finder (2 agents)
Focus: Public APIs and complex modules

**Agent 7.1** - Public SDK APIs
```
TARGET:
crate/lib/sinex-satellite-sdk/src/
crate/lib/sinex-core/src/types/
```

**Agent 7.2** - Service Interfaces
```
TARGET:
crate/core/sinex-ingestd/src/
crate/core/sinex-gateway/src/
crate/lib/sinex-services/src/
```

---

### Prompt 8: Dependency Hygiene Checker (2 agents)
Focus: Cargo.toml files and dependency patterns

**Agent 8.1** - Workspace & Core Dependencies
```
TARGET:
Cargo.toml
crate/lib/sinex-core/Cargo.toml
crate/lib/sinex-satellite-sdk/Cargo.toml
crate/lib/sinex-test-utils/Cargo.toml
crate/lib/sinex-schema/Cargo.toml
```

**Agent 8.2** - Service & Satellite Dependencies
```
TARGET:
crate/core/*/Cargo.toml
crate/satellites/*/Cargo.toml
```

---

### Prompt 9: Test Quality Inspector (3 agents)
Focus: Test modules and test utilities

**Agent 9.1** - Test Infrastructure
```
TARGET:
crate/lib/sinex-test-utils/
tests/
```

**Agent 9.2** - Unit Tests in Core
```
TARGET:
crate/lib/sinex-core/src/**/*test*.rs
crate/lib/sinex-core/src/**/*tests.rs
crate/lib/sinex-schema/tests/
```

**Agent 9.3** - Integration & Property Tests
```
TARGET:
tests/integration/
tests/property/
tests/adversarial/
crate/core/sinex-gateway/tests/
```

---

### Prompt 10: Concurrency and Performance Microscope (5 agents)
Focus: Performance-critical paths

**Agent 10.1** - Event Processing Pipeline
```
TARGET:
crate/lib/sinex-core/src/db/repositories/events.rs
crate/core/sinex-ingestd/src/service.rs
crate/core/sinex-ingestd/src/validator.rs
```

**Agent 10.2** - High-Volume Satellites
```
TARGET:
crate/satellites/sinex-terminal-satellite/src/
crate/satellites/sinex-system-satellite/src/
```

**Agent 10.3** - Stream Processing
```
TARGET:
crate/lib/sinex-satellite-sdk/src/stream_processor.rs
crate/lib/sinex-satellite-sdk/src/stage_as_you_go.rs
crate/lib/sinex-satellite-sdk/src/nats/
```

**Agent 10.4** - Database Pool & Caching
```
TARGET:
crate/lib/sinex-test-utils/src/database_pool.rs
crate/lib/sinex-core/src/db/query_helpers.rs
crate/lib/sinex-satellite-sdk/src/checkpoint.rs
```

**Agent 10.5** - Command Canonicalizer (Heavy Processing)
```
TARGET:
crate/satellites/sinex-terminal-command-canonicalizer/src/
crate/core/sinex-gateway/src/cascade_analyzer.rs
```

---

## Execution Strategy

### Phase 1: Critical Path Analysis (10 agents)
Run first to identify high-impact issues:
- Agents 2.1, 2.2 (Error Handling in core services)
- Agents 3.1, 3.2 (Async hygiene in critical paths)
- Agents 6.1, 6.2 (Database patterns)
- Agents 10.1, 10.2, 10.3, 10.4 (Performance bottlenecks)

### Phase 2: Code Quality (10 agents)
Improve overall code quality:
- Agents 1.1, 1.2, 1.3, 1.4 (Rust idioms)
- Agents 4.1, 4.2, 4.3 (Type system usage)
- Agents 9.1, 9.2, 9.3 (Test quality)

### Phase 3: Maintenance & Cleanup (10 agents)
Clean up and document:
- Agents 2.3, 3.3 (Error/async in satellites)
- Agents 5.1, 5.2 (Dead code removal)
- Agents 6.3 (Query optimization)
- Agents 7.1, 7.2 (Documentation)
- Agents 8.1, 8.2 (Dependencies)
- Agent 10.5 (Performance in canonicalizer)

## Expected Outcomes

1. **Performance**: Identify N+1 queries, unnecessary allocations, blocking in async
2. **Safety**: Find unwraps, panics, missing error context
3. **Idioms**: Make code more Rust-idiomatic and maintainable
4. **Types**: Leverage type system for compile-time guarantees
5. **Tests**: Improve test coverage and quality
6. **Documentation**: Update stale docs, add missing safety docs
7. **Dependencies**: Reduce compile times, remove duplicates
8. **Dead Code**: Remove entropy and technical debt

## Success Metrics

- Reduction in `unwrap()`/`expect()` calls
- Increased use of type-safe patterns
- Improved async performance characteristics
- Better test coverage for error paths
- Cleaner dependency tree
- More idiomatic Rust patterns
- Better documentation coverage

## Notes

- Each agent should process ~8-10 files for thorough analysis
- Larger files (2000+ lines) get dedicated attention
- Repository layer gets multiple passes (SQL, error handling, performance)
- Test infrastructure gets dedicated quality inspection
- Satellites analyzed both individually and as a group