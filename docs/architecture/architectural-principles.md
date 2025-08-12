# Sinex Architectural Principles

This document outlines the key architectural principles that guide all design and implementation decisions in the Sinex system.

## Core Principles

### Satellite Constellation
Independent services orchestrated by systemd/NixOS with StatefulStreamProcessor interface. Each satellite operates autonomously while participating in the larger system through well-defined interfaces.

### Redis Streams Message Bus
Durable, real-time event distribution with consumer groups and checkpointing. Provides reliable message delivery, automatic recovery, and horizontal scalability.

### Unified Events Table
Single source of truth with comprehensive provenance tracking. All system state can be reconstructed from the immutable event log.

### Time-Ordered Keys
ULID primary keys for natural chronological ordering and distributed generation. Enables efficient time-based queries and conflict-free distributed event creation.

### GitOps Schema Management
Version-controlled JSON Schema validation with automatic deployment. Schema changes are tracked, reviewed, and deployed through standard git workflows.

### Journald Heartbeat Pattern
Elegant observability through structured logging and systemd integration. System health is monitored through standardized heartbeat messages in the journal.

### Command/Response Architecture
Asynchronous API patterns with full auditability via message bus. All commands and responses flow through the event system for complete traceability.

### Local-First & User Sovereign
Complete functionality and control without cloud dependencies. Users maintain full ownership and control of their data with no external service requirements.

### Advisory Lock Coordination
PostgreSQL advisory locks provide distributed coordination without architectural violations. System services use advisory locks for leadership election, verification gates, and resource coordination while maintaining the single-writer principle.

## Implementation Guidelines

These principles are not just theoretical - they directly influence implementation:

1. **Every service** must implement StatefulStreamProcessor
2. **All communication** flows through Redis Streams  
3. **All state changes** create events in core.events (via satellites only)
4. **All identifiers** use ULID format
5. **All schemas** live in the /schemas directory
6. **All services** emit structured heartbeats
7. **All APIs** use command/response patterns
8. **All features** work completely offline
9. **System coordination** uses PostgreSQL advisory locks with ResourceGuard RAII

## Architectural Coherence

These principles work together to create a coherent system:
- Satellites enable modularity while the message bus provides integration
- ULIDs enable distribution while the events table provides consistency  
- GitOps enables evolution while schemas provide stability
- Local-first enables privacy while command/response enables auditability

The result is a system that is simultaneously distributed and unified, flexible and structured, powerful and comprehensible.

## Coordination Patterns

### Single-Writer Principle

Only satellite services may write to `core.events`. This architectural constraint ensures:
- Clear data lineage and provenance
- Consistent event ingestion pipeline
- Simplified troubleshooting and debugging
- Natural system boundaries

**Violation Example (Forbidden):**
```rust
// ❌ Direct event insertion by system services
pool.events().insert(event).await?;
```

**Correct Pattern:**
```rust
// ✅ Advisory lock for system coordination
let coordination = DistributedCoordination::new(pool.clone());
if let Some(lock_guard) = coordination.try_become_leader("service-name").await? {
    // Lock acquisition proves database connectivity and write capability
    // Store results in appropriate system tables (not events)
    // Use structured logging for observability
}
```

### Advisory Lock Coordination

PostgreSQL advisory locks provide distributed coordination for system services:

#### Leadership Election
```rust
let leadership = coordination.try_become_leader("service-name").await?;
if let Some(guard) = leadership {
    // This service is now the leader
    // Automatic cleanup on guard drop
}
```

#### Verification Gates
```rust
// Preflight verification pattern
let verification_lock = coordination.try_become_leader("sinex-preflight").await?;
if let Some(guard) = verification_lock {
    // Verification in progress - lock acquisition proves DB access
    // Store results in service_leadership table with metadata
    // Other services can check AdvisoryLock::is_locked("sinex-preflight")
}
```

#### Resource Coordination
```rust
let resource_guard = coordination
    .acquire_resource_lock("shared-resource", timeout)
    .await?;
// Exclusive access to shared resource
// Automatic release on guard drop
```

### ResourceGuard RAII Pattern

All coordination uses ResourceGuard for automatic cleanup:

```rust
pub struct ResourceGuard<T> {
    resource: Arc<Mutex<Option<T>>>,
    cleanup_sender: Option<tokio::sync::oneshot::Sender<T>>,
}

impl<T> ResourceGuard<T> {
    pub fn new<F, Fut>(resource: T, cleanup: F) -> Self 
    where
        F: FnOnce(T) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        // Automatic async cleanup on drop
    }
}
```

**Benefits:**
- Guaranteed resource cleanup even on panic/cancellation
- Async cleanup support for database operations
- Composable with any resource type
- Zero-cost abstraction over manual resource management

### Integration with Satellite Architecture

Advisory locks integrate seamlessly with satellite coordination:

1. **Preflight Verification**: System readiness checks use advisory locks to prove database capabilities without violating single-writer principle

2. **Leadership Election**: Satellites use `DistributedCoordination::try_become_leader()` for service leadership

3. **Graceful Handoff**: Version-based handoff uses coordination tables and advisory locks for atomic transitions  

4. **Failure Detection**: Advisory lock status provides immediate visibility into service coordination state

This approach maintains architectural purity while providing robust distributed coordination capabilities.