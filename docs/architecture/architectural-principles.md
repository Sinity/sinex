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

## Implementation Guidelines

These principles are not just theoretical - they directly influence implementation:

1. **Every service** must implement StatefulStreamProcessor
2. **All communication** flows through Redis Streams
3. **All state changes** create events in core.events
4. **All identifiers** use ULID format
5. **All schemas** live in the /schemas directory
6. **All services** emit structured heartbeats
7. **All APIs** use command/response patterns
8. **All features** work completely offline

## Architectural Coherence

These principles work together to create a coherent system:
- Satellites enable modularity while the message bus provides integration
- ULIDs enable distribution while the events table provides consistency  
- GitOps enables evolution while schemas provide stability
- Local-first enables privacy while command/response enables auditability

The result is a system that is simultaneously distributed and unified, flexible and structured, powerful and comprehensible.