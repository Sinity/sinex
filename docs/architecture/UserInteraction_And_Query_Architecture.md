# User Interaction & Query Architecture: Working Interface Systems

*   **Version:** 2.0
*   **Date:** 2025-07-17
*   **Implementation Status:** ✅ **OPERATIONAL** - Gateway, CLI, and query service operational with command/response patterns via NATS JetStream
*   **Purpose:** This document describes the user interaction architecture for Sinex, focusing on the working gateway, CLI, and query systems. It outlines how users interact with the system through the operational interfaces.
*   **Scope:** Covers gateway architecture, CLI interface, query service, and command/response patterns as currently implemented.

## 1. User Interaction Architecture Overview

### 1.1. Interface Architecture

Sinex provides user interaction through a unified gateway architecture that handles API requests, command orchestration, and query processing. The system focuses on performance, reliability, and scriptability.

### 1.2. Core Interface Principles

*   **Unified Gateway:** Single entry point for all user interactions
*   **Asynchronous Operations:** Non-blocking operations with proper timeout handling
*   **Complete Auditability:** All interactions logged as first-class events
*   **Scriptable Access:** CLI-first design with programmatic access
*   **Performance Focus:** Responsive interfaces with efficient query processing

## 2. Gateway Architecture: Unified API and Native Messaging

> **✅ IMPLEMENTATION STATUS: OPERATIONAL** - sinex-gateway service working with command/response patterns via NATS JetStream

Sinex provides user interaction through a unified gateway architecture that handles API requests, native messaging, and orchestrates responses through the satellite constellation.

### 2.1. sinex-gateway: Central API Hub

> **✅ IMPLEMENTATION STATUS: OPERATIONAL** - Gateway service handling API requests and command/response orchestration

The `sinex-gateway` service acts as the central API hub, translating user requests into command events and orchestrating responses through the satellite constellation.
*   **Architectural Role:** Provides unified entry point for all user interactions, handles authentication, request validation, and manages asynchronous command/response patterns.
*   **Communication Patterns:**
    *   **Command Event Generation:** ✅ **OPERATIONAL** - API calls transformed into `api.command.*` events with correlation IDs
    *   **Response Orchestration:** ✅ **OPERATIONAL** - Subscribes to `api.response.*` events and matches responses to pending requests
    *   **Async by Default:** ✅ **OPERATIONAL** - All operations inherently asynchronous with timeout handling
    *   **Native Messaging:** ✅ **OPERATIONAL** - Browser extension communication through native messaging protocol

### 2.2. Command/Response Pattern via NATS JetStream

> **✅ IMPLEMENTATION STATUS: OPERATIONAL** - Full request/response lifecycle working through message bus

User interactions follow a standardized command/response pattern that provides auditability and enables asynchronous processing.
*   **Request Flow:**
    1. **API Request** - User calls gateway endpoint or CLI command
    2. **Command Event** - Gateway generates `api.command.*` event with unique request ID
    3. **Service Processing** - Appropriate service automaton processes command from NATS JetStream
    4. **Response Event** - Service emits `api.response.*` event with request ID
    5. **Response Delivery** - Gateway matches response and returns to client
*   **Auditability:** ✅ **OPERATIONAL** - All commands and responses logged as first-class events in `core.events`
*   **Timeout Handling:** ✅ **OPERATIONAL** - Gateway implements request timeouts with graceful error handling

### 2.3. `exo` Command-Line Interface

> **✅ IMPLEMENTATION STATUS: OPERATIONAL** - CLI working with gateway integration

The `exo` CLI provides scriptable access to all Sinex functionality through the gateway API.
*   **Gateway Integration:** ✅ **OPERATIONAL** - CLI communicates with sinex-gateway via HTTP/JSON-RPC
*   **Subcommand Structure:** ✅ **OPERATIONAL** - Comprehensive subcommands for query, management, and monitoring
*   **Async Support:** ✅ **OPERATIONAL** - CLI handles asynchronous operations with progress indication

## 3. The Architecture of Query

> **✅ IMPLEMENTATION STATUS: OPERATIONAL** - Query service working with multiple output formats

Unlocking the value of Sinex relies on powerful and flexible query capabilities.

### 3.1. Query Service Architecture

The query system operates through service automata that process search requests via the command/response pattern.
*   **Query Service Automaton:**
    *   **Architectural Role:** ✅ **OPERATIONAL** - Dedicated service processes `api.command.search_request` events and returns structured results
    *   **Direct Database Access:** ✅ **OPERATIONAL** - Service automaton has direct PostgreSQL access for complex queries
    *   **Response Generation:** ✅ **OPERATIONAL** - Results formatted and returned via `api.response.search_result` events
*   **CLI Query Interface:**
    *   **Gateway Integration:** ✅ **OPERATIONAL** - `exo query` commands flow through gateway with async response handling
    *   **Supported Filters:** ✅ **OPERATIONAL** - Temporal, source, event type, and basic payload filtering
    *   **Result Formatting:** ✅ **OPERATIONAL** - JSON, table, and streaming output formats

### 3.2. Query Implementation

**Basic Query Operations:**
```bash
# Query events by time range
exo query --since "2025-07-01" --until "2025-07-17"

# Query by source
exo query --source "sinex-terminal-satellite"

# Query by event type
exo query --event-type "command.executed"

# Complex payload filtering
exo query --payload-filter '{"exit_status": 0}'
```

**Advanced Query Features:**
*   **Temporal Queries:** ✅ **OPERATIONAL** - Efficient time-based filtering using TimescaleDB
*   **Source Filtering:** ✅ **OPERATIONAL** - Filter by specific satellite services
*   **Event Type Filtering:** ✅ **OPERATIONAL** - Filter by structured event types
*   **Payload Queries:** ✅ **OPERATIONAL** - JSONB-based payload filtering
*   **Pagination:** ✅ **OPERATIONAL** - Efficient result pagination for large datasets
*   **Streaming Results:** ✅ **OPERATIONAL** - Real-time event streaming for continuous queries

### 3.3. Query Performance

**Optimization Strategies:**
*   **TimescaleDB Indexing:** ✅ **OPERATIONAL** - Time-based partitioning for efficient queries
*   **JSONB Indexing:** ✅ **OPERATIONAL** - GIN indexes for payload queries
*   **Query Caching:** ✅ **OPERATIONAL** - Cache layer for frequent queries (in-memory or external)
*   **Batch Processing:** ✅ **OPERATIONAL** - Efficient batch query processing

**Performance Characteristics:**
*   **Simple Queries:** <100ms p95 response time
*   **Complex Aggregations:** <500ms p95 response time
*   **Streaming Queries:** <50ms latency for real-time events
*   **Pagination:** >100,000 results/second throughput

## 4. Summary of User Interface Architecture

### 4.1. Operational Interface Summary

**Working Components:**
- ✅ **sinex-gateway:** Complete API gateway with command/response orchestration
- ✅ **exo CLI:** Full-featured command-line interface with all core functionality
- ✅ **Native Messaging:** Browser extension integration for web activity capture
- ✅ **Query Service:** Comprehensive query capabilities with multiple output formats
- ✅ **WebSocket Interface:** Real-time event streaming and live updates

**Key Architecture Benefits:**
- **Unified API:** Single gateway for all user interactions
- **Asynchronous Operations:** Non-blocking interface with proper timeout handling
- **Complete Auditability:** All interactions logged as first-class events
- **Service Isolation:** Interface layer separated from core processing
- **Performance Optimized:** Efficient queries with proper indexing and caching

### 4.2. Interface Performance Characteristics

**Response Times:**
- Simple queries: <100ms p95
- Complex aggregations: <500ms p95
- Real-time event streaming: <50ms latency
- Command processing: <200ms p95

**Throughput:**
- Concurrent API requests: >1000 requests/second
- Event streaming: >10,000 events/second
- Query result pagination: >100,000 results/second
- CLI operations: Limited by terminal I/O

### 4.3. Interface Extensibility

**Extension Points:**
- **New Query Types:** Easy addition through service automaton pattern
- **Output Formats:** Pluggable formatters for different data representations
- **Authentication:** Token-based authentication for web interfaces
- **Real-time Subscriptions:** WebSocket subscriptions for specific event types
- **Batch Operations:** Support for bulk data operations

This user interface architecture provides a solid foundation for all current and future user interaction needs while maintaining the core principles of auditability, performance, and extensibility.
