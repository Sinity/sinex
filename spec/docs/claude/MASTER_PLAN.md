# Sinex Master Implementation Plan

This document provides a comprehensive, hierarchical plan for implementing the Sinex Exocortex. Each layer builds upon the previous, ensuring features like correlation_id make sense in context.

## Layer 0: Foundation (Current State)
✅ **Completed**
- Basic PostgreSQL schema with UUID
- Hyprland ingestor capturing window events
- CLI query tool
- NixOS module

## Layer 1: Core Infrastructure
Prerequisites: Layer 0

### 1.1 Database Evolution
- [ ] **Issue #4**: Replace custom ULID with PostgreSQL extension
- [ ] **Issue #1**: Migrate to Phase 2 schema
  - ULID primary keys for time-ordering
  - Add event_type field for categorization
  - Add host and ingestor_version for provenance
  - Schema validation infrastructure

### 1.2 Reliability Infrastructure  
- [ ] **Issue #3**: Implement Dead Letter Queue
  - Failed event handling
  - Retry mechanisms
  - Error reporting events
- [ ] **Issue #8**: Add health check endpoints
  - Database connectivity
  - Source connectivity (IPC sockets, etc.)
  - Readiness/liveness probes

### 1.3 Performance Infrastructure
- [ ] **Issue #5**: Batch insert optimization
  - Buffering strategies
  - COPY command usage
  - Backpressure handling

## Layer 2: Observability & Operations
Prerequisites: Layer 1 complete

### 2.1 Metrics & Monitoring
- [ ] **Issue #7**: Prometheus metrics integration
  - Event counts and rates
  - Processing latency
  - Queue depths
  - Error rates
- [ ] Grafana dashboards
  - System health overview
  - Per-ingestor metrics
  - Historical trends

### 2.2 Operational Tools
- [ ] Event replay capability
- [ ] Backup/restore procedures
- [ ] Schema migration tooling
- [ ] DLQ inspection CLI

## Layer 3: Extended Data Sources
Prerequisites: Layers 1-2 complete

### 3.1 Terminal Ecosystem
- [ ] **Issue #6**: Kitty remote control
  - Window/tab state
  - Working directory tracking
  - Scrollback snapshots
- [ ] Atuin integration
  - Structured command history
  - Cross-shell support
- [ ] Asciinema capture
  - Full session recording
  - Replay capability

### 3.2 Desktop Integration
- [ ] AT-SPI2 accessibility events
  - Application UI state
  - Text selection/focus
- [ ] Clipboard monitoring
  - Copy/paste tracking
  - Content type detection
- [ ] Screenshot/screen recording
  - Wayland native capture
  - Intelligent storage (dedup)

### 3.3 Application-Specific
- [ ] Browser extension
  - Page visits with content
  - Form interactions
  - Download tracking
- [ ] Email integration
  - IMAP monitoring
  - Send/receive events
- [ ] IDE/Editor plugins
  - Code navigation
  - Build/test events

## Layer 4: Event Processing Pipeline
Prerequisites: Layers 1-3 complete

### 4.1 Processing Infrastructure
- [ ] Work queue implementation
  - PostgreSQL-based queue
  - Agent registration
  - Priority handling
- [ ] Agent framework
  - Base agent traits
  - Lifecycle management
  - Communication patterns

### 4.2 Event Correlation ← **This is where correlation_id becomes relevant**
- [ ] **Issue #2**: Correlation ID support
  - Generated at interaction initiation points:
    - CLI command start
    - Browser navigation
    - Terminal session start
    - Neovim command execution
  - Propagated through `payload._provenance.correlation_id`
  - Enables tracing multi-step workflows:
    - "Show all events from researching bug X"
    - "Trace the full context of writing document Y"
- [ ] Session tracking
  - Logical session boundaries
  - Activity clustering
  - Interaction detection

### 4.3 Event Enrichment
- [ ] Metadata augmentation
- [ ] Entity extraction
- [ ] Temporal correlation
- [ ] Semantic tagging

## Layer 5: Knowledge Construction
Prerequisites: Layers 1-4 complete

### 5.1 Domain Tables
- [ ] Hyprland domain model
  - Window states
  - Application sessions
  - Workspace organization
- [ ] Terminal domain model
  - Command sequences
  - Directory contexts
  - Output correlation
- [ ] Filesystem domain model
  - File lifecycle
  - Change tracking
  - Content indexing

### 5.2 Entity System
- [ ] Entity extraction agents
- [ ] Relation discovery
- [ ] Knowledge graph construction
- [ ] Entity resolution/merging

### 5.3 Artifact Management
- [ ] PKM note integration
- [ ] Web page archiving
- [ ] Document versioning
- [ ] Blob storage (git-annex)

## Layer 6: Intelligence Layer
Prerequisites: Layers 1-5 complete

### 6.1 Embeddings & Search
- [ ] Text embedding generation
- [ ] pgvector integration
- [ ] Semantic search API
- [ ] Hybrid search (keyword + semantic)

### 6.2 Analysis Agents
- [ ] Pattern detection
- [ ] Anomaly identification
- [ ] Trend analysis
- [ ] Recommendation engine

### 6.3 LLM Integration
- [ ] Local model hosting (Ollama)
- [ ] Prompt management
- [ ] Context windowing
- [ ] Cost tracking

## Layer 7: Advanced Features
Prerequisites: Layers 1-6 complete

### 7.1 Proactive Assistance
- [ ] Task prediction
- [ ] Context switching detection
- [ ] Workflow optimization suggestions
- [ ] Automated report generation

### 7.2 External Integrations
- [ ] Mobile companion app
- [ ] Cloud backup (encrypted)
- [ ] API for third-party tools
- [ ] Export capabilities

### 7.3 Research Features
- [ ] CRDT-based collaboration
- [ ] Federated instances
- [ ] Privacy-preserving sharing
- [ ] Experimental visualizations

## Implementation Strategy

### Phase Boundaries
- **Phase 1** (Now): Layers 0-1 - Foundation & reliability
- **Phase 2** (Q1 2025): Layers 2-3 - Observability & data sources  
- **Phase 3** (Q2 2025): Layers 4-5 - Processing & knowledge
- **Phase 4** (Q3 2025): Layers 6-7 - Intelligence & advanced

### Success Criteria
Each layer must be:
1. Fully tested with >80% coverage
2. Documented with examples
3. Observable via metrics
4. Resilient to failures
5. Backward compatible

### Development Principles
1. **Bottom-up**: Each layer enables the next
2. **Incremental**: Ship working features continuously  
3. **Data-first**: Capture before analysis
4. **User-controlled**: Privacy and ownership paramount