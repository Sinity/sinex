# Sinex: System Technical Architecture Document (STAD) v1.3

> **📊 IMPLEMENTATION STATUS**:
>
> - ✅ **Satellite Architecture** (80%) - Independent satellite services operational, StatefulStreamProcessor interface implemented
> - ✅ **Message Bus** (75%) - Redis Streams fully operational with consumer groups, checkpoint management, command/response patterns
> - ✅ **Data Substrate** (70%) - PostgreSQL + TimescaleDB with ULID keys, core.events table operational, comprehensive provenance tracking
> - 🚧 **Event Sources** (50%) - Four satellite domains active (filesystem, terminal, desktop, system), expanding coverage
> - 🚧 **Automaton Ecosystem** (40%) - Processing framework operational, deterministic automata working, agentic layer planned
> - 🚧 **Gateway & APIs** (65%) - sinex-gateway operational, command/response patterns working, CLI integrated
> - 🚧 **NixOS Module** (60%) - Satellite orchestration working, observability patterns operational
> - 🔨 **AI/LLM Integration** (15%) - Framework ready, schema designed, integration in progress

## Purpose

This System Technical Architecture Document provides a high-level map of Sinex's architecture. It introduces major architectural domains and links to detailed specifications in Architectural Modules, Technical Implementation Modules (TIMs), and Architectural Decision Records (ADRs).

### Document Relationships
- **[VISION.md](VISION.md)** - Project philosophy and long-term goals
- **[SADI.md](SADI.md)** - Central documentation navigation hub
- **[Architectural Modules](docs/arch_modules/)** - Domain-specific deep dives
- **[ADRs](docs/adr/)** - Architectural decision rationale
- **[TIMs](implemented/)** - Implementation specifications

## 1. System Overview

### Mission
Sinex is a "sentient archive" that augments human intellect by comprehensively capturing digital experiences, structuring data intelligently, and enabling powerful query and analysis capabilities while maintaining complete user control.

### Satellite Constellation Architecture
```
┌─────────────────────────────────────────────────────────────────┐
│                        User Interfaces                           │
│         CLI (exo.py)    │    Future: Web UI    │   Neovim       │
└────────────────────────┬────────────────────────────────────────┘
                         │
┌────────────────────────┴────────────────────────────────────────┐
│                      sinex-gateway                               │
│            API Gateway & Command/Response Handler                │
└────────────────────────┬────────────────────────────────────────┘
                         │
┌────────────────────────┴────────────────────────────────────────┐
│                   Message Bus (Redis Streams)                    │
│      Real-time Event Distribution & Consumer Groups              │
└───┬────────────────────┴────────────────────────────────────┬───┐
    │                                                        │   │
┌───▼──────────────────┐  ┌─────────────────────────────────▼───┐ │
│   Satellite Services  │  │          sinex-ingestd             │ │
│ ┌─────────────────┐   │  │    Ingestion Hub & Validator       │ │
│ │ StatefulStream  │   │  └─────────────────┬───────────────────┘ │
│ │ Processors:     │   │                    │                     │
│ │ - fs-watcher    │   │  ┌─────────────────▼───────────────────┐ │
│ │ - terminal      │   │  │        Data Substrate               │ │
│ │ - desktop       │   │  │ core.events + source_material_registry │ │
│ │ - system        │   │  │ PostgreSQL + TimescaleDB + Git-Annex│ │
│ └─────────────────┘   │  └─────────────────────────────────────┘ │
│ ┌─────────────────┐   │                                          │
│ │ Automata        │   │──────────────────────────────────────────┘
│ │ - health        │   │  (Consumer Groups: stream processing)
│ │ - canonicalizer │   │
│ │ - analytics     │   │
│ │ - content       │   │
│ │ - search        │   │
│ │ - pkm           │   │
│ └─────────────────┘   │
└───────────────────────┘
```

### Key Architectural Principles
- **Satellite Constellation** - Independent services orchestrated by systemd/NixOS with StatefulStreamProcessor interface
- **Redis Streams Message Bus** - Durable, real-time event distribution with consumer groups and checkpointing
- **Unified Events Table** - Single source of truth with comprehensive provenance tracking
- **Time-Ordered Keys** - ULID primary keys for natural chronological ordering and distributed generation
- **GitOps Schema Management** - Version-controlled JSON Schema validation with automatic deployment
- **Journald Heartbeat Pattern** - Elegant observability through structured logging and systemd integration
- **Command/Response Architecture** - Asynchronous API patterns with full auditability via message bus
- **Local-First & User Sovereign** - Complete functionality and control without cloud dependencies

## 2. Data Substrate Architecture

The foundation of Sinex built on PostgreSQL with specialized extensions.

### Core Components
- **PostgreSQL 16** with extensions:
  - **TimescaleDB** - Time-series optimization for events
  - **pgx_ulid** - Time-ordered primary keys ([ADR-001](docs/adr/ADR-001-PrimaryKeyStrategy.md))
  - **pg_jsonschema** - Event payload validation
  - **pgvector** - Future semantic search capabilities

### Event Storage
- **Unified Events Table** (`core.events`) - Single source of truth for all captured data with comprehensive provenance tracking via source_event_ids
- **Source Material Registry** (`raw.source_material_registry`) - Immutable ground truth preservation with blob_id references
- **Processor Manifests** (`sinex_schemas.processor_manifests`) - GitOps-driven processor registration and metadata
- **Schema Registry** (`sinex_schemas.event_payload_schemas`) - Versioned JSON schemas with GitOps management
- **Checkpoint System** (`core.automaton_checkpoints`) - Stateful processor recovery with unified interface
- **Message Bus** - Redis Streams for real-time event distribution with consumer groups

### Knowledge Representation (Future)
- **Knowledge Graph** (`core.entities`, `core.entity_relations`)
- **Artifacts** (`core.artifacts`) - Documents, notes, media
- **Embeddings** (`artifact_embeddings`) - Semantic search vectors
- **Blob Storage** - Git-annex for large files

**Detailed Architecture:** [DataSubstrate_Architecture.md](docs/arch_modules/DataSubstrate_Architecture.md)

## 3. Event Collection Architecture

Unified event collection system managing multiple data sources.

### Satellite Architecture
- **sinex-ingestd** - Central ingestion hub receiving events via gRPC
- **StatefulStreamProcessor Interface** - Unified pattern for both ingestors and automata
- **Event Source Satellites** - Independent services capturing domain-specific data
- **Automaton Satellites** - Independent services processing events into insights
- **Satellite SDK** - Shared library providing common infrastructure
- **Schema Validation** - GitOps-driven validation with version control

### Current Event Sources (35% System Coverage)

**Operational Sources:**
- **Filesystem Monitor** - File creation, modification, deletion (5% coverage)
- **Clipboard Monitor** - Copy/paste events with git-annex storage (2% coverage)
- **Terminal Sources** - Kitty, Asciinema, shell history (8% coverage)
- **Window Manager** - Hyprland IPC, basic X11 support (5% coverage)
- **System Sources** - Git events, downloads, SQLite history (15% coverage)

**Critical Missing Sources (65% gap):**
See [TIM-ComprehensiveEventSources.md](planned/event-sources/TIM-ComprehensiveEventSources.md) for detailed roadmap to 80%+ coverage including:
- Browser Activity Monitor (40-60% of knowledge work)
- Process Execution Tracker (all non-terminal programs)
- Network Activity Monitor (external interactions)
- Screen Capture with OCR (visual context)
- Input Pattern Monitor (activity detection)

### Planned Sources
- **Browser** - History and activity via extension
- **Audio** - PipeWire capture and transcription
- **Email** - IMAP/Exchange integration
- **Accessibility** - AT-SPI2 UI event capture

**Detailed Architecture:** [IngestionArchitecture_And_TelemetrySources.md](docs/arch_modules/IngestionArchitecture_And_TelemetrySources.md)

## 4. Processing Pipeline

Event-driven processing system with distributed workers.

### Satellite Constellation
- **Independent Services** - Each satellite runs as separate systemd service
- **StatefulStreamProcessor Pattern** - Unified scan(from: Checkpoint, until: TimeHorizon) interface
- **Message Streaming** - Redis Streams for real-time event distribution
- **Checkpoint Management** - Stateful recovery and replay capabilities
- **Schema Validation** - GitOps-driven schema registry with version control

### Current Automata
- **Analytics Automaton** - Pattern detection and insight generation
- **Content Automaton** - Document processing and enrichment
- **Search Automaton** - Query processing and result ranking
- **PKM Automaton** - Personal knowledge management operations

### Expanding Automaton Ecosystem
- **LLM Integration** - Local (Ollama) and remote models for semantic processing
- **Prompt Registry** - Versioned prompt management with GitOps
- **Entity Resolution** - Cross-event entity linking and knowledge graph construction
- **Context Synthesis** - Intelligent summarization and narrative generation

**Detailed Architecture:** [AgenticEcosystem_Architecture.md](docs/arch_modules/AgenticEcosystem_Architecture.md) - Note: "Agentic" refers to AI-powered intelligence; "Automaton" refers to deterministic event processors

## 5. User Interfaces & Query

Multiple interfaces for data access and exploration.

### Current Interfaces
- **CLI** (`exo.py`) - Query events, manage schemas, monitor processors with command/response patterns
- **Direct SQL** - Full database access for power users
- **Configuration** - TOML-based processor configuration

### Planned Interfaces
- **Web Dashboard** - Visual exploration and analytics
- **Neovim Plugin** - Integrated development environment
- **Query DSL** - Simplified query language
- **Grafana Dashboards** - Metrics and monitoring

### Current Analytics Limitations (35% of Vision)
- **Basic Processing** - Mechanical event routing via automaton satellites
- **Health Metrics Focus** - System metrics (CPU, memory, event counts) with some pattern detection
- **SQL-Based Queries** - Advanced SQL with time/source filtering via core.events
- **Limited Pattern Detection** - Basic cross-event correlation, expanding insight generation

### Planned Analytics Infrastructure (80% Gap)
See [TIM-AnalyticsInfrastructure.md](planned/infrastructure/TIM-AnalyticsInfrastructure.md) for transformation roadmap including:
- **SinexQL Query Language** - Domain-specific pattern matching language
- **Multi-Tier Processing** - Real-time stream + historical batch analysis
- **Personal AI Models** - Productivity analytics, anomaly detection, predictive insights
- **Real-Time Dashboards** - WebSocket-powered visualization with pattern alerts

**Detailed Architecture:** [UserInteraction_And_Query_Architecture.md](docs/arch_modules/UserInteraction_And_Query_Architecture.md)

## 6. System Operations & Deployment

Infrastructure for reliable, secure operation.

### NixOS Integration
- **Declarative Module** (`services.sinex`) - Complete system configuration
- **Systemd Services** - Satellite processors, hub services, maintenance jobs
- **Database Setup** - Automatic migrations and extensions
- **VM Testing** - Comprehensive integration tests

### Monitoring & Observability
- **Prometheus Metrics** - Queue depth, processing latency
- **Health Checks** - Agent heartbeats and status
- **Structured Logging** - JSON logs for analysis
- **Performance Tracking** - Resource usage monitoring

### Security & Privacy
- **Access Control** - PostgreSQL roles, systemd users
- **Process Isolation** - Sandboxed services
- **Secrets Management** - Agenix for sensitive data ([ADR-006](docs/adr/ADR-006-NixOSSecretsManagementTool.md))
- **User Consent** - Configurable data collection

### Backup & Recovery (Planned)
- **pgBackRest** - PostgreSQL point-in-time recovery
- **Git-Annex** - Distributed blob backup
- **Configuration** - Version-controlled NixOS

**Detailed Architecture:** [SystemOperations_And_Integrity_Architecture.md](docs/arch_modules/SystemOperations_And_Integrity_Architecture.md)

## 7. Summary & Next Steps

### Current State
Sinex has successfully implemented a sophisticated satellite constellation architecture with operational event collection, real-time message distribution, and processing infrastructure. The system captures events across four major domains (filesystem, terminal, desktop, system) through independent satellite services implementing the StatefulStreamProcessor interface, provides reliable storage in the core.events table with comprehensive provenance tracking via source_event_ids, and offers a unified API through the gateway service with command/response patterns. Redis Streams enable scalable, durable event processing with checkpoint management for stateful recovery. The source material registry preserves immutable ground truth with blob_id references, while processor manifests enable GitOps-driven service management.

### Near-Term Priorities
1. **Expand Automaton Ecosystem** - Build specialized processors for different data domains
2. **Enhance LLM Integration** - Connect automata with local and remote language models
3. **Add Event Sources** - Browser extension, audio capture, email integration
4. **Advanced Query Interface** - Rich CLI and web-based exploration tools

### Long-Term Vision
Realize the full "sentient archive" vision through the mature satellite constellation supporting AI-powered analysis, semantic search, knowledge graph construction, and multi-device synchronization. The satellite architecture enables independent evolution of each component while maintaining system coherence through the unified message bus and shared substrate.

---

*For detailed specifications, see the linked Architectural Modules, TIMs, and ADRs. For the philosophical foundation, see [VISION.md](VISION.md).*
