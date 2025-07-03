# Sinex: System Technical Architecture Document (STAD) v1.3

> **📊 IMPLEMENTATION STATUS**:
>
> - 🚧 **Core Infrastructure** (45%) - Basic PostgreSQL + TimescaleDB working, needs hardening
> - 🚧 **Event Sources** (35%) - Four sources operational, many more planned
> - 🔨 **Processing Pipeline** (25%) - Basic queue works, minimal processing logic
> - 🚧 **NixOS Module** (40%) - Basic services, needs production features
> - 🔨 **Query Interface** (15%) - Minimal CLI only
> - ❌ **AI/LLM Integration** (0%) - Schema only, no implementation
> - ❌ **Knowledge Graph** (5%) - Schema only, no population logic

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

### Architecture Diagram
```
┌─────────────────────────────────────────────────────────────────┐
│                        User Interfaces                           │
│         CLI (exo.py)    │    Future: Web UI    │   Neovim       │
└────────────────────────┬────────────────────────────────────────┘
                         │
┌────────────────────────┴────────────────────────────────────────┐
│                     Query & Analysis Layer                       │
│         SQL    │    Query DSL    │    Future: AI/LLM            │
└────────────────────────┬────────────────────────────────────────┘
                         │
┌────────────────────────┴────────────────────────────────────────┐
│                    Processing Pipeline                           │
│    Work Queue    │    Workers    │    Event Routing             │
└────────────────────────┬────────────────────────────────────────┘
                         │
┌────────────────────────┴────────────────────────────────────────┐
│                      Data Substrate                              │
│    PostgreSQL + TimescaleDB    │    Git-Annex Blobs             │
└────────────────────────┬────────────────────────────────────────┘
                         │
┌────────────────────────┴────────────────────────────────────────┐
│                    Event Collection                              │
│  Unified Collector  │  Event Sources  │  Schema Validation       │
└─────────────────────────────────────────────────────────────────┘
```

### Key Architectural Principles
- **Immutable Event Log** - All data preserved in `raw.events`
- **Time-Ordered Keys** - ULID primary keys for natural ordering
- **Schema Validation** - JSON Schema enforcement on all events
- **Distributed Processing** - Lock-free work queue distribution
- **Local-First** - Complete functionality without cloud dependencies
- **User Agency** - Full control over data collection and processing

## 2. Data Substrate Architecture

The foundation of Sinex built on PostgreSQL with specialized extensions.

### Core Components
- **PostgreSQL 16** with extensions:
  - **TimescaleDB** - Time-series optimization for events
  - **pgx_ulid** - Time-ordered primary keys ([ADR-001](docs/adr/ADR-001-PrimaryKeyStrategy.md))
  - **pg_jsonschema** - Event payload validation
  - **pgvector** - Future semantic search capabilities

### Event Storage
- **Immutable Event Log** (`raw.events`) - Source of truth for all captured data
- **Schema Registry** (`event_payload_schemas`) - Versioned JSON schemas
- **Work Queue** (`work_queue`) - Distributed event processing ([ADR-002](docs/adr/ADR-002-EventProcessingNotificationMechanism.md))
- **Routing Cache** - Materialized view for efficient distribution ([ADR-014](docs/adr/ADR-014-routing-cache.md))

### Knowledge Representation (Future)
- **Knowledge Graph** (`core.entities`, `core.entity_relations`)
- **Artifacts** (`core.artifacts`) - Documents, notes, media
- **Embeddings** (`artifact_embeddings`) - Semantic search vectors
- **Blob Storage** - Git-annex for large files

**Detailed Architecture:** [DataSubstrate_Architecture.md](docs/arch_modules/DataSubstrate_Architecture.md)

## 3. Event Collection Architecture

Unified event collection system managing multiple data sources.

### Unified Collector
- **Single Binary** (`sinex-collector`) - Coordinates all event sources
- **EventSource Trait** - Common interface for all sources
- **Hot-Reload Config** - Dynamic source management without restart
- **Schema Validation** - All events validated before storage

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

### Work Queue System
- **Lock-Free Distribution** - `SELECT FOR UPDATE SKIP LOCKED` pattern
- **Agent Registration** - Manifests define capabilities and routing
- **Dead Letter Queue** - Failed event handling and retry logic
- **Metrics Export** - Prometheus metrics for monitoring

### Current Workers
- **Promotion Worker** - Transforms raw events to structured data
- **Health Monitor** - Agent heartbeat tracking

### Future AI Integration
- **LLM Integration** - Local (Ollama) and remote models
- **Prompt Registry** - Versioned prompt management
- **Entity Resolution** - Identify and link entities across events
- **Context Synthesis** - Generate meaningful summaries

**Detailed Architecture:** [AgenticEcosystem_Architecture.md](docs/arch_modules/AgenticEcosystem_Architecture.md)

## 5. User Interfaces & Query

Multiple interfaces for data access and exploration.

### Current Interfaces
- **CLI** (`exo.py`) - Query events, manage schemas, monitor agents
- **Direct SQL** - Full database access for power users
- **Configuration** - TOML-based collector configuration

### Planned Interfaces
- **Web Dashboard** - Visual exploration and analytics
- **Neovim Plugin** - Integrated development environment
- **Query DSL** - Simplified query language
- **Grafana Dashboards** - Metrics and monitoring

### Current Analytics Limitations (20% of Vision)
- **Basic Routing** - Mechanical event routing via promotion worker
- **Health Metrics Only** - System metrics (CPU, memory, event counts)
- **Simple Queries** - Basic SQL with time/source filtering
- **No Pattern Detection** - No cross-event correlation or insight generation

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
- **Systemd Services** - Collector, workers, maintenance jobs
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
Sinex has a solid foundation with working event collection, storage, and processing infrastructure. The system successfully captures filesystem, terminal, clipboard, and window manager events, storing them reliably in a time-series optimized PostgreSQL database.

### Near-Term Priorities
1. **Complete Promotion Worker** - Transform raw events to structured data
2. **Enhance Query Interface** - Build advanced query capabilities
3. **Add Event Sources** - Browser history, audio capture
4. **Performance Optimization** - Database indexing and query tuning

### Long-Term Vision
Build towards the full "sentient archive" vision with AI-powered analysis, semantic search, knowledge graph construction, and multi-device synchronization. The modular architecture ensures each component can evolve independently while maintaining system coherence.

---

*For detailed specifications, see the linked Architectural Modules, TIMs, and ADRs. For the philosophical foundation, see [VISION.md](VISION.md).*
