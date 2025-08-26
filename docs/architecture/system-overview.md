# Sinex System Architecture Overview

This document provides a comprehensive technical overview of the Sinex architecture, extracted from the System Technical Architecture Document (STAD).

## System Overview

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
│                   Message Bus (NATS JetStream)                   │
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
- **Satellite Constellation** - Independent services orchestrated by systemd/NixOS
- **NATS JetStream Message Bus** - Durable internal messaging (older docs mentioning Redis Streams are historical)
- **Unified Events Table** - Single source of truth with comprehensive provenance tracking
- **Time-Ordered Keys** - ULID primary keys for natural chronological ordering and distributed generation
- **GitOps Schema Management** - Version-controlled JSON Schema validation with automatic deployment
- **Journald Heartbeat Pattern** - Elegant observability through structured logging and systemd integration
- **Command/Response Architecture** - Asynchronous API patterns with full auditability via message bus
- **Local-First & User Sovereign** - Complete functionality and control without cloud dependencies

## Data Substrate Architecture

The foundation of Sinex built on PostgreSQL with specialized extensions.

### Core Components
- **PostgreSQL 16** with extensions:
  - **TimescaleDB** - Time-series optimization for events
  - **pgx_ulid** - Time-ordered primary keys
  - **pg_jsonschema** - Event payload validation
  - **pgvector** - Future semantic search capabilities

### Event Storage
- **Unified Events Table** (`core.events`) - Single source of truth for all captured data with comprehensive provenance tracking via source_event_ids
- **Source Material Registry** (`raw.source_material_registry`) - Immutable ground truth preservation with blob_id references
- **Processor Manifests** (`sinex_schemas.processor_manifests`) - GitOps-driven processor registration and metadata
- **Schema Registry** (`sinex_schemas.event_payload_schemas`) - Versioned JSON schemas with GitOps management
- **Checkpoint System** (`core.automaton_checkpoints`) - Stateful processor recovery with unified interface
- **Message Bus** - NATS JetStream for real-time event distribution

### Knowledge Representation (Future)
- **Knowledge Graph** (`core.entities`, `core.entity_relations`)
- **Artifacts** (`core.artifacts`) - Documents, notes, media
- **Embeddings** (`artifact_embeddings`) - Semantic search vectors
- **Blob Storage** - Git-annex for large files

## Event Collection Architecture

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

## Processing Pipeline

Event-driven processing system with distributed workers.

### Satellite Constellation
- **Independent Services** - Each satellite runs as separate systemd service
- **StatefulStreamProcessor Pattern** - Unified scan(from: Checkpoint, until: TimeHorizon) interface
- **Message Streaming** - NATS JetStream for real-time event distribution
- **Consumer Groups** - Horizontal scaling with exactly-once guarantees

### Processing Stages
1. **Raw Capture** - Satellites capture events with minimal processing
2. **Validation** - Schema validation at ingestion
3. **Storage** - Atomic writes to PostgreSQL
4. **Distribution** - NATS JetStream events fan-out
5. **Processing** - Automata create synthesis events
6. **Enrichment** - Knowledge graph updates

## Implementation Status

### ✅ Operational Components (>70% Complete)
- **Satellite Architecture** (80%) - Independent satellite services operational, StatefulStreamProcessor interface implemented
- **Message Bus** (75%) - NATS JetStream operational with durable consumers, checkpoint management, command/response patterns
- **Data Substrate** (70%) - PostgreSQL + TimescaleDB with ULID keys, core.events table operational, comprehensive provenance tracking

### 🚧 In Progress Components (25-70% Complete)
- **Event Sources** (50%) - Four satellite domains active (filesystem, terminal, desktop, system), expanding coverage
- **Automaton Ecosystem** (40%) - Processing framework operational, deterministic automata working, agentic layer planned
- **Gateway & APIs** (65%) - sinex-gateway operational, command/response patterns working, CLI integrated
- **NixOS Module** (60%) - Satellite orchestration working, observability patterns operational

### 🔨 Early Development (<25% Complete)
- **AI/LLM Integration** (15%) - Framework ready, schema designed, integration in progress
- **Knowledge Graph** (20%) - Schema defined, basic operations working
- **Multi-device Sync** (0%) - Architecture supports it, not implemented
