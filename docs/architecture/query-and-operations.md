# Query Architecture and System Operations

This document details the query interfaces and operational aspects of Sinex, extracted from the System Technical Architecture Document (STAD).

## User Interfaces & Query

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
The transformation roadmap includes:
- **SinexQL Query Language** - Domain-specific pattern matching language
- **Multi-Tier Processing** - Real-time stream + historical batch analysis
- **Personal AI Models** - Productivity analytics, anomaly detection, predictive insights
- **Real-Time Dashboards** - WebSocket-powered visualization with pattern alerts

## System Operations & Deployment

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
- **Secrets Management** - Agenix for sensitive data
- **User Consent** - Configurable data collection

### Backup & Recovery (Planned)
- **pgBackRest** - PostgreSQL point-in-time recovery
- **Git-Annex** - Distributed blob backup
- **Configuration** - Version-controlled NixOS

## Checkpoint & Replay

Sinex persists processor checkpoints in Postgres to enable fast restarts and targeted reprocessing without scanning the entire event log.

- Storage: `core.processor_checkpoints` holds `processor_name`, `consumer_group`, `consumer_name`, `last_processed_id`, `processed_count`, `checkpoint_data`, and activity timestamps.
- Semantics: processors advance `last_processed_id` atomically; replay tools can resume from the last checkpoint or override via time/id filters.
- Operations:
  - Inspect: list recent checkpoints ordered by `updated_at`.
  - Reset: delete a checkpoint for a processor/consumer to force full replay.
  - Update: upsert on progress (increments `processed_count`, updates activity timestamps).

Best practices
- Keep consumer group/name stable; prefer `default` unless isolation is required.
- During incident replay, write via ingestd to maintain invariants; do not mutate `core.events` directly.
- Instrument processors with tracing spans (`processor_name`, `advance_count`, `lag`).

## Processing Pipeline Details

### Processing Stages
1. **Raw Capture** - Satellites capture events with minimal processing
2. **Validation** - Schema validation at ingestion
3. **Storage** - Atomic writes to PostgreSQL
4. **Distribution** - NATS JetStream events fan-out via transactional outbox
5. **Processing** - Automata create synthesis events
6. **Enrichment** - Knowledge graph updates

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

Note: "Agentic" refers to AI-powered intelligence; "Automaton" refers to deterministic event processors.

## Near-Term Priorities

1. **Expand Automaton Ecosystem** - Build specialized processors for different data domains
2. **Enhance LLM Integration** - Connect automata with local and remote language models
3. **Add Event Sources** - Browser extension, audio capture, email integration
4. **Advanced Query Interface** - Rich CLI and web-based exploration tools

## Long-Term Vision

Realize the full "sentient archive" vision through the mature satellite constellation supporting:
- AI-powered analysis
- Semantic search
- Knowledge graph construction
- Multi-device synchronization

The satellite architecture enables independent evolution of each component while maintaining system coherence through the unified message bus and shared substrate.
