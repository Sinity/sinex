# Sinex: System Architecture & Document Interrelation (SADI) - v0.4

> **📊 IMPLEMENTATION STATUS**: Satellite architecture ✅ **OPERATIONAL** (80%), Message bus ✅ **ROBUST** (75%), Data substrate ✅ **MATURE** (70%), Event sources 🚧 **EXPANDING** (50%), Automaton ecosystem 🚧 **ACTIVE** (40%), Gateway & APIs 🚧 **FUNCTIONAL** (65%)

**Purpose**

This document serves as the central navigation hub for the Sinex project documentation. It provides:

1. A map of all project documentation and their relationships
2. Quick access to key architectural decisions and technical specifications
3. Implementation status tracking across all components

## 📚 Documentation Overview

### Core Documents

- **[VISION.md](VISION.md)** - Project philosophy and long-term goals
- **[STAD.md](STAD.md)** - System Technical Architecture Document 
- **[PLAN.md](PLAN.md)** - Development roadmap and phase tracking
- **[DEPENDENCIES.md](DEPENDENCIES.md)** - Feature dependency graph
- **[PATHWAYS.md](PATHWAYS.md)** - Contribution guide by role/interest
- **[MATURITY.md](MATURITY.md)** - Specification maturity model
- **[GLOSSARY.md](GLOSSARY.md)** - Project terminology reference

### Technical Specifications

- **`implemented/`** - Features with working code (70%+ complete)
- **`ready/`** - Fully specified, ready to implement
- **`planned/`** - Future features requiring design work

### Architecture & Design

- **`docs/arch_modules/`** - Domain-specific architecture deep-dives
- **`docs/adr/`** - Architectural Decision Records
- **`diagram/`** - Visual architecture documentation

## 🏗️ Core Architecture

### System Philosophy
Sinex is a "sentient archive" implemented as a satellite constellation architecture – independent services orchestrated by NixOS/systemd that comprehensively capture, intelligently process, and powerfully query personal digital experiences through Redis Streams message bus, PostgreSQL persistence with unified events table, and StatefulStreamProcessor interface for both ingestors and automata.

### Technical Stack

**Foundation:**
- **OS**: NixOS for reproducible, declarative deployment
- **Database**: PostgreSQL 16 with extensions:
  - TimescaleDB for time-series event storage
  - pgx_ulid for time-ordered primary keys
  - pg_jsonschema for event validation
  - pgvector for semantic search (future)
- **Language**: Rust for core system, Python for CLI tools

**Key Architectural Decisions:**
- **ULID Primary Keys** - Time-ordered, globally unique ([ADR-001](docs/adr/ADR-001-PrimaryKeyStrategy.md))
- **Satellite Constellation** - Independent services with unified StatefulStreamProcessor interface ([ADR-010](docs/adr/ADR-010-UnifiedCollectorEventCentricArchitecture.md))
- **Redis Streams Message Bus** - Real-time event distribution with consumer groups ([ADR-002](docs/adr/ADR-002-EventProcessingNotificationMechanism.md))
- **Unified Events Table** - Single core.events table with comprehensive provenance tracking
- **Checkpoint-based Recovery** - Unified state management for processors ([ADR-009](docs/adr/ADR-009-ULID-Primary-Key-With-TimescaleDB.md))
- **Source Material Registry** - Immutable ground truth preservation with blob_id references
- **Processor Manifests** - GitOps-driven processor registration and metadata
- **Terminal Capture** - Layered approach with multiple sources ([ADR-008](docs/adr/ADR-008-TerminalActivityCaptureStrategy.md))
- **Clock Regression** - Handling time jumps gracefully ([ADR-011](docs/adr/ADR-011-clock-regression-handling.md))

## 📖 Document Structure & Navigation

### Vision & Strategy
- **[VISION.md](VISION.md)** - The "Why": Philosophy, manifesto, and long-term goals
- **[PLAN.md](PLAN.md)** - Development phases and current progress tracking

### Architecture Documents
- **[STAD.md](STAD.md)** - System Technical Architecture Document (comprehensive overview)
- **Architecture Modules** - Domain-specific deep dives (see STAD.md for details)

### Implementation Specifications (TIMs)

Technical Implementation Modules follow a consistent structure:

**Status Dashboard** (required for feature TIMs):
```markdown
## Status Dashboard
**Maturity Level**: L2/L3/L4 - Ready/Implemented
**Implementation**: X% (Verified against codebase)
**Dependencies**: Required components
**Blocks**: Features that depend on this TIM
```

**Maturity Levels**:
- **L2 - Ready**: Complete specification, clear implementation plan
- **L3 - Partial**: Core functionality implemented, missing features  
- **L4 - Complete**: Feature working with tests and documentation

**Implementation Percentages** (based on codebase verification):
- **0-25%**: Design complete, minimal implementation
- **25-50%**: Core infrastructure exists, missing functionality
- **50-75%**: Major components implemented, missing integration
- **75-90%**: Substantially complete, minor features missing
- **90-100%**: Production-ready with comprehensive testing

**TIM Categories**:
- **Feature TIMs** - Implementable features by status:
  - `implemented/` - Working features (70%+ complete)
  - `ready/` - Fully designed, ready to build
  - `planned/` - Future features needing design
- **Process TIMs** - Documentation only:
  - `docs/processes/` - Development practices
  - `docs/operations/` - Operational procedures
  - `docs/security/` - Security documentation

### Design Decisions
- **[ADR Directory](docs/adr/)** - Architectural Decision Records explaining "why"

### Development Resources
- **[CLAUDE.md](../CLAUDE.md)** - Development workflows and patterns
- **[plan.md](../plan.md)** - Unified architecture and implementation guide
- **[PATHWAYS.md](PATHWAYS.md)** - Where to start contributing
- **[DEPENDENCIES.md](DEPENDENCIES.md)** - Feature dependency tracking
- **[MATURITY.md](MATURITY.md)** - Specification maturity levels
- **[GLOSSARY.md](GLOSSARY.md)** - Project terminology

## 🚦 Implementation Status by Component

### Implementation Status Summary
See [STAD.md](STAD.md) for detailed implementation status. Key highlights:

**Operational (70%+ Complete):**
- Satellite Architecture with StatefulStreamProcessor interface (80%)
- Redis Streams message bus with consumer groups (75%)
- Core.events table with comprehensive provenance (70%)
- Gateway & APIs with command/response patterns (65%)

**In Progress (25-70% Complete):**
- Event Sources across four domains (50%)
- Automaton processing ecosystem (40%)
- Testing framework and operations (60%)

**Planned (<25% Complete):**
- AI/LLM integration framework (15%)
- Advanced event sources (browser, audio) (5%)
- Multi-device synchronization (0%)

## 🔄 Quick Links to Key Components

### Database Schema
- [Event Substrate DDL](implemented/infrastructure/TIM-EventSubstrateDDL.md) - Core.events table with provenance
- [Event Schema Registry](implemented/infrastructure/TIM-EventSchemaRegistry.md) - GitOps schema management
- [Knowledge Graph Schema](implemented/infrastructure/TIM-KnowledgeGraphSchema.md) - Entities and relations

### Event Sources
- [Filesystem Monitoring](implemented/event-sources/TIM-FilesystemMonitoringWatchers.md)
- [Terminal Logging](implemented/event-sources/TIM-GenericTerminalLogging.md)
- [Clipboard Monitoring](implemented/event-sources/TIM-ClipboardMonitoring.md)
- [Hyprland IPC](implemented/event-sources/TIM-HyprlandIPCInterface.md)

### Infrastructure
- [Event Ingestion Processing](implemented/infrastructure/TIM-EventIngestionProcessing.md) - StatefulStreamProcessor architecture
- [Agent Manifest Management](implemented/infrastructure/TIM-AgentManifestManagement.md) - Processor manifests
- [Test Framework](implemented/infrastructure/TIM-TestFrameworkInfrastructure.md) - Comprehensive testing infrastructure

## 📝 Recent Updates

- **2025-07**: Unified architecture implemented - core.events table, StatefulStreamProcessor interface, source material registry, processor manifests
- **2025-07**: Documentation updated to reflect current implementation reality
- **2025-07**: Satellite constellation architecture operational with Redis Streams and checkpoint management
- **2025-01**: Major documentation cleanup and reorganization
- **2024-12**: Comprehensive TIM restructuring with accurate implementation tracking
- **2024-11**: NixOS module implementation and VM testing framework

---

*This document serves as the central navigation hub for the Sinex project. For detailed information on any component, follow the links to the relevant documentation.*
