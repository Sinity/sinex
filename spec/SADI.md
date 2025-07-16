# Sinex: System Architecture & Document Interrelation (SADI) - v0.4

> **📊 IMPLEMENTATION STATUS**: Satellite architecture ✅ **OPERATIONAL** (75%), Message bus 🚧 **ROBUST** (70%), Data substrate 🚧 **MATURE** (65%), Event sources 🚧 **EXPANDING** (45%), Automaton ecosystem 🚧 **ACTIVE** (35%), Gateway & APIs 🚧 **FUNCTIONAL** (60%)

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
Sinex is a "sentient archive" implemented as a satellite constellation architecture – independent services orchestrated by NixOS/systemd that comprehensively capture, intelligently process, and powerfully query personal digital experiences through Redis Streams message bus, PostgreSQL persistence, and deep symmetry between ingestors and automata.

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
- **Checkpoint-based Recovery** - Unified state management for ingestors and automata ([ADR-009](docs/adr/ADR-009-ULID-Primary-Key-With-TimescaleDB.md))
- **Terminal Capture** - Layered approach with multiple sources ([ADR-008](docs/adr/ADR-008-TerminalActivityCaptureStrategy.md))
- **Clock Regression** - Handling time jumps gracefully ([ADR-011](docs/adr/ADR-011-clock-regression-handling.md))

## 📖 Document Structure & Navigation

### Vision & Strategy
- **[VISION.md](VISION.md)** - The "Why": Philosophy, manifesto, and long-term goals
- **[PLAN.md](PLAN.md)** - Development phases and current progress tracking

### Architecture Documents
- **[STAD.md](STAD.md)** - High-level system architecture overview
- **Architecture Modules** - Domain-specific deep dives:
  - [DataSubstrate_Architecture.md](docs/arch_modules/DataSubstrate_Architecture.md) - Database and storage layer
  - [IngestionArchitecture_And_TelemetrySources.md](docs/arch_modules/IngestionArchitecture_And_TelemetrySources.md) - Event capture system
  - [AgenticEcosystem_Architecture.md](docs/arch_modules/AgenticEcosystem_Architecture.md) - AI and processing agents
  - [UserInteraction_And_Query_Architecture.md](docs/arch_modules/UserInteraction_And_Query_Architecture.md) - Query interfaces
  - [SystemOperations_And_Integrity_Architecture.md](docs/arch_modules/SystemOperations_And_Integrity_Architecture.md) - Operations and deployment

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
- **[PATHWAYS.md](PATHWAYS.md)** - Where to start contributing
- **[DEPENDENCIES.md](DEPENDENCIES.md)** - Feature dependency tracking
- **[MATURITY.md](MATURITY.md)** - Specification maturity levels
- **[GLOSSARY.md](GLOSSARY.md)** - Project terminology

## 🚦 Implementation Status by Component

### ✅ Operational Components (60-85% Complete)
- **Satellite Architecture** (75%) - Independent services operational, StatefulStreamProcessor interface implemented
- **Message Bus** (70%) - Redis Streams with consumer groups, checkpoint management working
- **Data Substrate** (65%) - PostgreSQL + TimescaleDB + ULID, unified events table operational
- **Gateway & APIs** (60%) - Command/response patterns, CLI integration working

### 🚧 Partially Implemented (25-60% Complete)
- **Event Sources** (45%) - Four domain satellites operational, expanding coverage
- **NixOS Module** (55%) - Satellite orchestration working, observability patterns
- **Testing Framework** (75%) - Robust test infrastructure with transaction isolation
- **Git-Annex Integration** (50%) - Basic blob storage with content addressing

### 🚧 Active Development (25-40% Complete)
- **Automaton Ecosystem** (35%) - Processing framework operational, deterministic automata working
- **Query Interface** (25%) - CLI operational with gateway integration, expanding capabilities
- **Health Monitoring** (40%) - Journald heartbeat pattern, structured observability

### 🔨 Planned/Early Stage (<25% Complete)
- **AI/LLM Integration** (10%) - Framework ready, schema designed, integration starting
- **Knowledge Graph** (15%) - Schema implemented, basic relations, expanding
- **Advanced Event Sources** (5%) - Browser extension planned, audio/email concepts
- **Semantic Search** (5%) - pgvector ready, embedding framework designed
- **Multi-device Sync** (0%) - Architecture planned, not implemented
- **Web Dashboard** (0%) - Gateway ready for web UI, not built

## 🔄 Quick Links to Key Components

### Database Schema
- [Event Substrate DDL](implemented/infrastructure/TIM-EventSubstrateDDL.md)
- [Event Schema Registry](implemented/infrastructure/TIM-EventSchemaRegistry.md)
- [Knowledge Graph Schema](implemented/infrastructure/TIM-KnowledgeGraphSchema.md)

### Event Sources
- [Filesystem Monitoring](implemented/event-sources/TIM-FilesystemMonitoringWatchers.md)
- [Terminal Logging](implemented/event-sources/TIM-GenericTerminalLogging.md)
- [Clipboard Monitoring](implemented/event-sources/TIM-ClipboardMonitoring.md)
- [Hyprland IPC](implemented/event-sources/TIM-HyprlandIPCInterface.md)

### Infrastructure
- [Event Ingestion Processing](implemented/infrastructure/TIM-EventIngestionProcessing.md)
- [Agent Manifest Management](implemented/infrastructure/TIM-AgentManifestManagement.md)
- [Test Framework](implemented/infrastructure/TIM-TestFrameworkInfrastructure.md)

## 📝 Recent Updates

- **2025-07**: Satellite constellation architecture operational - Redis Streams, checkpoint management, unified events table
- **2025-07**: StatefulStreamProcessor interface implemented with deep symmetry between ingestors and automata
- **2025-07**: Documentation enhanced to match current implementation reality
- **2025-01**: Major documentation cleanup and reorganization
- **2024-12**: Comprehensive TIM restructuring with accurate implementation tracking
- **2024-11**: NixOS module implementation and VM testing framework

---

*This document serves as the central navigation hub for the Sinex project. For detailed information on any component, follow the links to the relevant documentation.*
