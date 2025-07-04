# Sinex: System Architecture & Document Interrelation (SADI) - v0.4

> **📊 IMPLEMENTATION STATUS**: Core infrastructure 🚧 **PARTIAL** (45%), Event sources 🚧 **PARTIAL** (35%), Processing pipeline 🚧 **BASIC** (25%), NixOS module 🚧 **BASIC** (40%), Query interface 🚧 **MINIMAL** (15%), AI features ❌ **PLANNED** (0%)

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
Sinex is conceived as a "sentient archive" – a comprehensive personal data system designed to combat digital amnesia and augment human intellect through universal capture, intelligent processing, and powerful query capabilities.

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
- **Event Processing** - Work queue with `SELECT FOR UPDATE SKIP LOCKED` ([ADR-002](docs/adr/ADR-002-EventProcessingNotificationMechanism.md))
- **Routing Cache** - Materialized view for efficient event routing ([ADR-014](docs/adr/ADR-014-routing-cache.md))
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

### 🚧 Partially Implemented (25-70% Complete)
- **Database Infrastructure** (45%) - PostgreSQL + TimescaleDB working, needs optimization
- **Event Sources** (35%) - 4 sources working out of many planned
- **NixOS Module** (40%) - Basic services work, needs polish
- **Testing Framework** (75%) - Robust test infrastructure with database pooling and FK handling
- **Git-Annex Integration** (50%) - Basic blob storage works

### 🔨 Basic Implementation (10-25% Complete)
- **Processing Pipeline** (25%) - Queue works but minimal processing logic
- **Unified Collector** (20%) - Coordinates sources but needs robustness
- **Query Interface** (15%) - Minimal CLI, no advanced features
- **Health Monitoring** (15%) - Very basic heartbeats only

### 📋 Planned/Minimal (<10% Complete)
- **AI Integration** (0%) - Only database schema exists
- **Knowledge Graph** (5%) - Schema only, no implementation
- **Promotion Workers** (10%) - Skeleton code only
- **Advanced Event Sources** (0%) - Not started (Browser, Audio, Email)
- **Semantic Search** (0%) - pgvector installed but unused
- **Multi-device Sync** (0%) - Concept only
- **Web Dashboard** (0%) - No implementation

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

- **2025-01**: Major documentation cleanup and reorganization
- **2024-12**: Comprehensive TIM restructuring with accurate implementation tracking
- **2024-11**: NixOS module implementation and VM testing framework
- **2024-10**: Unified collector architecture with hot-reload configuration

---

*This document serves as the central navigation hub for the Sinex project. For detailed information on any component, follow the links to the relevant documentation.*
