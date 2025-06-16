# Sinex Dependency Graph

## Overview

This document defines the dependency relationships between Sinex components, organized into implementation tiers. Features in higher tiers depend on components from lower tiers, creating a clear implementation roadmap.

## Dependency Tier System

### Tier 0: Foundation Components
**Core infrastructure with no internal dependencies**

```
Event Storage Infrastructure
├── PostgreSQL Database ✅ [L4 - Implemented]
├── TimescaleDB Extension ✅ [L4 - Implemented]  
├── ULID Primary Keys ✅ [L4 - Implemented]
├── Basic Event Schema ✅ [L4 - Implemented]
└── Promotion Queue System 🚧 [L3 - Ready]

Git Integration
├── Git-annex Blob Storage ✅ [L4 - Implemented via sinex-annex crate]
├── Repository State Tracking ✅ [L4 - Implemented]
└── Blob Content Addressing ✅ [L4 - Implemented]

Core Event Sources
├── Filesystem Events ✅ [L4 - Implemented]
├── Terminal Events ✅ [L4 - Implemented] 
├── Clipboard Events ✅ [L4 - Implemented]
├── Basic Process Monitoring ✅ [L4 - Implemented]
├── Hyprland IPC Interface ✅ [L4 - Implemented]
└── Generic Terminal Logging ✅ [L4 - Implemented]
```

**Implementation Status:** 85% complete
**Blocking:** Promotion queue worker completion

---

### Tier 1: Independent Extensions  
**Features that depend only on Tier 0 components**

```
Enhanced Event Sources
├── Audio Capture (PipeWire) [L3 - Ready] 
│   └── Depends: Event Storage ✅
├── Advanced Terminal Context [L2 - Technical]
│   └── Depends: Terminal Events ✅
├── Email Integration [L2 - Technical]
│   └── Depends: Event Storage ✅
└── Rich Hyprland Context Enhancement [L2 - Technical]
    └── Depends: Hyprland IPC Interface ✅

Infrastructure Components  
├── pgBackRest Backup Setup [L3 - Ready]
│   └── Depends: PostgreSQL ✅
├── Basic LLM Integration [L2 - Technical]
│   └── Depends: Event Storage ✅
├── System Monitoring [L2 - Technical]
│   └── Depends: Event Storage ✅
└── Configuration Management [L2 - Technical]
    └── Depends: Core Infrastructure ✅
```

**Implementation Status:** 20% complete
**Next Priority:** Audio capture, pgBackRest, basic LLM

---

### Tier 2: Dependent Features
**Components requiring multiple Tier 1 dependencies**

```
Browser Integration
├── Browser Extension [L3 - Ready]
│   ├── Depends: Native Messaging Protocol [L2]
│   └── Depends: Event Storage ✅
└── Web History Analysis [L2 - Technical]
    ├── Depends: Browser Extension [L3]
    └── Depends: LLM Integration [L2]

AI Processing Pipeline
├── Embedding Generation [L2 - Technical]
│   ├── Depends: LLM Integration [L2]
│   └── Depends: Event Storage ✅
├── Entity Resolution [L2 - Technical]
│   ├── Depends: Embedding Generation [L2]
│   └── Depends: LLM Integration [L2]
└── Context Synthesis [L1 - Concept]
    ├── Depends: Entity Resolution [L2]
    └── Depends: Rich Event Sources [Mixed]

Search and Query
├── Semantic Search [L1 - Concept]
│   ├── Depends: Embedding Generation [L2]
│   └── Depends: Vector Storage [L2]
├── Advanced Query Language [L2 - Technical]
│   └── Depends: Event Storage ✅
└── Query Optimization [L1 - Concept]
    └── Depends: Advanced Query Language [L2]
```

**Implementation Status:** 5% complete  
**Blocked By:** Tier 1 LLM integration, Native messaging

---

### Tier 3: Complex Integrations
**Advanced features with multiple complex dependencies**

```
Knowledge Management
├── Living Documents [L1 - Concept]
│   ├── Depends: CRDT Implementation [L0]
│   ├── Depends: Semantic Search [L1]
│   └── Depends: Context Synthesis [L1]
├── PKM Integration [L2 - Technical]
│   ├── Depends: Living Documents [L1]
│   └── Depends: Entity Resolution [L2]
└── Knowledge Graph Building [L1 - Concept]
    ├── Depends: Entity Resolution [L2]
    └── Depends: Semantic Search [L1]

User Interfaces
├── Neovim Plugin [L2 - Technical]
│   ├── Depends: Query Language [L2]
│   └── Depends: PKM Integration [L2]
├── Web Dashboard [L2 - Technical]  
│   ├── Depends: Semantic Search [L1]
│   └── Depends: Advanced Query [L2]
└── Mobile Interface [L1 - Concept]
    └── Depends: Multi-device Sync [L0]

Advanced Processing
├── Activity Segmentation [L1 - Concept]
│   ├── Depends: Context Synthesis [L1]
│   └── Depends: LLM Integration [L2]
└── Behavioral Pattern Analysis [L1 - Concept]
    ├── Depends: Activity Segmentation [L1]
    └── Depends: Knowledge Graph [L1]
```

**Implementation Status:** 2% complete
**Blocked By:** Multiple Tier 2 dependencies

---

### Tier 4: Distributed Systems
**Multi-device and federation capabilities**

```
Synchronization
├── Multi-device Sync [L0 - Vision]
│   ├── Depends: Conflict Resolution [L0]
│   ├── Depends: Event Storage ✅
│   └── Depends: Security Framework [L0]
├── Privacy-preserving Federation [L0 - Vision]
│   ├── Depends: Multi-device Sync [L0]
│   └── Depends: Cryptographic Protocol [L0]
└── Distributed Query Processing [L0 - Vision]
    ├── Depends: Federation [L0]
    └── Depends: Query Optimization [L1]

Advanced Security
├── End-to-end Encryption [L0 - Vision]
│   └── Depends: Cryptographic Framework [L0]
├── Zero-knowledge Proofs [L0 - Vision]
│   └── Depends: E2E Encryption [L0]
└── Audit and Compliance [L1 - Concept]
    └── Depends: Security Framework [L0]
```

**Implementation Status:** 0% complete
**Blocked By:** Foundational research required

## Critical Dependency Paths

### Primary Implementation Sequence

1. **Complete Tier 0** (Current Priority)
   - Finish promotion queue system
   - Stabilize core event processing
   - **Estimated:** 2-3 weeks

2. **Tier 1 Foundation Features**
   - Audio capture via PipeWire
   - Basic LLM integration with Ollama
   - pgBackRest backup configuration
   - **Estimated:** 4-6 weeks

3. **Tier 2 AI Pipeline**
   - Embedding generation
   - Browser extension
   - Semantic search foundation
   - **Estimated:** 10-12 weeks

### Parallel Development Opportunities

**Independent Workstreams:**
- Infrastructure (pgBackRest, monitoring) 
- Event sources (audio, email, advanced terminal)
- UI components (with mock data during development)

**Shared Dependencies:**
- LLM integration blocks all AI features
- Event storage stability affects all tiers
- Native messaging protocol blocks browser features

### Risk Mitigation

**Single Points of Failure:**
- PostgreSQL/TimescaleDB stability
- LLM integration architecture decisions
- Native messaging security model

**Dependency Risk Levels:**
- 🟢 **Low Risk:** Event storage, Git-annex, file systems
- 🟡 **Medium Risk:** LLM integration, browser APIs
- 🔴 **High Risk:** CRDT implementation, multi-device sync

## Implementation Strategy

### MVP Pathway (3-month horizon)
```
Month 1: Complete Tier 0
├── Promotion queue system
└── Infrastructure hardening

Month 2: Tier 1 Essentials  
├── Hyprland IPC
├── pgBackRest setup
└── Basic LLM integration

Month 3: Early Tier 2
├── Simple embedding generation
└── Browser extension MVP
```

### Full Feature Development (12-month horizon)
- Months 1-3: Tier 0-1 completion
- Months 4-6: Tier 2 foundation 
- Months 7-9: Tier 3 early features
- Months 10-12: Advanced Tier 2-3 features

### Research Timeline (18+ month horizon)
- Months 1-6: CRDT and living documents research
- Months 7-12: Multi-device sync architecture
- Months 13-18: Federation and privacy protocols

## Dependency Management

### Adding New Dependencies
1. Identify minimum tier placement
2. Document all blocking relationships
3. Update implementation estimates
4. Consider parallel development opportunities

### Resolving Blockers
1. **Technical Blockers:** Research, prototyping, architecture decisions
2. **Resource Blockers:** Prioritization, contributor allocation
3. **External Blockers:** Third-party APIs, upstream projects

### Dependency Health Monitoring
- Track completion percentage by tier
- Monitor critical path progression  
- Identify and resolve dependency cycles
- Regular review of blocking relationships

## Contributor Guidance

### New Contributors
**Recommended Starting Points:**
- Tier 0 enhancements and testing
- Tier 1 independent features (audio, email)
- Documentation and testing for implemented features

### Experienced Contributors  
**High-Impact Areas:**
- LLM integration architecture (unblocks Tier 2)
- Audio capture implementation (enables multimedia events)
- Browser extension (enables web integration)
- Living documents research (advances Tier 3)

### Specialists
**Domain-Specific Opportunities:**
- **Database:** Advanced PostgreSQL optimizations
- **AI/ML:** Embedding and LLM integration
- **Security:** Cryptographic protocols and privacy
- **UI/UX:** Interface design and user experience