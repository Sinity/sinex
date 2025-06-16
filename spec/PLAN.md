# Refined Sinex Spec Organization Plan

## Core Principles
- Make the ambitious vision navigable without reducing it
- Create multiple entry points for different contributors
- Clearly show what can be built now vs what needs prerequisites

## 1. Specification Maturity Model

Create `spec/MATURITY.md` defining levels:
- **L0 - Vision**: Aspirational goals, no technical details
- **L1 - Concept**: Architecture and data flow defined
- **L2 - Technical**: APIs, schemas, algorithms specified
- **L3 - Ready**: All dependencies met, clear acceptance criteria
- **L4 - Implemented**: Built with coverage percentage

Tag each TIM with:
```markdown
**Maturity**: L2 - Technical Specification
**Blocks**: Browser extension native messaging
**Blocked By**: None
**To Reach L3**: Define message protocol, security model
```

## 2. Dependency Graph System

Create `spec/DEPENDENCIES.md`:
```
Tier 0 (Foundation - 90% Done)
├── Event Storage (PostgreSQL) ✅
├── ULID Keys ✅
├── Basic Event Sources ✅
└── Promotion Queue 🚧

Tier 1 (No External Dependencies)
├── Git Annex Blob Storage ✅ (sinex-annex crate implemented)
├── Rich Hyprland IPC [L3]
├── Basic LLM Integration [L2]
└── pgBackRest Setup [L3]

Tier 2 (Needs Tier 1)
├── Browser Extension [L3] → needs native messaging
├── Embedding Generation [L2] → needs LLM
└── Semantic Search [L1] → needs embeddings

Tier 3 (Complex Dependencies)
├── Living Documents [L1] → needs CRDT, PKM
└── Multi-device Sync [L0] → needs conflict resolution
```

## 3. Implementation Pathways

Create `spec/PATHWAYS.md` with role-based guides:

**"I want to add a new event source"**
- Start here: `ready/event-sources/`
- Prerequisites: Understand EventSource trait
- First project: Extend Hyprland IPC (builds on existing)

**"I want to work on AI features"**
- Start here: `ready/ai/basic-ollama-integration.md`
- Prerequisites: Tier 0 complete
- First project: Local embedding generation

**"I want to improve existing features"**
- Start here: `implemented/` for current coverage
- Look for "Enhancement Opportunities" sections

## 4. Restructured Documentation

```
spec/
├── MATURITY.md          # Maturity level definitions
├── DEPENDENCIES.md      # Feature dependency graph
├── PATHWAYS.md         # Contributor entry points
├── STATUS.md           # Current implementation status
│
├── implemented/        # What's built (with coverage)
│   ├── event-storage.md (90% coverage)
│   └── basic-sources.md (100% coverage)
│
├── ready/             # Can implement now (L3)
│   ├── event-sources/
│   │   ├── hyprland-rich-context.md
│   │   └── audio-pipewire.md
│   ├── infrastructure/
│   │   └── pgbackrest-setup.md
│   └── ai/
│       └── basic-ollama.md
│
├── blocked/           # Waiting on dependencies (L2)
│   ├── browser-extension.md (needs native msg)
│   └── embedding-generation.md (needs LLM)
│
├── design/            # Needs more work (L1)
│   ├── living-documents.md
│   └── semantic-desktop.md
│
└── vision/            # Long-term goals (L0)
    ├── multi-device-sync.md
    └── privacy-federation.md
```

## 5. TIM Enhancement Format

Update each TIM with:
```markdown
# TIM-Name

## Status Dashboard
**Maturity Level**: L2 - Technical Specification
**Implementation**: 30% (basic events only)
**Dependencies**: PostgreSQL, EventSource trait
**Blocks**: Rich context features, AI analysis

## MVP Specification
[What can be built right now]

## Enhanced Features
[What needs other components first]

## Implementation Checklist
- [ ] Database migrations
- [ ] Core structs
- [ ] EventSource impl
- [ ] Tests
- [ ] Documentation
```

## 6. Progress Tracking

Create `spec/PROGRESS.md`:
```markdown
# Sinex Implementation Progress

## Overall: 20% of Vision

### By Domain
- Core Infrastructure: 80% ████████░░
- Event Capture: 30% ███░░░░░░░
- AI Integration: 5% ░░░░░░░░░░
- User Interface: 10% █░░░░░░░░░

### Next Milestones
1. Complete Tier 1 features (Q1)
2. Basic AI integration (Q2)
3. Browser extension (Q3)
```

## Implementation Order

1. Create the new structure and organizing documents
2. Categorize existing TIMs by maturity level
3. Split TIMs into MVP vs Enhanced sections
4. Move TIMs to appropriate directories
5. Add dependency and blocking information
6. Create pathway guides for contributors

This organization maintains the full vision while making it clear what can be built today, what needs prerequisites, and how different contributors can engage with the project.

## Implementation Gap Analysis

### Current Implementation: ~20% of Vision

### What's Built:
- Core event storage infrastructure
- Basic event sources (filesystem, terminal, clipboard)
- Simple promotion worker
- Database schema (mostly complete)
- Git-annex blob storage (sinex-annex crate with BlobManager)

### Major Unimplemented Categories:

#### 1. AI/LLM Integration (95% unimplemented)
- LLM router, embedding generation, entity resolution
- Tables exist but no actual AI code

#### 2. Rich Event Sources (70% unimplemented)
- Browser extension, audio/video capture, email
- Advanced terminal capture, accessibility events
- Full Hyprland IPC implementation

#### 3. Advanced Processing (90% unimplemented)
- Living documents, CRDT integration
- Semantic search, knowledge graph building
- Activity segmentation, context synthesis
- Git-annex blob storage ✅ (sinex-annex crate fully implemented)

#### 4. User Interfaces (90% unimplemented)
- Neovim plugin, web UI, advanced CLI
- Query language, visualization

#### 5. System Operations (80% unimplemented)
- Monitoring, advanced backup, multi-device sync
- Security hardening, CI/CD

### Ready for Implementation (Clear Specs Exist)
1. Hyprland IPC rich context extraction
2. Browser extension with native messaging
3. Basic LLM integration with Ollama
4. Embedding generation with local models
5. pgBackRest backup configuration
6. Basic Neovim plugin for PKM
7. Audio capture via PipeWire

### Requires More Design Work
1. Living Document full implementation
2. Multi-device sync architecture
3. Advanced agent coordination
4. Semantic Desktop Stream
5. Privacy-preserving federation