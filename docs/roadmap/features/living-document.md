# Living Document System

## Overview
The Living Document (LD) serves as an externalized, persistent working memory - a dynamic artifact that facilitates frictionless capture of stream-of-consciousness thoughts, iterative development of plans and drafts, and AI-augmented structuring of information.

## MVP Specification
- Yjs-based CRDT document storage (consistent with PKM system)
- Basic text capture and persistence
- Event-sourced change tracking
- Markdown snapshot generation
- Simple Neovim plugin for editing
- Basic artifact extraction (tasks, notes)

## Enhanced Features
- Advanced AI-powered structuring and segmentation
- Multi-agent processing pipeline
- Knowledge graph integration with entity linking
- Interactive outliner UI with node navigation
- Canvas/graph view for spatial organization
- Voice input via Whisper ASR integration
- Real-time collaborative editing support
- Automated artifact extraction and linking

## Technical Architecture

### Data Model
- Root artifact in `core.artifacts` with type `living_document_main`
- Yjs document contains entire structured text
- Changes stored as Yjs update blobs in `core.living_document_yjs_deltas`
- Periodic Markdown snapshots in `core.artifact_contents`

### Agent Pipeline
1. **Input Ingestion Agent**
   - Handles text, voice, and pasted content
   - Segments into thought units
   - Identifies commands and directives

2. **Living Document Manager Agent**
   - Applies changes to canonical Yjs document
   - Persists update blobs
   - Generates change events
   - Triggers snapshot generation

3. **Artifact Extraction Agent**
   - Monitors changes
   - Identifies tasks, claims, hypotheses
   - Creates linked artifacts
   - Maintains source references

4. **Knowledge Graph Integration Agent**
   - Performs NER on content
   - Links to existing entities
   - Creates entity relations
   - Identifies semantic relationships

### Event Flow
- All changes emit `livingdoc.yjs_update_applied` events
- Events contain delta IDs, actor info, and operation summaries
- Agents subscribe to events for processing
- Change tracking enables time-travel and audit

## Implementation Roadmap

### Phase 1: Foundation
- [ ] Core Yjs document structure
- [ ] Basic persistence layer
- [ ] Simple text input/output
- [ ] Markdown rendering
- [ ] Change event system

### Phase 2: Intelligence
- [ ] LLM-powered structuring
- [ ] Task extraction
- [ ] Entity recognition
- [ ] Basic Neovim plugin
- [ ] Snapshot optimization

### Phase 3: Advanced Features
- [ ] Multi-agent pipeline
- [ ] Graph visualization
- [ ] Voice input integration
- [ ] Collaborative editing
- [ ] Advanced UI plugins

## Technical Challenges

### Yjs Integration
- Markdown to Yjs conversion preserving structure
- Stable node identifiers within document
- Efficient delta storage and retrieval
- Conflict resolution for concurrent edits

### Performance
- Large document handling (100MB+ Yjs docs)
- Real-time sync with multiple agents
- Efficient entity extraction at scale
- Snapshot generation optimization

## Use Cases
- Stream-of-consciousness capture
- Meeting notes with automatic task extraction
- Research documentation with entity linking
- Project planning with visual organization
- Daily journaling with knowledge connections

## Related Components
- TIM-PKMContentCRDT_Yjs: Shared Yjs infrastructure
- TIM-ASR_WhisperCpp: Voice input processing
- TIM-EntityResolutionTechniques: Entity linking
- Core artifact and event systems