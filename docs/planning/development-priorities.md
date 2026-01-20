# Sinex Development Priorities

This document outlines the near-term and long-term development priorities for the Sinex project.

## Near-Term Priorities (Next 6 Months)

### 1. Expand Automaton Ecosystem
Build specialized processors for different data domains:
- **Health Metrics Aggregator**: System resource usage patterns
- **Command Canonicalizer**: Normalize terminal commands
- **Content Processor**: Extract text from documents
- **Entity Resolver**: Link mentions across events

### 2. Enhance LLM Integration
Connect automata with language models:
- **Local Models**: Ollama integration for privacy
- **Prompt Registry**: Version-controlled prompts
- **Context Windows**: Efficient context management
- **Result Caching**: Avoid redundant processing

### 3. Add Critical Event Sources
Expand coverage to capture more digital activity:
- **Browser Extension**: Web activity tracking
- **Audio Capture**: PipeWire integration
- **Email Integration**: IMAP/Exchange support
- **Screen Capture**: Wayland screenshot support

### 4. Advanced Query Interface
Rich tools for data exploration:
- **Query DSL**: Domain-specific query language
- **Web Dashboard**: Visual exploration interface
- **Time Navigation**: Browse by time period
- **Pattern Search**: Find behavioral patterns

## Long-Term Vision (1-2 Years)

### Sentient Archive Capabilities

The mature system will support:
- **AI-Powered Analysis**: Automatic insight generation
- **Semantic Search**: Find by meaning, not keywords
- **Knowledge Graph**: Automatically constructed relationships
- **Multi-Device Sync**: Unified view across machines

### Architectural Evolution

The node architecture enables:
- **Independent Evolution**: Each component can advance separately
- **System Coherence**: Unified through message bus
- **Horizontal Scaling**: Add nodes as needed
- **Feature Composition**: Combine simple parts for complex behavior

## Development Philosophy

### Incremental Progress
- Each feature should provide immediate value
- Build on solid foundations
- Maintain system stability

### User-Driven Priorities
- Address real pain points
- Gather feedback continuously
- Adapt roadmap based on usage

### Technical Excellence
- Comprehensive testing
- Performance optimization
- Security by design
- Documentation quality

## Success Metrics

### Coverage
- Event source coverage >80%
- Query response time <100ms
- Zero data loss guarantee

### Utility
- Daily active usage
- Meaningful insights generated
- Reduced cognitive load

### Sustainability
- Maintainable codebase
- Active contributor community
- Clear upgrade paths

## Next Steps

1. Complete in-progress automata
2. Design browser extension architecture
3. Implement query DSL parser
4. Create web dashboard prototype

The path forward is clear: expand coverage, enhance intelligence, and improve accessibility while maintaining the core principles of user sovereignty and system integrity.