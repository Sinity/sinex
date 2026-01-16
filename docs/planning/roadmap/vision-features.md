# Vision Features Roadmap

This document outlines the aspirational features from the Sinex vision that are not yet implemented. These represent the long-term goals of creating a true "sentient archive" and cognitive augmentation system.

## Core Capabilities (Not Yet Implemented)

### 1. Dynamic Ideation & The Living Document
**Status**: Designed, not implemented  
**Priority**: High

A persistent, externalized working memory for fluid thought capture:
- **Frictionless Multi-Modal Input**: Global hotkeys, voice dictation, instant capture
- **Seamless Structure Emergence**: Natural language with implicit structure inference
- **Agentic Partnership**: AI assistants suggesting refactoring, linking, clarifications

### 2. Integrated Personal Knowledge Management (PKM)
**Status**: Partially designed  
**Priority**: High

Deep unification of curated knowledge with live event streams:
- **Unified Storage**: Notes, web archives, media as eventified artifacts
- **CRDT-based Conflict Resolution**: Using Yjs for distributed editing
- **Rich Web Archives**: Full WARC/WACZ capture with content extraction
- **Universal Tagging**: Cross-domain categorization system

### 3. Emergent Knowledge & Structuring
**Status**: Framework exists, intelligence pending  
**Priority**: Medium

Transform raw data into structured, actionable knowledge:
- **Semantic Tagging**: Automatic categorization and labeling
- **Entity Resolution**: Cross-event entity linking
- **Vector Embeddings**: Semantic similarity search
- **Real-Time Context Synthesis**: Live desktop state modeling

### 4. Intelligent Partnership & Agentic Ecosystem
**Status**: Architecture ready, agents not built  
**Priority**: Medium

Active AI partnership through specialized agents:
- **Task Breakdown Agents**: Decompose complex goals
- **Insight Generation**: Pattern detection and anomaly alerts
- **Contextual Assistance**: Proactive, relevant suggestions
- **Knowledge Synthesis**: Automated summarization and connection

### 5. Human-Centric Design Features
**Status**: Conceptual  
**Priority**: Medium

Support for diverse cognitive styles and neurodivergent needs:

#### ADHD Support
- **Augment Working Memory**: Frictionless capture as external buffer
- **Enhance Object/Task Permanence**: Easy resurfacing of "out of sight" items
- **Lower Activation Energy**: Minimal capture effort, agent-assisted task breakdown
- **Temporal Scaffolding**: Objective timestamped records for time perception
- **Manage Distraction**: Log and analyze distraction patterns
- **Leverage Hyperfocus**: Acknowledge and analyze deep work periods
- **Build Emotional Self-Awareness**: Correlate logged states with activities

#### Autism Spectrum Support
- **User-Defined Structure**: Transparent data models, declarative configuration
- **Special Interest Support**: Deep information aggregation and semantic linking
- **Information Flow Management**: Customizable UI density, controlled notifications
- **Explicit Semantics**: Clear, unambiguous information with raw data integrity
- **Leverage Systemizing**: Hackable nature engages systemizing strengths

#### Executive Function Scaffolding
- **Planning & Organization**: Living Document and structured task artifacts
- **Task Initiation**: Agentic reminders and contextual cues
- **Working Memory**: Universal capture as infallible external memory
- **Time Management**: Precise timestamps and time allocation visualization
- **Self-Monitoring**: Queryable task statuses and agent-generated summaries
- **Cognitive Flexibility**: Easy context retrieval and knowledge graph exploration

## User Experience Enhancements

### Query & Exploration
- **Natural Language Queries**: "What was I working on last Tuesday?"
- **Temporal Navigation**: Time-based browsing of personal history
- **Causal Chain Visualization**: See how ideas and actions connect
- **Pattern Discovery**: Automated insight generation

### Privacy & Control
- **Selective Capture**: Fine-grained control over what to record
- **Retention Policies**: Automatic data expiry rules
- **Pseudonymization**: Replace identifiers for sharing
- **Export Controls**: Limit what can be extracted

### Multi-Device & Collaboration
- **Device Synchronization**: Unified capture across computers
- **Selective Sharing**: Share specific contexts or insights
- **Collaborative Spaces**: Joint knowledge building
- **Mobile Integration**: Capture from phones and tablets

## Technical Enablers Needed

### AI/LLM Integration
- Local model support (Ollama integration)
- Prompt registry and management
- Fine-tuning on personal data
- Privacy-preserving inference

### Advanced Storage
- Vector database integration (pgvector)
- Graph traversal optimization
- Incremental indexing
- Compression strategies

### Real-Time Processing
- Stream processing enhancements
- Live pattern matching
- Predictive caching
- Latency optimization

## Implementation Priorities

### Phase 1: Foundation (Next 6 months)
1. Living Document MVP
2. Basic PKM integration
3. Enhanced query interface
4. Simple automation agents

### Phase 2: Intelligence (6-12 months)
1. LLM integration framework
2. Entity resolution system
3. Pattern detection engine
4. Context synthesis

### Phase 3: Augmentation (12+ months)
1. Full agentic ecosystem
2. Multi-device sync
3. Advanced visualization
4. Cognitive style adaptations

## Success Metrics

- **Capture Coverage**: >80% of digital activity
- **Query Latency**: <100ms for complex queries
- **Insight Generation**: Daily actionable insights
- **User Satisfaction**: Measurable cognitive load reduction
- **Privacy Preservation**: Zero data leaks or unauthorized access

## Long-Term Commitments

### Security, Privacy & Data Sovereignty
- **User Sovereignty**: Absolute control and ownership, local-first by default
- **Robust Security**: Layered access controls, encryption at rest and in transit
- **User Consent**: Opt-in sensitive capture with clear indicators
- **Process Sandboxing**: Hardened agents using seccomp-bpf and AppArmor

### Permanence & Data Integrity
- **Comprehensive Backups**: pgBackRest for PostgreSQL, multi-remote git-annex strategy
- **Disaster Recovery**: Documented plan for full system restoration
- **Integrity Checks**: Regular validation of data consistency
- **Lifelong Archive**: Built for decades of continuous operation

### Graceful Evolution & Openness
- **Performance Scaling**: Database tuning, asynchronous processing, parallelization
- **Schema Evolution**: Versioned migrations with stable change tracking
- **Open Architecture**: Open standards, open-source components, declarative configuration
- **User Hackability**: Everything inspectable and modifiable

### Future Vision: Distributed & Federated
- **Multi-Device Coherence**: CRDTs for conflict-free sync across devices
- **Local-First Architecture**: Syncthing, git-annex remotes for distributed storage
- **Privacy-Preserving Federation**: Potential future trusted sharing mechanisms

## Why Build in the Age of AI?

The Sinex project addresses the developer's existential question directly:

1. **Irreproducible Personal Context**: Future AIs cannot retroactively create YOUR unique digital life history
2. **Unsimulable Situated Expertise**: The maker's knowledge from building such an intimate system
3. **Immediate Utility**: Each component solves current, felt problems now
4. **Ideal AI Substrate**: Rich, structured, historical data for truly personalized future AI

Building Sinex is an investment in personal data sovereignty, deep self-knowledge, and authoring your own cognitive future.

## Development Philosophy

### Friction-Driven Prioritization
The primary driver for feature selection is alleviating personally felt pain, inefficiency, or missing cognitive leverage. This ensures:
- Maximum immediate personal utility
- Alignment with real-world needs
- Sustainable development motivation

### Iterative Co-Evolution
The Exocortex evolves with its user through:
- User interaction feedback
- System customization
- Agent refinement
- Increasingly attuned cognitive partnership

## Conclusion

The full vision of Sinex as a "sentient archive" requires significant additional development beyond the current operational system. However, the architectural foundation - node constellation, event substrate, and processing pipeline - provides a solid base for implementing these advanced features. 

Each enhancement should be driven by real user needs and implemented incrementally to maintain system stability and user trust. The goal is not just to build a system, but to cultivate a lifelong practice of attentive self-authorship and continuous learning through a deeply personal cognitive infrastructure.
