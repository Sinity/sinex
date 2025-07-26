# Future Architectural Directions

This document outlines the strategic vision and planned evolution of the Sinex architecture, extracted from the comprehensive understanding of the system.

## Near-Term Evolution (3-6 months)

### 1. Unify Processor Model
**Status**: In Progress  
**Priority**: High

Migrate all automata to use the `StatefulStreamProcessor` interface instead of the legacy `HotlogAutomaton` trait. This will achieve true "deep symmetry" between all satellite types.

**Benefits**:
- Consistent behavior across all components
- Simplified testing and maintenance
- Unified checkpoint management

### 2. PKM as Events
**Status**: Designed  
**Priority**: High

Transform Personal Knowledge Management documents into source material with event decomposition. Documents will be treated as any other data source, with events generated for sections, links, and metadata changes.

**Implementation**:
- Document ingestion creates source material records
- Markdown parsing generates structured events
- Cross-references become entity relations
- Version history preserved through event chain

### 3. Browser Extension
**Status**: Not Started  
**Priority**: Critical

Capture the ~40% of digital activity that occurs in web browsers. This is a major gap in the current system.

**Features**:
- Page visit tracking with content snapshots
- Form interaction capture
- Scroll and click behavior
- Tab relationship mapping
- Integration with existing event pipeline

### 4. SQL-as-Automaton MVP
**Status**: Conceptual  
**Priority**: Medium

Implement declarative processing through SQL queries that act as automata, moving toward the vision of logic-as-data.

**Example**:
```sql
-- Automaton defined purely in SQL
CREATE MATERIALIZED VIEW command_frequency AS
SELECT 
    date_trunc('hour', ts_orig) as hour,
    payload->>'command' as command,
    count(*) as execution_count
FROM core.events
WHERE event_type = 'terminal.command.executed'
GROUP BY 1, 2;
```

## Medium-Term Vision (6-12 months)

### 1. Prompt-as-Automaton
**Status**: Research Phase  
**Priority**: High

Enable LLM-powered synthesis through natural language automaton definitions.

**Architecture**:
- Prompt templates define processing logic
- LLM interprets events and generates synthesis
- Human-in-the-loop validation
- Cost optimization through batching

### 2. Active Inference Loop
**Status**: Designed  
**Priority**: Medium

Close the observation-action loop to enable the system to act on its understanding.

**Components**:
- Actuator satellites for system actions
- Instructional event processing
- Safety constraints and rollback
- User approval workflows

### 3. Vector Search Integration
**Status**: Enabled but Unused  
**Priority**: Medium

Leverage pgvector for semantic queries over the personal archive.

**Use Cases**:
- "Find all discussions about distributed systems"
- "Show me code similar to this pattern"
- Concept clustering and visualization
- Semantic timeline navigation

### 4. Advanced Analytics
**Status**: Conceptual  
**Priority**: Medium

Pattern detection and insight generation from the event stream.

**Features**:
- Behavioral pattern recognition
- Anomaly detection
- Productivity analytics
- Correlation discovery
- Predictive suggestions

## Long-Term Ambition (1-2 years)

### 1. Dissolve User/Developer Boundary
**Status**: Vision  
**Priority**: Low (but foundational)

Enable system extension through natural use rather than code development.

**Approach**:
- Visual automaton builders
- Natural language processing definitions
- Example-based learning
- Automatic code generation from patterns

### 2. Multi-Device Sync
**Status**: Architecture Supports  
**Priority**: Medium

Distributed personal infrastructure across devices.

**Challenges**:
- Conflict resolution strategies
- Selective sync policies
- Bandwidth optimization
- Privacy preservation

### 3. Full Declarative Core
**Status**: Long-term Vision  
**Priority**: Low

Minimize imperative code to only inherently complex operations.

**Goal State**:
- 90% of processing defined declaratively
- SQL and configuration drive behavior
- Code only for I/O and complex algorithms
- Self-modifying system capabilities

### 4. True Exocortex
**Status**: Ultimate Vision  
**Priority**: Aspirational

Augmented cognition through comprehensive understanding and intelligent assistance.

**Capabilities**:
- Proactive information surfacing
- Context-aware suggestions
- Memory augmentation
- Cognitive load balancing
- Collaborative intelligence

## Key Innovations to Preserve

These architectural innovations should be maintained and enhanced:

1. **Stage-as-You-Go**: Real-time provenance for streaming data
2. **Unified Stream Processing**: All components share one interface
3. **Event Symmetry**: Elegant active inference without special commands
4. **Archive and Replace**: Evolution without destruction
5. **Deep Oneness**: Philosophical coherence throughout

## Implementation Priorities

### Critical Path
1. Browser extension (fills major gap)
2. Unified processor model (technical debt)
3. PKM integration (user value)
4. SQL-as-Automaton (architectural evolution)

### Quick Wins
1. Enable vector search (already available)
2. Basic analytics dashboards
3. Improved query interface
4. Documentation improvements

### Research Projects
1. Prompt-as-Automaton feasibility
2. Active inference safety model
3. Multi-device sync protocols
4. Declarative processing languages

## Success Metrics

- **Coverage**: >90% of digital activity captured
- **Performance**: Sustained 10k events/second
- **Latency**: <100ms query response
- **Storage**: <1GB/day with compression
- **Usability**: Extension through natural use
- **Intelligence**: Proactive insights daily

## Architectural Principles to Maintain

1. **Local-First**: User owns and controls all data
2. **Privacy by Design**: No external dependencies for core function
3. **Composability**: Small, focused components
4. **Observability**: Complete system transparency
5. **Evolvability**: Change without disruption

## Conclusion

The Sinex architecture has strong foundations that support ambitious future directions. The path forward balances practical user needs (browser capture, better queries) with architectural evolution (declarative core, active inference). By maintaining philosophical coherence while pragmatically addressing gaps, Sinex can evolve from a sophisticated capture system to a true cognitive prosthesis.