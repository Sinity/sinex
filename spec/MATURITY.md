# Sinex Specification Maturity Model

## Overview

The Sinex project uses a 5-level maturity model to classify the development state of features and components. This system helps contributors understand what can be implemented immediately versus what requires additional design work or dependencies.

## Maturity Levels

### L0 - Vision
**Aspirational goals with no technical details**

- High-level concepts and long-term objectives
- User stories and value propositions
- No specific implementation requirements
- May not have clear technical approach yet

**Examples:**
- Multi-device synchronization
- Privacy-preserving federation
- Advanced agent coordination

**Typical Duration:** Months to years of research

### L1 - Concept
**Architecture and data flow defined**

- Overall system design established
- Major components and their interactions identified
- Data flow and processing pipeline defined
- Missing specific technical implementation details

**Examples:**
- Living Documents system design
- Semantic Desktop Stream architecture
- Advanced activity segmentation

**Typical Duration:** Weeks of design refinement

### L2 - Technical Specification
**APIs, schemas, and algorithms specified**

- Detailed technical specifications exist
- Database schemas, API contracts, and data structures defined
- Algorithms and processing logic specified
- Missing some implementation details or dependency resolution

**Examples:**
- LLM integration framework
- Embedding generation pipeline
- Browser extension architecture

**Typical Duration:** Days to weeks of implementation

### L3 - Ready for Implementation
**All dependencies met, clear acceptance criteria**

- Complete technical specification
- All prerequisite components available
- Clear implementation checklist and acceptance criteria
- Can be implemented without additional design work

**Examples:**
- Hyprland IPC rich context extraction
- pgBackRest backup setup
- Audio capture via PipeWire

**Typical Duration:** Days of focused implementation

### L4 - Implemented
**Built with measurable coverage**

- Feature is implemented and tested
- Integration tests pass
- Documentation exists
- Coverage percentage tracked

**Examples:**
- Event storage infrastructure (90% coverage)
- Basic event sources (filesystem, terminal, clipboard)
- Git-annex blob storage integration

**Tracking Metrics:**
- Test coverage percentage
- Documentation completeness
- Performance benchmarks

## Maturity Progression

### Advancement Criteria

**L0 → L1:**
- System architecture diagrams created
- Component responsibilities defined
- Data flow documented
- Major technical challenges identified

**L1 → L2:**
- Database schemas designed
- API specifications written
- Data structures and interfaces defined
- Processing algorithms specified

**L2 → L3:**
- All dependencies available or implemented
- Implementation checklist created
- Acceptance criteria defined
- No blocking technical decisions remain

**L3 → L4:**
- Code implementation complete
- Tests written and passing
- Documentation updated
- Performance requirements met

### Regression Conditions

Features may regress in maturity level if:
- Dependencies become unavailable
- Technical blockers discovered during implementation
- Requirements change significantly
- Architecture decisions invalidated

## TIM Status Template

Each Technical Implementation Memo (TIM) should include a status section:

```markdown
## Status Dashboard
**Maturity Level**: L2 - Technical Specification
**Implementation**: 30% (basic events only)
**Dependencies**: PostgreSQL, EventSource trait
**Blocks**: Rich context features, AI analysis
**Blocked By**: None
**To Reach L3**: Define message protocol, security model
```

## Implementation Priority

### High Priority Advancement
Focus on moving L2→L3 features that:
- Have no external dependencies
- Build on existing infrastructure
- Enable other features (high blocking coefficient)

### Balanced Portfolio
Maintain features across all maturity levels:
- **L0-L1**: 20% (research and design)
- **L2**: 30% (specification work)
- **L3**: 30% (ready to implement)
- **L4**: 20% (maintaining implemented features)

## Contributor Guidance

### For New Contributors
- Start with L3 features that match your skills
- Review L4 features for enhancement opportunities
- Avoid L0-L1 features unless experienced with the domain

### For System Architects
- Focus on advancing L0→L1 and L1→L2
- Identify dependency chains that block advancement
- Design MVP subsets of complex L1 features

### For Implementation Teams
- Prioritize L3→L4 advancement
- Report blocking issues that cause L3→L2 regression
- Suggest L2→L3 advancement paths based on implementation experience

## Metrics and Tracking

### Project Health Indicators
- **Implementation Rate**: L3→L4 features per sprint
- **Design Velocity**: L1→L2 advancement rate  
- **Vision Clarity**: L0→L1 progression
- **Blocking Coefficient**: Features enabled per L4 completion

### Quality Gates
- L2 features must pass architecture review
- L3 features must have complete acceptance criteria
- L4 features must maintain >80% test coverage
- All levels must have clear dependency documentation

## Review and Updates

This maturity model is reviewed quarterly and updated based on:
- Implementation experience and lessons learned
- Contributor feedback on classification accuracy
- Changes in project scope or technical landscape
- Discovery of new dependencies or blockers