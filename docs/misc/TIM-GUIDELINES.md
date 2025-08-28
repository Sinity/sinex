# Technical Implementation Module (TIM) Guidelines

This document defines the structure and standards for Technical Implementation Modules in the Sinex project.

## TIM Structure

### Status Dashboard (Required for Feature TIMs)

Every feature TIM must include a status dashboard at the top:

```markdown
## Status Dashboard
**Maturity Level**: L2/L3/L4 - Ready/Implemented
**Implementation**: X% (Verified against codebase)
**Dependencies**: Required components
**Blocks**: Features that depend on this TIM
```

## Maturity Levels

### L1 - Concept
- Initial idea or proposal
- May lack technical details
- Not ready for implementation

### L2 - Ready
- Complete specification
- Clear implementation plan
- All technical details resolved
- Ready to build

### L3 - Partial
- Core functionality implemented
- Missing some features
- May lack tests or documentation
- 25-75% complete

### L4 - Complete
- Feature fully working
- Comprehensive tests
- Documentation complete
- 75-100% implementation

## Implementation Percentages

Based on codebase verification:

- **0-25%**: Design complete, minimal implementation
- **25-50%**: Core infrastructure exists, missing functionality
- **50-75%**: Major components implemented, missing integration
- **75-90%**: Substantially complete, minor features missing
- **90-100%**: Production-ready with comprehensive testing

## TIM Categories

### Feature TIMs
Implementable features organized by status:

- **`implemented/`** - Working features (70%+ complete)
- **`ready/`** - Fully designed, ready to build
- **`planned/`** - Future features needing design

### Process TIMs
Documentation-only modules:

- **`docs/processes/`** - Development practices
- **`docs/operations/`** - Operational procedures
- **`docs/security/`** - Security documentation

## Required Sections

### 1. Status Dashboard
As shown above, required for all feature TIMs.

### 2. Overview
Brief description of what the feature does and why it's needed.

### 3. Technical Specification
Detailed technical design including:
- Architecture
- Data structures
- Algorithms
- API definitions

### 4. Implementation Plan
Step-by-step guide for building the feature:
- Prerequisites
- Implementation phases
- Testing approach
- Integration points

### 5. Dependencies
- Required system components
- External libraries
- Other TIMs that must be implemented first

### 6. Testing Strategy
- Unit test approach
- Integration test requirements
- Performance benchmarks
- Acceptance criteria

## Writing Guidelines

1. **Be Specific**: Include concrete technical details, not vague descriptions
2. **Show Examples**: Include code snippets, SQL schemas, API examples
3. **Track Reality**: Update implementation percentages based on actual code
4. **Link Dependencies**: Reference other TIMs and components by name
5. **Maintain Status**: Keep the status dashboard current with implementation

## Example TIM Header

```markdown
# TIM-FeatureName: Brief Description

## Status Dashboard
**Maturity Level**: L2 - Ready for Implementation
**Implementation**: 0% (Design complete, implementation not started)
**Dependencies**: PostgreSQL, NATS JetStream, sinex-satellite-sdk
**Blocks**: Advanced analytics features, multi-device sync

## Overview
This feature enables... [2-3 sentences about what and why]

## Technical Specification
[Detailed technical design...]
```

## Verification Process

When updating TIM status:

1. **Check Code**: Verify actual implementation in codebase
2. **Test Coverage**: Confirm tests exist and pass
3. **Documentation**: Ensure inline docs and README updates
4. **Integration**: Verify feature works with rest of system
5. **Update Status**: Adjust percentage and maturity level

## Migration Path

As features are implemented:

1. **L1 → L2**: Complete technical design
2. **L2 → L3**: Implement core functionality
3. **L3 → L4**: Add tests, docs, polish
4. **L4 → Archived**: Extract to code documentation

TIMs are living documents that track feature evolution from concept to completion.
