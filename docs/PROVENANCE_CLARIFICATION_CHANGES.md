# Provenance Clarification Changes

## Summary

This document tracks the changes made to clarify the provenance requirements in the Sinex codebase, addressing the misconception that events can exist without provenance.

## The Core Issue

The codebase documentation and APIs suggested that "raw events" could have no provenance (`provenance: None`). This violates the canonical architecture which states that ALL events MUST have provenance - either external (Source Material) or internal (parent events).

## Changes Made

### 1. Documentation Files Created

#### `/docs/PROVENANCE_REQUIREMENTS.md`
- Comprehensive explanation of the provenance model
- Clarifies that (NULL, NULL) provenance is NEVER valid
- Explains the two types of provenance (XOR constraint)
- Lists common misconceptions and corrections

#### `/docs/PROVENANCE_MIGRATION_GUIDE.md`
- Step-by-step guide for migrating existing code
- Code patterns for first-order vs synthesized events
- Examples of how to fix common violations
- Testing and verification strategies

#### `/docs/PROVENANCE_CLARIFICATION_CHANGES.md`
- This file - tracking all changes made

### 2. Code Documentation Updates

#### `crate/lib/sinex-core/src/db/models/event.rs`

**Struct Documentation (lines 16-28):**
- Changed from "Raw Event: provenance is None" 
- To explicit XOR constraint documentation
- Added "CRITICAL" warning about provenance requirements

**Constructor Documentation (lines 124-140):**
- Added WARNING that `new()` creates invalid events
- Provided correct examples for both provenance types
- Shows proper use of `.with_provenance()`

**Method Updates (lines 208-233):**
- Deprecated `is_raw_event()` method (was checking for None)
- Added `is_first_order_event()` - checks for Material provenance
- Added `is_synthesized_event()` - checks for Events provenance
- Added `violates_provenance_requirements()` - detects invalid state

**Factory Methods Added (lines 124-153):**
- Added `from_material()` - creates event with external provenance
- Added `from_events()` - creates event with internal provenance
- These methods ensure valid provenance from creation

#### `crate/lib/sinex-core/src/types/events/typed_event.rs`

**Similar Updates:**
- Added WARNING to `new()` constructor
- Added `from_material()` factory method
- Added `from_events()` factory method
- Updated documentation to clarify requirements

### 3. Cancer Analysis Update

#### `/docs/cancer_analysis.md`
- Section 2 updated to identify provenance violations as CRITICAL
- Added corrected understanding in "Misconceptions" section
- Updated conclusion to highlight this architectural violation
- Changed Invariant #3 status from ✅ to ❌

## Key Architectural Principle

From the canonical architecture:
> "Source Material is Ground Truth: The raw bytes captured from the external world are the immutable evidence. Events are interpretations of that evidence."

This means:
1. ALL events are interpretations
2. No event exists independently 
3. Every event must trace back to either Source Material or other events

## Impact on Existing Code

### Code That Needs Migration
- Any satellite creating events without provenance
- Test code using `RawEvent::new()` without `.with_provenance()`
- System monitoring code creating "standalone" events

### Migration Strategy
1. Complete sensd implementation for Source Material capture
2. Update satellites to reference Source Material
3. Update automata to track parent events
4. Add validation to catch violations early

## Validation and Enforcement

### Compile-Time
- New factory methods guide correct usage
- Deprecation warnings on misleading methods

### Runtime
- `violates_provenance_requirements()` method for validation
- Database CHECK constraint enforces XOR

### Testing
- Add tests to verify no (NULL, NULL) events exist
- CI pipeline should check for violations

## Next Steps

1. **Immediate**: Use new factory methods in new code
2. **Short-term**: Migrate existing violating code
3. **Medium-term**: Complete sensd for proper Source Material capture
4. **Long-term**: Consider making provenance non-Option in the type system

## Lessons Learned

### Why This Happened
1. Misleading terminology ("raw event" suggested no provenance needed)
2. Default constructors made it easy to create invalid events
3. Documentation didn't emphasize the absolute nature of the constraint

### Prevention
1. Clear, unambiguous documentation
2. API design that makes invalid states harder to represent
3. Factory methods that guide correct usage
4. Validation at multiple levels (compile, runtime, database)

## Conclusion

These changes clarify that the Sinex provenance model is absolute: every event MUST have provenance. The updates to documentation, APIs, and validation ensure this requirement is clear and enforceable going forward.