# Documentation Migration Guide

This guide captures the complete process and lessons learned from migrating Sinex's `/spec/` documentation to rustdoc and appropriate code locations.

## Overview

The goal is to transform standalone documentation files into integrated rustdoc comments and code-level documentation. This ensures documentation lives close to the code it describes, reducing drift and improving maintainability.

## Migration Principles

### 1. Documentation Should Live Near Code
- **Implemented features**: Extract details into rustdoc comments at relevant code locations
- **Unimplemented but important**: Move to `/docs/roadmap/` with clear design documentation
- **Outdated/unclear**: Move to `/docs/_todo/leftover/` for future evaluation
- **High-level overviews**: Keep in `/docs/architecture/` or similar

### 2. Focus on Extracting Value
- Don't just move files - extract meaningful information
- Remove redundant content after extraction
- Update cross-references to point to new locations
- Preserve important design decisions even if not implemented

### 3. Process, Don't Just Move
The key lesson: **Process documentation properly rather than just moving files around**. This means:
- Read and understand each document
- Extract relevant technical details
- Place information in appropriate locations
- Remove what's been extracted from source files

## Migration Process

### Step 1: Assess Current State
```bash
# Check what needs migration
ls docs/_todo/
ls docs/_todo/archive/
ls docs/_todo/planned/
ls docs/_todo/ready/
```

### Step 2: Categorize Documentation

#### Implemented Features
Look for features that are actually implemented in the codebase:
- Check if code exists for the feature
- Verify implementation matches documentation
- Extract technical details to rustdoc

Example indicators:
- GitOps schema management → Actually implemented in `/schemas/`
- Clipboard monitoring → Implemented in `sinex-desktop-satellite`
- StatefulStreamProcessor → Core architecture pattern

#### Unimplemented Features
Features that are designed but not built:
- Clear technical specifications
- Important for future development
- Should be preserved in `/docs/roadmap/`

Examples:
- Tagging system (important but not implemented)
- Event relations (designed but not built)
- Advanced event sources (browser, audio)

#### Outdated/Superseded
Documentation that no longer reflects current architecture:
- Old implementation approaches
- Superseded designs
- Outdated technical decisions

### Step 3: Extract and Place Content

#### For Implemented Features

1. **Find the relevant code location**
   ```bash
   # Use grep/ripgrep to find implementations
   rg "feature_name" --type rust
   fd "feature_name" crate/
   ```

2. **Add rustdoc comments**
   ```rust
   //! # Module-level documentation
   //! 
   //! This module implements X feature as described in ADR-NNN.
   //! 
   //! ## Technical Details
   //! [Extract relevant details from documentation]
   //! 
   //! ## Implementation Notes
   //! [Current implementation specifics]
   ```

3. **Add inline documentation**
   ```rust
   /// Detailed function documentation
   /// 
   /// ## Algorithm (from TIM-XXX)
   /// [Extract algorithm description]
   pub fn important_function() { }
   ```

4. **Create supplementary .md files if needed**
   - For complex implementations, create `feature_name.md` next to the code
   - Link from rustdoc: `//! See [detailed docs](./feature_name.md)`

#### For Database Schema

1. **Add SQL comments**
   ```sql
   -- ## Architectural Decision: ULID Primary Key (ADR-009)
   -- [Extract decision rationale]
   
   CREATE TABLE core.events (
       -- Time-ordered ULID for efficient indexing
       id ULID PRIMARY KEY DEFAULT gen_ulid(),
   ```

2. **Document in models**
   ```rust
   /// Event model representing immutable system events
   /// 
   /// ## Design Decisions (from ADR-001)
   /// - ULID primary keys for time ordering
   /// - JSONB payload for flexibility
   #[derive(Debug, Clone)]
   pub struct Event { }
   ```

#### For Unimplemented Features

1. **Create roadmap documentation**
   ```bash
   # Create clear structure
   mkdir -p docs/roadmap/features
   mkdir -p docs/roadmap/architecture
   ```

2. **Document the design**
   ```markdown
   # Feature: Tagging System
   
   **Status**: Designed, not implemented
   **Priority**: High
   **Blocks**: Advanced categorization features
   
   ## Design
   [Extract design from TIM/ADR]
   
   ## Implementation Plan
   [Extract or create implementation steps]
   ```

### Step 4: Clean Up Source Files

After extracting content:

1. **For fully extracted files**: Delete them
2. **For partially extracted files**: 
   - Remove extracted sections
   - Add note about what was extracted
   - Move to `_todo/leftover/` if still has value

Example cleanup:
```markdown
# Original TIM File

[EXTRACTED to crate/sinex-automaton/src/lib.rs]
~~Detailed implementation of automaton architecture~~

## Remaining Design Considerations
[Content not yet extracted]
```

### Step 5: Update References

1. **Update cross-document links**
   ```markdown
   <!-- Old -->
   See [ADR-001](../adr/ADR-001.md)
   
   <!-- New -->
   See ULID implementation in `crate/sinex-db/src/models.rs`
   ```

2. **Update CLAUDE.md and README.md**
   - Remove references to moved documentation
   - Add references to new locations
   - Update development workflows

## Common Patterns and Examples

### Pattern: Technical Implementation Module (TIM)

**Before**: `spec/implemented/TIM-EventIngestionProcessing.md`

**After**:
1. Core concepts → `crate/sinex-satellite-sdk/src/lib.rs` rustdoc
2. Implementation details → `crate/sinex-ingestd/src/main.rs` 
3. Architecture overview → `docs/architecture/satellite-implementation.md`
4. Deleted original TIM file

### Pattern: Architecture Decision Record (ADR)

**Before**: `spec/adr/ADR-009-ULID-Primary-Key.md`

**After**:
1. Decision rationale → `migrations/00000000000002_create_core_tables.sql` comments
2. Implementation → `crate/sinex-db/src/models.rs` rustdoc
3. Deleted original ADR file

### Pattern: Unimplemented Feature

**Before**: `spec/planned/TIM-TaggingSystem.md`

**After**:
1. Moved to `docs/roadmap/features/tagging-system.md`
2. Added implementation status and priority
3. Preserved all design details

## Validation Checklist

After migration, verify:

- [ ] All implemented features have rustdoc documentation
- [ ] Database schemas have comprehensive SQL comments
- [ ] Important unimplemented features are in `/docs/roadmap/`
- [ ] No duplicate information across documentation
- [ ] All cross-references are updated
- [ ] `docs/_todo/` is empty or only contains true unknowns
- [ ] Code compiles with `cargo doc --open`
- [ ] No documentation references deleted files

## Common Mistakes to Avoid

1. **Don't just move files** - Extract and process content
2. **Don't claim features aren't implemented without checking** - GitOps was implemented!
3. **Don't lose important designs** - Unimplemented doesn't mean unimportant
4. **Don't create redundant docs** - If it's in rustdoc, don't duplicate in .md
5. **Don't forget SQL documentation** - Migrations need context too
6. **Don't mix concerns in commits** - Separate documentation from code changes

## Tools and Commands

```bash
# Find where features are implemented
rg "FeatureName" --type rust
rg "feature_name" crate/ -A 5 -B 5

# Check for existing documentation
fd "feature" docs/
rg "FeatureName" docs/ --type md

# Verify rustdoc builds
cargo doc --no-deps --open

# Check for broken links
fd -e md | xargs -I {} rg "\[.*\]\(.*\)" {} -o | sort | uniq

# Find TODOs in documentation
rg "TODO|FIXME|XXX" docs/ --type md
```

## Next Steps for Current Migration

Based on the current state in `docs/_todo/`:

1. **High Priority - Core Documentation**:
   - `SADI.md` - System Architecture Document Index
   - `STAD.md` - System Technical Architecture Document  
   - `VISION.md` - Extract philosophy to main README, implementation to code

2. **Medium Priority - Technical Specs**:
   - Archive ADRs - Extract to relevant code locations
   - Archive TIMs - Process based on implementation status
   - Operations docs - Move to NixOS module documentation

3. **Low Priority - Analysis/Planning**:
   - `misc-including-high-level-overviews-and-plans/` - Review for insights
   - Diagrams - Keep useful ones in `/docs/architecture/diagrams/`

## Summary

The key to successful documentation migration is to **process, not just move**. Each document should be understood, its valuable content extracted to appropriate locations, and only then should the original be removed. This ensures documentation stays close to code while preserving important design decisions and future plans.