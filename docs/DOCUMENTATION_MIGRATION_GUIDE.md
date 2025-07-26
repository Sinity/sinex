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
ls docs/_todo/archive/  # Skip - already processed files
ls docs/_todo/planned/  # Process - future features
ls docs/_todo/ready/    # Process - designs ready for implementation
```

### Step 2: Carefully Check Implementation Status

**CRITICAL**: "Ready" means the design is ready, NOT that it's implemented!

Before extracting anything:
1. **Search for actual implementation**:
   ```bash
   # Look for code that implements the feature
   rg "feature_name" crate/ --type rust
   # Check if database tables exist
   grep "CREATE TABLE.*table_name" migrations/
   # Look for existing modules
   fd "feature_name" crate/
   ```

2. **Verify implementation matches documentation**:
   - Does the code actually implement what's described?
   - Are the database tables already created?
   - Is this aspirational design or working code?

### Step 3: Categorize Based on Reality

#### Actually Implemented Features
**Only if you find working code**:
- Extract technical details to rustdoc at implementation site
- Extract SQL schemas to migration file comments
- Remove extracted content and add markers

Example verification:
```bash
# Example: Checking if embedding tables exist
grep -r "artifact_embeddings" migrations/
# If not found, it's NOT implemented!
```

#### Unimplemented but Valuable Designs
**For "ready" or "planned" features without implementation**:
- Extract complete design to `/docs/roadmap/features/`
- Include SQL schemas, algorithms, architecture
- Preserve implementation examples
- This includes most TIMs in ready/ and planned/

#### Outdated/Superseded
Documentation that no longer reflects current architecture:
- Old implementation approaches
- Superseded designs
- Outdated technical decisions

### Step 4: Extract and Mark Content

#### For Actually Implemented Features

1. **Find the implementation**:
   ```bash
   rg "feature_name" --type rust
   fd "module_name" crate/
   ```

2. **Extract to rustdoc**:
   ```rust
   //! # Feature Documentation
   //! 
   //! [Extracted technical details]
   ```

3. **Mark extraction in original**:
   ```markdown
   [EXTRACTED to crate/sinex-xyz/src/lib.rs - Technical implementation details]
   ~~Original content that was extracted~~
   ```

#### For Unimplemented Designs

1. **Create roadmap document**:
   ```bash
   # For ready designs
   docs/roadmap/features/feature-name.md
   # For planned features  
   docs/roadmap/planned/feature-name.md
   ```

2. **Extract full design**:
   - Move all technical specifications
   - Include SQL schemas, algorithms
   - Preserve example implementations
   - Add implementation status header

3. **Mark extraction**:
   ```markdown
   [EXTRACTED to docs/roadmap/features/embeddings.md - Complete embedding system design]
   ~~Original design content~~
   ```

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

### Step 5: Complete the Migration

1. **Process the file with markers**:
   - Leave all extraction markers in place
   - Keep any unextracted content
   - The file now shows what was moved where

2. **Move to archive**:
   ```bash
   mv docs/_todo/ready/ai/TIM-Feature.md docs/_todo/archive/
   # or
   mv docs/_todo/planned/feature/TIM-Feature.md docs/_todo/archive/
   ```

3. **Document the migration**:
   ```bash
   # In your commit message
   git commit -m "docs: extract TIM-Feature to roadmap/archive
   
   - Extracted design to docs/roadmap/features/feature.md
   - Feature not yet implemented (verified no code exists)
   - Moved processed file to archive with markers"
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
7. **Don't confuse "ready" with "implemented"** - Ready means the design is complete, NOT that code exists!
8. **Don't create migrations for non-existent tables** - Check if tables actually exist first
9. **Don't add docs to unrelated modules** - Embedding docs don't belong in knowledge_graph.rs if there's no embedding code there

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
   - **SKIP archive/ directory** - Contains previously reviewed files deemed not useful for integration
   - Ready TIMs - Process based on implementation status
   - Planned TIMs - Move unimplemented features to `/docs/roadmap/`
   - Operations docs - Move to NixOS module documentation

3. **Low Priority - Analysis/Planning**:
   - `misc-including-high-level-overviews-and-plans/` - Review for insights
   - Diagrams - Keep useful ones in `/docs/architecture/diagrams/`

## Important Note About Archive Directory

The `archive/` directory contains documentation that has already been reviewed and determined not to be suitable for integration into the codebase. These files should be **skipped during migration** unless explicitly instructed otherwise. They are kept for potential future review but are not part of the active documentation migration process.

## Complete Example: Processing a "Ready" TIM

Let's say we're processing `TIM-EmbeddingGenerationModels.md`:

1. **Check implementation**:
   ```bash
   grep -r "artifact_embeddings" migrations/  # Not found!
   rg "embedding" crate/ --type rust          # Only mentions, no implementation
   ```
   
2. **Conclusion**: Feature is designed but NOT implemented

3. **Create roadmap file**:
   `docs/roadmap/features/embeddings.md` with full design

4. **Update original TIM**:
   ```markdown
   [EXTRACTED to docs/roadmap/features/embeddings.md - Complete embedding system design]
   ~~All the technical content~~
   ```

5. **Move to archive**:
   ```bash
   mv docs/_todo/ready/ai/TIM-EmbeddingGenerationModels.md docs/_todo/archive/
   ```

## Summary

The key to successful documentation migration is to **process, not just move**. Each document should be understood, its implementation status verified, valuable content extracted to appropriate locations based on that status, and only then should the original be moved to archive with clear markers showing what went where.