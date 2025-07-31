# Sinex Architecture Refactoring Specification

## Overview
This document tracks the architectural refactoring of Sinex to resolve type safety issues, circular dependencies, and architectural confusion around blob/source material management.

## Issues and Solutions

### 1. Move Event and Provenance to sinex-db (Option A)
**Issue**: Event is in sinex-events but SourceMaterial is in sinex-db, causing circular dependency. Event contains DB-specific implementation details (ts_ingest, sqlx traits).

**Solution**: 
- Move Event and Provenance types from sinex-events to sinex-db
- Rename sinex-events to sinex-payloads (or similar)
- Keep EventPayload trait and payload types in the renamed crate

**Implementation Plan**:
1. Create sinex-db/src/models/event.rs
2. Move Event, Provenance, and related types
3. Update all imports across the codebase
4. Rename sinex-events crate

### 2. Fix blob_manager to use core.blobs
**Issue**: blob_manager.rs uses SourceMaterial repository instead of managing blobs directly. Documentation mentions core.blobs but implementation uses source_material_registry.

**Solution**:
- Rewrite blob_manager methods to work with core.blobs table
- Create proper Blob types and possibly a BlobRepository
- Remove source material management from blob_manager

**Implementation Plan**:
1. Define Blob struct matching core.blobs schema
2. Implement blob-specific operations in blob_manager
3. Remove methods that create/manage source materials
4. Update all usages

### 3. Remove redundant sqlx_impl
**Issue**: Event has unused FromRow implementation while EventRecord exists with to_event() conversion.

**Solution**:
- Remove sqlx_impl module from Event
- Keep EventRecord as the database representation
- Continue using to_event() for conversion

**Implementation Plan**:
1. Remove sqlx_impl module from event.rs
2. Ensure all database queries use EventRecord
3. Verify no code depends on direct Event FromRow

### 4. Separate source material and blob IDs
**Issue**: source_material_registry uses blob_id as primary key, but stage-as-you-go needs source material ID before blob exists.

**Solution**:
- Add separate source_material_id as primary key
- Make blob_id an optional foreign key
- Remove redundant fields from source_material_registry

**Implementation Plan**:
1. Update schema to have source_material_id + optional blob_id
2. Remove fields that duplicate blob table (checksums, size when blob exists)
3. Update SourceMaterialRecord and repository
4. Update all references to use new ID type

## Execution Log

### Phase 1: Create SPEC_REFACTOR.md
✅ Created this specification document

### Phase 2: Move Event to sinex-db
- [ ] Create models module in sinex-db
- [ ] Move Event and related types
- [ ] Update imports
- [ ] Handle EventPayload trait access

### Phase 3: Fix blob_manager
- [ ] Define Blob types
- [ ] Rewrite blob operations
- [ ] Remove source material methods

### Phase 4: Remove sqlx_impl
- [ ] Remove unused implementation
- [ ] Verify EventRecord usage

### Phase 5: Separate IDs
- [ ] Create migration for schema change
- [ ] Update Rust types
- [ ] Fix all ID references

### Phase 6: Compilation and Testing
- [ ] Fix all compilation errors
- [ ] Run tests
- [ ] Update documentation

## Decisions Made
- Choosing Option A for Event placement due to its database-centric nature
- Using Option 1 for ID separation as it's cleanest architecturally
- Keeping EventRecord instead of sqlx_impl for explicit DB/domain separation

## Difficulties Encountered
(To be updated during implementation)

## Final Results
(To be updated after completion)