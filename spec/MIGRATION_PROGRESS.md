# Spec Documentation Migration Progress

This document tracks the incremental migration of /spec/ documentation to rustdoc.

## Completed Migrations

### General Documentation
- ✅ **GLOSSARY.md** → `sinex-core-types::glossary` module
  - Only Sinex-specific terms migrated
  - Generic technology definitions excluded
  
- ✅ **MATURITY.md** → `sinex-core-types::development` module  
  - 5-level maturity model embedded
  
- ✅ **PATHWAYS.md** → `sinex-core-types::development` module
  - Contribution pathways integrated
  
- ✅ **DEPENDENCIES.md** → `sinex-core-types::development` module
  - Dependency graph structure preserved

### Architectural Decision Records (ADRs)
- ✅ **ADR-001** (ULID Primary Keys) → `sinex-ulid` crate docs
- ✅ **ADR-002** (PostgreSQL Work Queue) → `redis_client.rs` (as superseded)
- ✅ **ADR-011** (Clock Regression) → `Ulid::default()` implementation
- ✅ **ADR-014** (Routing Cache) → `redis_client.rs` (as historical context)

### Technical Implementation Modules (TIMs)
- ✅ **TIM-EventSchemaRegistry** → `EventPayloadSchema` struct docs
- ✅ **TIM-FilesystemMonitoringWatchers** → `fs-watcher/unified_processor.rs`
- ✅ **TIM-TimescaleDBConfiguration** → `migrations/00000000000002_create_core_tables.sql`

## Migration Strategy

1. **Focus on Sinex-specific content** - Skip generic technology explanations
2. **Embed at implementation points** - Documentation lives with the code it describes
3. **Verify current relevance** - Don't migrate outdated architectural decisions
4. **Show concrete progress** - Delete migrated files from /spec/

## Lessons Learned

- **Critical**: Must understand code evolution before migrating historical decisions
- **ADR-010 incident**: Documented a pivot that was later reversed - removed misleading content
- **Success metric**: Spec files can be deleted after migration, showing tangible progress

## Next Candidates

High-value migrations to consider:
- SADI.md / STAD.md → sinex-core crate-level documentation
- Ready ADRs that reflect current architecture
- Implemented TIMs that document existing features
EOF < /dev/null