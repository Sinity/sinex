# Documentation Archive

This directory contains historical documentation, design specs, and materials that have been superseded or extracted into the main codebase.

## What's Here

### Architecture Decision Records (ADRs)
- Original design decisions that have been implemented
- Most content has been extracted to relevant code locations
- Kept for historical reference and understanding evolution

### Technical Implementation Modules (TIMs)
- Detailed implementation specifications
- Useful content has been extracted to:
  - Code documentation (rustdoc)
  - Migration files
  - Module documentation
- Files include notes about where content was moved

### Migration Guides
- `stream_processor_migration.md` - Migration from EventSource to StatefulStreamProcessor
- Key concepts extracted to `/crate/sinex-satellite-sdk/src/stream_processor.rs`

### Unimplemented Designs
- `TIM-CoreArtifactsSchema.md` - Comprehensive artifact system (partially implemented as `km.artifacts`)
- `TIM-LinkingTablesSchema.md` - Event relations design (not yet implemented)
- `TIM-GitAnnexLargeFileMgmt-operations.md` - Advanced git-annex operations

## Why Keep These?

1. **Historical Context**: Understanding design evolution
2. **Future Reference**: Unimplemented features may be revisited
3. **Detailed Specifications**: Contains implementation details beyond what's in code
4. **Alternative Approaches**: Documents paths not taken

## Using This Archive

- For current documentation, see `/docs/` main directories
- For implementation details, check rustdoc and code comments
- Use these files to understand "why" behind current design
- Reference for implementing currently missing features