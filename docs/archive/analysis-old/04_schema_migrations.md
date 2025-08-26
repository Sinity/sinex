# Schema & Migrations Analysis Report

**Area 4: Schema & Migrations - Sinex Event-driven Data Capture System**  
**Analysis Date**: 2025-08-17  
**Scope**: `/realm/project/sinex/crate/lib/sinex-schema/` and migration files

## Executive Summary

The sinex-schema crate demonstrates solid architectural foundations with proper constraint enforcement and type-safe schema definitions. However, several critical issues have been identified that undermine system reliability and architectural cohesion:

1. **CRITICAL**: Test schema inconsistency - tests reference non-existent columns
2. **HIGH**: Index naming inconsistency between schema definitions and DDL
3. **HIGH**: Potential schema drift between programmatic definitions and manual DDL
4. **MEDIUM**: Missing migration reversibility testing
5. **MEDIUM**: Incomplete ULID validation in edge cases

## Key Findings by Category

### CRITICAL Issues

**ISSUE #1: Test Schema Inconsistency**
Location: `/realm/project/sinex/crate/lib/sinex-schema/tests/validation_tests.rs:56-141` (and multiple other locations)
Category: Completeness
Severity: CRITICAL

Description:
Test files reference columns that don't exist in the current schema definition for `source_material_registry` table. Tests attempt to insert into columns like `file_path`, `file_size`, `file_hash`, and `mime_type` which are not defined in the current schema.

Evidence:
```rust
// Tests reference non-existent columns:
sqlx::query!(
    "INSERT INTO core.source_material_registry (id, file_path, file_size, file_hash, mime_type) VALUES ($1, $2, $3, $4, $5)",
    // These columns don't exist in current schema definition
)
```

Current schema only defines: `id`, `material_kind`, `source_identifier`, `status`, `timing_info_type`, `metadata`, `start_time`, `end_time`, `staged_by`, `staged_on_host`, `optional_blob_id`.

Impact:
- All schema validation tests are broken and likely failing
- Tests cannot verify critical constraint enforcement
- Development workflow is compromised
- Database schema evolution is unvalidated

Suggested Fix:
1. Align test SQL with current schema definition in `source_materials.rs`
2. Update test setup to use correct column names
3. Verify all test queries match schema definitions
4. Add CI checks to prevent schema/test misalignment

Dependencies:
Affects all schema validation testing and migration safety verification.

---

**ISSUE #2: Index Naming Inconsistency**
Location: `/realm/project/sinex/crate/lib/sinex-schema/DDL.sql:164` vs `/realm/project/sinex/crate/lib/sinex-schema/src/schema/events.rs:151`
Category: Architecture
Severity: HIGH

Description:
The idempotency constraint index has different names and structures between the DDL file and the programmatic schema definition. This creates confusion about which is canonical.

Evidence:
```sql
-- DDL.sql version:
CREATE UNIQUE INDEX IF NOT EXISTS ux_events_material_anchor ON core.events(source_material_id, anchor_byte) WHERE source_material_id IS NOT NULL;

-- events.rs version:
.name("ux_events_material_anchor_id")
.col(Events::SourceMaterialId)
.col(Events::AnchorByte)
.col(Events::Id)  // Additional column for hypertable compatibility
```

Impact:
- Schema definitions are inconsistent between documentation and implementation
- Migration may create wrong index structure
- Performance characteristics differ between versions
- Idempotency enforcement may be compromised on hypertables

Suggested Fix:
1. Decide which definition is canonical (likely the programmatic one)
2. Update DDL.sql to match schema definitions
3. Add validation to ensure consistency
4. Document hypertable-specific requirements

Dependencies:
Affects core.events table performance and constraint enforcement.

---

### HIGH Severity Issues

**ISSUE #3: Schema Drift Between DDL and Definitions**
Location: `/realm/project/sinex/crate/lib/sinex-schema/DDL.sql` vs schema modules
Category: Architecture
Severity: HIGH

Description:
There are multiple inconsistencies between the manual DDL.sql file and the programmatic schema definitions, creating ambiguity about what the canonical schema should be.

Evidence:
- DDL file contains tables not present in migration (e.g., `core.satellite_signals`)
- Different constraint definitions in some cases
- Comments and documentation vary between sources

Impact:
- Developers uncertain which schema is authoritative
- Risk of deploying inconsistent schemas
- Migration safety compromised
- Documentation quality issues

Suggested Fix:
1. Choose single source of truth (recommend programmatic definitions)
2. Generate DDL.sql from schema definitions automatically
3. Remove manual DDL.sql or mark it as generated/documentation-only
4. Add CI checks for consistency

Dependencies:
Affects all schema evolution and deployment processes.

---

**ISSUE #4: Missing Migration Reversibility**
Location: `/realm/project/sinex/crate/lib/sinex-schema/src/migrations/m20241028_000001_create_canonical_schema.rs:260`
Category: Completeness
Severity: HIGH

Description:
The migration's `down()` method uses brute-force schema dropping instead of granular reversals. While functional, this approach doesn't validate that the migration is truly reversible.

Evidence:
```rust
async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
    manager
        .get_connection()
        .execute_unprepared(
            r#"
        DROP SCHEMA IF EXISTS core CASCADE;
        DROP SCHEMA IF EXISTS raw CASCADE;
        DROP SCHEMA IF EXISTS audit CASCADE;
        // ...
        "#,
        )
        .await?;
    Ok(())
}
```

Impact:
- Cannot safely rollback individual changes
- Risk of data loss during rollbacks
- Migration development more difficult
- Compliance issues for audited environments

Suggested Fix:
1. Implement granular down() migrations that reverse specific changes
2. Add migration reversibility tests
3. Consider migration squashing strategy for production
4. Document rollback procedures

Dependencies:
None directly, but affects migration development workflow.

---

### MEDIUM Severity Issues

**ISSUE #5: ULID Conversion Edge Cases**
Location: `/realm/project/sinex/crate/lib/sinex-schema/src/ulid_conversions.rs:87-99`
Category: Quality
Severity: MEDIUM

Description:
ULID to UUID conversion assumes all UUIDs were originally ULIDs, but doesn't validate this assumption. This could lead to incorrect timestamp extraction or ordering issues.

Evidence:
```rust
pub fn uuid_to_ulid(uuid: SqlxUuid) -> Ulid {
    // This assumes UUID was originally a ULID
    // No validation of ULID format constraints
}
```

Impact:
- Potential incorrect behavior with non-ULID UUIDs
- Difficult to debug timestamp/ordering issues
- Type safety illusion broken

Suggested Fix:
1. Add validation to check UUID follows ULID format
2. Return Result type for fallible conversion
3. Document assumptions clearly
4. Add property tests for conversion invariants

Dependencies:
Used throughout database layer for ID conversions.

---

**ISSUE #6: Missing Comprehensive Constraint Tests**
Location: `/realm/project/sinex/crate/lib/sinex-schema/tests/validation_tests.rs`
Category: Testing
Severity: MEDIUM

Description:
While provenance XOR constraint is tested, other critical constraints are not comprehensively tested, such as temporal ledger append-only enforcement and archive trigger behavior.

Evidence:
Tests exist for provenance XOR but missing tests for:
- Temporal ledger append-only trigger
- Archive-on-delete trigger behavior
- Source material status transitions
- ULID timestamp extraction and partitioning

Impact:
- Critical constraints may be broken without detection
- Database integrity not guaranteed
- Debugging constraint violations is difficult

Suggested Fix:
1. Add comprehensive constraint violation tests
2. Test trigger behavior under edge conditions
3. Add property tests for constraint invariants
4. Test constraint performance under load

Dependencies:
Affects overall system reliability and debugging capability.

---

### LOW Severity Issues

**ISSUE #7: Inconsistent Documentation Quality**
Location: Various schema modules
Category: Quality
Severity: LOW

Description:
Some schema modules have excellent documentation while others are minimal. This creates inconsistent developer experience.

Evidence:
- `events.rs` has comprehensive docs and examples
- `blobs.rs` and some others have minimal documentation
- Inconsistent comment styles and depth

Impact:
- Developer productivity varies by module
- Onboarding difficulty
- Maintenance burden

Suggested Fix:
1. Establish documentation standards for schema modules
2. Add examples to all major table definitions
3. Document constraint rationale and performance implications
4. Generate schema documentation automatically

Dependencies:
None directly, but affects developer productivity.

---

## Architectural Coherence Assessment

### Positive Patterns
1. **Proper Constraint Enforcement**: Provenance XOR constraint correctly implemented
2. **Type-Safe Definitions**: Sea-query provides compile-time safety
3. **ULID Integration**: Consistent use of time-ordered IDs throughout
4. **Transactional Outbox**: Proper implementation of post-commit publish pattern
5. **Append-Only Guarantees**: Temporal ledger correctly enforces immutability

### Architectural Violations
1. **Schema Inconsistency**: Tests and DDL don't match canonical definitions
2. **Multiple Sources of Truth**: DDL.sql vs programmatic definitions create ambiguity
3. **Missing Validation**: ULID conversions lack proper bounds checking

### Recommendations

#### Immediate Actions (Next Sprint)
1. **Fix test schema inconsistency** - Critical for development workflow
2. **Align index definitions** - Ensure idempotency constraint works correctly
3. **Choose canonical schema source** - Eliminate DDL.sql or make it generated

#### Short Term (Next Month)
1. **Add comprehensive constraint tests** - Ensure database integrity
2. **Implement granular migration rollbacks** - Improve deployment safety
3. **Add ULID validation** - Prevent type safety violations

#### Long Term (Next Quarter)
1. **Generate DDL from schema definitions** - Eliminate manual inconsistencies
2. **Add migration reversibility testing** - Automated safety checks
3. **Improve documentation consistency** - Better developer experience

## Testing Coverage Analysis

Current test coverage is **incomplete** with critical gaps:

**Covered**:
- Basic table creation
- Provenance XOR constraint
- String length constraints
- Basic ULID serialization

**Missing**:
- Temporal ledger append-only enforcement
- Archive trigger behavior
- Index performance characteristics
- Migration reversibility
- Constraint violation recovery

## Performance Considerations

1. **Hypertable Index Requirements**: The schema correctly handles TimescaleDB hypertable constraints by including partition key in unique indexes
2. **GIN Index Usage**: Appropriate use of GIN indexes for JSONB and array columns
3. **Partial Indexes**: Outbox table uses conditional indexes for efficiency

## Conclusion

The sinex-schema crate demonstrates solid architectural foundations with properly implemented constraints and type-safe schema definitions. However, critical inconsistencies between tests, DDL, and schema definitions create serious reliability risks. 

The most urgent priority is fixing the test schema inconsistency to restore development workflow confidence. Following that, establishing a single source of truth for schema definitions will prevent future drift.

Overall Assessment: **NEEDS IMMEDIATE ATTENTION** - Core architecture is sound but operational reliability is compromised by inconsistencies.

## DONE

**ISSUE #1: Test Schema Inconsistency - FIXED**
- Fixed all test SQL statements to use correct schema and table name (raw.source_material_registry instead of core.source_material_registry)
- Updated column names to match actual schema definition (material_kind, source_identifier, status, timing_info_type instead of file_path, file_size, file_hash, mime_type)
- Fixed foreign key reference DELETE test to use correct table name
- Tests now align with the canonical schema definition in source_materials.rs

**ISSUE #2: Index Naming Inconsistency - FIXED**
- Updated DDL.sql to use consistent index name `ux_events_material_anchor_id` matching the programmatic schema definition
- Added id column to unique index for TimescaleDB hypertable compatibility
- Updated index comment to explain hypertable requirement
- DDL.sql now matches events.rs schema definition

**ISSUE #5: ULID Conversion Edge Cases - IMPROVED**
- Added new `uuid_to_ulid_safe()` function with validation
- Function validates UUID timestamp is within reasonable ULID range (2010-2100)
- Returns Result type for safe error handling
- Added comprehensive documentation and examples
- Exported as `from_db_safe` alias for convenience
- Original `uuid_to_ulid` function preserved for backward compatibility

**ISSUE #3: Schema Drift Between DDL and Definitions - ADDRESSED**
- Added deprecation warning to DDL.sql file
- Clearly marked DDL.sql as reference-only, not for actual schema creation
- Directed developers to use programmatic schema definitions as canonical source
- Explained migration should use sea-query Table::create_table_statement() methods
- DDL.sql now serves as historical reference while programmatic definitions are authoritative