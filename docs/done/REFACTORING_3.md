# Repository Pattern Enhancement with SeaQuery Integration

## Overview

This document outlines the plan to enhance the Repository pattern by leveraging SeaQuery schemas for generic operations, eliminating boilerplate code across repositories, with two possible approaches: simple SQL generation or full sea-orm-migration integration.

## ✅ Implementation Status

**COMPLETED** - All phases have been successfully implemented as of 2025-08-02.

### Summary of Implemented Changes

1. **Phase 1: Schema Verification & Fixes** ✅
   - Fixed table name: `Checkpoints` → `ProcessorCheckpoints`
   - Fixed table name: `Schemas` → `EventPayloadSchemas`
   - Added missing columns from migration 12 (content_hash, source, event_type)
   - Aligned `OperationsLog` with actual SQL schema
   - All SeaQuery schemas now match SQL migrations exactly

2. **Phase 2: sea-orm-migration Integration** ✅
   - Added sea-orm-migration as optional dependency with "migration" feature
   - Created complete migration structure at `crate/sinex-db/migration/`
   - Implemented initial schema migration using SeaQuery definitions
   - Added validation functions migration (migrations 13-14)
   - Integrated migration commands into Justfile
   - Created migration utilities module for application integration

3. **Phase 3: Enhanced Repository Pattern** ✅
   - Created `TableDef` trait for generic table operations
   - Implemented `EnhancedRepository` trait with:
     - `count_all()` - Count all records
     - `exists_by_id()` - Check if record exists
     - `select_query()` - Build SeaQuery select statements
   - Added `BatchRepository` trait for bulk operations
   - Added `TransactionalRepository` trait for transaction management

4. **Phase 4: Repository Updates** ✅
   - All repositories now implement `EnhancedRepository`:
     - EventRepository → Events table
     - CheckpointRepository → ProcessorCheckpoints table
     - SourceMaterialRepository → SourceMaterials table
     - StateRepository → OperationsLog table
     - KnowledgeGraphRepository → Entities table
   - Re-exported new traits in module interface

5. **Phase 5: Testing** ✅
   - Created comprehensive test suite in `common_test.rs`
   - Tests verify:
     - Enhanced repository operations (count_all, exists_by_id)
     - Repository polymorphism
     - SeaQuery integration
     - TableDef constant correctness
   - All tests pass successfully

### Key Achievements

- **Single Source of Truth**: SeaQuery schemas define database structure
- **Zero Boilerplate**: Generic operations available on all repositories
- **Type Safety**: Compile-time checked table and column names
- **Backward Compatible**: Existing sqlx queries continue to work
- **Migration System**: Professional schema management with sea-orm-migration
- **Testable**: Comprehensive test coverage for all new functionality

### Usage Example

```rust
// Generic operations now available on all repositories
let pool = create_pool(&database_url).await?;
let events_repo = pool.events();

// These methods are automatically available
let count = events_repo.count_all().await?;
let exists = events_repo.exists_by_id(&event_id).await?;

// Build queries with SeaQuery
let query = EventRepository::select_query()
    .and_where(Expr::col(Events::col("source")).eq("test"))
    .build(PostgresQueryBuilder);
```

### Next Steps

The implementation is complete and ready for use. Future enhancements could include:
- Additional generic operations (find_by_field, paginate, etc.)
- Query caching layer
- Metrics collection for repository operations
- Automatic audit trail generation

All code is tested, documented, and integrated into the existing codebase.

## Approach Comparison: sqlx + SeaQuery vs Full SeaORM

### Current Stack: sqlx + SeaQuery
**What you have:**
- Direct SQL queries with sqlx macros
- SeaQuery for type-safe query building
- Manual schema definitions
- Hand-written migrations

**Pros:**
- Full control over queries and performance
- Compile-time checked queries with sqlx
- No ORM abstraction overhead
- Can optimize specific queries precisely
- Already integrated in your codebase

**Cons:**
- Manual repository boilerplate
- Schema defined in two places (SeaQuery + SQL)
- No automatic migrations
- More code to maintain

### Alternative: Full SeaORM
**What it offers:**
- Complete ORM with ActiveRecord pattern
- Automatic CRUD operations
- Built-in migration system
- Schema discovery and code generation
- Relationship handling

**Pros:**
- Less boilerplate code
- Automatic migrations from schema changes
- Built-in connection pooling and transactions
- Active development and community

**Cons:**
- ORM abstraction overhead (minor but present)
- Less control over exact queries
- Learning curve for ORM patterns
- May generate suboptimal queries for complex cases
- Would require significant refactoring

### Recommendation for Your Use Case

**Stick with sqlx + SeaQuery + sea-orm-migration** because:
1. You already have significant sqlx investment
2. Your event-driven architecture benefits from precise query control
3. You can adopt just the migration system without the full ORM
4. TimescaleDB and special PostgreSQL features work better with direct SQL
5. Performance critical paths (event ingestion) need optimization

## Phase 1: Verify SeaQuery Schemas Match SQL Migrations

[Same as before - keeping this section unchanged]

### Discrepancies to fix:
- [ ] Checkpoints: `satellite_id` vs `processor_name` naming
- [ ] Add missing columns from recent migrations (12, 13, 14)
- [ ] Verify all constraints and foreign keys
- [ ] Table name mismatch: `Schemas` struct vs `event_payload_schemas` table

## Phase 1.5: Migration System Choice

### Option A: Simple SQL Generation (Original Plan)

[Previous content about simple SQL generation]

### Option B: sea-orm-migration Integration (Recommended)

## Phase 2: Integrate sea-orm-migration

### Step 1: Add Dependencies

Update `/crate/sinex-db/Cargo.toml`:

```toml
[dependencies]
sea-orm-migration = { version = "1.0", features = [
    "sqlx-postgres",
    "runtime-tokio-rustls",
]}

[dev-dependencies]
sea-orm-cli = { version = "1.0", default-features = false, features = [
    "codegen",
    "runtime-tokio-rustls",
    "sqlx-postgres",
]}
```

### Step 2: Initialize Migration Structure

```bash
cd crate/sinex-db
sea-orm-cli migrate init
```

This creates:
```
crate/sinex-db/
├── migration/
│   ├── Cargo.toml
│   ├── src/
│   │   ├── lib.rs
│   │   ├── m20240101_000001_initial_schema.rs
│   │   └── main.rs
```

### Step 3: Convert Existing Schema to Migration

Create `/crate/sinex-db/migration/src/m20240101_000001_initial_schema.rs`:

```rust
use sea_orm_migration::prelude::*;
use crate::schema::{Events, Checkpoints, Schemas, SourceMaterials, OperationsLog, ArchivedEvents, Entities, EntityRelations};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Create extensions
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
                CREATE EXTENSION IF NOT EXISTS "timescaledb";
                CREATE EXTENSION IF NOT EXISTS "pg_jsonschema";
                "#
            )
            .await?;

        // Create schemas
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                CREATE SCHEMA IF NOT EXISTS core;
                CREATE SCHEMA IF NOT EXISTS raw;
                CREATE SCHEMA IF NOT EXISTS audit;
                CREATE SCHEMA IF NOT EXISTS sinex_schemas;
                "#
            )
            .await?;

        // Create tables using SeaQuery definitions
        manager.create_table(Table::from_raw(Events::create_table())).await?;
        
        // Create indexes
        for index_sql in Events::create_indexes() {
            manager.get_connection().execute_unprepared(&index_sql).await?;
        }

        // Convert to TimescaleDB hypertable
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                SELECT create_hypertable(
                    'core.events',
                    by_range('event_id', partition_func => 'ulid_to_timestamptz'::regproc)
                );
                "#
            )
            .await?;

        // Repeat for other tables...
        manager.create_table(Table::from_raw(Checkpoints::create_table())).await?;
        manager.create_table(Table::from_raw(Schemas::create_table())).await?;
        // etc...

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Drop tables in reverse order
        manager.drop_table(Table::drop().table(EntityRelations::Table).to_owned()).await?;
        manager.drop_table(Table::drop().table(Entities::Table).to_owned()).await?;
        // ... etc
        Ok(())
    }
}
```

### Step 4: Create Migration for Schema Updates

When you modify a SeaQuery schema, create a new migration:

```bash
sea-orm-cli migrate generate add_new_column
```

```rust
// m20240102_000002_add_new_column.rs
use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(Events::Table)
                    .add_column(
                        ColumnDef::new(Alias::new("new_field"))
                            .string()
                            .null()
                    )
                    .to_owned()
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(Events::Table)
                    .drop_column(Alias::new("new_field"))
                    .to_owned()
            )
            .await
    }
}
```

### Step 5: Update Justfile

```makefile
# Generate new migration
migration-new name:
    cd crate/sinex-db && sea-orm-cli migrate generate {{name}}

# Run migrations
migrate:
    cd crate/sinex-db && sea-orm-cli migrate up

# Rollback last migration
migrate-down:
    cd crate/sinex-db && sea-orm-cli migrate down

# Check migration status
migrate-status:
    cd crate/sinex-db && sea-orm-cli migrate status

# Refresh schema (down all, then up)
migrate-refresh:
    cd crate/sinex-db && sea-orm-cli migrate refresh

# Generate SQL from current schema for sqlx
generate-sqlx-schema:
    cd crate/sinex-db && sea-orm-cli migrate up --dry-run > ../../migrations/schema.sql

# Prepare sqlx offline cache after migrations
sqlx-prepare: migrate generate-sqlx-schema
    cargo sqlx prepare --workspace
```

### Step 6: Integrate with Application

In your application startup:

```rust
use migration::{Migrator, MigratorTrait};

pub async fn run_migrations(pool: &PgPool) -> Result<()> {
    let conn = SqlxPostgresConnector::from_sqlx_postgres_pool(pool.clone());
    Migrator::up(&conn, None).await?;
    Ok(())
}

// Or check for pending migrations
pub async fn check_migrations(pool: &PgPool) -> Result<Vec<String>> {
    let conn = SqlxPostgresConnector::from_sqlx_postgres_pool(pool.clone());
    let pending = Migrator::get_pending_migrations(&conn).await?;
    Ok(pending.iter().map(|m| m.name().to_string()).collect())
}
```

### Benefits of sea-orm-migration

1. **Automatic Migration Tracking**: No manual version management
2. **Rollback Support**: Every migration has up/down methods
3. **Type-Safe Migrations**: Use SeaQuery schemas directly
4. **Migration History**: Built-in table tracks applied migrations
5. **Dry Run**: Preview SQL before applying
6. **Programmatic Control**: Run migrations from code or CLI

### Migration Workflow

1. **Schema Change**: Update SeaQuery schema definition
2. **Generate Migration**: `just migration-new describe_change`
3. **Write Migration**: Use SeaQuery to define changes
4. **Test Migration**: `just migrate` (applies) and `just migrate-down` (rollback)
5. **Generate sqlx Schema**: `just generate-sqlx-schema`
6. **Update sqlx Cache**: `just sqlx-prepare`
7. **Commit**: Both migration and .sqlx cache

## Phase 3: Create Enhanced Repository Trait

[Rest remains the same as the original document...]

## Conclusion

The sea-orm-migration approach provides:
- Single source of truth (SeaQuery schemas)
- Automatic migration management
- Type-safe schema changes
- Compatible with existing sqlx queries
- Professional migration workflow

While full SeaORM would require major refactoring, adopting just the migration system gives you the best of both worlds: precise control with sqlx queries and automated schema management with SeaQuery.

## Phase 2: Create Enhanced Repository Trait

### File: `/crate/sinex-db/src/repositories/common.rs`

```rust
use sea_query::{Alias, Expr, Iden, Query, SelectStatement, InsertStatement, UpdateStatement, DeleteStatement};
use sqlx::PgPool;

/// Enhanced repository trait with SeaQuery integration
pub trait Repository<'a>: Sized {
    /// Database pool reference
    fn pool(&self) -> &'a PgPool;
    
    /// Create new instance
    fn new(pool: &'a PgPool) -> Self;
    
    /// Associated table schema from SeaQuery definitions
    type Table: TableDef;
    
    /// Row type for this repository (must have FromRow)
    type Row: for<'r> FromRow<'r, sqlx::postgres::PgRow> + Send + Unpin;
    
    /// ID type (must be ULID-based)
    type Id: Into<Ulid> + From<Ulid> + Copy + Debug;
    
    // ===== Query Builders =====
    
    /// Start a SELECT query
    fn select(&self) -> SelectStatement {
        Query::select()
            .from(Self::Table::table_ref())
            .to_owned()
    }
    
    /// Start an INSERT query
    fn insert(&self) -> InsertStatement {
        Query::insert()
            .into_table(Self::Table::table_ref())
            .to_owned()
    }
    
    /// Start an UPDATE query
    fn update(&self) -> UpdateStatement {
        Query::update()
            .table(Self::Table::table_ref())
            .to_owned()
    }
    
    /// Start a DELETE query
    fn delete(&self) -> DeleteStatement {
        Query::delete()
            .from_table(Self::Table::table_ref())
            .to_owned()
    }
    
    // ===== Generic Operations =====
    
    /// Check if record exists by ID
    async fn exists(&self, id: Self::Id) -> DbResult<bool> {
        let uuid = ulid_to_uuid(id.into());
        let (sql, values) = self.select()
            .expr(Expr::value(1))
            .and_where(Expr::col(Self::Table::id_column()).eq(uuid))
            .limit(1)
            .build_sqlx(PostgresQueryBuilder);
            
        let result = sqlx::query_with(&sql, values)
            .fetch_optional(self.pool())
            .await
            .map_err(|e| db_error(e, "check exists"))?;
            
        Ok(result.is_some())
    }
    
    /// Count all records
    async fn count_all(&self) -> DbResult<i64> {
        let (sql, values) = self.select()
            .expr(Func::count(Expr::col(Asterisk)))
            .build_sqlx(PostgresQueryBuilder);
            
        let row = sqlx::query_with(&sql, values)
            .fetch_one(self.pool())
            .await
            .map_err(|e| db_error(e, "count all"))?;
            
        Ok(row.try_get::<i64, _>(0)?)
    }
    
    /// Get by ID
    async fn get_by_id(&self, id: Self::Id) -> DbResult<Option<Self::Row>> {
        let uuid = ulid_to_uuid(id.into());
        
        let (sql, values) = self.select()
            .columns(Self::Table::all_columns())
            .and_where(Expr::col(Self::Table::id_column()).eq(uuid))
            .build_sqlx(PostgresQueryBuilder);
            
        sqlx::query_as_with(&sql, values)
            .fetch_optional(self.pool())
            .await
            .map_err(|e| db_error(e, "get by id"))
    }
    
    /// Delete by ID
    async fn delete_by_id(&self, id: Self::Id) -> DbResult<bool> {
        let uuid = ulid_to_uuid(id.into());
        
        let (sql, values) = self.delete()
            .and_where(Expr::col(Self::Table::id_column()).eq(uuid))
            .build_sqlx(PostgresQueryBuilder);
            
        let result = sqlx::query_with(&sql, values)
            .execute(self.pool())
            .await
            .map_err(|e| db_error(e, "delete by id"))?;
            
        Ok(result.rows_affected() > 0)
    }
}

/// Trait for SeaQuery table definitions
pub trait TableDef: Copy + Clone {
    /// Get table reference (schema, table)
    fn table_ref() -> (Alias, Alias);
    
    /// Get primary key column  
    fn id_column() -> Alias;
    
    /// Get all columns for SELECT *
    fn all_columns() -> Vec<(Alias, Alias, Alias)>;
    
    /// Default ordering column
    fn default_order() -> (Alias, Alias, Alias);
}
```

## Phase 3: Implement TableDef for All Tables

### Enhance `/crate/sinex-db/src/schema.rs`

Add `TableDef` implementations for each table:

```rust
impl TableDef for Events {
    fn table_ref() -> (Alias, Alias) {
        (Alias::new(Self::SCHEMA), Alias::new(Self::TABLE))
    }
    
    fn id_column() -> Alias {
        Alias::new(Self::EVENT_ID)
    }
    
    fn all_columns() -> Vec<(Alias, Alias, Alias)> {
        vec![
            (Alias::new(Self::SCHEMA), Alias::new(Self::TABLE), Alias::new(Self::EVENT_ID)),
            (Alias::new(Self::SCHEMA), Alias::new(Self::TABLE), Alias::new(Self::SOURCE)),
            // ... all other columns
        ]
    }
    
    fn default_order() -> (Alias, Alias, Alias) {
        (Alias::new(Self::SCHEMA), Alias::new(Self::TABLE), Alias::new(Self::TS_INGEST))
    }
}

// Repeat for Checkpoints, Schemas, SourceMaterials, etc.
```

## Phase 4: Update Existing Repositories

### Update each repository to use the enhanced trait:

1. **EventRepository**
   ```rust
   impl<'a> Repository<'a> for EventRepository<'a> {
       type Table = Events;
       type Row = Event;
       type Id = EventId;
       
       fn pool(&self) -> &'a PgPool { self.pool }
       fn new(pool: &'a PgPool) -> Self { Self { pool } }
   }
   ```

2. **CheckpointRepository**
   ```rust
   impl<'a> Repository<'a> for CheckpointRepository<'a> {
       type Table = Checkpoints;
       type Row = CheckpointRecord;
       type Id = CheckpointId;
       
       fn pool(&self) -> &'a PgPool { self.pool }
       fn new(pool: &'a PgPool) -> Self { Self { pool } }
   }
   ```

3. Remove duplicate methods that are now provided by the trait:
   - `exists()`
   - `count_all()`
   - `get_by_id()` (if signature matches)
   - `delete_by_id()`

## Phase 5: Add Additional Trait Extensions

### BatchRepository for efficient bulk operations:

```rust
pub trait BatchRepository<'a>: Repository<'a> {
    /// Insert multiple records using PostgreSQL COPY
    async fn insert_batch(&self, items: Vec<Self::Row>) -> DbResult<u64> {
        // Implementation using SeaQuery batch insert
    }
}
```

### TransactionalRepository for transaction support:

```rust
pub trait TransactionalRepository<'a>: Repository<'a> {
    /// Execute operation in a transaction
    async fn with_transaction<T, F>(&self, f: F) -> DbResult<T>
    where
        F: for<'t> FnOnce(&'t mut Transaction<'_, Postgres>) -> 
            Pin<Box<dyn Future<Output = DbResult<T>> + Send + 't>>,
    {
        crate::query_helpers::with_transaction(self.pool(), f).await
    }
}
```

## Phase 6: Testing

### Create comprehensive tests for generic operations:

1. Test each TableDef implementation
2. Test generic Repository methods
3. Verify existing repository-specific methods still work
4. Performance benchmarks comparing sqlx vs SeaQuery approaches

## Benefits

1. **Reduced Boilerplate**: Common operations implemented once
2. **Type Safety**: SeaQuery ensures correct column names
3. **Consistency**: All repositories have the same basic operations
4. **Refactoring Safety**: Change column name in one place
5. **Performance**: SeaQuery generates optimal SQL
6. **Flexibility**: Repositories can override defaults when needed

## Migration Strategy

1. **Phase 1-2**: Can be done without breaking changes
2. **Phase 3-4**: Update repositories incrementally
3. **Phase 5**: Add extensions as needed
4. **Phase 6**: Comprehensive testing before removing old code

## Notes

- Keep sqlx macros for complex, static queries where they're cleaner
- Use SeaQuery for dynamic queries and generic operations
- Both approaches coexist - use the right tool for each job
- No need to migrate existing SQL migrations - just ensure schemas match