# Database Migration Guide

This guide explains how to work with database migrations in Sinex after the transition from raw SQL files to sea-orm-migration.

## Migration System Overview

Sinex now uses [sea-orm-migration](https://www.sea-ql.org/SeaORM/docs/migration/setting-up-migration/) for database migrations. Migrations are:

- Written in Rust (type-safe)
- Located in `crate/sinex-db/migration/src/`
- Run manually (not automatically on service startup)
- Support both up and down migrations

## Running Migrations

### Development

```bash
# Run all pending migrations
just migrate

# Check migration status
just migrate-status

# Rollback last migration
just migrate-down

# Fresh start (rollback all, then apply all)
just migrate-refresh
```

### Testing

Tests automatically set up the database with migrations:
```bash
just db-setup    # Creates database and runs migrations
just test        # Tests use migrated database
```

### Production

Migrations should be run as a separate deployment step:
```bash
DATABASE_URL="postgresql://..." just migrate
```

## Creating New Migrations

### 1. Generate Migration File

```bash
just migrate-create "add_user_preferences_table"
```

This creates a new migration file like:
```
crate/sinex-db/migration/src/m20240201_000011_add_user_preferences_table.rs
```

### 2. Write the Migration

Use SeaQuery builder API for simple operations:

```rust
use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table((Alias::new("core"), Alias::new("user_preferences")))
                    .if_not_exists()
                    .col(
                        ColumnDef::new(Alias::new("user_id"))
                            .uuid()
                            .not_null()
                            .primary_key(),
                    )
                    .col(
                        ColumnDef::new(Alias::new("theme"))
                            .string()
                            .not_null()
                            .default("light"),
                    )
                    .col(
                        ColumnDef::new(Alias::new("created_at"))
                            .timestamp_with_time_zone()
                            .not_null()
                            .default("NOW()"),
                    )
                    .to_owned(),
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(
                Table::drop()
                    .table((Alias::new("core"), Alias::new("user_preferences")))
                    .to_owned(),
            )
            .await?;

        Ok(())
    }
}
```

For PostgreSQL-specific features, use raw SQL:

```rust
// TimescaleDB hypertable
manager
    .get_connection()
    .execute_unprepared(
        r#"
        SELECT create_hypertable(
            'metrics.events_per_hour',
            by_range('bucket')
        );
        "#
    )
    .await?;

// Stored procedures
manager
    .get_connection()
    .execute_unprepared(
        r#"
        CREATE OR REPLACE FUNCTION calculate_event_rate()
        RETURNS TABLE(hour TIMESTAMPTZ, rate NUMERIC) AS $$
        BEGIN
            RETURN QUERY
            SELECT date_trunc('hour', ts_ingest) as hour,
                   COUNT(*)::NUMERIC / 3600 as rate
            FROM core.events
            GROUP BY 1
            ORDER BY 1 DESC;
        END;
        $$ LANGUAGE plpgsql;
        "#
    )
    .await?;
```

### 3. Register the Migration

Add the migration to `crate/sinex-db/migration/src/lib.rs`:

```rust
mod m20240201_000011_add_user_preferences_table;

// In migrations() function:
vec![
    // ... existing migrations
    Box::new(m20240201_000011_add_user_preferences_table::Migration),
]
```

### 4. Test the Migration

```bash
# Run the migration
just migrate

# Verify it worked
just psql -c "\\d core.user_preferences"

# Test rollback
just migrate-down
```

## Important Notes

### SQLX Offline Mode

The SQLX offline cache depends on the actual database schema. After running migrations, you MUST update the cache:

```bash
just sqlx-prepare  # This runs migrations first, then updates cache
```

This is required for:
- Nix builds (which can't access the database)
- Compile-time query verification
- CI/CD pipelines

The workflow is:
1. Migrations create/update the database schema
2. SQLX analyzes the schema and generates `.sqlx/` cache files
3. The cache files must be committed to git

## Current Migration Structure

The database schema is organized into three logical migrations:

1. **m20240101_000001_core_infrastructure** - Core tables, extensions, and indexes
   - PostgreSQL extensions (TimescaleDB, ULID, pg_jsonschema)
   - Core schemas and tables (events, entities, source material)
   - Essential indexes and constraints
   - Schema validation infrastructure

2. **m20240102_000002_functions_and_views** - Helper functions and analytics
   - Query helper functions
   - Analytics materialized views
   - Schema management utilities
   - Entity relationship functions

3. **m20240103_000003_advanced_features** - LLM and advanced capabilities
   - LLM infrastructure tables
   - Semantic search with embeddings
   - Event annotations and clustering
   - Retention policies and pipelines

**Important**: If you modify migrations, you must regenerate the SQLX cache!

### Migration Feature Flag

The migration system is behind a feature flag. It's enabled by default, but if you need to disable it:

```toml
# In Cargo.toml
[dependencies]
sinex-db = { version = "*", default-features = false }
```

### Programmatic Usage

While migrations are typically run via CLI, you can also run them programmatically:

```rust
use sinex_db;

// Run migrations
sinex_db::run_migrations(&pool).await?;

// Check pending migrations
let pending = sinex_db::migration::get_pending_migrations(&pool).await?;

// Get applied migrations
let applied = sinex_db::migration::get_applied_migrations(&pool).await?;
```

## Migration Best Practices

1. **Always provide down migrations** - Even if it's just dropping tables
2. **Test both up and down** - Ensure migrations are reversible
3. **Use transactions** - Migrations run in transactions by default
4. **Keep migrations focused** - One logical change per migration
5. **Never modify existing migrations** - Create new ones instead
6. **Document complex changes** - Add comments for non-obvious operations

## Troubleshooting

### Migration fails with "table already exists"

The database might have remnants from old sqlx migrations. Clean start:
```bash
just db-reset
just migrate
```

### Can't find migration files

Ensure you're in the project root:
```bash
cd /realm/project/sinex
just migrate
```

### Feature not enabled error

Add the migration feature to your Cargo.toml:
```toml
sinex-db = { version = "*", features = ["migration"] }
```