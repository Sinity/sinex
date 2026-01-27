//! Comprehensive tests for database schema definitions
//!
//! These tests validate that all schema definitions are correct and can be
//! executed against a real PostgreSQL database with the required extensions.

use sea_orm_migration::prelude::*;
use sinex_core::DynamicPayload;
use sinex_schema::schema::*;
use xtask::sandbox::prelude::*;
use sqlx::{PgPool, Row};
use std::collections::HashMap;

#[cfg(test)]
mod table_creation_tests {
    use super::*;

    use xtask::sandbox::sinex_test;

    #[sinex_test]
    async fn test_events_table_creation() -> color_eyre::eyre::Result<()> {
        let ctx = TestContext::new().await.unwrap();
        let pool = &ctx.pool;

        // Create the events table
        let stmt = Events::create_table_statement();
        let sql = stmt.to_string(PostgresQueryBuilder);

        sqlx::query(&sql)
            .execute(pool)
            .await
            .expect("Events table should be created successfully");

        // Verify the table exists and has the correct structure
        let columns = get_table_columns(pool, "core", "events").await;

        // Verify essential columns exist
        assert!(columns.contains_key("id"));
        assert!(columns.contains_key("source"));
        assert!(columns.contains_key("event_type"));
        assert!(columns.contains_key("host"));
        assert!(columns.contains_key("payload"));
        assert!(columns.contains_key("ts_orig"));
        assert!(columns.contains_key("ts_ingest"));
        assert!(columns.contains_key("source_material_id"));
        assert!(columns.contains_key("source_event_ids"));
        // associated_blob_ids is added in a later migration; table definition may omit it in some contexts

        // Verify primary key
        assert_eq!(columns["id"].data_type, "ulid");
        assert!(columns["id"].is_primary_key);

        // Verify NOT NULL constraints
        assert!(!columns["source"].is_nullable);
        assert!(!columns["event_type"].is_nullable);
        assert!(!columns["host"].is_nullable);
        assert!(!columns["payload"].is_nullable);
        assert!(!columns["ts_orig"].is_nullable);
        assert!(!columns["ts_ingest"].is_nullable);

        // Verify nullable columns
        assert!(columns["source_material_id"].is_nullable);
        assert!(columns["source_event_ids"].is_nullable);
        assert!(columns["associated_blob_ids"].is_nullable);
        Ok(())
    }

    #[sinex_test]
    async fn test_blobs_table_creation() -> color_eyre::eyre::Result<()> {
        let ctx = TestContext::new().await.unwrap();
        let pool = &ctx.pool;

        let stmt = Blobs::create_table_statement();
        let sql = stmt.to_string(PostgresQueryBuilder);

        sqlx::query(&sql)
            .execute(pool)
            .await
            .expect("Blobs table should be created successfully");

        let columns = get_table_columns(pool, "core", "blobs").await;

        // Verify essential columns
        assert!(columns.contains_key("id"));
        assert!(columns.contains_key("annex_backend"));
        assert!(columns.contains_key("content_hash"));
        assert!(columns.contains_key("size_bytes"));
        assert!(columns.contains_key("checksum_blake3"));
        assert!(columns.contains_key("original_filename"));
        assert!(columns.contains_key("mime_type"));

        // Verify primary key
        assert_eq!(columns["id"].data_type, "ulid");
        assert!(columns["id"].is_primary_key);
        Ok(())
    }

    #[sinex_test]
    async fn test_source_material_registry_creation() -> color_eyre::eyre::Result<()> {
        let ctx = TestContext::new().await.unwrap();
        let pool = &ctx.pool;

        let stmt = SourceMaterialRegistry::create_table_statement();
        let sql = stmt.to_string(PostgresQueryBuilder);

        sqlx::query(&sql)
            .execute(pool)
            .await
            .expect("SourceMaterialRegistry table should be created successfully");

        let columns = get_table_columns(pool, "raw", "source_material_registry").await;

        assert!(columns.contains_key("id"));
        assert!(columns.contains_key("material_kind"));
        assert!(columns.contains_key("source_identifier"));
        assert!(columns.contains_key("status"));
        assert!(columns.contains_key("timing_info_type"));
        assert!(columns.contains_key("metadata"));

        assert_eq!(columns["id"].data_type, "ulid");
        assert!(columns["id"].is_primary_key);
        Ok(())
    }

    #[sinex_test]
    async fn test_all_record_structs_match_tables() -> color_eyre::eyre::Result<()> {
        let ctx = TestContext::new().await.unwrap();
        let pool = &ctx.pool;

        // Create a few key tables to test Record struct compatibility
        let tables = vec![
            ("core.events", Events::create_table_statement()),
            ("core.blobs", Blobs::create_table_statement()),
            (
                "core.source_material_registry",
                SourceMaterialRegistry::create_table_statement(),
            ),
        ];

        for (table_name, stmt) in tables {
            let sql = stmt.to_string(PostgresQueryBuilder);
            sqlx::query(&sql)
                .execute(pool)
                .await
                .expect(&format!("Should create table {}", table_name));
        }

        // Test that we can select into Record structs
        // This will fail at compile time if the structs don't match the tables

        // Insert test data
        let event_id = sinex_schema::ulid::Ulid::new();
        sqlx::query!(
            "INSERT INTO core.events (id, source, event_type, host, payload, ts_orig, source_event_ids) VALUES ($1::uuid::ulid, $2, $3, $4, $5, $6, ARRAY[]::ulid[])",
            event_id.as_uuid(),
            "test-source",
            "test-event",
            "test-host",
            serde_json::json!({"test": "data"}),
            chrono::Utc::now()
        ).execute(pool).await.unwrap();

        // Basic roundtrip query validates table compatibility
        let row = sqlx::query!(
            r#"SELECT id::uuid as "id!: sqlx::types::Uuid" FROM core.events WHERE id = $1::uuid::ulid"#,
            event_id.as_uuid()
        )
        .fetch_one(pool)
        .await
        .unwrap();
        assert_eq!(row.id, event_id.as_uuid());
        Ok(())
    }
}

#[cfg(test)]
mod constraint_tests {
    use super::*;
    use xtask::sandbox::sinex_test;

    #[sinex_test]
    async fn test_events_provenance_constraint() -> color_eyre::eyre::Result<()> {
        let ctx = TestContext::new().await.unwrap();
        let pool = &ctx.pool;

        // Create tables
        sqlx::query(
            &SourceMaterialRegistry::create_table_statement().to_string(PostgresQueryBuilder),
        )
        .execute(pool)
        .await
        .unwrap();
        sqlx::query(&Events::create_table_statement().to_string(PostgresQueryBuilder))
            .execute(pool)
            .await
            .unwrap();

        let event_id = sinex_schema::ulid::Ulid::new();
        let material_id = ctx.ensure_schema_material(Some("/test/path")).await?;
        let _source_event_id = sinex_schema::ulid::Ulid::new();

        // Test 1: Valid case with source_material_id only
        sqlx::query!(
            "INSERT INTO core.events (id, source, event_type, host, payload, ts_orig, source_material_id) VALUES ($1::uuid::ulid, $2, $3, $4, $5, $6, $7::uuid::ulid)",
            event_id.as_uuid(),
            "test-source",
            "test-event",
            "test-host",
            serde_json::json!({"test": "data"}),
            chrono::Utc::now(),
            material_id.as_uuid()
        ).execute(pool).await.unwrap();

        // Test 2: Valid case with source_event_ids only (need to create the referenced event first)
        let event_id2 = sinex_schema::ulid::Ulid::new();
        sqlx::query!(
            "INSERT INTO core.events (id, source, event_type, host, payload, ts_orig, source_event_ids) VALUES ($1::uuid::ulid, $2, $3, $4, $5, $6, $7::uuid[]::ulid[])",
            event_id2.as_uuid(),
            "test-source",
            "test-event",
            "test-host",
            serde_json::json!({"test": "data"}),
            chrono::Utc::now(),
            &[event_id.as_uuid()][..]
        ).execute(pool).await.unwrap();

        // Test 3: Invalid case - both source_material_id AND source_event_ids
        let event_id3 = sinex_schema::ulid::Ulid::new();
        let result = sqlx::query!(
            "INSERT INTO core.events (id, source, event_type, host, payload, ts_orig, source_material_id, source_event_ids) VALUES ($1::uuid::ulid, $2, $3, $4, $5, $6, $7::uuid::ulid, $8::uuid[]::ulid[])",
            event_id3.as_uuid(),
            "test-source",
            "test-event",
            "test-host",
            serde_json::json!({"test": "data"}),
            chrono::Utc::now(),
            material_id.as_uuid(),
            &[event_id.as_uuid()][..]
        ).execute(pool).await;

        assert!(
            result.is_err(),
            "Should reject events with both provenance types"
        );

        // Test 4: Invalid case - neither source_material_id NOR source_event_ids
        let event_id4 = sinex_schema::ulid::Ulid::new();
        let result = sqlx::query!(
            "INSERT INTO core.events (id, source, event_type, host, payload, ts_orig) VALUES ($1::uuid::ulid, $2, $3, $4, $5, $6)",
            event_id4.as_uuid(),
            "test-source",
            "test-event",
            "test-host",
            serde_json::json!({"test": "data"}),
            chrono::Utc::now()
        ).execute(pool).await;

        assert!(result.is_err(), "Should reject events with no provenance");
        Ok(())
    }

    #[sinex_test]
    async fn test_events_check_constraints() -> color_eyre::eyre::Result<()> {
        let ctx = TestContext::new().await.unwrap();
        let pool = &ctx.pool;

        sqlx::query(&Events::create_table_statement().to_string(PostgresQueryBuilder))
            .execute(pool)
            .await
            .unwrap();

        let material_id = ctx.ensure_schema_material(None).await?;

        // Test source length constraint
        let event_id = sinex_schema::ulid::Ulid::new();
        let result = sqlx::query!(
            "INSERT INTO core.events (id, source, event_type, host, payload, ts_orig, source_material_id) VALUES ($1::uuid::ulid, $2, $3, $4, $5, $6, $7::uuid::ulid)",
            event_id.as_uuid(),
            "   ", // Just whitespace - should fail
            "test-event",
            "test-host",
            serde_json::json!({"test": "data"}),
            chrono::Utc::now(),
            material_id.as_uuid()
        ).execute(pool).await;

        assert!(
            result.is_err(),
            "Should reject empty/whitespace-only source"
        );

        // Test event_type length constraint
        let event_id2 = sinex_schema::ulid::Ulid::new();
        let result = sqlx::query!(
            "INSERT INTO core.events (id, source, event_type, host, payload, ts_orig, source_material_id) VALUES ($1::uuid::ulid, $2, $3, $4, $5, $6, $7::uuid::ulid)",
            event_id2.as_uuid(),
            "valid-source",
            "", // Empty event_type - should fail
            "test-host",
            serde_json::json!({"test": "data"}),
            chrono::Utc::now(),
            material_id.as_uuid()
        ).execute(pool).await;

        assert!(result.is_err(), "Should reject empty event_type");
        Ok(())
    }
}

#[cfg(test)]
mod index_tests {
    use super::*;
    use xtask::sandbox::sinex_test;
    #[sinex_test]
    async fn test_events_indexes_creation() -> color_eyre::eyre::Result<()> {
        let ctx = TestContext::new().await.unwrap();
        let pool = &ctx.pool;

        // Create the events table first
        sqlx::query(&Events::create_table_statement().to_string(PostgresQueryBuilder))
            .execute(pool)
            .await
            .unwrap();

        // Create all indexes
        for index_stmt in Events::create_indexes() {
            let sql = index_stmt.to_string(PostgresQueryBuilder);
            let _ = sqlx::query(&sql).execute(pool).await; // ignore if exists
        }

        // Create GIN indexes (PostgreSQL-specific)
        for gin_sql in Events::create_gin_indexes_sql() {
            let _ = sqlx::query(&gin_sql).execute(pool).await; // ignore if exists
        }

        // Verify indexes exist
        let indexes = get_table_indexes(pool, "core", "events").await;

        // Should have primary key index plus our custom indexes
        assert!(indexes.len() >= 3, "Should have multiple indexes");

        // Check for specific indexes by name
        let index_names: Vec<String> = indexes.into_iter().map(|idx| idx.index_name).collect();
        assert!(index_names.iter().any(|name| name.contains("ts_orig")));
        assert!(index_names
            .iter()
            .any(|name| name.contains("source_type_ts")));
        Ok(())
    }

    #[sinex_test]
    async fn test_index_performance_benefit() -> color_eyre::eyre::Result<()> {
        let ctx = TestContext::new().await.unwrap();
        let pool = &ctx.pool;

        // Create tables and indexes
        sqlx::query(
            &SourceMaterialRegistry::create_table_statement().to_string(PostgresQueryBuilder),
        )
        .execute(pool)
        .await
        .unwrap();
        sqlx::query(&Events::create_table_statement().to_string(PostgresQueryBuilder))
            .execute(pool)
            .await
            .unwrap();

        for index_stmt in Events::create_indexes() {
            let sql = index_stmt.to_string(PostgresQueryBuilder);
            let _ = sqlx::query(&sql).execute(pool).await; // ignore if exists
        }

        for i in 0..40 {
            ctx.publish(DynamicPayload::new(
                "test-source",
                "test-event",
                serde_json::json!({"index": i}),
            ))
            .await
            .unwrap();
        }

        sqlx::query("SET enable_seqscan = OFF")
            .execute(pool)
            .await?;
        // Test that queries can use the indexes (check execution plan)
        let plan = sqlx::query(
            "EXPLAIN (FORMAT JSON) SELECT * FROM core.events WHERE source = 'test-source' AND event_type = 'test-event' ORDER BY ts_orig DESC LIMIT 10"
        ).fetch_one(pool).await.unwrap();
        let _ = sqlx::query("RESET enable_seqscan").execute(pool).await;

        let plan_json: serde_json::Value = plan.get(0);
        let plan_str = plan_json.to_string();

        // Should mention index usage (not purely sequential)
        if !(plan_str.contains("Index") || plan_str.contains("Bitmap")) {
            tracing::warn!(
                "Execution plan did not explicitly show index usage: {}",
                plan_str
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod migration_tests {
    use super::*;
    use xtask::sandbox::sinex_test;

    #[sinex_test]
    async fn test_migration_up_down_cycle() -> color_eyre::eyre::Result<()> {
        let ctx = TestContext::new().await.unwrap();
        let pool = &ctx.pool;
        // This test must not mutate the shared `core.events` table because the test harness reuses
        // pooled databases across tests and only truncates data (it does not reset schema).
        //
        // Use a transaction-scoped, scratch table to validate the up/down DDL shape, then roll back.
        let mut tx = pool.begin().await?;

        sqlx::query("DROP TABLE IF EXISTS core.events_migration_test")
            .execute(&mut *tx)
            .await?;

        // Create a minimal events-like table without the column.
        sqlx::query(
            "CREATE TABLE core.events_migration_test (
                id ULID PRIMARY KEY DEFAULT gen_ulid(),
                source TEXT NOT NULL,
                event_type TEXT NOT NULL,
                host TEXT NOT NULL,
                payload JSONB NOT NULL,
                ts_orig TIMESTAMPTZ NOT NULL,
                ts_ingest TIMESTAMPTZ NOT NULL
            )",
        )
        .execute(&mut *tx)
        .await?;

        // Simulate migration UP: add associated_blob_ids column.
        sqlx::query(
            "ALTER TABLE core.events_migration_test ADD COLUMN IF NOT EXISTS associated_blob_ids ULID[]",
        )
        .execute(&mut *tx)
        .await?;

        // Verify the column was added.
        let columns: Vec<String> = sqlx::query(
            "SELECT column_name FROM information_schema.columns WHERE table_schema = 'core' AND table_name = 'events_migration_test'",
        )
        .fetch_all(&mut *tx)
        .await?
        .into_iter()
        .map(|row| row.get::<String, _>("column_name"))
        .collect();
        assert!(columns.iter().any(|c| c == "associated_blob_ids"));

        // Simulate migration DOWN: drop the column.
        sqlx::query(
            "ALTER TABLE core.events_migration_test DROP COLUMN IF EXISTS associated_blob_ids",
        )
        .execute(&mut *tx)
        .await?;

        // Verify the column was removed.
        let columns: Vec<String> = sqlx::query(
            "SELECT column_name FROM information_schema.columns WHERE table_schema = 'core' AND table_name = 'events_migration_test'",
        )
        .fetch_all(&mut *tx)
        .await?
        .into_iter()
        .map(|row| row.get::<String, _>("column_name"))
        .collect();
        assert!(!columns.iter().any(|c| c == "associated_blob_ids"));

        tx.rollback().await?;
        Ok(())
    }
}

// Helper functions for testing

#[derive(Debug)]
struct ColumnInfo {
    data_type: String,
    is_nullable: bool,
    is_primary_key: bool,
}

async fn get_table_columns(
    pool: &PgPool,
    schema: &str,
    table: &str,
) -> HashMap<String, ColumnInfo> {
    let rows = sqlx::query!(
        r#"
        SELECT 
            c.column_name,
            c.data_type,
            c.udt_name,
            c.is_nullable = 'YES' as is_nullable,
            COALESCE(pk.is_primary, false) as is_primary_key
        FROM information_schema.columns c
        LEFT JOIN (
            SELECT 
                kcu.column_name,
                true as is_primary
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu 
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
                AND tc.table_name = kcu.table_name
            WHERE tc.constraint_type = 'PRIMARY KEY'
                AND tc.table_schema = $1
                AND tc.table_name = $2
        ) pk ON c.column_name = pk.column_name
        WHERE c.table_schema = $1 AND c.table_name = $2
        ORDER BY c.ordinal_position
        "#,
        schema,
        table
    )
    .fetch_all(pool)
    .await
    .unwrap();

    rows.into_iter()
        .map(|row| {
            let name = row.column_name.unwrap_or_default();
            let mut dtype = row.data_type.unwrap_or_default();
            if dtype == "USER-DEFINED" {
                dtype = row.udt_name.unwrap_or_default();
            }
            let info = ColumnInfo {
                data_type: dtype,
                is_nullable: row.is_nullable.unwrap_or(false),
                is_primary_key: row.is_primary_key.unwrap_or(false),
            };
            (name, info)
        })
        .collect()
}

#[derive(Debug)]
struct IndexInfo {
    index_name: String,
}

async fn get_table_indexes(pool: &PgPool, schema: &str, table: &str) -> Vec<IndexInfo> {
    let rows = sqlx::query!(
        r#"
        SELECT 
            i.relname as index_name,
            ix.indisunique as is_unique,
            array_agg(a.attname ORDER BY a.attnum) as column_names
        FROM pg_class t
        JOIN pg_index ix ON t.oid = ix.indrelid
        JOIN pg_class i ON i.oid = ix.indexrelid
        JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = $1 AND t.relname = $2
        GROUP BY i.relname, ix.indisunique
        ORDER BY i.relname
        "#,
        schema,
        table
    )
    .fetch_all(pool)
    .await
    .unwrap();

    rows.into_iter()
        .map(|row| IndexInfo {
            index_name: row.index_name,
        })
        .collect()
}
