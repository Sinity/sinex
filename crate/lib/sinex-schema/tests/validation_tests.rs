//! Tests for database validation system and constraints
//!
//! These tests validate that the sophisticated constraint system works correctly,
//! including CHECK constraints, foreign keys, and custom validation logic.

use chrono::Utc;
use sea_orm_migration::prelude::PostgresQueryBuilder;
use sinex_schema::schema::*;
use sinex_schema::ulid::Ulid;
use sinex_test_utils::{sinex_test, TestContext};
use sqlx::PgPool;
#[cfg(test)]
mod constraint_validation_tests {
    use super::*;
    #[allow(dead_code)]
    type Result<T> = color_eyre::eyre::Result<T>;

    async fn setup_test_tables(pool: &PgPool) {
        // Create all necessary tables for constraint testing
        sqlx::query(
            &SourceMaterialRegistry::create_table_statement().to_string(PostgresQueryBuilder),
        )
        .execute(pool)
        .await
        .unwrap();
        sqlx::query(&EventPayloadSchemas::create_table_statement().to_string(PostgresQueryBuilder))
            .execute(pool)
            .await
            .unwrap();
        sqlx::query(&Events::create_table_statement().to_string(PostgresQueryBuilder))
            .execute(pool)
            .await
            .unwrap();
        sqlx::query(&Blobs::create_table_statement().to_string(PostgresQueryBuilder))
            .execute(pool)
            .await
            .unwrap();
    }

    #[sinex_test]
    async fn test_events_provenance_xor_constraint() -> Result<()> {
        let ctx = TestContext::new().await.unwrap();
        let pool = &ctx.pool;
        setup_test_tables(pool).await;

        // Insert required dependencies
        let material_id = Ulid::new();
        sqlx::query!(
            "INSERT INTO raw.source_material_registry (id, material_kind, source_identifier, status, timing_info_type) VALUES ($1::uuid::ulid, $2, $3, $4, $5)",
            material_id.as_uuid(),
            "annex",
            "/test/path",
            "completed",
            "realtime"
        ).execute(pool).await.unwrap();

        // Test Case 1: Valid - source_material_id only
        let event_id1 = Ulid::new();
        let result = sqlx::query!(
            "INSERT INTO core.events (id, source, event_type, host, payload, ts_orig, source_material_id) VALUES ($1::uuid::ulid, $2, $3, $4, $5, $6, $7::uuid::ulid)",
            event_id1.as_uuid(),
            "test-source",
            "test-event",
            "test-host",
            serde_json::json!({"test": "data"}),
            Utc::now(),
            material_id.as_uuid()
        ).execute(pool).await;
        assert!(
            result.is_ok(),
            "Should accept event with source_material_id only"
        );

        // Test Case 2: Valid - source_event_ids only
        let event_id2 = Ulid::new();
        let result = sqlx::query!(
            "INSERT INTO core.events (id, source, event_type, host, payload, ts_orig, source_event_ids) VALUES ($1::uuid::ulid, $2, $3, $4, $5, $6, $7::uuid[]::ulid[])",
            event_id2.as_uuid(),
            "test-source",
            "derived-event",
            "test-host",
            serde_json::json!({"derived": "from_event"}),
            Utc::now(),
            &[event_id1.as_uuid()][..]
        ).execute(pool).await;
        assert!(
            result.is_ok(),
            "Should accept event with source_event_ids only"
        );

        // Test Case 3: Invalid - both source_material_id AND source_event_ids
        let event_id3 = Ulid::new();
        let result = sqlx::query!(
            "INSERT INTO core.events (id, source, event_type, host, payload, ts_orig, source_material_id, source_event_ids) VALUES ($1::uuid::ulid, $2, $3, $4, $5, $6, $7::uuid::ulid, $8::uuid[]::ulid[])",
            event_id3.as_uuid(),
            "test-source",
            "invalid-event",
            "test-host",
            serde_json::json!({"invalid": "both_provenance"}),
            Utc::now(),
            material_id.as_uuid(),
            &[event_id1.as_uuid()][..]
        ).execute(pool).await;
        assert!(
            result.is_err(),
            "Should reject event with both provenance types"
        );

        // Test Case 4: Invalid - neither source_material_id NOR source_event_ids
        let event_id4 = Ulid::new();
        let result = sqlx::query!(
            "INSERT INTO core.events (id, source, event_type, host, payload, ts_orig) VALUES ($1::uuid::ulid, $2, $3, $4, $5, $6)",
            event_id4.as_uuid(),
            "test-source",
            "orphan-event",
            "test-host",
            serde_json::json!({"orphan": "no_provenance"}),
            Utc::now()
        ).execute(pool).await;
        assert!(result.is_err(), "Should reject event with no provenance");
        Ok(())
    }

    #[sinex_test]
    async fn test_events_string_length_constraints() -> Result<()> {
        let ctx = TestContext::new().await.unwrap();
        let pool = &ctx.pool;
        setup_test_tables(pool).await;

        let material_id = Ulid::new();
        sqlx::query!(
            "INSERT INTO raw.source_material_registry (id, material_kind, source_identifier, status, timing_info_type) VALUES ($1::uuid::ulid, $2, $3, $4, $5)",
            material_id.as_uuid(),
            "annex",
            "/test/path",
            "completed",
            "realtime"
        ).execute(pool).await.unwrap();

        // Test Case 1: Empty source should fail
        let event_id1 = Ulid::new();
        let result = sqlx::query!(
            "INSERT INTO core.events (id, source, event_type, host, payload, ts_orig, source_material_id) VALUES ($1::uuid::ulid, $2, $3, $4, $5, $6, $7::uuid::ulid)",
            event_id1.as_uuid(),
            "",
            "test-event",
            "test-host",
            serde_json::json!({}),
            Utc::now(),
            material_id.as_uuid()
        ).execute(pool).await;
        assert!(result.is_err(), "Should reject empty source");

        // Test Case 2: Whitespace-only source should fail
        let event_id2 = Ulid::new();
        let result = sqlx::query!(
            "INSERT INTO core.events (id, source, event_type, host, payload, ts_orig, source_material_id) VALUES ($1::uuid::ulid, $2, $3, $4, $5, $6, $7::uuid::ulid)",
            event_id2.as_uuid(),
            "   \t\n   ",
            "test-event",
            "test-host",
            serde_json::json!({}),
            Utc::now(),
            material_id.as_uuid()
        ).execute(pool).await;
        assert!(result.is_err(), "Should reject whitespace-only source");

        // Test Case 3: Empty event_type should fail
        let event_id3 = Ulid::new();
        let result = sqlx::query!(
            "INSERT INTO core.events (id, source, event_type, host, payload, ts_orig, source_material_id) VALUES ($1::uuid::ulid, $2, $3, $4, $5, $6, $7::uuid::ulid)",
            event_id3.as_uuid(),
            "valid-source",
            "",
            "test-host",
            serde_json::json!({}),
            Utc::now(),
            material_id.as_uuid()
        ).execute(pool).await;
        assert!(result.is_err(), "Should reject empty event_type");

        // Test Case 4: Whitespace-only event_type should fail
        let event_id4 = Ulid::new();
        let result = sqlx::query!(
            "INSERT INTO core.events (id, source, event_type, host, payload, ts_orig, source_material_id) VALUES ($1::uuid::ulid, $2, $3, $4, $5, $6, $7::uuid::ulid)",
            event_id4.as_uuid(),
            "valid-source",
            "  \t  ",
            "test-host",
            serde_json::json!({}),
            Utc::now(),
            material_id.as_uuid()
        ).execute(pool).await;
        assert!(result.is_err(), "Should reject whitespace-only event_type");

        // Test Case 5: Valid strings should pass
        let event_id5 = Ulid::new();
        let result = sqlx::query!(
            "INSERT INTO core.events (id, source, event_type, host, payload, ts_orig, source_material_id) VALUES ($1::uuid::ulid, $2, $3, $4, $5, $6, $7::uuid::ulid)",
            event_id5.as_uuid(),
            "valid-source",
            "valid-event-type",
            "test-host",
            serde_json::json!({}),
            Utc::now(),
            material_id.as_uuid()
        ).execute(pool).await;
        assert!(result.is_ok(), "Should accept valid strings");
        Ok(())
    }

    #[sinex_test]
    async fn test_offset_kind_constraint() -> Result<()> {
        let ctx = TestContext::new().await.unwrap();
        let pool = &ctx.pool;
        setup_test_tables(pool).await;

        let material_id = Ulid::new();
        sqlx::query!(
            "INSERT INTO raw.source_material_registry (id, material_kind, source_identifier, status, timing_info_type) VALUES ($1::uuid::ulid, $2, $3, $4, $5)",
            material_id.as_uuid(),
            "annex",
            "/test/path",
            "completed",
            "realtime"
        ).execute(pool).await.unwrap();

        // Test valid offset_kind values
        let valid_kinds = ["byte", "line", "rowid", "logical"];

        for (i, kind) in valid_kinds.iter().enumerate() {
            let event_id = Ulid::new();
            let result = sqlx::query!(
                "INSERT INTO core.events (id, source, event_type, host, payload, ts_orig, source_material_id, offset_kind) VALUES ($1::uuid::ulid, $2, $3, $4, $5, $6, $7::uuid::ulid, $8)",
                event_id.as_uuid(),
                "test-source",
                format!("test-event-{}", i),
                "test-host",
                serde_json::json!({"kind": kind}),
                Utc::now(),
                material_id.as_uuid(),
                *kind
            ).execute(pool).await;
            assert!(result.is_ok(), "Should accept valid offset_kind: {}", kind);
        }

        // Test invalid offset_kind values
        let invalid_kinds = [
            "bytes",
            "lines",
            "character",
            "word",
            "paragraph",
            "invalid",
        ];

        for kind in invalid_kinds.iter() {
            let event_id = Ulid::new();
            let result = sqlx::query!(
                "INSERT INTO core.events (id, source, event_type, host, payload, ts_orig, source_material_id, offset_kind) VALUES ($1::uuid::ulid, $2, $3, $4, $5, $6, $7::uuid::ulid, $8)",
                event_id.as_uuid(),
                "test-source",
                "test-event",
                "test-host",
                serde_json::json!({"kind": kind}),
                Utc::now(),
                material_id.as_uuid(),
                *kind
            ).execute(pool).await;
            assert!(
                result.is_err(),
                "Should reject invalid offset_kind: {}",
                kind
            );
        }
        Ok(())
    }

    #[sinex_test]
    async fn test_foreign_key_constraints() -> Result<()> {
        let ctx = TestContext::new().await.unwrap();
        let pool = &ctx.pool;
        setup_test_tables(pool).await;

        // Test Case 1: Valid foreign key reference
        let material_id = Ulid::new();
        sqlx::query!(
            "INSERT INTO raw.source_material_registry (id, material_kind, source_identifier, status, timing_info_type) VALUES ($1::uuid::ulid, $2, $3, $4, $5)",
            material_id.as_uuid(),
            "annex",
            "/test/path",
            "completed",
            "realtime"
        ).execute(pool).await.unwrap();

        let event_id = Ulid::new();
        let result = sqlx::query!(
            "INSERT INTO core.events (id, source, event_type, host, payload, ts_orig, source_material_id) VALUES ($1::uuid::ulid, $2, $3, $4, $5, $6, $7::uuid::ulid)",
            event_id.as_uuid(),
            "test-source",
            "test-event",
            "test-host",
            serde_json::json!({}),
            Utc::now(),
            material_id.as_uuid()
        ).execute(pool).await;
        assert!(result.is_ok(), "Should accept valid foreign key reference");

        // Test Case 2: Invalid foreign key reference (non-existent material)
        let nonexistent_material = Ulid::new();
        let event_id2 = Ulid::new();
        let result = sqlx::query!(
            "INSERT INTO core.events (id, source, event_type, host, payload, ts_orig, source_material_id) VALUES ($1::uuid::ulid, $2, $3, $4, $5, $6, $7::uuid::ulid)",
            event_id2.as_uuid(),
            "test-source",
            "test-event",
            "test-host",
            serde_json::json!({}),
            Utc::now(),
            nonexistent_material.as_uuid()
        ).execute(pool).await;
        assert!(
            result.is_err(),
            "Should reject invalid foreign key reference"
        );

        // Test Case 3: Cascade behavior (if implemented)
        // This would test what happens when a referenced record is deleted
        // Currently our schema doesn't define CASCADE behavior, so we test the default RESTRICT
        let delete_result = sqlx::query!(
            "DELETE FROM raw.source_material_registry WHERE id = $1::uuid::ulid",
            material_id.as_uuid()
        )
        .execute(pool)
        .await;
        assert!(
            delete_result.is_err(),
            "Should prevent deletion of referenced material"
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_unique_constraints() -> Result<()> {
        let ctx = TestContext::new().await.unwrap();
        let pool = &ctx.pool;
        setup_test_tables(pool).await;

        // Create a source material and initial event
        let material_id = Ulid::new();
        sqlx::query!(
            "INSERT INTO raw.source_material_registry (id, material_kind, source_identifier, status, timing_info_type) VALUES ($1::uuid::ulid, $2, $3, $4, $5)",
            material_id.as_uuid(),
            "annex",
            "/test/path",
            "completed",
            "realtime"
        ).execute(pool).await.unwrap();

        // Create indexes that enforce unique constraints
        for index_stmt in Events::create_indexes() {
            let sql = index_stmt.to_string(PostgresQueryBuilder);
            let _ = sqlx::query(&sql).execute(pool).await; // May fail if index exists
        }

        let event_id1 = Ulid::new();
        let anchor_byte = 100i64;

        // Insert first event with specific anchor_byte
        sqlx::query!(
            "INSERT INTO core.events (id, source, event_type, host, payload, ts_orig, source_material_id, anchor_byte) VALUES ($1::uuid::ulid, $2, $3, $4, $5, $6, $7::uuid::ulid, $8)",
            event_id1.as_uuid(),
            "test-source",
            "test-event",
            "test-host",
            serde_json::json!({}),
            Utc::now(),
            material_id.as_uuid(),
            anchor_byte
        ).execute(pool).await.unwrap();

        // Try to insert another event with same material_id and anchor_byte
        let event_id2 = Ulid::new();
        let result = sqlx::query!(
            "INSERT INTO core.events (id, source, event_type, host, payload, ts_orig, source_material_id, anchor_byte) VALUES ($1::uuid::ulid, $2, $3, $4, $5, $6, $7::uuid::ulid, $8)",
            event_id2.as_uuid(),
            "test-source",
            "test-event-2",
            "test-host",
            serde_json::json!({}),
            Utc::now(),
            material_id.as_uuid(),
            anchor_byte
        ).execute(pool).await;

        // This should fail due to the unique constraint on (source_material_id, anchor_byte)
        assert!(
            result.is_err(),
            "Should reject duplicate (source_material_id, anchor_byte) combination"
        );

        // But different anchor_byte should work
        let event_id3 = Ulid::new();
        let result = sqlx::query!(
            "INSERT INTO core.events (id, source, event_type, host, payload, ts_orig, source_material_id, anchor_byte) VALUES ($1::uuid::ulid, $2, $3, $4, $5, $6, $7::uuid::ulid, $8)",
            event_id3.as_uuid(),
            "test-source",
            "test-event-3",
            "test-host",
            serde_json::json!({}),
            Utc::now(),
            material_id.as_uuid(),
            anchor_byte + 1
        ).execute(pool).await;
        assert!(result.is_ok(), "Should accept different anchor_byte");
        Ok(())
    }

    #[sinex_test]
    async fn test_not_null_constraints() -> Result<()> {
        let ctx = TestContext::new().await.unwrap();
        let pool = &ctx.pool;
        setup_test_tables(pool).await;

        let material_id = Ulid::new();
        sqlx::query!(
            "INSERT INTO raw.source_material_registry (id, material_kind, source_identifier, status, timing_info_type) VALUES ($1::uuid::ulid, $2, $3, $4, $5)",
            material_id.as_uuid(),
            "annex",
            "/test/path",
            "completed",
            "realtime"
        ).execute(pool).await.unwrap();

        // Test missing required fields
        let event_id = Ulid::new();

        // Missing source
        let result = sqlx::query(
            "INSERT INTO core.events (id, event_type, host, payload, ts_orig, source_material_id) VALUES ($1::uuid::ulid, $2, $3, $4, $5, $6)"
        )
        .bind(event_id.as_uuid())
        .bind("test-event")
        .bind("test-host")
        .bind(serde_json::json!({}))
        .bind(Utc::now())
        .bind(material_id.as_uuid())
        .execute(pool).await;
        assert!(result.is_err(), "Should reject missing source");

        // Missing event_type
        let result = sqlx::query(
            "INSERT INTO core.events (id, source, host, payload, ts_orig, source_material_id) VALUES ($1::uuid::ulid, $2, $3, $4, $5, $6)"
        )
        .bind(event_id.as_uuid())
        .bind("test-source")
        .bind("test-host")
        .bind(serde_json::json!({}))
        .bind(Utc::now())
        .bind(material_id.as_uuid())
        .execute(pool).await;
        assert!(result.is_err(), "Should reject missing event_type");

        // Missing payload
        let result = sqlx::query(
            "INSERT INTO core.events (id, source, event_type, host, ts_orig, source_material_id) VALUES ($1::uuid::ulid, $2, $3, $4, $5, $6)"
        )
        .bind(event_id.as_uuid())
        .bind("test-source")
        .bind("test-event")
        .bind("test-host")
        .bind(Utc::now())
        .bind(material_id.as_uuid())
        .execute(pool).await;
        assert!(result.is_err(), "Should reject missing payload");
        Ok(())
    }

    #[sinex_test]
    async fn test_json_payload_validation() -> Result<()> {
        let ctx = TestContext::new().await.unwrap();
        let pool = &ctx.pool;
        setup_test_tables(pool).await;

        let material_id = Ulid::new();
        sqlx::query!(
            "INSERT INTO raw.source_material_registry (id, material_kind, source_identifier, status, timing_info_type) VALUES ($1::uuid::ulid, $2, $3, $4, $5)",
            material_id.as_uuid(),
            "annex",
            "/test/path",
            "completed",
            "realtime"
        )
        .execute(pool)
        .await
        .unwrap();

        // Test valid JSON payloads
        let valid_payloads = vec![
            serde_json::json!({}),
            serde_json::json!({"simple": "value"}),
            serde_json::json!({"nested": {"object": {"with": ["arrays", 123, true, null]}}}),
            serde_json::json!({"unicode": "🦀 Rust is awesome! 你好世界"}),
            serde_json::json!({"numbers": {"int": 42, "float": 3.14159, "negative": -123}}),
        ];

        for (i, payload) in valid_payloads.iter().enumerate() {
            let event_id = Ulid::new();
            let result = sqlx::query!(
                "INSERT INTO core.events (id, source, event_type, host, payload, ts_orig, source_material_id) VALUES ($1::uuid::ulid, $2, $3, $4, $5, $6, $7::uuid::ulid)",
                event_id.as_uuid(),
                "test-source",
                format!("test-event-{}", i),
                "test-host",
                payload,
                Utc::now(),
                material_id.as_uuid()
            )
            .execute(pool)
            .await;
            assert!(
                result.is_ok(),
                "Should accept valid JSON payload: {:?}",
                payload
            );
        }

        Ok(())
    }

    #[sinex_test]
    async fn test_array_constraints() -> Result<()> {
        let ctx = TestContext::new().await.unwrap();
        let pool = &ctx.pool;
        setup_test_tables(pool).await;

        // Create initial event for referencing
        let material_id = Ulid::new();
        sqlx::query!(
            "INSERT INTO raw.source_material_registry (id, material_kind, source_identifier, status, timing_info_type) VALUES ($1::uuid::ulid, $2, $3, $4, $5)",
            material_id.as_uuid(),
            "annex",
            "/test/path",
            "completed",
            "realtime"
        ).execute(pool).await.unwrap();

        let source_event_id = Ulid::new();
        sqlx::query!(
            "INSERT INTO core.events (id, source, event_type, host, payload, ts_orig, source_material_id) VALUES ($1::uuid::ulid, $2, $3, $4, $5, $6, $7::uuid::ulid)",
            source_event_id.as_uuid(),
            "source-event",
            "original",
            "test-host",
            serde_json::json!({}),
            Utc::now(),
            material_id.as_uuid()
        ).execute(pool).await.unwrap();

        // Test valid ULID arrays
        let event_id1 = Ulid::new();
        let result = sqlx::query!(
            "INSERT INTO core.events (id, source, event_type, host, payload, ts_orig, source_event_ids) VALUES ($1::uuid::ulid, $2, $3, $4, $5, $6, $7::uuid[]::ulid[])",
            event_id1.as_uuid(),
            "derived-source",
            "derived-event",
            "test-host",
            serde_json::json!({}),
            Utc::now(),
            &[source_event_id.as_uuid()][..]
        ).execute(pool).await;
        assert!(result.is_ok(), "Should accept valid ULID array");

        // Test multiple ULIDs in array
        let event_id2 = Ulid::new();
        let source_event_id2 = Ulid::new();
        sqlx::query!(
            "INSERT INTO core.events (id, source, event_type, host, payload, ts_orig, source_material_id) VALUES ($1::uuid::ulid, $2, $3, $4, $5, $6, $7::uuid::ulid)",
            source_event_id2.as_uuid(),
            "source-event-2",
            "original-2",
            "test-host",
            serde_json::json!({}),
            Utc::now(),
            material_id.as_uuid()
        ).execute(pool).await.unwrap();

        let result = sqlx::query!(
            "INSERT INTO core.events (id, source, event_type, host, payload, ts_orig, source_event_ids) VALUES ($1::uuid::ulid, $2, $3, $4, $5, $6, $7::uuid[]::ulid[])",
            event_id2.as_uuid(),
            "multi-derived",
            "multi-event",
            "test-host",
            serde_json::json!({}),
            Utc::now(),
            &[source_event_id.as_uuid(), source_event_id2.as_uuid()][..]
        ).execute(pool).await;
        assert!(result.is_ok(), "Should accept multiple ULIDs in array");

        // Test empty array (should be valid)
        let event_id3 = Ulid::new();
        let empty_array: Vec<uuid::Uuid> = vec![];
        let result = sqlx::query!(
            "INSERT INTO core.events (id, source, event_type, host, payload, ts_orig, source_event_ids) VALUES ($1::uuid::ulid, $2, $3, $4, $5, $6, $7::uuid[]::ulid[])",
            event_id3.as_uuid(),
            "empty-array",
            "empty-event",
            "test-host",
            serde_json::json!({}),
            Utc::now(),
            &empty_array[..]
        ).execute(pool).await;
        assert!(result.is_ok(), "Should accept empty ULID array");
        Ok(())
    }
}

#[cfg(test)]
mod performance_constraint_tests {
    use super::*;

    #[sinex_test]
    async fn test_constraint_check_performance() -> Result<()> {
        let ctx = TestContext::new().await.unwrap();
        let pool = &ctx.pool;

        // Setup tables
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

        let material_id = Ulid::new();
        sqlx::query!(
            "INSERT INTO raw.source_material_registry (id, material_kind, source_identifier, status, timing_info_type) VALUES ($1::uuid::ulid, $2, $3, $4, $5)",
            material_id.as_uuid(),
            "annex",
            "/test/path",
            "completed",
            "realtime"
        ).execute(pool).await.unwrap();

        // Insert many events to test constraint performance under load
        let start = std::time::Instant::now();

        for i in 0..100 {
            let event_id = Ulid::new();
            sqlx::query!(
                "INSERT INTO core.events (id, source, event_type, host, payload, ts_orig, source_material_id, anchor_byte) VALUES ($1::uuid::ulid, $2, $3, $4, $5, $6, $7::uuid::ulid, $8)",
                event_id.as_uuid(),
                "bulk-source",
                "bulk-event",
                "test-host",
                serde_json::json!({"index": i}),
                Utc::now(),
                material_id.as_uuid(),
                i as i64
            ).execute(pool).await.unwrap();
        }

        let duration = start.elapsed();
        println!("Inserted 100 events with constraints in {:?}", duration);

        // Constraint checking should not significantly slow down inserts
        assert!(
            duration.as_millis() < 5000,
            "Constraint checking should be fast"
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_index_constraint_interaction() -> Result<()> {
        let ctx = TestContext::new().await.unwrap();
        let pool = &ctx.pool;

        // Setup tables with indexes
        sqlx::query(&Events::create_table_statement().to_string(PostgresQueryBuilder))
            .execute(pool)
            .await
            .unwrap();

        for index_stmt in Events::create_indexes() {
            let sql = index_stmt.to_string(PostgresQueryBuilder);
            let _ = sqlx::query(&sql).execute(pool).await; // May fail if already exists
        }

        // Create source material
        sqlx::query(
            &SourceMaterialRegistry::create_table_statement().to_string(PostgresQueryBuilder),
        )
        .execute(pool)
        .await
        .unwrap();

        let material_id = Ulid::new();
        sqlx::query!(
            "INSERT INTO raw.source_material_registry (id, material_kind, source_identifier, status, timing_info_type) VALUES ($1::uuid::ulid, $2, $3, $4, $5)",
            material_id.as_uuid(),
            "annex",
            "/test/path",
            "completed",
            "realtime"
        ).execute(pool).await.unwrap();

        // Test that constraints work correctly with indexes present
        let event_id1 = Ulid::new();
        sqlx::query!(
            "INSERT INTO core.events (id, source, event_type, host, payload, ts_orig, source_material_id, anchor_byte) VALUES ($1::uuid::ulid, $2, $3, $4, $5, $6, $7::uuid::ulid, $8)",
            event_id1.as_uuid(),
            "indexed-source",
            "indexed-event",
            "test-host",
            serde_json::json!({}),
            Utc::now(),
            material_id.as_uuid(),
            42
        ).execute(pool).await.unwrap();

        // Duplicate should still be rejected even with indexes
        let event_id2 = Ulid::new();
        let result = sqlx::query!(
            "INSERT INTO core.events (id, source, event_type, host, payload, ts_orig, source_material_id, anchor_byte) VALUES ($1::uuid::ulid, $2, $3, $4, $5, $6, $7::uuid::ulid, $8)",
            event_id2.as_uuid(),
            "indexed-source",
            "indexed-event-2",
            "test-host",
            serde_json::json!({}),
            Utc::now(),
            material_id.as_uuid(),
            42
        ).execute(pool).await;

        assert!(
            result.is_err(),
            "Unique constraint should work with indexes"
        );
        Ok(())
    }
}
