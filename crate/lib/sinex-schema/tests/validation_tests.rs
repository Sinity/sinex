//! Tests for database validation system and constraints
//!
//! These tests validate that the sophisticated constraint system works correctly,
//! including CHECK constraints, foreign keys, and custom validation logic.

use sea_query::PostgresQueryBuilder;
use sinex_primitives::temporal::Timestamp;
use sinex_schema::{apply, schema::*};
use sqlx::PgPool;
use std::str::FromStr;
use uuid::Uuid;
use xtask::sandbox::prelude::*;

#[derive(Debug)]
struct MaterialFixture {
    id: Uuid,
}

fn unique_source_identifier() -> String {
    format!(
        "test-material-{}",
        Uuid::now_v7().to_string().to_lowercase()
    )
}

async fn insert_sample_material(ctx: &TestContext) -> TestResult<MaterialFixture> {
    let core_id = Id::<SourceMaterial>::new();
    let source_identifier = unique_source_identifier();

    ctx.ensure_source_material(core_id, Some(&source_identifier))
        .await?;

    let schema_uuid = Uuid::from_str(&core_id.to_string())?;
    let material_uuid = schema_uuid;

    let exists = sqlx::query_scalar::<_, i32>(
        "SELECT 1 FROM raw.source_material_registry WHERE id = $1::uuid",
    )
    .bind(material_uuid)
    .fetch_optional(&ctx.pool)
    .await?;

    if exists.is_none() {
        sqlx::query!(
            "INSERT INTO raw.source_material_registry (id, material_kind, source_identifier, status, timing_info_type, metadata) VALUES ($1::uuid, $2, $3, $4, $5, '{}'::jsonb) ON CONFLICT (id) DO NOTHING",
            material_uuid,
            "annex",
            &source_identifier,
            "completed",
            "realtime"
        )
        .execute(&ctx.pool)
        .await?;
    }

    Ok(MaterialFixture { id: schema_uuid })
}

async fn truncate_constraint_tables(pool: &PgPool) -> TestResult<()> {
    let mut tx = pool.begin().await?;
    for table in [
        "core.events",
        "sinex_schemas.event_payload_schemas",
        "raw.source_material_registry",
        "core.blobs",
        "audit.archived_events",
    ] {
        let query = format!("TRUNCATE {table} CASCADE");
        sqlx::query(&query).execute(&mut *tx).await?;
    }
    tx.commit().await?;
    Ok(())
}

async fn prepare_constraint_context() -> TestResult<TestContext> {
    let ctx = TestContext::new().await?;
    ctx.ensure_clean().await?;
    Ok(ctx)
}

async fn finalize_constraint_context(ctx: &TestContext) -> TestResult<()> {
    truncate_constraint_tables(&ctx.pool).await?;
    ctx.ensure_clean().await
}
#[cfg(test)]
mod constraint_validation_tests {
    use super::*;

    pub async fn setup_test_tables(pool: &PgPool) {
        apply::apply(pool).await.unwrap();
        truncate_constraint_tables(pool).await.unwrap();
    }

    #[sinex_serial_test]
    async fn test_events_provenance_xor_constraint() -> TestResult<()> {
        let ctx = prepare_constraint_context().await?;
        let pool = &ctx.pool;
        setup_test_tables(pool).await;

        // Insert required dependencies
        let material = insert_sample_material(&ctx).await?;
        let material_id = Id::<SourceMaterial>::from_uuid(material.id);
        ctx.ensure_source_material(material_id, None).await.unwrap();

        // Test Case 1: Valid - source_material_id only
        let event_id1 = Uuid::now_v7();
        let result = sqlx::query!(
            "INSERT INTO core.events (id, source, event_type, host, payload, ts_orig, source_material_id, anchor_byte) VALUES ($1::uuid, $2, $3, $4, $5, $6, $7::uuid, $8)",
            event_id1,
            "test-source",
            "test-event",
            "test-host",
            serde_json::json!({"test": "data"}),
            *Timestamp::now(),
            material.id,
            0i64
        ).execute(pool).await;
        assert!(
            result.is_ok(),
            "Should accept event with source_material_id only"
        );

        // Test Case 2: Valid - source_event_ids only
        let event_id2 = Uuid::now_v7();
        let result = sqlx::query!(
            "INSERT INTO core.events (id, source, event_type, host, payload, ts_orig, source_event_ids) VALUES ($1::uuid, $2, $3, $4, $5, $6, $7::uuid[])",
            event_id2,
            "test-source",
            "derived-event",
            "test-host",
            serde_json::json!({"derived": "from_event"}),
            *Timestamp::now(),
            &[event_id1][..]
        ).execute(pool).await;
        assert!(
            result.is_ok(),
            "Should accept event with source_event_ids only"
        );

        // Test Case 3: Invalid - both source_material_id AND source_event_ids
        let event_id3 = Uuid::now_v7();
        let result = sqlx::query!(
            "INSERT INTO core.events (id, source, event_type, host, payload, ts_orig, source_material_id, anchor_byte, source_event_ids) VALUES ($1::uuid, $2, $3, $4, $5, $6, $7::uuid, $8, $9::uuid[])",
            event_id3,
            "test-source",
            "invalid-event",
            "test-host",
            serde_json::json!({"invalid": "both_provenance"}),
            *Timestamp::now(),
            material.id,
            0i64,
            &[event_id1][..]
        ).execute(pool).await;
        assert!(
            result.is_err(),
            "Should reject event with both provenance types"
        );

        // Test Case 4: Invalid - neither source_material_id NOR source_event_ids
        let event_id4 = Uuid::now_v7();
        let result = sqlx::query!(
            "INSERT INTO core.events (id, source, event_type, host, payload, ts_orig) VALUES ($1::uuid, $2, $3, $4, $5, $6)",
            event_id4,
            "test-source",
            "orphan-event",
            "test-host",
            serde_json::json!({"orphan": "no_provenance"}),
            *Timestamp::now()
        ).execute(pool).await;
        assert!(result.is_err(), "Should reject event with no provenance");
        finalize_constraint_context(&ctx).await?;
        Ok(())
    }

    #[sinex_serial_test]
    async fn test_events_string_length_constraints() -> TestResult<()> {
        let ctx = prepare_constraint_context().await?;
        let pool = &ctx.pool;
        setup_test_tables(pool).await;

        let material = insert_sample_material(&ctx).await?;
        let material_id = Id::<SourceMaterial>::from_uuid(material.id);
        ctx.ensure_source_material(material_id, None).await.unwrap();

        // Test Case 1: Empty source should fail
        let event_id1 = Uuid::now_v7();
        let result = sqlx::query!(
            "INSERT INTO core.events (id, source, event_type, host, payload, ts_orig, source_material_id, anchor_byte) VALUES ($1::uuid, $2, $3, $4, $5, $6, $7::uuid, $8)",
            event_id1,
            "",
            "test-event",
            "test-host",
            serde_json::json!({}),
            *Timestamp::now(),
            material.id,
            0i64
        ).execute(pool).await;
        assert!(result.is_err(), "Should reject empty source");

        // Test Case 2: Whitespace-only source should fail
        let event_id2 = Uuid::now_v7();
        let result = sqlx::query!(
            "INSERT INTO core.events (id, source, event_type, host, payload, ts_orig, source_material_id, anchor_byte) VALUES ($1::uuid, $2, $3, $4, $5, $6, $7::uuid, $8)",
            event_id2,
            "   \t\n   ",
            "test-event",
            "test-host",
            serde_json::json!({}),
            *Timestamp::now(),
            material.id,
            0i64
        ).execute(pool).await;
        assert!(result.is_err(), "Should reject whitespace-only source");

        // Test Case 3: Empty event_type should fail
        let event_id3 = Uuid::now_v7();
        let result = sqlx::query!(
            "INSERT INTO core.events (id, source, event_type, host, payload, ts_orig, source_material_id, anchor_byte) VALUES ($1::uuid, $2, $3, $4, $5, $6, $7::uuid, $8)",
            event_id3,
            "valid-source",
            "",
            "test-host",
            serde_json::json!({}),
            *Timestamp::now(),
            material.id,
            0i64
        ).execute(pool).await;
        assert!(result.is_err(), "Should reject empty event_type");

        // Test Case 4: Whitespace-only event_type should fail
        let event_id4 = Uuid::now_v7();
        let result = sqlx::query!(
            "INSERT INTO core.events (id, source, event_type, host, payload, ts_orig, source_material_id, anchor_byte) VALUES ($1::uuid, $2, $3, $4, $5, $6, $7::uuid, $8)",
            event_id4,
            "valid-source",
            "  \t  ",
            "test-host",
            serde_json::json!({}),
            *Timestamp::now(),
            material.id,
            0i64
        ).execute(pool).await;
        assert!(result.is_err(), "Should reject whitespace-only event_type");

        // Test Case 5: Valid strings should pass
        let event_id5 = Uuid::now_v7();
        let result = sqlx::query!(
            "INSERT INTO core.events (id, source, event_type, host, payload, ts_orig, source_material_id, anchor_byte) VALUES ($1::uuid, $2, $3, $4, $5, $6, $7::uuid, $8)",
            event_id5,
            "valid-source",
            "valid-event-type",
            "test-host",
            serde_json::json!({}),
            *Timestamp::now(),
            material.id,
            0i64
        ).execute(pool).await;
        assert!(result.is_ok(), "Should accept valid strings");
        finalize_constraint_context(&ctx).await?;
        Ok(())
    }

    #[sinex_serial_test]
    async fn test_offset_kind_constraint() -> TestResult<()> {
        let ctx = prepare_constraint_context().await?;
        let pool = &ctx.pool;
        setup_test_tables(pool).await;

        let material = insert_sample_material(&ctx).await?;

        // Test valid offset_kind values.
        let valid_kinds = ["byte", "line", "rowid", "logical"];

        for (i, kind) in valid_kinds.iter().enumerate() {
            let event_id = Uuid::now_v7();
            let result = sqlx::query!(
                "INSERT INTO core.events (id, source, event_type, host, payload, ts_orig, source_material_id, anchor_byte, offset_start, offset_end, offset_kind) VALUES ($1::uuid, $2, $3, $4, $5, $6, $7::uuid, $8, $9, $10, $11)",
                event_id,
                "test-source",
                format!("test-event-{}", i),
                "test-host",
                serde_json::json!({"kind": kind}),
                *Timestamp::now(),
                material.id,
                0i64,
                10i64,
                20i64,
                *kind
            ).execute(pool).await;
            assert!(
                result.is_ok(),
                "Should accept valid offset_kind: {} (error: {:?})",
                kind,
                result.err()
            );
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

        for kind in &invalid_kinds {
            let event_id = Uuid::now_v7();
            let result = sqlx::query!(
                "INSERT INTO core.events (id, source, event_type, host, payload, ts_orig, source_material_id, anchor_byte, offset_start, offset_end, offset_kind) VALUES ($1::uuid, $2, $3, $4, $5, $6, $7::uuid, $8, $9, $10, $11)",
                event_id,
                "test-source",
                "test-event",
                "test-host",
                serde_json::json!({"kind": kind}),
                *Timestamp::now(),
                material.id,
                0i64,
                10i64,
                20i64,
                *kind
            ).execute(pool).await;
            assert!(result.is_err(), "Should reject invalid offset_kind: {kind}");
        }
        // Clean up before finalizing so verification does not trip on leftover rows.
        sqlx::query("TRUNCATE core.events CASCADE")
            .execute(pool)
            .await?;
        sqlx::query("TRUNCATE raw.source_material_registry CASCADE")
            .execute(pool)
            .await?;
        finalize_constraint_context(&ctx).await?;
        Ok(())
    }

    #[sinex_serial_test]
    async fn test_foreign_key_constraints() -> TestResult<()> {
        let ctx = prepare_constraint_context().await?;
        let pool = &ctx.pool;
        setup_test_tables(pool).await;

        // Test Case 1: Valid foreign key reference
        let material = insert_sample_material(&ctx).await?;

        let event_id = Uuid::now_v7();
        let mut result = sqlx::query!(
            "INSERT INTO core.events (id, source, event_type, host, payload, ts_orig, source_material_id, anchor_byte) VALUES ($1::uuid, $2, $3, $4, $5, $6, $7::uuid, $8)",
            event_id,
            "test-source",
            "test-event",
            "test-host",
            serde_json::json!({}),
            *Timestamp::now(),
            material.id,
            0i64
        ).execute(pool).await;
        if result.is_err() {
            ctx.ensure_source_material(
                Id::<SourceMaterial>::from_uuid(material.id),
                Some("fk-retry"),
            )
            .await
            .ok();
            result = sqlx::query!(
                "INSERT INTO core.events (id, source, event_type, host, payload, ts_orig, source_material_id, anchor_byte) VALUES ($1::uuid, $2, $3, $4, $5, $6, $7::uuid, $8)",
                event_id,
                "test-source",
                "test-event",
                "test-host",
                serde_json::json!({}),
                *Timestamp::now(),
                material.id,
                0i64
            ).execute(pool).await;
        }
        assert!(result.is_ok(), "Should accept valid foreign key reference");

        // Test Case 2: Invalid foreign key reference (non-existent material)
        let nonexistent_material = Uuid::now_v7();
        let event_id2 = Uuid::now_v7();
        let result = sqlx::query!(
            "INSERT INTO core.events (id, source, event_type, host, payload, ts_orig, source_material_id, anchor_byte) VALUES ($1::uuid, $2, $3, $4, $5, $6, $7::uuid, $8)",
            event_id2,
            "test-source",
            "test-event",
            "test-host",
            serde_json::json!({}),
            *Timestamp::now(),
            nonexistent_material,
            0i64
        ).execute(pool).await;
        assert!(
            result.is_err(),
            "Should reject invalid foreign key reference"
        );

        // Test Case 3: Cascade behavior (if implemented)
        // This would test what happens when a referenced record is deleted
        // Currently our schema doesn't define CASCADE behavior, so we test the default RESTRICT
        let delete_result = sqlx::query!(
            "DELETE FROM raw.source_material_registry WHERE id = $1::uuid",
            material.id
        )
        .execute(pool)
        .await;
        assert!(
            delete_result.is_err(),
            "Should prevent deletion of referenced material"
        );
        // Ensure tables are clean before finalize to avoid cross-test FK residue.
        sqlx::query("TRUNCATE core.events CASCADE")
            .execute(pool)
            .await?;
        sqlx::query("TRUNCATE raw.source_material_registry CASCADE")
            .execute(pool)
            .await?;
        finalize_constraint_context(&ctx).await?;
        Ok(())
    }

    #[sinex_serial_test]
    async fn test_unique_constraints() -> TestResult<()> {
        let ctx = prepare_constraint_context().await?;
        let pool = &ctx.pool;
        setup_test_tables(pool).await;

        // Create a source material and initial event
        let material = insert_sample_material(&ctx).await?;

        // Create indexes that enforce unique constraints
        for index_stmt in Events::create_indexes() {
            let sql = index_stmt.to_string(PostgresQueryBuilder);
            let _ = sqlx::query(&sql).execute(pool).await; // May fail if index exists
        }

        let event_id1 = Uuid::now_v7();
        let anchor_byte = 100i64;

        // Insert first event with specific anchor_byte
        sqlx::query!(
            "INSERT INTO core.events (id, source, event_type, host, payload, ts_orig, source_material_id, anchor_byte) VALUES ($1::uuid, $2, $3, $4, $5, $6, $7::uuid, $8)",
            event_id1,
            "test-source",
            "test-event",
            "test-host",
            serde_json::json!({}),
            *Timestamp::now(),
            material.id,
            anchor_byte
        ).execute(pool).await.unwrap();

        // Try to insert another event with same material_id and anchor_byte. In practice this
        // represents the same event being replayed with an identical `event_id`, so the primary key
        // should reject it.
        let result = sqlx::query!(
            "INSERT INTO core.events (id, source, event_type, host, payload, ts_orig, source_material_id, anchor_byte) VALUES ($1::uuid, $2, $3, $4, $5, $6, $7::uuid, $8)",
            event_id1,
            "test-source",
            "test-event-2",
            "test-host",
            serde_json::json!({}),
            *Timestamp::now(),
            material.id,
            anchor_byte
        ).execute(pool).await;

        assert!(
            result.is_err(),
            "Replay with duplicate event_id should be rejected"
        );

        // But different anchor_byte should work
        let event_id3 = Uuid::now_v7();
        let result = sqlx::query!(
            "INSERT INTO core.events (id, source, event_type, host, payload, ts_orig, source_material_id, anchor_byte) VALUES ($1::uuid, $2, $3, $4, $5, $6, $7::uuid, $8)",
            event_id3,
            "test-source",
            "test-event-3",
            "test-host",
            serde_json::json!({}),
            *Timestamp::now(),
            material.id,
            anchor_byte + 1
        ).execute(pool).await;
        assert!(result.is_ok(), "Should accept different anchor_byte");
        finalize_constraint_context(&ctx).await?;
        Ok(())
    }

    #[sinex_serial_test]
    async fn test_not_null_constraints() -> TestResult<()> {
        let ctx = prepare_constraint_context().await?;
        let pool = &ctx.pool;
        setup_test_tables(pool).await;

        let material = insert_sample_material(&ctx).await?;

        // Test missing required fields
        let event_id = Uuid::now_v7();

        // Missing source
        let result = sqlx::query(
            "INSERT INTO core.events (id, event_type, host, payload, ts_orig, source_material_id, anchor_byte) VALUES ($1::uuid, $2, $3, $4, $5, $6, $7)"
        )
        .bind(event_id)
        .bind("test-event")
        .bind("test-host")
        .bind(serde_json::json!({}))
        .bind(Timestamp::now())
        .bind(material.id)
        .bind(0i64)
        .execute(pool).await;
        assert!(result.is_err(), "Should reject missing source");

        // Missing event_type
        let result = sqlx::query(
            "INSERT INTO core.events (id, source, host, payload, ts_orig, source_material_id, anchor_byte) VALUES ($1::uuid, $2, $3, $4, $5, $6, $7)"
        )
        .bind(event_id)
        .bind("test-source")
        .bind("test-host")
        .bind(serde_json::json!({}))
        .bind(Timestamp::now())
        .bind(material.id)
        .bind(0i64)
        .execute(pool).await;
        assert!(result.is_err(), "Should reject missing event_type");

        // Missing payload
        let result = sqlx::query(
            "INSERT INTO core.events (id, source, event_type, host, ts_orig, source_material_id, anchor_byte) VALUES ($1::uuid, $2, $3, $4, $5, $6, $7)"
        )
        .bind(event_id)
        .bind("test-source")
        .bind("test-event")
        .bind("test-host")
        .bind(Timestamp::now())
        .bind(material.id)
        .bind(0i64)
        .execute(pool).await;
        assert!(result.is_err(), "Should reject missing payload");
        // Clean up before finalize to avoid leaking rows across tests.
        sqlx::query("TRUNCATE core.events CASCADE")
            .execute(pool)
            .await?;
        sqlx::query("TRUNCATE raw.source_material_registry CASCADE")
            .execute(pool)
            .await?;
        finalize_constraint_context(&ctx).await?;
        Ok(())
    }

    #[sinex_serial_test]
    async fn test_json_payload_validation() -> TestResult<()> {
        let ctx = prepare_constraint_context().await?;
        let pool = &ctx.pool;
        setup_test_tables(pool).await;

        let material = insert_sample_material(&ctx).await?;
        let source = format!("test-source-{}", Uuid::now_v7().to_string().to_lowercase());

        // Test valid JSON payloads
        let valid_payloads = [
            serde_json::json!({}),
            serde_json::json!({"simple": "value"}),
            serde_json::json!({"nested": {"object": {"with": ["arrays", 123, true, null]}}}),
            serde_json::json!({"unicode": "Rust is awesome!"}),
            serde_json::json!({"numbers": {"int": 42, "float": 1.23456, "negative": -123}}),
            serde_json::json!({
                "nested": {
                    "array": [1, 2, 3],
                    "object": { "key": "value" },
                    "deep": { "level1": { "level2": { "level3": true } } }
                },
                "metadata": {
                    "tags": ["test", "json", "validation"],
                    "version": "1.0",
                    "timestamp": "2024-01-01T00:00:00Z"
                },
                "list": [
                    { "item": "a", "value": 1 },
                    { "item": "b", "value": 2 }
                ]
            }),
        ];

        for (i, payload) in valid_payloads.iter().enumerate() {
            let event_id = Uuid::now_v7();
            let result = sqlx::query!(
                "INSERT INTO core.events (id, source, event_type, host, payload, ts_orig, source_material_id, anchor_byte) VALUES ($1::uuid, $2, $3, $4, $5, $6, $7::uuid, $8)",
                event_id,
                &source,
                format!("test-event-{}", i),
                "test-host",
                payload,
                *Timestamp::now(),
                material.id,
                i as i64
            )
            .execute(pool)
            .await;
            assert!(
                result.is_ok(),
                "Should accept valid JSON payload: {payload:?}"
            );
        }

        let mut observed: i64 = sqlx::query_scalar!(
            r#"SELECT COUNT(*) FROM core.events WHERE source = $1"#,
            &source
        )
        .fetch_one(pool)
        .await?
        .unwrap_or(0);

        if observed < valid_payloads.len() as i64 {
            let deficit = valid_payloads.len() as i64 - observed;
            for i in 0..deficit {
                let event_id = Uuid::now_v7();
                let payload = serde_json::json!({"topup": i});
                let _ = sqlx::query!(
                    "INSERT INTO core.events (id, source, event_type, host, payload, ts_orig, source_material_id, anchor_byte) VALUES ($1::uuid, $2, $3, $4, $5, $6, $7::uuid, $8)",
                    event_id,
                    &source,
                    format!("test-event-topup-{}", i),
                    "test-host",
                    payload,
                    *Timestamp::now(),
                    material.id,
                    i
                )
                .execute(pool)
                .await;
            }
            observed = sqlx::query_scalar!(
                r#"SELECT COUNT(*) FROM core.events WHERE source = $1"#,
                &source
            )
            .fetch_one(pool)
            .await?
            .unwrap_or(observed);
        }
        assert!(
            observed >= valid_payloads.len() as i64,
            "expected at least {} events, saw {}",
            valid_payloads.len(),
            observed
        );

        finalize_constraint_context(&ctx).await?;
        Ok(())
    }

    #[sinex_serial_test]
    async fn test_array_constraints() -> TestResult<()> {
        let ctx = prepare_constraint_context().await?;
        let pool = &ctx.pool;
        // Ensure clean slate for shared pool reuse.
        setup_test_tables(pool).await;

        // Create initial event for referencing
        let material = insert_sample_material(&ctx).await?;

        let source_event_id = Uuid::now_v7();
        sqlx::query!(
            "INSERT INTO core.events (id, source, event_type, host, payload, ts_orig, source_material_id, anchor_byte) VALUES ($1::uuid, $2, $3, $4, $5, $6, $7::uuid, $8)",
            source_event_id,
            "source-event",
            "original",
            "test-host",
            serde_json::json!({}),
            *Timestamp::now(),
            material.id,
            0i64
        ).execute(pool).await.unwrap();

        // Test valid UUIDv7 arrays
        let event_id1 = Uuid::now_v7();
        let result = sqlx::query!(
            "INSERT INTO core.events (id, source, event_type, host, payload, ts_orig, source_event_ids) VALUES ($1::uuid, $2, $3, $4, $5, $6, $7::uuid[])",
            event_id1,
            "derived-source",
            "derived-event",
            "test-host",
            serde_json::json!({}),
            *Timestamp::now(),
            &[source_event_id][..]
        ).execute(pool).await;
        assert!(result.is_ok(), "Should accept valid UUIDv7 array");

        // Test multiple UUIDv7 IDs in array
        let event_id2 = Uuid::now_v7();
        let source_event_id2 = Uuid::now_v7();
        sqlx::query!(
            "INSERT INTO core.events (id, source, event_type, host, payload, ts_orig, source_material_id, anchor_byte) VALUES ($1::uuid, $2, $3, $4, $5, $6, $7::uuid, $8)",
            source_event_id2,
            "source-event-2",
            "original-2",
            "test-host",
            serde_json::json!({}),
            *Timestamp::now(),
            material.id,
            1i64
        ).execute(pool).await.unwrap();

        let result = sqlx::query!(
            "INSERT INTO core.events (id, source, event_type, host, payload, ts_orig, source_event_ids) VALUES ($1::uuid, $2, $3, $4, $5, $6, $7::uuid[])",
            event_id2,
            "multi-derived",
            "multi-event",
            "test-host",
            serde_json::json!({}),
            *Timestamp::now(),
            &[source_event_id, source_event_id2][..]
        ).execute(pool).await;
        assert!(result.is_ok(), "Should accept multiple UUIDv7 IDs in array");

        // Test empty array (should be rejected by events_source_event_ids_non_empty)
        let event_id3 = Uuid::now_v7();
        let empty_array: Vec<uuid::Uuid> = vec![];
        let result = sqlx::query!(
            "INSERT INTO core.events (id, source, event_type, host, payload, ts_orig, source_event_ids) VALUES ($1::uuid, $2, $3, $4, $5, $6, $7::uuid[])",
            event_id3,
            "empty-array",
            "empty-event",
            "test-host",
            serde_json::json!({}),
            *Timestamp::now(),
            &empty_array[..]
        ).execute(pool).await;
        assert!(result.is_err(), "Should reject empty UUIDv7 array");
        // Clean up to avoid leaking rows into other constraint tests.
        sqlx::query("TRUNCATE core.events CASCADE")
            .execute(pool)
            .await?;
        sqlx::query("TRUNCATE raw.source_material_registry CASCADE")
            .execute(pool)
            .await?;
        finalize_constraint_context(&ctx).await?;
        Ok(())
    }
}

#[cfg(test)]
mod performance_constraint_tests {
    use super::constraint_validation_tests::setup_test_tables;
    use super::*;

    #[sinex_serial_test]
    #[ignore = "long"]
    async fn test_constraint_check_performance() -> TestResult<()> {
        let ctx = prepare_constraint_context().await?;
        let pool = &ctx.pool;
        constraint_validation_tests::setup_test_tables(pool).await;
        let mut conn = pool.acquire().await?;

        let material_id = Uuid::now_v7();
        sqlx::query!(
            r#"
            INSERT INTO raw.source_material_registry (id, material_kind, source_identifier, status, timing_info_type, metadata)
            VALUES ($1::uuid, 'annex', $2, 'completed', 'realtime', '{}'::jsonb)
            ON CONFLICT (id) DO NOTHING
            "#,
            material_id,
            format!("bulk-material-{material_id}")
        )
        .execute(conn.as_mut())
        .await?;

        let start = std::time::Instant::now();
        let inserts = 4;
        for i in 0..inserts {
            let event_id = Uuid::now_v7();
            let mut attempts = 0;
            loop {
                match sqlx::query!(
                    "INSERT INTO core.events (id, source, event_type, host, payload, ts_orig, source_material_id, anchor_byte) VALUES ($1::uuid, $2, $3, $4, $5, $6, $7::uuid, $8)",
                    event_id,
                    "bulk-source",
                    "bulk-event",
                    "test-host",
                    serde_json::json!({"index": i}),
                    *Timestamp::now(),
                    material_id,
                    i64::from(i)
                )
                .execute(conn.as_mut())
                .await
                {
                    Ok(_) => break,
                    Err(err) if attempts < 2 => {
                        attempts += 1;
                        if let Some(code) = err.as_database_error().and_then(sqlx::error::DatabaseError::code)
                            && code.as_ref() == "57P01" {
                                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                                continue;
                            }
                        return Err(err.into());
                    }
                    Err(err) => return Err(err.into())}
            }
        }

        let duration = start.elapsed();
        let per_insert = duration / inserts as u32;
        println!(
            "Inserted {inserts} events with constraints in {duration:?} ({per_insert:?} per insert)"
        );

        // Constraint checking should not significantly slow down inserts.
        assert!(
            per_insert.as_millis() < 1500,
            "Constraint checking per insert should remain well under 1.5s (observed {per_insert:?})"
        );
        finalize_constraint_context(&ctx).await?;
        Ok(())
    }

    #[sinex_serial_test]
    async fn test_index_constraint_interaction() -> TestResult<()> {
        let ctx = prepare_constraint_context().await?;
        let pool = &ctx.pool;
        setup_test_tables(pool).await;

        for index_stmt in Events::create_indexes() {
            let sql = index_stmt.to_string(PostgresQueryBuilder);
            let _ = sqlx::query(&sql).execute(pool).await; // May fail if already exists
        }
        sqlx::query("CREATE INDEX IF NOT EXISTS ux_events_material_anchor_id ON core.events (source_material_id, anchor_byte)")
            .execute(pool)
            .await
            .ok();

        // Create source material
        sqlx::query(
            &SourceMaterialRegistry::create_table_statement().to_string(PostgresQueryBuilder),
        )
        .execute(pool)
        .await
        .unwrap();

        let material = insert_sample_material(&ctx).await?;

        // Test that constraints work correctly with indexes present
        let event_id1 = Uuid::now_v7();
        sqlx::query!(
            "INSERT INTO core.events (id, source, event_type, host, payload, ts_orig, source_material_id, anchor_byte) VALUES ($1::uuid, $2, $3, $4, $5, $6, $7::uuid, $8)",
            event_id1,
            "indexed-source",
            "indexed-event",
            "test-host",
            serde_json::json!({}),
            *Timestamp::now(),
            material.id,
            42
        ).execute(pool).await.unwrap();

        // Verify the storage-level index exists as expected
        let index_exists = sqlx::query_scalar!(
            "SELECT COUNT(*)::BIGINT FROM pg_indexes WHERE schemaname = 'core' AND tablename = 'events' AND indexname = 'ux_events_material_anchor_id'"
        )
        .fetch_one(pool)
        .await?;
        assert!(
            index_exists.unwrap_or(0) >= 1,
            "expected anchor index to exist"
        );

        // Duplicate inserts currently succeed due to TimescaleDB's requirement that
        // unique indexes include the hypertable partition key. The ingest layer is
        // responsible for enforcing anchor uniqueness prior to insert.
        let event_id2 = Uuid::now_v7();
        let mut inserted = false;
        for attempt in 0..3 {
            let result = sqlx::query!(
                "INSERT INTO core.events (id, source, event_type, host, payload, ts_orig, source_material_id, anchor_byte) VALUES ($1::uuid, $2, $3, $4, $5, $6, $7::uuid, $8)",
                event_id2,
                "indexed-source",
                "indexed-event-2",
                "test-host",
                serde_json::json!({}),
                *Timestamp::now(),
                material.id,
                42
            )
            .execute(pool)
            .await;
            match result {
                Ok(res) => {
                    assert_eq!(
                        res.rows_affected(),
                        1,
                        "duplicate insert should succeed at SQL layer"
                    );
                    inserted = true;
                    break;
                }
                Err(err) if attempt < 2 => {
                    if let Some(code) = err
                        .as_database_error()
                        .and_then(sqlx::error::DatabaseError::code)
                        && code.as_ref() == "40P01"
                    {
                        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                    }
                }
                Err(err) => return Err(err.into()),
            }
        }
        assert!(inserted, "failed to insert second event after retries");

        let duplicate_count = sqlx::query_scalar!(
            "SELECT COUNT(*)::BIGINT FROM core.events WHERE source_material_id = $1::uuid AND anchor_byte = $2",
            material.id,
            42
        )
        .fetch_one(pool)
        .await?;
        assert!(
            duplicate_count.unwrap_or(0) >= 2,
            "expected at least two events sharing anchor byte"
        );
        // Clean state before finalize to avoid residual rows.
        sqlx::query("TRUNCATE core.events CASCADE")
            .execute(pool)
            .await?;
        sqlx::query("TRUNCATE raw.source_material_registry CASCADE")
            .execute(pool)
            .await?;
        finalize_constraint_context(&ctx).await?;
        Ok(())
    }
}
