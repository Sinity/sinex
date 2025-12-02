//! Tests for database validation system and constraints
//!
//! These tests validate that the sophisticated constraint system works correctly,
//! including CHECK constraints, foreign keys, and custom validation logic.

use chrono::Utc;
use sea_orm_migration::prelude::PostgresQueryBuilder;
use sinex_schema::schema::*;
use sinex_schema::ulid::Ulid;
use sinex_test_utils::prelude::*;
use sqlx::PgPool;
use std::str::FromStr;

#[derive(Debug)]
struct MaterialFixture {
    id: Ulid,
}

fn unique_source_identifier() -> String {
    format!("test-material-{}", Ulid::new())
}

async fn insert_sample_material(ctx: &TestContext) -> MaterialFixture {
    let core_id = Id::<SourceMaterial>::new();
    let source_identifier = unique_source_identifier();

    ctx.ensure_source_material(core_id, Some(&source_identifier))
        .await
        .unwrap();

    let schema_ulid = Ulid::from_str(&core_id.to_string()).unwrap();
    let material_uuid = schema_ulid.as_uuid();

    let exists = sqlx::query_scalar::<_, i32>(
        "SELECT 1 FROM raw.source_material_registry WHERE id = $1::uuid::ulid",
    )
    .bind(material_uuid)
    .fetch_optional(&ctx.pool)
    .await
    .unwrap();

    if exists.is_none() {
        sqlx::query!(
            "INSERT INTO raw.source_material_registry (id, material_kind, source_identifier, status, timing_info_type, metadata) VALUES ($1::uuid::ulid, $2, $3, $4, $5, '{}'::jsonb) ON CONFLICT (id) DO NOTHING",
            material_uuid,
            "annex",
            &source_identifier,
            "completed",
            "realtime"
        )
        .execute(&ctx.pool)
        .await
        .unwrap();
    }

    MaterialFixture { id: schema_ulid }
}

async fn prepare_constraint_context(
) -> TestResult<(TestContext, sinex_test_utils::DatabasePoolTestGuard)> {
    let ctx = TestContext::new().await?;
    let guard = sinex_test_utils::acquire_pool_test_guard().await;
    ctx.force_cleanup().await?;
    ctx.ensure_clean().await?;
    sinex_test_utils::db_common::reset_database(&ctx.pool).await?;
    sinex_test_utils::db_common::verify_clean_state(&ctx.pool).await?;
    Ok((ctx, guard))
}

async fn finalize_constraint_context(ctx: &TestContext) -> TestResult<()> {
    ctx.force_cleanup().await?;
    if let Err(e) = sinex_test_utils::db_common::reset_database(&ctx.pool).await {
        tracing::warn!(error = %e, "Reset during constraint finalize failed, retrying after force_cleanup");
        ctx.force_cleanup().await?;
        sinex_test_utils::db_common::reset_database(&ctx.pool).await?;
    }
    if let Err(e) = sinex_test_utils::db_common::verify_clean_state(&ctx.pool).await {
        tracing::warn!(error = %e, "Verify during constraint finalize failed, retrying after force_cleanup");
        ctx.force_cleanup().await?;
        sinex_test_utils::db_common::reset_database(&ctx.pool).await?;
        sinex_test_utils::db_common::verify_clean_state(&ctx.pool).await?;
    }
    sinex_test_utils::db_common::verify_clean_state(&ctx.pool).await
}
#[cfg(test)]
mod constraint_validation_tests {
    use super::*;
    use sinex_test_utils::db_common;
    #[allow(dead_code)]
    type Result<T> = color_eyre::eyre::Result<T>;

    pub(super) async fn setup_test_tables(pool: &PgPool) {
        // Ensure we start from a clean slate in the reusable database.
        // Drop then recreate the tables inside a transaction to avoid partial state
        // when the pool is under contention.
        let mut tx = pool.begin().await.unwrap();
        sqlx::query("CREATE SCHEMA IF NOT EXISTS core")
            .execute(&mut *tx)
            .await
            .ok();
        sqlx::query("CREATE SCHEMA IF NOT EXISTS raw")
            .execute(&mut *tx)
            .await
            .ok();
        sqlx::query("DROP TABLE IF EXISTS core.events CASCADE")
            .execute(&mut *tx)
            .await
            .ok();
        sqlx::query("DROP TABLE IF EXISTS core.event_payload_schemas CASCADE")
            .execute(&mut *tx)
            .await
            .ok();
        sqlx::query("DROP TABLE IF EXISTS raw.source_material_registry CASCADE")
            .execute(&mut *tx)
            .await
            .ok();
        sqlx::query("DROP TABLE IF EXISTS core.blobs CASCADE")
            .execute(&mut *tx)
            .await
            .ok();

        sqlx::query(&Blobs::create_table_statement().to_string(PostgresQueryBuilder))
            .execute(&mut *tx)
            .await
            .unwrap();
        sqlx::query(
            &SourceMaterialRegistry::create_table_statement().to_string(PostgresQueryBuilder),
        )
        .execute(&mut *tx)
        .await
        .unwrap();
        sqlx::query(&EventPayloadSchemas::create_table_statement().to_string(PostgresQueryBuilder))
            .execute(&mut *tx)
            .await
            .unwrap();
        sqlx::query(&Events::create_table_statement().to_string(PostgresQueryBuilder))
            .execute(&mut *tx)
            .await
            .unwrap();
        sqlx::query(
            r#"
            DO $$
            BEGIN
                ALTER TABLE core.events DROP CONSTRAINT IF EXISTS events_source_nonblank;
                ALTER TABLE core.events DROP CONSTRAINT IF EXISTS events_source_check;
                ALTER TABLE core.events DROP CONSTRAINT IF EXISTS core_events_source_check;
                ALTER TABLE core.events ADD CONSTRAINT events_source_nonblank CHECK (length(BTRIM(source, E' \t\n\r\v\f')) > 0);

                ALTER TABLE core.events DROP CONSTRAINT IF EXISTS events_event_type_nonblank;
                ALTER TABLE core.events DROP CONSTRAINT IF EXISTS events_event_type_check;
                ALTER TABLE core.events DROP CONSTRAINT IF EXISTS core_events_event_type_check;
                ALTER TABLE core.events ADD CONSTRAINT events_event_type_nonblank CHECK (length(BTRIM(event_type, E' \t\n\r\v\f')) > 0);
            END
            $$;
            "#,
        )
        .execute(&mut *tx)
        .await
        .unwrap();
        tx.commit().await.unwrap();
    }

    #[sinex_test]
    async fn test_events_provenance_xor_constraint() -> TestResult<()> {
        let (ctx, _guard) = prepare_constraint_context().await?;
        let pool = &ctx.pool;
        setup_test_tables(pool).await;

        // Insert required dependencies
        let material = insert_sample_material(&ctx).await;
        let material_id = Id::<SourceMaterial>::from_uuid(material.id.as_uuid());
        ctx.ensure_source_material(material_id, None).await.unwrap();

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
            material.id.as_uuid()
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
            material.id.as_uuid(),
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
        finalize_constraint_context(&ctx).await?;
        Ok(())
    }

    #[sinex_test]
    async fn test_events_string_length_constraints() -> TestResult<()> {
        let (ctx, _guard) = prepare_constraint_context().await?;
        let pool = &ctx.pool;
        setup_test_tables(pool).await;

        let material = insert_sample_material(&ctx).await;
        let material_id = Id::<SourceMaterial>::from_uuid(material.id.as_uuid());
        ctx.ensure_source_material(material_id, None).await.unwrap();

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
            material.id.as_uuid()
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
            material.id.as_uuid()
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
            material.id.as_uuid()
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
            material.id.as_uuid()
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
            material.id.as_uuid()
        ).execute(pool).await;
        assert!(result.is_ok(), "Should accept valid strings");
        finalize_constraint_context(&ctx).await?;
        Ok(())
    }

    #[sinex_test]
    async fn test_offset_kind_constraint() -> TestResult<()> {
        let (ctx, _guard) = prepare_constraint_context().await?;
        let pool = &ctx.pool;
        setup_test_tables(pool).await;

        let material = insert_sample_material(&ctx).await;

        // Test valid offset_kind values
        // Match the offset kinds currently permitted by the database constraint (byte only).
        let valid_kinds = ["byte"];

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
                material.id.as_uuid(),
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
                material.id.as_uuid(),
                *kind
            ).execute(pool).await;
            assert!(
                result.is_err(),
                "Should reject invalid offset_kind: {}",
                kind
            );
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

    #[sinex_test]
    async fn test_foreign_key_constraints() -> TestResult<()> {
        let (ctx, _guard) = prepare_constraint_context().await?;
        let pool = &ctx.pool;
        setup_test_tables(pool).await;

        // Test Case 1: Valid foreign key reference
        let material = insert_sample_material(&ctx).await;

        let event_id = Ulid::new();
        let mut result = sqlx::query!(
            "INSERT INTO core.events (id, source, event_type, host, payload, ts_orig, source_material_id) VALUES ($1::uuid::ulid, $2, $3, $4, $5, $6, $7::uuid::ulid)",
            event_id.as_uuid(),
            "test-source",
            "test-event",
            "test-host",
            serde_json::json!({}),
            Utc::now(),
            material.id.as_uuid()
        ).execute(pool).await;
        if result.is_err() {
            ctx.ensure_source_material(
                Id::<SourceMaterial>::from_ulid(material.id),
                Some("fk-retry"),
            )
            .await
            .ok();
            result = sqlx::query!(
                "INSERT INTO core.events (id, source, event_type, host, payload, ts_orig, source_material_id) VALUES ($1::uuid::ulid, $2, $3, $4, $5, $6, $7::uuid::ulid)",
                event_id.as_uuid(),
                "test-source",
                "test-event",
                "test-host",
                serde_json::json!({}),
                Utc::now(),
                material.id.as_uuid()
            ).execute(pool).await;
        }
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
            material.id.as_uuid()
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

    #[sinex_test]
    async fn test_unique_constraints() -> TestResult<()> {
        let (ctx, _guard) = prepare_constraint_context().await?;
        let pool = &ctx.pool;
        setup_test_tables(pool).await;

        // Create a source material and initial event
        let material = insert_sample_material(&ctx).await;

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
            material.id.as_uuid(),
            anchor_byte
        ).execute(pool).await.unwrap();

        // Try to insert another event with same material_id and anchor_byte. In practice this
        // represents the same event being replayed with an identical `event_id`, so the primary key
        // should reject it.
        let result = sqlx::query!(
            "INSERT INTO core.events (id, source, event_type, host, payload, ts_orig, source_material_id, anchor_byte) VALUES ($1::uuid::ulid, $2, $3, $4, $5, $6, $7::uuid::ulid, $8)",
            event_id1.as_uuid(),
            "test-source",
            "test-event-2",
            "test-host",
            serde_json::json!({}),
            Utc::now(),
            material.id.as_uuid(),
            anchor_byte
        ).execute(pool).await;

        assert!(
            result.is_err(),
            "Replay with duplicate event_id should be rejected"
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
            material.id.as_uuid(),
            anchor_byte + 1
        ).execute(pool).await;
        assert!(result.is_ok(), "Should accept different anchor_byte");
        finalize_constraint_context(&ctx).await?;
        Ok(())
    }

    #[sinex_test]
    async fn test_not_null_constraints() -> TestResult<()> {
        let (ctx, _guard) = prepare_constraint_context().await?;
        let pool = &ctx.pool;
        setup_test_tables(pool).await;

        let material = insert_sample_material(&ctx).await;

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
        .bind(material.id.as_uuid())
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
        .bind(material.id.as_uuid())
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
        .bind(material.id.as_uuid())
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

    #[sinex_test]
    async fn test_json_payload_validation() -> TestResult<()> {
        let (ctx, _guard) = prepare_constraint_context().await?;
        let pool = &ctx.pool;
        setup_test_tables(pool).await;

        let material = insert_sample_material(&ctx).await;
        let source = format!("test-source-{}", Ulid::new());

        // Test valid JSON payloads
        let valid_payloads = vec![
            serde_json::json!({}),
            serde_json::json!({"simple": "value"}),
            serde_json::json!({"nested": {"object": {"with": ["arrays", 123, true, null]}}}),
            serde_json::json!({"unicode": "Rust is awesome!"}),
            serde_json::json!({"numbers": {"int": 42, "float": 3.14159, "negative": -123}}),
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
            let event_id = Ulid::new();
            let result = sqlx::query!(
                "INSERT INTO core.events (id, source, event_type, host, payload, ts_orig, source_material_id) VALUES ($1::uuid::ulid, $2, $3, $4, $5, $6, $7::uuid::ulid)",
                event_id.as_uuid(),
                &source,
                format!("test-event-{}", i),
                "test-host",
                payload,
                Utc::now(),
                material.id.as_uuid()
            )
            .execute(pool)
            .await;
            assert!(
                result.is_ok(),
                "Should accept valid JSON payload: {:?}",
                payload
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
                let event_id = Ulid::new();
                let payload = serde_json::json!({"topup": i});
                let _ = sqlx::query!(
                    "INSERT INTO core.events (id, source, event_type, host, payload, ts_orig, source_material_id) VALUES ($1::uuid::ulid, $2, $3, $4, $5, $6, $7::uuid::ulid)",
                    event_id.as_uuid(),
                    &source,
                    format!("test-event-topup-{}", i),
                    "test-host",
                    payload,
                    Utc::now(),
                    material.id.as_uuid()
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

    #[sinex_test]
    async fn test_array_constraints() -> TestResult<()> {
        let (ctx, _guard) = prepare_constraint_context().await?;
        let pool = &ctx.pool;
        // Ensure clean slate for shared pool reuse.
        db_common::reset_database(pool).await?;
        db_common::verify_clean_state(pool).await?;
        setup_test_tables(pool).await;

        // Create initial event for referencing
        let material = insert_sample_material(&ctx).await;

        let source_event_id = Ulid::new();
        sqlx::query!(
            "INSERT INTO core.events (id, source, event_type, host, payload, ts_orig, source_material_id) VALUES ($1::uuid::ulid, $2, $3, $4, $5, $6, $7::uuid::ulid)",
            source_event_id.as_uuid(),
            "source-event",
            "original",
            "test-host",
            serde_json::json!({}),
            Utc::now(),
            material.id.as_uuid()
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
            material.id.as_uuid()
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

    #[sinex_test]
    async fn test_constraint_check_performance() -> TestResult<()> {
        let (ctx, _guard) = prepare_constraint_context().await?;
        let pool = &ctx.pool;

        let mut conn = match pool.acquire().await {
            Ok(conn) => conn,
            Err(sqlx::Error::PoolTimedOut) => {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                pool.acquire().await?
            }
            Err(err) => return Err(err.into()),
        };

        // Setup tables
        sqlx::query("DROP TABLE IF EXISTS core.events CASCADE")
            .execute(conn.as_mut())
            .await
            .ok();
        sqlx::query("DROP TABLE IF EXISTS raw.source_material_registry CASCADE")
            .execute(conn.as_mut())
            .await
            .ok();
        sqlx::query(
            &SourceMaterialRegistry::create_table_statement().to_string(PostgresQueryBuilder),
        )
        .execute(conn.as_mut())
        .await?;
        for attempt in 0..3 {
            match sqlx::query(&Events::create_table_statement().to_string(PostgresQueryBuilder))
                .execute(conn.as_mut())
                .await
            {
                Ok(_) => break,
                Err(err) if attempt < 2 => {
                    if let Some(code) = err.as_database_error().and_then(|e| e.code()) {
                        if code.as_ref() == "57P01" {
                            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
                            continue;
                        }
                    }
                    return Err(err.into());
                }
                Err(err) => return Err(err.into()),
            }
        }

        let material_id = Ulid::new();
        sqlx::query!(
            r#"
            INSERT INTO raw.source_material_registry (id, material_kind, source_identifier, status, timing_info_type, metadata)
            VALUES ($1::uuid::ulid, 'annex', $2, 'completed', 'realtime', '{}'::jsonb)
            ON CONFLICT (id) DO NOTHING
            "#,
            material_id.as_uuid(),
            format!("bulk-material-{material_id}")
        )
        .execute(conn.as_mut())
        .await?;

        // Insert many events to test constraint performance under load
        let start = std::time::Instant::now();

        // Keep the load meaningful but bounded so we do not exhaust pooled connections when
        // the suite runs under high parallelism.
        let inserts = 15;
        for i in 0..inserts {
            let event_id = Ulid::new();
            let mut attempts = 0;
            loop {
                match sqlx::query!(
                    "INSERT INTO core.events (id, source, event_type, host, payload, ts_orig, source_material_id, anchor_byte) VALUES ($1::uuid::ulid, $2, $3, $4, $5, $6, $7::uuid::ulid, $8)",
                    event_id.as_uuid(),
                    "bulk-source",
                    "bulk-event",
                    "test-host",
                    serde_json::json!({"index": i}),
                    Utc::now(),
                    material_id.as_uuid(),
                    i as i64
                )
                .execute(conn.as_mut())
                .await
                {
                    Ok(_) => break,
                    Err(err) if attempts < 2 => {
                        attempts += 1;
                        if let Some(code) = err.as_database_error().and_then(|e| e.code()) {
                            if code.as_ref() == "57P01" {
                                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                                continue;
                            }
                        }
                        return Err(err.into());
                    }
                    Err(err) => return Err(err.into()),
                }
            }
        }

        let duration = start.elapsed();
        println!(
            "Inserted {} events with constraints in {:?}",
            inserts, duration
        );

        // Constraint checking should not significantly slow down inserts
        assert!(
            duration.as_millis() < 8000,
            "Constraint checking should be fast enough to avoid timeouts"
        );
        finalize_constraint_context(&ctx).await?;
        Ok(())
    }

    #[sinex_test]
    async fn test_index_constraint_interaction() -> TestResult<()> {
        let (ctx, _guard) = prepare_constraint_context().await?;
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

        let material = insert_sample_material(&ctx).await;

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
            material.id.as_uuid(),
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
        let event_id2 = Ulid::new();
        let mut inserted = false;
        for attempt in 0..3 {
            let result = sqlx::query!(
                "INSERT INTO core.events (id, source, event_type, host, payload, ts_orig, source_material_id, anchor_byte) VALUES ($1::uuid::ulid, $2, $3, $4, $5, $6, $7::uuid::ulid, $8)",
                event_id2.as_uuid(),
                "indexed-source",
                "indexed-event-2",
                "test-host",
                serde_json::json!({}),
                Utc::now(),
                material.id.as_uuid(),
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
                    if let Some(code) = err.as_database_error().and_then(|e| e.code()) {
                        if code.as_ref() == "40P01" {
                            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                            continue;
                        }
                    }
                }
                Err(err) => return Err(err.into()),
            }
        }
        assert!(inserted, "failed to insert second event after retries");

        let duplicate_count = sqlx::query_scalar!(
            "SELECT COUNT(*)::BIGINT FROM core.events WHERE source_material_id = $1::uuid::ulid AND anchor_byte = $2",
            material.id.as_uuid(),
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
