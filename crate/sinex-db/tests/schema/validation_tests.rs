//! Tests for database validation system and constraints
//!
//! These tests validate that the sophisticated constraint system works correctly,
//! including CHECK constraints, foreign keys, and custom validation logic.

use sea_query::PostgresQueryBuilder;
use sinex_primitives::temporal::Timestamp;
use sinex_db::schema::{apply, schema::*};
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

async fn insert_sample_material_with_total_bytes(
    ctx: &TestContext,
    total_bytes: i64,
) -> TestResult<MaterialFixture> {
    let material = insert_sample_material(ctx).await?;
    sqlx::query("UPDATE raw.source_material_registry SET total_bytes = $2 WHERE id = $1::uuid")
        .bind(material.id)
        .bind(total_bytes)
        .execute(&ctx.pool)
        .await?;
    Ok(material)
}

async fn insert_material_event(
    pool: &PgPool,
    material_id: Uuid,
    anchor_byte: i64,
    offset_start: Option<i64>,
    offset_end: Option<i64>,
    offset_kind: Option<&str>,
) -> Result<sqlx::postgres::PgQueryResult, sqlx::Error> {
    sqlx::query(
        r"
        INSERT INTO core.events (
            id, source, event_type, host, payload, ts_orig,
            source_material_id, anchor_byte, offset_start, offset_end, offset_kind
        )
        VALUES ($1::uuid, $2, $3, $4, $5, $6, $7::uuid, $8, $9, $10, $11)
        ",
    )
    .bind(Uuid::now_v7())
    .bind("test-source")
    .bind("test-material-bounds")
    .bind("test-host")
    .bind(serde_json::json!({}))
    .bind(Timestamp::now())
    .bind(material_id)
    .bind(anchor_byte)
    .bind(offset_start)
    .bind(offset_end)
    .bind(offset_kind)
    .execute(pool)
    .await
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
#[path = "validation_tests_constraint_validation_tests.rs"]
mod constraint_validation_tests;

#[cfg(test)]
#[path = "validation_tests_performance_constraint_tests.rs"]
mod performance_constraint_tests;
