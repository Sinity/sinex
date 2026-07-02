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

    let event_id = uuid::Uuid::now_v7();
    let material_id = ctx.ensure_schema_material(Some("/test/path")).await?;
    let _source_event_id = uuid::Uuid::now_v7();

    // Test 1: Valid case with source_material_id only
    sqlx::query!(
        "INSERT INTO core.events (id, source, event_type, host, payload, ts_orig, source_material_id, anchor_byte) VALUES ($1::uuid, $2, $3, $4, $5, $6, $7::uuid, $8)",
        event_id,
        "test-source",
        "test-event",
        "test-host",
        serde_json::json!({"test": "data"}),
        *sinex_primitives::temporal::now(),
        material_id,
        0i64
    ).execute(pool).await.unwrap();

    // Test 2: Valid case with source_event_ids only (need to create the referenced event first)
    let event_id2 = uuid::Uuid::now_v7();
    sqlx::query!(
        "INSERT INTO core.events (id, source, event_type, host, payload, ts_orig, source_event_ids) VALUES ($1::uuid, $2, $3, $4, $5, $6, $7::uuid[])",
        event_id2,
        "test-source",
        "test-event",
        "test-host",
        serde_json::json!({"test": "data"}),
        *sinex_primitives::temporal::now(),
        &[event_id][..]
    ).execute(pool).await.unwrap();

    // Test 3: Invalid case - both source_material_id AND source_event_ids
    let event_id3 = uuid::Uuid::now_v7();
    let result = sqlx::query!(
        "INSERT INTO core.events (id, source, event_type, host, payload, ts_orig, source_material_id, source_event_ids) VALUES ($1::uuid, $2, $3, $4, $5, $6, $7::uuid, $8::uuid[])",
        event_id3,
        "test-source",
        "test-event",
        "test-host",
        serde_json::json!({"test": "data"}),
        *sinex_primitives::temporal::now(),
        material_id,
        &[event_id][..]
    ).execute(pool).await;

    assert!(
        result.is_err(),
        "Should reject events with both provenance types"
    );

    // Test 4: Invalid case - neither source_material_id NOR source_event_ids
    let event_id4 = uuid::Uuid::now_v7();
    let result = sqlx::query!(
        "INSERT INTO core.events (id, source, event_type, host, payload, ts_orig) VALUES ($1::uuid, $2, $3, $4, $5, $6)",
        event_id4,
        "test-source",
        "test-event",
        "test-host",
        serde_json::json!({"test": "data"}),
        *sinex_primitives::temporal::now()
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
    let anchor_byte = 123i64;

    // Test source length constraint
    let event_id = uuid::Uuid::now_v7();
    let result = sqlx::query!(
        "INSERT INTO core.events (id, source, event_type, host, payload, ts_orig, source_material_id, anchor_byte) VALUES ($1::uuid, $2, $3, $4, $5, $6, $7::uuid, $8)",
        event_id,
        "   ", // Just whitespace - should fail
        "test-event",
        "test-host",
        serde_json::json!({"test": "data"}),
        *sinex_primitives::temporal::now(),
        material_id,
        anchor_byte
    ).execute(pool).await;

    assert!(
        result.is_err(),
        "Should reject empty/whitespace-only source"
    );

    // Test event_type length constraint
    let event_id2 = uuid::Uuid::now_v7();
    let result = sqlx::query!(
        "INSERT INTO core.events (id, source, event_type, host, payload, ts_orig, source_material_id, anchor_byte) VALUES ($1::uuid, $2, $3, $4, $5, $6, $7::uuid, $8)",
        event_id2,
        "valid-source",
        "", // Empty event_type - should fail
        "test-host",
        serde_json::json!({"test": "data"}),
        *sinex_primitives::temporal::now(),
        material_id,
        anchor_byte
    ).execute(pool).await;

    assert!(result.is_err(), "Should reject empty event_type");
    Ok(())
}
