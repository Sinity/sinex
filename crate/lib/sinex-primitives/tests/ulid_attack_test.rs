use serde_json::json;
use sinex_db::DbPool;
use sinex_db::validation::{EventValidator, ValidationError};
use sinex_primitives::{DynamicPayload, Id, JsonValue, SourceMaterial, Timestamp, Ulid};
use time::Duration;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn ulid_duplicate_insert_is_rejected(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool().clone();
    let material = ctx.create_source_material(Some("ulid-collision")).await?;
    let collision_id = Ulid::new();

    insert_material_event(&pool, collision_id, material, json!({"seq": 1})).await?;

    let err = insert_material_event(&pool, collision_id, material, json!({"seq": 2})).await;
    assert!(
        err.is_err(),
        "second insert should fail due to ULID collision"
    );

    let message = format!("{:?}", err.unwrap_err());
    assert!(
        message.contains("duplicate key value") || message.contains("already exists"),
        "expected duplicate key violation, got: {message}"
    );

    Ok(())
}

#[sinex_test]
async fn event_validator_blocks_ulid_time_skew_attack() -> TestResult<()> {
    let validator = EventValidator::new();
    let future_ulid = Ulid::from_datetime(Timestamp::now() + Duration::hours(1));

    let mut event = DynamicPayload::new(
        "ulid-security",
        "time.attack",
        json!({"scenario": "future-id"}),
    )
    .from_material(Id::<SourceMaterial>::new())
    .build()?;

    event.id = Some(Id::from_ulid(future_ulid));
    event.ts_orig = Some(Timestamp::now());

    let err = validator
        .validate(&event)
        .expect_err("validator must reject attacked ULID");
    assert!(
        matches!(err, ValidationError::SecurityValidation(_)),
        "expected security validation error, got {err:?}"
    );

    Ok(())
}

async fn insert_material_event(
    pool: &DbPool,
    event_id: Ulid,
    material_id: Id<SourceMaterial>,
    payload: JsonValue,
) -> TestResult<()> {
    let now = *Timestamp::now();
    sqlx::query!(
        r#"
        INSERT INTO core.events (
            id, source, event_type, host, payload,
            ts_orig, source_material_id, anchor_byte, offset_start, offset_end, offset_kind
        ) VALUES (
            $1::uuid, $2, $3, $4, $5,
            $6, $7::uuid, $8, $9, $10, 'byte'
        )
        "#,
        event_id.to_uuid(),
        "ulid-attack",
        "test.event",
        "ulid-security-suite",
        payload,
        now,
        material_id.as_ulid().to_uuid(),
        0,
        0,
        0
    )
    .execute(pool)
    .await?;

    Ok(())
}
