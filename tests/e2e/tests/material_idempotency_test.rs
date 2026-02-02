use serde_json::json;
use sinex_db::query_helpers::ulid_to_uuid;
use sinex_primitives::{DynamicPayload, Ulid};
use xtask::sandbox::prelude::*;
use xtask::sandbox::timing::{WaitHelpers, DEFAULT_WAIT_SECS};

#[sinex_test]
async fn material_stream_idempotency(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let scope = ctx.pipeline().await?;

    let material_id = Ulid::new();

    // Publish event directly using DynamicPayload
    scope
        .publish(DynamicPayload::new(
            "material-idempotency",
            "material.stream",
            json!({ "material_id": material_id.to_string(), "data": "alpha" }),
        ))
        .await?;

    // Publish second event with same material_id (should be idempotent)
    scope
        .publish(DynamicPayload::new(
            "material-idempotency",
            "material.stream",
            json!({ "material_id": material_id.to_string(), "data": "beta" }),
        ))
        .await?;

    WaitHelpers::wait_for_condition(
        || {
            let pool = scope.ctx().pool.clone();
            async move {
                let row = sqlx::query!(
                    "SELECT COUNT(*) as count FROM raw.source_material_registry WHERE id = $1::uuid::ulid",
                    ulid_to_uuid(material_id)
                )
                .fetch_one(&pool)
                .await?;
                Ok(row.count.unwrap_or(0) == 1)
            }
        },
        DEFAULT_WAIT_SECS,
    )
    .await?;

    let material_row = sqlx::query!(
        "SELECT source_identifier FROM raw.source_material_registry WHERE id = $1::uuid::ulid",
        ulid_to_uuid(material_id)
    )
    .fetch_one(&scope.ctx().pool)
    .await?;

    assert!(
        material_row
            .source_identifier
            .contains("material-idempotency"),
        "source identifier should reflect the test source"
    );

    let event_id = scope
        .publish(DynamicPayload::new(
            "material-idempotency",
            "material.ingested",
            json!({ "material_id": material_id.to_string() }),
        ))
        .await?;
    scope.wait_for_event_id(event_id).await?;

    scope.shutdown().await?;
    Ok(())
}
