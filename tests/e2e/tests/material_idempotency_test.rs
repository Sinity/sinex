use serde_json::json;
use sinex_core::db::query_helpers::ulid_to_uuid;
use sinex_core::Ulid;
use sinex_test_utils::prelude::*;
use sinex_test_utils::timing_utils::{WaitHelpers, DEFAULT_WAIT_SECS};

#[sinex_test]
async fn material_stream_idempotency(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_shared_nats().await?;
    let scope = ctx.pipeline_scope().await?;
    let publisher = scope.publisher("material-idempotency");

    let material_id = Ulid::new();
    let slices = vec![b"alpha".to_vec(), b"beta".to_vec(), b"gamma".to_vec()];

    publisher
        .publish_material_stream_with_id(material_id, slices.clone())
        .await?;
    publisher
        .publish_material_stream_with_id(material_id, slices)
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
        "source identifier should reflect the test publisher"
    );

    let event_id = scope
        .publish(
            "material-idempotency",
            "material.ingested",
            json!({ "material_id": material_id.to_string() }),
        )
        .await?;
    scope.wait_for_event_id(event_id).await?;

    scope.shutdown().await?;
    Ok(())
}
