use chrono::{Duration, Utc};
use serde_json::json;
use sinex_test_utils::prelude::*;

#[sinex_test]
async fn pipeline_preserves_ingest_order_over_ts_orig(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_shared_nats().await?;
    let scope = ctx.pipeline_scope().await?;

    let source = "ordering-guard";
    let event_type = "ordering.event";
    let now = Utc::now();
    let earlier = now - Duration::seconds(30);
    let later = now + Duration::seconds(30);

    let first = scope
        .publish_with_overrides(
            source,
            event_type,
            json!({"seq": 1}),
            EventOverrides {
                ts_orig: Some(later.to_rfc3339()),
                ..Default::default()
            },
        )
        .await?;
    let second = scope
        .publish_with_overrides(
            source,
            event_type,
            json!({"seq": 2}),
            EventOverrides {
                ts_orig: Some(earlier.to_rfc3339()),
                ..Default::default()
            },
        )
        .await?;

    scope.wait_for_event_id(first.clone()).await?;
    scope.wait_for_event_id(second.clone()).await?;

    let rows = sqlx::query!(
        r#"
        SELECT id::uuid as id, ts_orig
        FROM core.events
        WHERE source = $1 AND event_type = $2
        ORDER BY id ASC
        "#,
        source,
        event_type
    )
    .fetch_all(&ctx.pool)
    .await?;

    ensure!(rows.len() == 2, "expected two events, got {}", rows.len());
    ensure!(
        rows[0].id == Some(first.to_uuid()),
        "expected first row to be the first published event"
    );
    ensure!(
        rows[1].id == Some(second.to_uuid()),
        "expected second row to be the second published event"
    );

    let first_ts = rows[0].ts_orig;
    let second_ts = rows[1].ts_orig;
    ensure!(
        first_ts > second_ts,
        "ts_orig should be out of order to validate ingest ordering"
    );

    scope.shutdown().await?;
    Ok(())
}
