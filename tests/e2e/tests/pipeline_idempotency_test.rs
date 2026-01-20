use serde_json::json;
use sinex_core::{db::query_helpers::ulid_to_uuid, EventId, Ulid};
use sinex_test_utils::prelude::*;
use sinex_test_utils::timing_utils::{WaitHelpers, DEFAULT_WAIT_SECS};
use tokio::time::{timeout, Duration};
use tokio_stream::StreamExt;

async fn wait_for_single_row(ctx: &TestContext, event_ulid: Ulid) -> TestResult<()> {
    WaitHelpers::wait_for_condition(
        || {
            let pool = ctx.pool.clone();
            let ulid = event_ulid;
            async move {
                let rows = sqlx::query!(
                    "SELECT COUNT(*) as count FROM core.events WHERE id = $1::uuid::ulid",
                    ulid_to_uuid(ulid)
                )
                .fetch_one(&pool)
                .await?;
                Ok(rows.count.unwrap_or(0) == 1)
            }
        },
        DEFAULT_WAIT_SECS,
    )
    .await?;
    Ok(())
}

#[sinex_test]
async fn pipeline_rejects_duplicate_event_ids(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let scope = ctx.pipeline_scope().await?;
    let event_ulid = Ulid::new();
    let confirmation_subject = scope.subject(&format!("events.confirmations.{event_ulid}"));
    let mut confirmations = ctx.nats_client().subscribe(confirmation_subject).await?;

    let overrides = EventOverrides {
        id: Some(event_ulid),
        ..Default::default()
    };

    scope
        .publish_with_overrides(
            "integration-dup",
            "log.line",
            json!({"step": "first"}),
            overrides.clone(),
        )
        .await?;
    scope
        .publish_with_overrides(
            "integration-dup",
            "log.line",
            json!({"step": "duplicate"}),
            overrides,
        )
        .await?;

    scope.wait_for_event_id(EventId::from(event_ulid)).await?;
    wait_for_single_row(scope.ctx(), event_ulid).await?;

    let first = timeout(Duration::from_secs(5), confirmations.next())
        .await
        .map_err(|_| color_eyre::eyre::eyre!("confirmation wait timed out"))?
        .ok_or_else(|| color_eyre::eyre::eyre!("confirmation subscription closed"))?;
    assert!(
        !first.payload.is_empty(),
        "confirmation payload should not be empty"
    );
    assert!(
        timeout(Duration::from_millis(750), confirmations.next())
            .await
            .is_err(),
        "duplicate confirmations should not be emitted"
    );
    scope.shutdown().await?;
    Ok(())
}

#[sinex_test]
async fn pipeline_rejects_concurrent_duplicates(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let scope = ctx.pipeline_scope().await?;
    let publisher = scope.publisher("integration-dup-concurrent");

    let event_ulid = Ulid::new();
    let overrides = EventOverrides {
        id: Some(event_ulid),
        ..Default::default()
    };

    let mut tasks = Vec::new();
    for attempt in 0..8 {
        let publisher = publisher.clone();
        let overrides = overrides.clone();
        tasks.push(tokio::spawn(async move {
            publisher
                .publish_event_with_overrides("log.line", json!({ "attempt": attempt }), overrides)
                .await
        }));
    }

    for task in tasks {
        task.await??;
    }

    scope.wait_for_event_id(EventId::from(event_ulid)).await?;
    wait_for_single_row(scope.ctx(), event_ulid).await?;
    scope.shutdown().await?;
    Ok(())
}
