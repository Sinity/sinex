use serde_json::json;
use sinex_primitives::events::DynamicPayload;
use sinex_primitives::prelude::*;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn test_event_assert_supports_combined_source_and_type_count(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let _scope = ctx.pipeline().await?;

    let matching_source = EventSource::new(format!("event-assert-source-{}", Uuid::now_v7()))?;
    let other_source = EventSource::new(format!("event-assert-other-source-{}", Uuid::now_v7()))?;
    let matching_type = EventType::new(format!("event.assert.type.{}", Uuid::now_v7()))?;
    let other_type = EventType::new(format!("event.assert.other.{}", Uuid::now_v7()))?;

    ctx.publish(DynamicPayload::new(
        matching_source.as_str(),
        matching_type.as_str(),
        json!({ "match": 1 }),
    ))
    .await?;
    ctx.publish(DynamicPayload::new(
        matching_source.as_str(),
        matching_type.as_str(),
        json!({ "match": 2 }),
    ))
    .await?;
    ctx.publish(DynamicPayload::new(
        matching_source.as_str(),
        other_type.as_str(),
        json!({ "other_type": true }),
    ))
    .await?;
    ctx.publish(DynamicPayload::new(
        other_source.as_str(),
        matching_type.as_str(),
        json!({ "other_source": true }),
    ))
    .await?;

    ctx.timing()
        .wait_for_condition(
            || async {
                Ok::<bool, color_eyre::Report>(
                    ctx.pool
                        .events()
                        .count_by_source_and_event_type(&matching_source, &matching_type)
                        .await
                        .map(|count| count == 2)?,
                )
            },
            10,
        )
        .await?;

    let actual = ctx
        .assert_event()
        .source(matching_source.clone())
        .event_type(matching_type.clone())
        .count(2)
        .await?;

    assert_eq!(actual, 2);
    Ok(())
}

#[sinex_test]
async fn test_event_assert_combined_filters_work_with_default_await(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let _scope = ctx.pipeline().await?;

    let source = EventSource::new(format!("event-assert-await-source-{}", Uuid::now_v7()))?;
    let event_type = EventType::new(format!("event.assert.await.{}", Uuid::now_v7()))?;

    ctx.publish(DynamicPayload::new(
        source.as_str(),
        event_type.as_str(),
        json!({ "ok": true }),
    ))
    .await?;

    ctx.timing()
        .wait_for_condition(
            || async {
                Ok::<bool, color_eyre::Report>(
                    ctx.pool
                        .events()
                        .count_by_source_and_event_type(&source, &event_type)
                        .await
                        .map(|count| count == 1)?,
                )
            },
            10,
        )
        .await?;

    let actual = ctx
        .assert_event()
        .source(source)
        .event_type(event_type)
        .await?;

    assert_eq!(actual, 1);
    Ok(())
}
