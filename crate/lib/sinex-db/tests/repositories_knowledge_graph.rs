use sinex_db::repositories::{CreateEntity, DbPoolExt};
use sinex_primitives::Timestamp;
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn merge_entities_preserves_earliest_created_at_precision(
    ctx: TestContext,
) -> TestResult<()> {
    let repo = ctx.pool.knowledge_graph();
    let source = repo
        .create_entity(CreateEntity::person("source-person"))
        .await?;
    let target = repo
        .create_entity(CreateEntity::person("target-person"))
        .await?;

    let source_created_at = Timestamp::from_unix_timestamp_nanos(1_700_000_000_123_456_000)
        .expect("test timestamp should be valid");
    let target_created_at = Timestamp::from_unix_timestamp_nanos(1_700_000_100_654_321_000)
        .expect("test timestamp should be valid");

    sqlx::query!(
        r#"
        UPDATE core.entities
        SET created_at = $2, updated_at = $2
        WHERE id = $1
        "#,
        *source.id.as_uuid() as _,
        *source_created_at
    )
    .execute(&ctx.pool)
    .await?;

    sqlx::query!(
        r#"
        UPDATE core.entities
        SET created_at = $2, updated_at = $2
        WHERE id = $1
        "#,
        *target.id.as_uuid() as _,
        *target_created_at
    )
    .execute(&ctx.pool)
    .await?;

    repo.merge_entities(source.id, target.id).await?;

    let merged_target = repo
        .get_entity(target.id)
        .await?
        .expect("target entity should still exist after merge");
    let merged_source = repo
        .get_entity(source.id)
        .await?
        .expect("source entity should remain queryable after merge");

    assert_eq!(merged_target.created_at, source_created_at);
    assert!(merged_source.is_merged);
    assert_eq!(merged_source.merged_into_id, Some(target.id));
    Ok(())
}
