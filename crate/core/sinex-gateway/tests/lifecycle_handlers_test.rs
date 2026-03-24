//! Lifecycle handler regression coverage for persisted audit state and tombstone execution.

use serde_json::json;
use sinex_db::DbPoolExt;
use sinex_gateway::handlers::{
    handle_audit_get, handle_lifecycle_archive, handle_lifecycle_restore, handle_tombstone_approve,
    handle_tombstone_create,
};
use sinex_gateway::rpc_server::RpcAuthContext;
use sinex_primitives::events::DynamicPayload;
use sinex_primitives::rpc::audit::AuditGetResponse;
use sinex_primitives::rpc::lifecycle::{
    LifecycleArchiveResponse, LifecycleRestoreResponse, TombstoneApproveResponse,
    TombstoneCreateResponse,
};
use xtask::sandbox::prelude::*;

async fn publish_event(
    ctx: &TestContext,
    source: &str,
    sequence: i64,
) -> TestResult<sinex_primitives::events::Event<serde_json::Value>> {
    let material_id = ctx.create_source_material(Some(source)).await?;
    Ok(ctx
        .pool()
        .events()
        .insert(
            DynamicPayload::new(
                source,
                "test.lifecycle",
                json!({ "sequence": sequence }),
            )
            .from_material(material_id)
            .build()?,
        )
        .await?)
}

async fn archived_count(ctx: &TestContext, event_id: &str) -> TestResult<i64> {
    Ok(
        sqlx::query_scalar!(r#"SELECT COUNT(*)::bigint as "count!" FROM audit.archived_events WHERE id = $1::uuid"#, event_id.parse::<uuid::Uuid>()?)
            .fetch_one(ctx.pool())
            .await?,
    )
}

async fn tombstone_count(ctx: &TestContext, event_id: &str) -> TestResult<i64> {
    Ok(
        sqlx::query_scalar!(r#"SELECT COUNT(*)::bigint as "count!" FROM core.event_tombstones WHERE id = $1::uuid"#, event_id.parse::<uuid::Uuid>()?)
            .fetch_one(ctx.pool())
            .await?,
    )
}

#[sinex_test]
async fn archive_and_restore_operations_are_persisted_and_auditable(ctx: TestContext) -> TestResult<()> {
    let auth = RpcAuthContext::system();
    let event = publish_event(&ctx, "test.lifecycle.archive", 1).await?;
    let event_id = event.id.expect("published event should have an id").to_string();

    let archive_value = handle_lifecycle_archive(
        ctx.pool(),
        json!({
            "event_ids": [event_id.clone()],
            "dry_run": false,
            "reason": "archive regression test",
        }),
        &auth,
    )
    .await?;
    let archive: LifecycleArchiveResponse = serde_json::from_value(archive_value)?;
    assert_eq!(archive.archived_count, 1);

    let archive_audit: AuditGetResponse = serde_json::from_value(
        handle_audit_get(ctx.pool(), json!({ "operation_id": archive.operation_id })).await?,
    )?;
    assert_eq!(archive_audit.event_count, 1);
    assert_eq!(
        archive_audit.audit_trail.affected_events[0].id.to_string(),
        event_id
    );
    assert_eq!(archived_count(&ctx, &event_id).await?, 1);

    let restore_value = handle_lifecycle_restore(
        ctx.pool(),
        json!({
            "event_ids": [event_id.clone()],
            "dry_run": false,
        }),
        &auth,
    )
    .await?;
    let restore: LifecycleRestoreResponse = serde_json::from_value(restore_value)?;
    assert_eq!(restore.restored_count, 1);

    let restore_audit: AuditGetResponse = serde_json::from_value(
        handle_audit_get(ctx.pool(), json!({ "operation_id": restore.operation_id })).await?,
    )?;
    assert_eq!(restore_audit.event_count, 1);
    assert_eq!(
        restore_audit.audit_trail.affected_events[0].id.to_string(),
        event_id
    );
    assert_eq!(archived_count(&ctx, &event_id).await?, 0);

    Ok(())
}

#[sinex_test]
async fn tombstone_approve_uses_previewed_event_set_and_audits_tombstones(
    ctx: TestContext,
) -> TestResult<()> {
    let auth = RpcAuthContext::system();
    let source = "test.lifecycle.tombstone";
    let first = publish_event(&ctx, source, 1).await?;
    let first_id = first.id.expect("published first event should have an id").to_string();

    let archive_first: LifecycleArchiveResponse = serde_json::from_value(
        handle_lifecycle_archive(
            ctx.pool(),
            json!({
                "event_ids": [first_id.clone()],
                "dry_run": false,
                "reason": "prepare tombstone preview",
            }),
            &auth,
        )
        .await?,
    )?;
    assert_eq!(archive_first.archived_count, 1);

    let create: TombstoneCreateResponse = serde_json::from_value(
        handle_tombstone_create(
            ctx.pool(),
            json!({
                "source": source,
                "limit": 1,
                "reason": "preview exact archived set",
            }),
            &auth,
        )
        .await?,
    )?;

    let second = publish_event(&ctx, source, 2).await?;
    let second_id = second.id.expect("published second event should have an id").to_string();
    let archive_second: LifecycleArchiveResponse = serde_json::from_value(
        handle_lifecycle_archive(
            ctx.pool(),
            json!({
                "event_ids": [second_id.clone()],
                "dry_run": false,
                "reason": "introduce later archived sibling",
            }),
            &auth,
        )
        .await?,
    )?;
    assert_eq!(archive_second.archived_count, 1);

    let approve: TombstoneApproveResponse = serde_json::from_value(
        handle_tombstone_approve(
            ctx.pool(),
            json!({
                "operation_id": create.operation.operation_id,
                "yes_i_understand_data_is_gone": true,
            }),
            &auth,
        )
        .await?,
    )?;
    assert_eq!(approve.operation.tombstoned_count, Some(1));

    let audit: AuditGetResponse = serde_json::from_value(
        handle_audit_get(
            ctx.pool(),
            json!({ "operation_id": approve.operation.operation_id }),
        )
        .await?,
    )?;
    assert_eq!(audit.event_count, 1);
    assert_eq!(audit.audit_trail.affected_events[0].id.to_string(), first_id);
    assert_eq!(archived_count(&ctx, &first_id).await?, 0);
    assert_eq!(tombstone_count(&ctx, &first_id).await?, 1);
    assert_eq!(archived_count(&ctx, &second_id).await?, 1);
    assert_eq!(tombstone_count(&ctx, &second_id).await?, 0);

    Ok(())
}
