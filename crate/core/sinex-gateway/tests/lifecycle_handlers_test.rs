//! Lifecycle handler regression coverage for persisted audit state and tombstone execution.

use serde_json::json;
use sinex_db::DbPoolExt;
use sinex_gateway::handlers::{
    handle_audit_get, handle_lifecycle_archive, handle_lifecycle_restore, handle_tombstone_approve,
    handle_tombstone_cancel, handle_tombstone_create, handle_tombstone_list,
    handle_tombstone_status,
};
use sinex_gateway::rpc_server::RpcAuthContext;
use sinex_primitives::events::DynamicPayload;
use sinex_primitives::rpc::audit::AuditGetResponse;
use sinex_primitives::rpc::lifecycle::{
    LifecycleArchiveResponse, LifecycleRestoreResponse, TombstoneApproveResponse,
    TombstoneCancelResponse, TombstoneCreateResponse, TombstoneListResponse,
    TombstoneOperationState, TombstoneStatusResponse,
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
            DynamicPayload::new(source, "test.lifecycle", json!({ "sequence": sequence }))
                .from_material(material_id)
                .build()?,
        )
        .await?)
}

async fn archived_count(ctx: &TestContext, event_id: &str) -> TestResult<i64> {
    Ok(sqlx::query_scalar!(
        r#"SELECT COUNT(*)::bigint as "count!" FROM audit.archived_events WHERE id = $1::uuid"#,
        event_id.parse::<uuid::Uuid>()?
    )
    .fetch_one(ctx.pool())
    .await?)
}

async fn tombstone_count(ctx: &TestContext, event_id: &str) -> TestResult<i64> {
    Ok(sqlx::query_scalar!(
        r#"SELECT COUNT(*)::bigint as "count!" FROM core.event_tombstones WHERE id = $1::uuid"#,
        event_id.parse::<uuid::Uuid>()?
    )
    .fetch_one(ctx.pool())
    .await?)
}

#[sinex_test]
async fn archive_and_restore_operations_are_persisted_and_auditable(
    ctx: TestContext,
) -> TestResult<()> {
    let auth = RpcAuthContext::system();
    let event = publish_event(&ctx, "test.lifecycle.archive", 1).await?;
    let event_id = event
        .id
        .expect("published event should have an id")
        .to_string();

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
        archive_audit.audit_trail.operation.operator,
        auth.actor_id()
    );
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
        restore_audit.audit_trail.operation.operator,
        auth.actor_id()
    );
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
    let first_id = first
        .id
        .expect("published first event should have an id")
        .to_string();

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
    let second_id = second
        .id
        .expect("published second event should have an id")
        .to_string();
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
    assert_eq!(approve.operation.created_by, auth.actor_id());
    assert_eq!(
        approve.operation.approved_by.as_deref(),
        Some(auth.actor_id())
    );

    let audit: AuditGetResponse = serde_json::from_value(
        handle_audit_get(
            ctx.pool(),
            json!({ "operation_id": approve.operation.operation_id }),
        )
        .await?,
    )?;
    assert_eq!(audit.event_count, 1);
    assert_eq!(
        audit.audit_trail.affected_events[0].id.to_string(),
        first_id
    );
    assert_eq!(archived_count(&ctx, &first_id).await?, 0);
    assert_eq!(tombstone_count(&ctx, &first_id).await?, 1);
    assert_eq!(archived_count(&ctx, &second_id).await?, 1);
    assert_eq!(tombstone_count(&ctx, &second_id).await?, 0);

    Ok(())
}

#[sinex_test]
async fn tombstone_cancel_persists_terminal_metadata(ctx: TestContext) -> TestResult<()> {
    let auth = RpcAuthContext::system();
    let source = "test.lifecycle.tombstone.cancel";
    let event = publish_event(&ctx, source, 1).await?;
    let event_id = event
        .id
        .expect("published event should have an id")
        .to_string();

    let archive: LifecycleArchiveResponse = serde_json::from_value(
        handle_lifecycle_archive(
            ctx.pool(),
            json!({
                "event_ids": [event_id],
                "dry_run": false,
                "reason": "prepare tombstone cancel",
            }),
            &auth,
        )
        .await?,
    )?;
    assert_eq!(archive.archived_count, 1);

    let created: TombstoneCreateResponse = serde_json::from_value(
        handle_tombstone_create(
            ctx.pool(),
            json!({
                "source": source,
                "reason": "cancel me",
            }),
            &auth,
        )
        .await?,
    )?;

    let cancelled: TombstoneCancelResponse = serde_json::from_value(
        handle_tombstone_cancel(
            ctx.pool(),
            json!({
                "operation_id": created.operation.operation_id,
                "reason": "operator requested stop",
            }),
            &auth,
        )
        .await?,
    )?;
    assert_eq!(cancelled.status, "cancelled");

    let status: TombstoneStatusResponse = serde_json::from_value(
        handle_tombstone_status(
            ctx.pool(),
            json!({ "operation_id": created.operation.operation_id }),
            &auth,
        )
        .await?,
    )?;
    assert_eq!(status.operation.state, TombstoneOperationState::Cancelled);
    assert_eq!(status.operation.created_by, auth.actor_id());
    assert!(status.operation.finished_at.is_some());
    assert_eq!(
        status.operation.error_details.as_deref(),
        Some("Cancelled by system:local: operator requested stop")
    );

    let persisted_duration_ms: i32 = sqlx::query_scalar!(
        r#"SELECT duration_ms as "duration_ms!" FROM core.operations_log WHERE id = $1::uuid"#,
        created.operation.operation_id.parse::<uuid::Uuid>()?
    )
    .fetch_one(ctx.pool())
    .await?;
    assert!(persisted_duration_ms >= 0);

    Ok(())
}

#[sinex_test]
async fn tombstone_expiry_persists_terminal_metadata(ctx: TestContext) -> TestResult<()> {
    let auth = RpcAuthContext::system();
    let source = "test.lifecycle.tombstone.expiry";
    let event = publish_event(&ctx, source, 1).await?;
    let event_id = event
        .id
        .expect("published event should have an id")
        .to_string();

    let archive: LifecycleArchiveResponse = serde_json::from_value(
        handle_lifecycle_archive(
            ctx.pool(),
            json!({
                "event_ids": [event_id],
                "dry_run": false,
                "reason": "prepare tombstone expiry",
            }),
            &auth,
        )
        .await?,
    )?;
    assert_eq!(archive.archived_count, 1);

    let created: TombstoneCreateResponse = serde_json::from_value(
        handle_tombstone_create(
            ctx.pool(),
            json!({
                "source": source,
                "reason": "expire me",
            }),
            &auth,
        )
        .await?,
    )?;

    sqlx::query!(
        r#"
        UPDATE core.operations_log
        SET scope = jsonb_set(scope, '{expires_at}', to_jsonb($2::text), false)
        WHERE id = $1::uuid
        "#,
        created.operation.operation_id.parse::<uuid::Uuid>()?,
        "2000-01-01T00:00:00Z"
    )
    .execute(ctx.pool())
    .await?;

    let status: TombstoneStatusResponse = serde_json::from_value(
        handle_tombstone_status(
            ctx.pool(),
            json!({ "operation_id": created.operation.operation_id }),
            &auth,
        )
        .await?,
    )?;
    assert_eq!(status.operation.state, TombstoneOperationState::Expired);
    assert!(status.operation.finished_at.is_some());
    assert_eq!(
        status.operation.error_details.as_deref(),
        Some("Expired before approval")
    );

    let persisted_duration_ms: i32 = sqlx::query_scalar!(
        r#"SELECT duration_ms as "duration_ms!" FROM core.operations_log WHERE id = $1::uuid"#,
        created.operation.operation_id.parse::<uuid::Uuid>()?
    )
    .fetch_one(ctx.pool())
    .await?;
    assert!(persisted_duration_ms >= 0);

    Ok(())
}

#[sinex_test]
async fn tombstone_cancel_rejects_expired_operation_and_keeps_expired_state(
    ctx: TestContext,
) -> TestResult<()> {
    let auth = RpcAuthContext::system();
    let source = "test.lifecycle.tombstone.cancel-expired";
    let event = publish_event(&ctx, source, 1).await?;
    let event_id = event
        .id
        .expect("published event should have an id")
        .to_string();

    let archive: LifecycleArchiveResponse = serde_json::from_value(
        handle_lifecycle_archive(
            ctx.pool(),
            json!({
                "event_ids": [event_id],
                "dry_run": false,
                "reason": "prepare expired cancel",
            }),
            &auth,
        )
        .await?,
    )?;
    assert_eq!(archive.archived_count, 1);

    let created: TombstoneCreateResponse = serde_json::from_value(
        handle_tombstone_create(
            ctx.pool(),
            json!({
                "source": source,
                "reason": "expire before cancel",
            }),
            &auth,
        )
        .await?,
    )?;

    sqlx::query!(
        r#"
        UPDATE core.operations_log
        SET scope = jsonb_set(scope, '{expires_at}', to_jsonb($2::text), false)
        WHERE id = $1::uuid
        "#,
        created.operation.operation_id.parse::<uuid::Uuid>()?,
        "2000-01-01T00:00:00Z"
    )
    .execute(ctx.pool())
    .await?;

    let error = handle_tombstone_cancel(
        ctx.pool(),
        json!({
            "operation_id": created.operation.operation_id,
            "reason": "too late",
        }),
        &auth,
    )
    .await
    .expect_err("expired tombstone operation should not be cancellable");
    assert!(
        error.to_string().contains("has expired"),
        "unexpected error: {error}"
    );

    let status: TombstoneStatusResponse = serde_json::from_value(
        handle_tombstone_status(
            ctx.pool(),
            json!({ "operation_id": created.operation.operation_id }),
            &auth,
        )
        .await?,
    )?;
    assert_eq!(status.operation.state, TombstoneOperationState::Expired);
    assert_eq!(
        status.operation.error_details.as_deref(),
        Some("Expired before approval")
    );

    Ok(())
}

#[sinex_test]
async fn tombstone_cancel_rejects_invalid_created_at_metadata(ctx: TestContext) -> TestResult<()> {
    let auth = RpcAuthContext::system();
    let source = "test.lifecycle.tombstone.cancel-invalid-created-at";
    let event = publish_event(&ctx, source, 1).await?;
    let event_id = event
        .id
        .expect("published event should have an id")
        .to_string();

    let archive: LifecycleArchiveResponse = serde_json::from_value(
        handle_lifecycle_archive(
            ctx.pool(),
            json!({
                "event_ids": [event_id],
                "dry_run": false,
                "reason": "prepare invalid created_at cancel",
            }),
            &auth,
        )
        .await?,
    )?;
    assert_eq!(archive.archived_count, 1);

    let created: TombstoneCreateResponse = serde_json::from_value(
        handle_tombstone_create(
            ctx.pool(),
            json!({
                "source": source,
                "reason": "cancel with corrupt metadata",
            }),
            &auth,
        )
        .await?,
    )?;

    sqlx::query!(
        r#"
        UPDATE core.operations_log
        SET scope = jsonb_set(scope, '{created_at}', to_jsonb($2::text), false)
        WHERE id = $1::uuid
        "#,
        created.operation.operation_id.parse::<uuid::Uuid>()?,
        "not-a-timestamp"
    )
    .execute(ctx.pool())
    .await?;

    let error = handle_tombstone_cancel(
        ctx.pool(),
        json!({
            "operation_id": created.operation.operation_id,
            "reason": "operator requested stop",
        }),
        &auth,
    )
    .await
    .expect_err("invalid created_at should fail honestly");
    assert!(error.to_string().contains("invalid created_at"));

    Ok(())
}

#[sinex_test]
async fn tombstone_status_rejects_invalid_created_at_during_expiry(
    ctx: TestContext,
) -> TestResult<()> {
    let auth = RpcAuthContext::system();
    let source = "test.lifecycle.tombstone.expiry-invalid-created-at";
    let event = publish_event(&ctx, source, 1).await?;
    let event_id = event
        .id
        .expect("published event should have an id")
        .to_string();

    let archive: LifecycleArchiveResponse = serde_json::from_value(
        handle_lifecycle_archive(
            ctx.pool(),
            json!({
                "event_ids": [event_id],
                "dry_run": false,
                "reason": "prepare invalid created_at expiry",
            }),
            &auth,
        )
        .await?,
    )?;
    assert_eq!(archive.archived_count, 1);

    let created: TombstoneCreateResponse = serde_json::from_value(
        handle_tombstone_create(
            ctx.pool(),
            json!({
                "source": source,
                "reason": "expire with corrupt metadata",
            }),
            &auth,
        )
        .await?,
    )?;

    sqlx::query!(
        r#"
        UPDATE core.operations_log
        SET scope = jsonb_set(
                jsonb_set(scope, '{created_at}', to_jsonb($2::text), false),
                '{expires_at}',
                to_jsonb($3::text),
                false
            )
        WHERE id = $1::uuid
        "#,
        created.operation.operation_id.parse::<uuid::Uuid>()?,
        "not-a-timestamp",
        "2000-01-01T00:00:00Z"
    )
    .execute(ctx.pool())
    .await?;

    let error = handle_tombstone_status(
        ctx.pool(),
        json!({ "operation_id": created.operation.operation_id }),
        &auth,
    )
    .await
    .expect_err("invalid created_at should fail honestly during expiry reconciliation");
    assert!(error.to_string().contains("invalid created_at"));

    Ok(())
}

#[sinex_test]
async fn tombstone_list_state_filter_applies_before_limit(ctx: TestContext) -> TestResult<()> {
    let auth = RpcAuthContext::system();
    let source = "test.lifecycle.tombstone.list";
    let event = publish_event(&ctx, source, 1).await?;
    let event_id = event
        .id
        .expect("published event should have an id")
        .to_string();

    let archive: LifecycleArchiveResponse = serde_json::from_value(
        handle_lifecycle_archive(
            ctx.pool(),
            json!({
                "event_ids": [event_id],
                "dry_run": false,
                "reason": "prepare tombstone list regression",
            }),
            &auth,
        )
        .await?,
    )?;
    assert_eq!(archive.archived_count, 1);

    let cancelled: TombstoneCreateResponse = serde_json::from_value(
        handle_tombstone_create(
            ctx.pool(),
            json!({
                "source": source,
                "limit": 1,
                "reason": "cancelled tombstone operation",
            }),
            &auth,
        )
        .await?,
    )?;
    let _: TombstoneCancelResponse = serde_json::from_value(
        handle_tombstone_cancel(
            ctx.pool(),
            json!({
                "operation_id": cancelled.operation.operation_id,
                "reason": "regression filter target",
            }),
            &auth,
        )
        .await?,
    )?;

    let previewed: TombstoneCreateResponse = serde_json::from_value(
        handle_tombstone_create(
            ctx.pool(),
            json!({
                "source": source,
                "limit": 1,
                "reason": "newer previewed tombstone operation",
            }),
            &auth,
        )
        .await?,
    )?;

    let listed: TombstoneListResponse = serde_json::from_value(
        handle_tombstone_list(
            ctx.pool(),
            json!({
                "state": "cancelled",
                "limit": 1,
            }),
            &auth,
        )
        .await?,
    )?;

    assert_eq!(listed.operations.len(), 1);
    assert_eq!(
        listed.operations[0].operation_id, cancelled.operation.operation_id,
        "state filter should be applied before the result limit"
    );
    assert_eq!(
        listed.operations[0].state,
        TombstoneOperationState::Cancelled
    );
    assert_ne!(
        listed.operations[0].operation_id, previewed.operation.operation_id,
        "newer previewed rows must not hide older cancelled rows"
    );

    Ok(())
}

#[sinex_test]
async fn tombstone_list_fails_on_malformed_persisted_scope(ctx: TestContext) -> TestResult<()> {
    let auth = RpcAuthContext::system();
    ctx.pool()
        .state()
        .start_operation(
            "tombstone",
            "tester",
            json!({ "not": "a tombstone operation" }),
        )
        .await?;

    let error = handle_tombstone_list(ctx.pool(), json!({ "limit": 10 }), &auth)
        .await
        .expect_err("malformed tombstone rows must fail loudly");
    assert!(
        error.to_string().contains("malformed scope"),
        "unexpected error: {error}"
    );

    Ok(())
}

#[sinex_test]
async fn lifecycle_archive_rejects_non_positive_limits(ctx: TestContext) -> TestResult<()> {
    let auth = RpcAuthContext::system();
    let error = handle_lifecycle_archive(
        ctx.pool(),
        json!({
            "source": "test.lifecycle.invalid-limit",
            "limit": 0,
            "dry_run": true,
        }),
        &auth,
    )
    .await
    .expect_err("archive should reject non-positive limits");
    assert!(
        error
            .to_string()
            .contains("lifecycle.archive limit must be positive")
    );
    Ok(())
}

#[sinex_test]
async fn lifecycle_archive_rejects_conflicting_explicit_event_filters(
    ctx: TestContext,
) -> TestResult<()> {
    let auth = RpcAuthContext::system();
    let error = handle_lifecycle_archive(
        ctx.pool(),
        json!({
            "event_ids": ["00000000-0000-0000-0000-000000000001"],
            "source": "test.lifecycle.conflict",
            "before": "30d",
            "dry_run": true,
            "reason": "conflicting archive filters",
        }),
        &auth,
    )
    .await
    .expect_err("archive should reject conflicting explicit event-id filters");
    assert!(
        error
            .to_string()
            .contains("does not allow `event_ids` together with `source` or `before`")
    );
    Ok(())
}

#[sinex_test]
async fn tombstone_create_rejects_non_positive_limits(ctx: TestContext) -> TestResult<()> {
    let auth = RpcAuthContext::system();
    let error = handle_tombstone_create(
        ctx.pool(),
        json!({
            "source": "test.lifecycle.invalid-limit",
            "limit": -1,
            "reason": "reject invalid limit",
        }),
        &auth,
    )
    .await
    .expect_err("tombstone create should reject non-positive limits");
    assert!(
        error
            .to_string()
            .contains("lifecycle.tombstone.create limit must be positive")
    );
    Ok(())
}

#[sinex_test]
async fn tombstone_create_rejects_conflicting_explicit_event_filters(
    ctx: TestContext,
) -> TestResult<()> {
    let auth = RpcAuthContext::system();
    let error = handle_tombstone_create(
        ctx.pool(),
        json!({
            "event_ids": ["00000000-0000-0000-0000-000000000001"],
            "source": "test.lifecycle.conflict",
            "before": "30d",
            "reason": "conflicting tombstone filters",
        }),
        &auth,
    )
    .await
    .expect_err("tombstone create should reject conflicting explicit event-id filters");
    assert!(
        error
            .to_string()
            .contains("does not allow `event_ids` together with `source` or `before`")
    );
    Ok(())
}

#[sinex_test]
async fn tombstone_list_rejects_non_positive_limits(ctx: TestContext) -> TestResult<()> {
    let auth = RpcAuthContext::system();
    let error = handle_tombstone_list(ctx.pool(), json!({ "limit": 0 }), &auth)
        .await
        .expect_err("tombstone list should reject non-positive limits");
    assert!(
        error
            .to_string()
            .contains("lifecycle.tombstone.list limit must be positive")
    );
    Ok(())
}
