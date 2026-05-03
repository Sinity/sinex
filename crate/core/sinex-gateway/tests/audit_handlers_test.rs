//! Tests for audit handler request/response behavior.

use serde_json::json;
use sinex_db::DbPoolExt;
use sinex_db::repositories::state::Operation as DbOperation;
use sinex_gateway::handlers::handle_audit_get;
use sinex_primitives::Id;
use sinex_primitives::Uuid;
use sinex_primitives::domain::OperationStatus;
use sinex_primitives::events::DynamicPayload;
use sinex_primitives::events::Event;
use sinex_primitives::rpc::audit::{
    AuditGetRequest, AuditGetResponse, AuditTrail, OperationRecord,
};
use sinex_primitives::rpc::lifecycle::LifecycleOperationSummary;
use sinex_primitives::events::builder::OperationMarker;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn request_defaults_limit_to_100() -> TestResult<()> {
    let id = Id::<OperationMarker>::new();
    let value = json!({ "operation_id": id });
    let req: AuditGetRequest = serde_json::from_value(value)?;
    assert_eq!(req.limit, 100);
    assert!(req.after_id.is_none());
    Ok(())
}

#[sinex_test]
async fn request_roundtrips_limit_and_cursor() -> TestResult<()> {
    let op_id = Id::<OperationMarker>::new();
    let cursor = Id::<Event>::new();
    let value = json!({
        "operation_id": op_id,
        "limit": 25,
        "after_id": cursor,
    });
    let req: AuditGetRequest = serde_json::from_value(value)?;
    assert_eq!(req.limit, 25);
    assert_eq!(req.after_id, Some(cursor));
    Ok(())
}

#[sinex_test]
async fn response_serializes_no_more() -> TestResult<()> {
    let op_id = Id::<OperationMarker>::new();
    let response = AuditGetResponse {
        audit_trail: AuditTrail {
            operation: OperationRecord {
                id: op_id,
                operation_type: "tombstone".into(),
                operator: "test".into(),
                scope: None,
                result_status: OperationStatus::Success,
                result_message: None,
                preview_summary: None,
                duration_ms: None,
            },
            affected_events: vec![],
        },
        event_count: 0,
        next_cursor: None,
        has_more: false,
    };

    let value = serde_json::to_value(&response)?;
    assert_eq!(value["has_more"], false);
    assert!(value.get("next_cursor").is_none());
    Ok(())
}

#[sinex_test]
async fn missing_operation_returns_not_found(ctx: TestContext) -> TestResult<()> {
    let fake_id = Id::<OperationMarker>::new();
    let err = handle_audit_get(ctx.pool(), json!({ "operation_id": fake_id }))
        .await
        .expect_err("missing operation should return not found");
    let message = err.to_string();
    assert!(message.contains("not found") || message.contains("Not found"));
    Ok(())
}

#[sinex_test]
async fn audit_get_uses_explicit_lifecycle_summary_across_tiers(
    ctx: TestContext,
) -> TestResult<()> {
    let pool = ctx.pool();
    let material_id = ctx.create_source_material(Some("audit-test")).await?;

    let live = pool
        .events()
        .insert(
            DynamicPayload::new("audit-test", "audit.live", json!({ "tier": "live" }))
                .from_material(material_id)
                .build()?,
        )
        .await?;
    let archived = pool
        .events()
        .insert(
            DynamicPayload::new("audit-test", "audit.archive", json!({ "tier": "archive" }))
                .from_material(material_id)
                .build()?,
        )
        .await?;
    let tombstoned = pool
        .events()
        .insert(
            DynamicPayload::new(
                "audit-test",
                "audit.tombstone",
                json!({ "tier": "tombstone" }),
            )
            .from_material(material_id)
            .build()?,
        )
        .await?;
    let live_id = live.id.expect("live event should have an id");
    let archived_id = archived.id.expect("archived event should have an id");
    let tombstoned_id = tombstoned.id.expect("tombstoned event should have an id");

    let archive_op = Uuid::now_v7().to_string();
    pool.events()
        .execute_cascade_archive(
            &[*archived_id.as_uuid()],
            "archive test row",
            &archive_op,
            "test",
        )
        .await?;

    let tombstone_archive_op = Uuid::now_v7().to_string();
    pool.events()
        .execute_cascade_archive(
            &[*tombstoned_id.as_uuid()],
            "archive before tombstone",
            &tombstone_archive_op,
            "test",
        )
        .await?;
    pool.events()
        .execute_cascade_tombstone(
            &[*tombstoned_id.as_uuid()],
            "tombstone test row",
            Uuid::now_v7(),
        )
        .await?;

    let operation = pool
        .state()
        .start_operation("archive", "tester", json!({ "source": "audit-test" }))
        .await?;
    let summary = LifecycleOperationSummary {
        dry_run: false,
        root_event_count: 3,
        cascade_total: 3,
        cascade_depth: 1,
        affected_event_ids: vec![
            live_id.to_string(),
            archived_id.to_string(),
            tombstoned_id.to_string(),
        ],
        message: Some("explicit audit summary".into()),
        ..Default::default()
    };
    pool.state()
        .update_operation_meta(
            &operation.id,
            OperationStatus::Running,
            Some("explicit audit summary"),
            serde_json::to_value(summary)?,
        )
        .await?;

    let response = handle_audit_get(ctx.pool(), json!({ "operation_id": operation.id })).await?;
    let response: AuditGetResponse = serde_json::from_value(response)?;
    assert_eq!(response.event_count, 3);

    let events_by_id = response
        .audit_trail
        .affected_events
        .into_iter()
        .map(|event| (event.id.to_string(), event))
        .collect::<std::collections::HashMap<_, _>>();

    assert_eq!(
        events_by_id
            .get(&live_id.to_string())
            .and_then(|event| event.tier),
        Some(sinex_primitives::domain::DataTier::Live)
    );
    assert_eq!(
        events_by_id
            .get(&archived_id.to_string())
            .and_then(|event| event.tier),
        Some(sinex_primitives::domain::DataTier::Archive)
    );
    assert_eq!(
        events_by_id
            .get(&tombstoned_id.to_string())
            .and_then(|event| event.tier),
        Some(sinex_primitives::domain::DataTier::Tombstone)
    );

    Ok(())
}

#[sinex_test]
async fn audit_get_rejects_malformed_lifecycle_preview_summary(ctx: TestContext) -> TestResult<()> {
    let operation = ctx
        .pool()
        .state()
        .start_operation("archive", "tester", json!({ "source": "audit-test" }))
        .await?;

    ctx.pool()
        .state()
        .update_operation_meta(
            &operation.id,
            OperationStatus::Running,
            Some("malformed lifecycle summary"),
            json!({ "affected_event_ids": "not-an-array" }),
        )
        .await?;

    let error = handle_audit_get(ctx.pool(), json!({ "operation_id": operation.id }))
        .await
        .expect_err("malformed lifecycle preview_summary should fail honestly");
    let message = error.to_string();
    assert!(message.contains("invalid lifecycle preview_summary"));
    assert!(message.contains("archive"));
    Ok(())
}

#[sinex_test]
async fn audit_get_ignores_non_lifecycle_preview_summary_shapes(
    ctx: TestContext,
) -> TestResult<()> {
    let record = ctx
        .pool()
        .state()
        .log_operation(DbOperation {
            id: None,
            operation_type: "content.store".to_string(),
            operator: "tester@localhost".to_string(),
            scope: Some(json!({ "source": "external" })),
            result_status: OperationStatus::Success,
            result_message: None,
            preview_summary: Some(json!({ "blob_key": "annex-key", "bytes": 128 })),
            duration_ms: Some(12),
        })
        .await?;
    let operation_id = Id::<OperationMarker>::from_uuid(*record.id.as_uuid());

    let response = handle_audit_get(ctx.pool(), json!({ "operation_id": operation_id })).await?;
    let response: AuditGetResponse = serde_json::from_value(response)?;
    assert_eq!(
        response.audit_trail.operation.operation_type,
        "content.store"
    );
    assert_eq!(response.event_count, 0);
    assert!(!response.has_more);
    Ok(())
}
