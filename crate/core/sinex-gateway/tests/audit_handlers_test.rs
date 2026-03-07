//! Tests for audit handler request/response behavior.

use serde_json::json;
use sinex_gateway::handlers::handle_audit_get;
use sinex_primitives::Id;
use sinex_primitives::domain::OperationStatus;
use sinex_primitives::events::Event;
use sinex_primitives::rpc::audit::{
    AuditGetRequest, AuditGetResponse, AuditTrail, OperationRecord,
};
use sinex_primitives::rpc::ops::Operation;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn request_defaults_limit_to_100() -> TestResult<()> {
    let id = Id::<Operation>::new();
    let value = json!({ "operation_id": id });
    let req: AuditGetRequest = serde_json::from_value(value)?;
    assert_eq!(req.limit, 100);
    assert!(req.after_id.is_none());
    Ok(())
}

#[sinex_test]
async fn request_roundtrips_limit_and_cursor() -> TestResult<()> {
    let op_id = Id::<Operation>::new();
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
    let op_id = Id::<Operation>::new();
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
    let fake_id = Id::<Operation>::new();
    let err = handle_audit_get(ctx.pool(), json!({ "operation_id": fake_id }))
        .await
        .expect_err("missing operation should return not found");
    let message = err.to_string();
    assert!(message.contains("not found") || message.contains("Not found"));
    Ok(())
}
