use serde_json::json;
use sinex_db::repositories::DbPoolExt;
use sinex_db::repositories::state::Operation;
use sinex_primitives::domain::OperationStatus;
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn state_repository_logs_operations(ctx: TestContext) -> TestResult<()> {
    let repo = ctx.pool.state();
    let operation = Operation {
        id: None,
        operation_type: "process".to_string(),
        operator: "ingestd@localhost".to_string(),
        scope: Some(json!({
            "node": "ingestd",
            "mode": "ingestor",
            "source": "fs-watcher"
        })),
        result_status: OperationStatus::Success,
        result_message: None,
        preview_summary: Some(json!({
            "events_count": 1,
            "types": ["file.created"]
        })),
        duration_ms: Some(100),
    };

    let logged = repo.log_operation(operation).await?;
    assert_eq!(logged.operation_type, "process");
    assert_eq!(logged.operator, "ingestd@localhost");
    assert_eq!(
        logged.scope,
        Some(json!({
            "node": "ingestd",
            "mode": "ingestor",
            "source": "fs-watcher"
        }))
    );
    assert_eq!(
        logged.preview_summary,
        Some(json!({
            "events_count": 1,
            "types": ["file.created"]
        }))
    );
    assert_eq!(logged.result_status, OperationStatus::Success);
    assert_eq!(logged.duration_ms, Some(100));

    let failed_op = Operation {
        id: None,
        operation_type: "validate".to_string(),
        operator: "api-user@localhost".to_string(),
        scope: Some(json!({
            "node": "schema-manager",
            "mode": "automaton",
            "target": "test-schema-1.0.0"
        })),
        result_status: OperationStatus::Failed,
        result_message: Some("Invalid JSON schema".to_string()),
        preview_summary: None,
        duration_ms: Some(50),
    };

    let failed = repo.log_operation(failed_op).await?;
    assert_eq!(failed.operation_type, "validate");
    assert_eq!(failed.operator, "api-user@localhost");
    assert_eq!(
        failed.scope,
        Some(json!({
            "node": "schema-manager",
            "mode": "automaton",
            "target": "test-schema-1.0.0"
        }))
    );
    assert_eq!(failed.result_status, OperationStatus::Failed);
    assert_eq!(
        failed.result_message.as_deref(),
        Some("Invalid JSON schema")
    );
    assert_eq!(failed.preview_summary, None);
    assert_eq!(failed.duration_ms, Some(50));

    let recent = repo.get_recent_operations(10).await?;
    assert_eq!(recent.len(), 2);
    assert_eq!(recent[0].id, failed.id);
    assert_eq!(recent[1].id, logged.id);

    let by_actor = repo
        .get_operations_by_actor("ingestd@localhost", None)
        .await?;
    assert_eq!(by_actor.len(), 1);
    assert_eq!(by_actor[0].id, logged.id);
    assert_eq!(by_actor[0].scope, logged.scope);

    let by_scope = repo
        .get_operations_by_scope(json!({"node": "schema-manager"}), None)
        .await?;
    assert_eq!(by_scope.len(), 1);
    assert_eq!(by_scope[0].id, failed.id);

    let failed_ops = repo.get_failed_operations(None, None).await?;
    assert_eq!(failed_ops.len(), 1);
    assert_eq!(failed_ops[0].id, failed.id);
    assert_eq!(
        failed_ops[0].result_message.as_deref(),
        Some("Invalid JSON schema")
    );
    Ok(())
}

#[sinex_test]
async fn state_repository_collects_operation_statistics(ctx: TestContext) -> TestResult<()> {
    let repo = ctx.pool.state();
    let operations: Vec<(OperationStatus, Option<String>)> = vec![
        (OperationStatus::Success, None),
        (OperationStatus::Success, None),
        (OperationStatus::Success, None),
        (OperationStatus::Failed, Some("Test error".to_string())),
        (OperationStatus::Cancelled, None),
    ];

    for (status, message) in operations {
        let operation = Operation {
            id: None,
            operation_type: "test".to_string(),
            operator: "test-service@localhost".to_string(),
            scope: Some(json!({
                "node": "test",
                "mode": "automaton"
            })),
            result_status: status,
            result_message: message,
            preview_summary: None,
            duration_ms: Some(100),
        };

        repo.log_operation(operation).await?;
    }

    let stats = repo.get_operation_statistics(None).await?;
    assert_eq!(stats.total, 5);
    assert_eq!(stats.successful, 3);
    assert_eq!(stats.failed, 1);
    assert_eq!(stats.cancelled, 1);
    assert!(stats.avg_duration_ms.is_some());
    Ok(())
}
