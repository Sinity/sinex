use serde_json::json;
use sinex_db::repositories::state::Operation;
use sinex_db::repositories::DbPoolExt;
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn state_repository_logs_operations(ctx: TestContext) -> TestResult<()> {
    let repo = ctx.pool.state();
    let operation = Operation {
        id: None,
        operation_type: "process".to_string(),
        operator: "ingestd@localhost".to_string(),
        scope: Some(json!({
            "processor": "ingestd",
            "mode": "ingestor",
            "source": "fs-watcher"
        })),
        result_status: "success".to_string(),
        result_message: None,
        preview_summary: Some(json!({
            "events_count": 1,
            "types": ["file.created"]
        })),
        duration_ms: Some(100),
    };

    let logged = repo.log_operation(operation).await?;
    assert_eq!(logged.operator, "ingestd@localhost");

    let failed_op = Operation {
        id: None,
        operation_type: "validate".to_string(),
        operator: "api-user@localhost".to_string(),
        scope: Some(json!({
            "processor": "schema-manager",
            "mode": "automaton",
            "target": "test-schema-1.0.0"
        })),
        result_status: "failure".to_string(),
        result_message: Some("Invalid JSON schema".to_string()),
        preview_summary: None,
        duration_ms: Some(50),
    };

    repo.log_operation(failed_op).await?;

    assert_eq!(repo.get_recent_operations(10).await?.len(), 2);
    assert_eq!(
        repo.get_operations_by_actor("ingestd@localhost", None)
            .await?
            .len(),
        1
    );
    assert_eq!(repo.get_failed_operations(None, None).await?.len(), 1);
    Ok(())
}

#[sinex_test]
async fn state_repository_collects_operation_statistics(ctx: TestContext) -> TestResult<()> {
    let repo = ctx.pool.state();
    let operations = vec![
        ("success", None),
        ("success", None),
        ("success", None),
        ("failure", Some("Test error".to_string())),
        ("cancelled", None),
    ];

    for (status, message) in operations {
        let operation = Operation {
            id: None,
            operation_type: "test".to_string(),
            operator: "test-service@localhost".to_string(),
            scope: Some(json!({
                "processor": "test",
                "mode": "automaton"
            })),
            result_status: status.to_string(),
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
