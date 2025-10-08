use serde_json::json;
use sinex_core::db::repositories::state::Operation;
use sinex_core::repositories::DbPoolExt;
use sinex_core::{Checkpoint, CheckpointRepository, Id};
use sinex_test_utils::{sinex_test, TestContext};

#[sinex_test]
async fn state_repository_handles_checkpoint_lifecycle(
    ctx: TestContext,
) -> color_eyre::eyre::Result<()> {
    let repo = ctx.pool.state();
    let id = Id::<sinex_core::db::models::Event<serde_json::Value>>::new();
    let checkpoint = Checkpoint::new("test-processor")
        .with_last_processed_id(id)
        .with_checkpoint_data(json!({ "batch_size": 100 }));

    let saved = repo.save_checkpoint(checkpoint).await?;
    assert_eq!(saved.processor_name.as_ref(), "test-processor");
    assert_eq!(saved.processed_count, 1);

    let new_id = Id::<sinex_core::db::models::Event<serde_json::Value>>::new();
    let update = Checkpoint::new("test-processor")
        .with_last_processed_id(new_id.clone())
        .with_checkpoint_data(json!({ "batch_size": 200 }));
    let updated = repo.save_checkpoint(update).await?;
    assert_eq!(updated.processed_count, 2);
    assert_eq!(updated.last_processed_id, Some(new_id));

    let retrieved = repo.get_checkpoint("test-processor").await?;
    assert_eq!(retrieved.unwrap().processed_count, 2);

    assert!(repo.delete_checkpoint("test-processor").await?);
    assert!(repo.get_checkpoint("test-processor").await?.is_none());
    Ok(())
}

#[sinex_test]
async fn state_repository_logs_operations(ctx: TestContext) -> color_eyre::eyre::Result<()> {
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
async fn state_repository_collects_operation_statistics(
    ctx: TestContext,
) -> color_eyre::eyre::Result<()> {
    let repo = ctx.pool.state();
    let operations = vec![
        ("success", None),
        ("success", None),
        ("success", None),
        ("failure", Some("Test error".to_string())),
        ("partial", None),
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
    assert_eq__(stats.successful, 3);
    assert_eq!(stats.failed, 1);
    assert_eq!(stats.cancelled, 1);
    assert!(stats.avg_duration_ms.is_some());
    Ok(())
}
