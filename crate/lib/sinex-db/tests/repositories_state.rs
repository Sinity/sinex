use serde_json::json;
use sinex_db::repositories::DbPoolExt;
use sinex_db::repositories::state::Operation;
use sinex_primitives::domain::{NodeName, NodeState, NodeType, OperationStatus};
use sinex_primitives::rpc::lifecycle::{
    TombstoneOperation, TombstoneOperationPhase, TombstoneOperationState,
};
use sinex_primitives::{Id, Timestamp, Uuid};
use std::time::Duration;
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn state_repository_logs_operations(ctx: TestContext) -> TestResult<()> {
    let repo = ctx.pool.state();
    let operation = Operation {
        id: None,
        operation_type: "archive".to_string(),
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
    assert_eq!(logged.operation_type, "archive");
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
        operation_type: "restore".to_string(),
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
    assert_eq!(failed.operation_type, "restore");
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
            operation_type: "purge".to_string(),
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

#[sinex_test]
async fn log_operation_accepts_custom_audit_operation_type(ctx: TestContext) -> TestResult<()> {
    let repo = ctx.pool.state();
    let logged = repo
        .log_operation(Operation {
            id: None,
            operation_type: "content.store".to_string(),
            operator: "tester@localhost".to_string(),
            scope: Some(json!({ "source": "external" })),
            result_status: OperationStatus::Running,
            result_message: None,
            preview_summary: None,
            duration_ms: None,
        })
        .await?;

    assert_eq!(logged.operation_type, "content.store");
    assert_eq!(logged.operator, "tester@localhost");
    assert_eq!(logged.scope, Some(json!({ "source": "external" })));
    Ok(())
}

#[sinex_test]
async fn log_operation_rejects_malformed_operation_type(ctx: TestContext) -> TestResult<()> {
    let repo = ctx.pool.state();
    let err = repo
        .log_operation(Operation {
            id: None,
            operation_type: "Bad Operation".to_string(),
            operator: "tester@localhost".to_string(),
            scope: None,
            result_status: OperationStatus::Running,
            result_message: None,
            preview_summary: None,
            duration_ms: None,
        })
        .await
        .expect_err("malformed operation type should be rejected before insert");

    assert!(err.to_string().contains("must match"));
    Ok(())
}

#[sinex_test]
async fn register_node_is_idempotent_per_manifest_version(ctx: TestContext) -> TestResult<()> {
    let repo = ctx.pool.state();
    let node_name = NodeName::new("idempotent-node");

    let first = repo
        .register_node(&node_name, NodeType::Service, "1.0.0", Some("first description"))
        .await?;
    let second = repo
        .register_node(
            &node_name,
            NodeType::Service,
            "1.0.0",
            Some("updated description"),
        )
        .await?;

    assert_eq!(first.id, second.id, "duplicate registration should reuse the manifest row");
    assert_eq!(second.description.as_deref(), Some("updated description"));

    let manifests = repo.get_all_nodes().await?;
    let matching = manifests
        .into_iter()
        .filter(|manifest| manifest.node_name == node_name)
        .collect::<Vec<_>>();
    assert_eq!(matching.len(), 1, "duplicate registration should not create extra rows");
    assert_eq!(matching[0].version, "1.0.0");
    assert_eq!(matching[0].description.as_deref(), Some("updated description"));

    Ok(())
}

#[sinex_test]
async fn update_operation_meta_rejects_missing_operation(ctx: TestContext) -> TestResult<()> {
    let repo = ctx.pool.state();
    let missing = Id::<Operation>::from_uuid(Uuid::now_v7());

    let err = repo
        .update_operation_meta(
            &missing,
            OperationStatus::Success,
            Some("done"),
            json!({ "message": "done" }),
        )
        .await
        .expect_err("missing operation updates must fail");

    assert!(err.to_string().contains("Operation not found"));
    Ok(())
}

#[sinex_test]
async fn update_tombstone_operation_rejects_missing_operation(ctx: TestContext) -> TestResult<()> {
    let repo = ctx.pool.state();
    let missing = Uuid::now_v7().to_string();

    let err = repo
        .update_tombstone_operation(
            &missing,
            OperationStatus::Cancelled,
            json!({ "operation_id": missing, "state": "Cancelled" }),
            None,
            Some("cancelled"),
            None,
        )
        .await
        .expect_err("missing tombstone operation updates must fail");

    assert!(err.to_string().contains("Tombstone operation not found"));
    Ok(())
}

#[sinex_test]
async fn cancel_tombstone_operation_rejects_invalid_created_at(ctx: TestContext) -> TestResult<()> {
    let repo = ctx.pool.state();
    let operation_id = Uuid::now_v7().to_string();
    let operation = TombstoneOperation {
        operation_id: operation_id.clone(),
        phase: TombstoneOperationPhase::Previewed,
        state: TombstoneOperationState::Previewed,
        before: None,
        source: None,
        event_ids: Some(vec![Uuid::now_v7().to_string()]),
        limit: 1000,
        reason: "test tombstone".to_string(),
        cascade_analysis: None,
        created_by: "tester@localhost".to_string(),
        created_at: "not-a-timestamp".to_string(),
        expires_at: (Timestamp::now() + time::Duration::hours(1)).format_rfc3339(),
        approved_by: None,
        approved_at: None,
        started_at: None,
        finished_at: None,
        tombstoned_count: None,
        error_details: None,
    };

    repo.create_tombstone_operation(
        &operation_id,
        "tester@localhost",
        serde_json::to_value(&operation)?,
        json!({ "message": "preview ready" }),
    )
    .await?;

    let err = repo
        .cancel_tombstone_operation(&operation_id, Some("cancelled for test"))
        .await
        .expect_err("invalid tombstone created_at must fail honestly");

    assert!(err.to_string().contains("invalid created_at"));
    Ok(())
}

#[sinex_test]
async fn node_manifest_heartbeat_updates_only_requested_version(ctx: TestContext) -> TestResult<()> {
    let repo = ctx.pool.state();
    let node_name = NodeName::new("versioned-heartbeat-node");

    repo.register_node(&node_name, NodeType::Service, "1.0.0", Some("older build"))
        .await?;
    repo.register_node(&node_name, NodeType::Service, "2.0.0", Some("newer build"))
        .await?;

    assert!(repo
        .mark_node_inactive_for_version(&node_name, "1.0.0")
        .await?);
    assert!(repo
        .update_node_heartbeat_for_version(&node_name, "2.0.0")
        .await?);

    let manifests = repo.get_all_nodes().await?;
    let older = manifests
        .iter()
        .find(|manifest| manifest.node_name == node_name && manifest.version == "1.0.0")
        .expect("older manifest version should exist");
    let newer = manifests
        .iter()
        .find(|manifest| manifest.node_name == node_name && manifest.version == "2.0.0")
        .expect("newer manifest version should exist");

    assert_eq!(older.status, "inactive");
    assert!(older.last_heartbeat_at.is_none());
    assert_eq!(newer.status, "active");
    assert!(
        newer.last_heartbeat_at.is_some(),
        "heartbeat should only be persisted for the requested version"
    );

    let live_nodes = repo.list_live_node_presence(Duration::from_secs(120)).await?;
    assert_eq!(live_nodes.len(), 1);
    assert_eq!(live_nodes[0].node_name, node_name);
    assert_eq!(live_nodes[0].version, "2.0.0");
    assert!(live_nodes[0].node_run_id.is_none());
    assert_eq!(live_nodes[0].heartbeat_source, "manifest");

    let health = repo.get_node_health(Duration::from_secs(120)).await?;
    assert_eq!(health.unique_nodes, 1);
    assert_eq!(health.active_count, 1);
    assert_eq!(health.inactive_count, 0);
    assert_eq!(health.active_run_count, 0);

    Ok(())
}

#[sinex_test]
async fn node_manifest_inactive_marks_only_requested_version(ctx: TestContext) -> TestResult<()> {
    let repo = ctx.pool.state();
    let node_name = NodeName::new("versioned-inactive-node");

    repo.register_node(&node_name, NodeType::Service, "1.0.0", Some("older build"))
        .await?;
    repo.register_node(&node_name, NodeType::Service, "2.0.0", Some("newer build"))
        .await?;
    assert!(repo
        .update_node_heartbeat_for_version(&node_name, "1.0.0")
        .await?);
    assert!(repo
        .update_node_heartbeat_for_version(&node_name, "2.0.0")
        .await?);

    assert!(repo
        .mark_node_inactive_for_version(&node_name, "1.0.0")
        .await?);

    let manifests = repo.get_all_nodes().await?;
    let older = manifests
        .iter()
        .find(|manifest| manifest.node_name == node_name && manifest.version == "1.0.0")
        .expect("older manifest version should exist");
    let newer = manifests
        .iter()
        .find(|manifest| manifest.node_name == node_name && manifest.version == "2.0.0")
        .expect("newer manifest version should exist");

    assert_eq!(older.status, "inactive");
    assert_eq!(newer.status, "active");
    assert!(
        newer.last_heartbeat_at.is_some(),
        "marking one version inactive must not clear the other version heartbeat"
    );

    let live_nodes = repo.list_live_node_presence(Duration::from_secs(120)).await?;
    assert_eq!(live_nodes.len(), 1);
    assert_eq!(live_nodes[0].version, "2.0.0");
    assert_eq!(live_nodes[0].heartbeat_source, "manifest");

    Ok(())
}

#[sinex_test]
async fn node_run_lifecycle_persists_status_and_config(ctx: TestContext) -> TestResult<()> {
    let repo = ctx.pool.state();
    let node_name = NodeName::new("node-run-lifecycle");

    let manifest = repo
        .register_node(&node_name, NodeType::Ingestor, "1.2.3", Some("node run test"))
        .await?;

    let config = json!({
        "history_sources": ["/tmp/history.sqlite"],
        "nested": { "enabled": true, "batch_size": 64 }
    });

    let run = repo
        .start_node_run(
            manifest.id,
            "sinex-terminal-ingestor",
            "host-123-run",
            "test-host",
            Some("b3-abc123"),
            Some(&config),
        )
        .await?;

    assert_eq!(run.node_manifest_id, manifest.id);
    assert_eq!(run.service_name, "sinex-terminal-ingestor");
    assert_eq!(run.instance_id, "host-123-run");
    assert_eq!(run.host, "test-host");
    assert_eq!(run.status, "running");
    assert!(run.last_heartbeat_at.is_some());
    assert_eq!(run.effective_config_hash.as_deref(), Some("b3-abc123"));
    assert_eq!(run.effective_config, Some(config.clone()));

    assert!(repo.update_node_run_heartbeat(run.id).await?);
    assert!(repo
        .update_node_run_status(run.id, NodeState::Stopped)
        .await?);

    let refreshed = sqlx::query!(
        r#"
        SELECT
            status,
            ended_at as "ended_at: sinex_primitives::temporal::Timestamp",
            effective_config_hash,
            effective_config
        FROM core.node_runs
        WHERE id = $1::uuid
        "#,
        run.id
    )
    .fetch_one(ctx.pool())
    .await?;

    assert_eq!(refreshed.status, "stopped");
    assert!(refreshed.ended_at.is_some());
    assert_eq!(refreshed.effective_config_hash.as_deref(), Some("b3-abc123"));
    assert_eq!(refreshed.effective_config, Some(config));

    Ok(())
}
