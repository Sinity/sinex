use crate::common::prelude::*;

#[sinex_test]
async fn test_agent_manifest_create(ctx: TestContext) -> TestResult {
    // Create a complete agent manifest
    let result = sqlx::query(
        "INSERT INTO sinex_schemas.agent_manifests
         (agent_name, description, version, status, agent_type,
          config_template_json, produces_event_types, subscribes_to_event_types,
          required_capabilities, llm_dependencies, repo_url)
         VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7::jsonb, $8::jsonb, $9::jsonb, $10::jsonb, $11)",
    )
    .bind("test_agent_crud")
    .bind("Test agent for CRUD operations")
    .bind("1.0.0")
    .bind("running")
    .bind("ingestor")
    .bind(json!({
        "api_key": "string",
        "batch_size": 100,
        "endpoints": ["http://example.com"]
    }))
    .bind(json!({
        "desktop.test": [
            {"type": "window_opened", "schema_id_ref": "01234567890123456789012345"},
            {"type": "window_closed", "schema_id_ref": "01234567890123456789012346"}
        ]
    }))
    .bind(json!({
        "raw.events_feed_all": [
            {"source_filter": "app.browser.*", "event_type_filter": "page_loaded"}
        ]
    }))
    .bind(json!({
        "filesystem_read": ["/var/log"],
        "network_host_allow": ["api.example.com:443"]
    }))
    .bind(json!({
        "models_used": ["ollama/mistral:7b"],
        "required_capabilities": ["function_calling"]
    }))
    .bind("https://github.com/example/test-agent")
    .execute(ctx.pool())
    .await;

    assert!(result.is_ok(), "Should be able to create agent manifest");

    // Verify all fields were stored
    let manifest: (
        String,
        Option<String>,
        String,
        String,
        String,
        Option<serde_json::Value>,
        Option<serde_json::Value>,
        Option<serde_json::Value>,
        Option<serde_json::Value>,
        Option<serde_json::Value>,
        Option<String>,
    ) = sqlx::query_as(
        "SELECT agent_name, description, version, status, agent_type,
                config_template_json, produces_event_types, subscribes_to_event_types,
                required_capabilities, llm_dependencies, repo_url
         FROM sinex_schemas.agent_manifests
         WHERE agent_name = $1",
    )
    .bind("test_agent_crud")
    .fetch_one(ctx.pool())
    .await
    .unwrap();

    pretty_assertions::assert_eq!(manifest.0, "test_agent_crud");
    pretty_assertions::assert_eq!(manifest.1.unwrap(), "Test agent for CRUD operations");
    pretty_assertions::assert_eq!(manifest.2, "1.0.0");
    pretty_assertions::assert_eq!(manifest.3, "running");
    pretty_assertions::assert_eq!(manifest.4, "ingestor");
    assert!(manifest.5.is_some());
    assert!(manifest.6.is_some());
    assert!(manifest.7.is_some());
    assert!(manifest.8.is_some());
    assert!(manifest.9.is_some());
    pretty_assertions::assert_eq!(
        manifest.10.unwrap(),
        "https://github.com/example/test-agent"
    );

    Ok(())
}

#[sinex_test]
async fn test_agent_manifest_update(ctx: TestContext) -> TestResult {
    // Create agent
    sqlx::query("INSERT INTO sinex_schemas.agent_manifests (agent_name, version) VALUES ($1, $2)")
        .bind("update_test_agent")
        .bind("1.0.0")
        .execute(ctx.pool())
        .await
        .unwrap();

    // Get initial timestamps
    let (registered, updated): (chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>) =
        sqlx::query_as(
            "SELECT registered_at, updated_at FROM sinex_schemas.agent_manifests WHERE agent_name = $1"
        )
        .bind("update_test_agent")
        .fetch_one(ctx.pool())
        .await
        .unwrap();

    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

    // Update various fields
    sqlx::query(
        "UPDATE sinex_schemas.agent_manifests
         SET version = $1,
             status = $2,
             last_heartbeat_ts = $3,
             produces_event_types = $4::jsonb
         WHERE agent_name = $5",
    )
    .bind("1.1.0")
    .bind("stopped")
    .bind(chrono::Utc::now())
    .bind(json!({
        "new.events": [{"type": "test_event"}]
    }))
    .bind("update_test_agent")
    .execute(ctx.pool())
    .await
    .unwrap();

    // Verify updates and trigger
    let (version, status, updated_new): (String, String, chrono::DateTime<chrono::Utc>) =
        sqlx::query_as(
            "SELECT version, status, updated_at FROM sinex_schemas.agent_manifests WHERE agent_name = $1"
        )
        .bind("update_test_agent")
        .fetch_one(ctx.pool())
        .await
        .unwrap();

    pretty_assertions::assert_eq!(version, "1.1.0");
    pretty_assertions::assert_eq!(status, "stopped");
    assert!(
        updated_new > updated,
        "updated_at should be updated by trigger"
    );
    pretty_assertions::assert_eq!(registered, registered, "registered_at should not change");

    Ok(())
}

#[sinex_test]
async fn test_agent_manifest_delete(ctx: TestContext) -> TestResult {
    // Create agent
    sqlx::query("INSERT INTO sinex_schemas.agent_manifests (agent_name, version) VALUES ($1, $2)")
        .bind("delete_test_agent")
        .bind("1.0.0")
        .execute(ctx.pool())
        .await
        .unwrap();

    // Create event and promotion queue item
    let event_id = sinex_ulid::Ulid::new();
    sqlx::query(
        "INSERT INTO raw.events (id, source, event_type, host, payload)
         VALUES ($1::ulid, $2, $3, $4, $5::jsonb)",
    )
    .bind(&event_id.to_string())
    .bind("delete_test")
    .bind("test_event")
    .bind("test_host")
    .bind(json!({"test": "data"}))
    .execute(ctx.pool())
    .await
    .unwrap();

    sqlx::query(
        "INSERT INTO sinex_schemas.work_queue (raw_event_id, target_agent_name)
         VALUES ($1::ulid, $2)",
    )
    .bind(&event_id.to_string())
    .bind("delete_test_agent")
    .execute(ctx.pool())
    .await
    .unwrap();

    // Delete agent - should cascade delete work queue items
    sqlx::query("DELETE FROM sinex_schemas.agent_manifests WHERE agent_name = $1")
        .bind("delete_test_agent")
        .execute(ctx.pool())
        .await
        .unwrap();

    // Verify agent is deleted
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sinex_schemas.agent_manifests WHERE agent_name = $1",
    )
    .bind("delete_test_agent")
    .fetch_one(ctx.pool())
    .await
    .unwrap();

    pretty_assertions::assert_eq!(count, 0, "Agent should be deleted");

    // Verify work queue items were cascade deleted
    let queue_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sinex_schemas.work_queue WHERE target_agent_name = $1",
    )
    .bind("delete_test_agent")
    .fetch_one(ctx.pool())
    .await
    .unwrap();

    pretty_assertions::assert_eq!(queue_count, 0, "Work queue items should be cascade deleted");

    Ok(())
}

#[sinex_test]
async fn test_agent_status_transitions(ctx: TestContext) -> TestResult {
    // Create agent in pending state
    sqlx::query(
        "INSERT INTO sinex_schemas.agent_manifests (agent_name, version, status)
         VALUES ($1, $2, $3)",
    )
    .bind("status_test_agent")
    .bind("1.0.0")
    .bind("pending_registration")
    .execute(ctx.pool())
    .await
    .unwrap();

    // Valid status transitions
    let valid_statuses = vec![
        "running",
        "stopped",
        "error_state",
        "disabled_by_user",
        "degraded",
        "unknown",
    ];

    for status in valid_statuses {
        let result = sqlx::query(
            "UPDATE sinex_schemas.agent_manifests SET status = $1 WHERE agent_name = $2",
        )
        .bind(status)
        .bind("status_test_agent")
        .execute(ctx.pool())
        .await;

        assert!(
            result.is_ok(),
            "Status transition to {} should be valid",
            status
        );
    }

    // Test error state with error tracking
    let error_time = chrono::Utc::now();
    sqlx::query(
        "UPDATE sinex_schemas.agent_manifests
         SET status = $1, last_error_ts = $2, last_error_summary = $3
         WHERE agent_name = $4",
    )
    .bind("error_state")
    .bind(&error_time)
    .bind("Connection timeout to data source")
    .bind("status_test_agent")
    .execute(ctx.pool())
    .await
    .unwrap();

    let (status, error_ts, error_msg): (
        String,
        Option<chrono::DateTime<chrono::Utc>>,
        Option<String>,
    ) = sqlx::query_as(
        "SELECT status, last_error_ts, last_error_summary
             FROM sinex_schemas.agent_manifests WHERE agent_name = $1",
    )
    .bind("status_test_agent")
    .fetch_one(ctx.pool())
    .await
    .unwrap();

    pretty_assertions::assert_eq!(status, "error_state");
    assert!(error_ts.is_some());
    pretty_assertions::assert_eq!(error_msg.unwrap(), "Connection timeout to data source");

    Ok(())
}

#[sinex_test]
async fn test_agent_capabilities_and_dependencies(ctx: TestContext) -> TestResult {
    // Create agent with complex capabilities
    let capabilities = json!({
        "filesystem_read": ["/home/user/documents", "/var/log/app"],
        "filesystem_write": ["/tmp/sinex"],
        "network_host_allow": ["api.openai.com:443", "github.com:443"],
        "db_tables_rw": ["core.artifacts", "core.entities"],
        "db_tables_ro": ["raw.events"],
        "system_commands": ["ps", "top", "df"]
    });

    let llm_deps = json!({
        "models_used": [
            "openai/gpt-4-turbo",
            "anthropic/claude-3-opus",
            "ollama/llama2:13b"
        ],
        "required_capabilities": [
            "function_calling",
            "json_mode",
            "vision"
        ],
        "estimated_tokens_per_hour": 50000,
        "fallback_model": "ollama/mistral:7b"
    });

    sqlx::query(
        "INSERT INTO sinex_schemas.agent_manifests
         (agent_name, version, required_capabilities, llm_dependencies)
         VALUES ($1, $2, $3::jsonb, $4::jsonb)",
    )
    .bind("capability_test_agent")
    .bind("1.0.0")
    .bind(&capabilities)
    .bind(&llm_deps)
    .execute(ctx.pool())
    .await
    .unwrap();

    // Query agents by capability
    let agents_with_fs_write: Vec<String> = sqlx::query_scalar(
        "SELECT agent_name FROM sinex_schemas.agent_manifests
         WHERE required_capabilities ? 'filesystem_write'",
    )
    .fetch_all(ctx.pool())
    .await
    .unwrap();

    assert!(agents_with_fs_write.contains(&"capability_test_agent".to_string()));

    // Query agents using specific LLM model
    let agents_using_gpt4: Vec<String> = sqlx::query_scalar(
        "SELECT agent_name FROM sinex_schemas.agent_manifests
         WHERE llm_dependencies @> '{\"models_used\": [\"openai/gpt-4-turbo\"]}'",
    )
    .fetch_all(ctx.pool())
    .await
    .unwrap();

    assert!(agents_using_gpt4.contains(&"capability_test_agent".to_string()));

    Ok(())
}

#[sinex_test]
async fn test_agent_event_subscription_queries(ctx: TestContext) -> TestResult {
    // Create multiple agents with different subscriptions
    let agents = vec![
        (
            "subscriber_1",
            json!({
                "raw.events_feed_all": [
                    {"source_filter": "desktop.hyprland.*", "event_type_filter": "window_*"}
                ]
            }),
        ),
        (
            "subscriber_2",
            json!({
                "raw.events_feed_all": [
                    {"source_filter": "app.browser.*", "event_type_filter": "page_loaded"},
                    {"source_filter": "app.terminal.*", "event_type_filter": "command_executed"}
                ]
            }),
        ),
        (
            "subscriber_3",
            json!({
                "sinex.pkm.note_updated": [{"schema_id_expected_ref": "01234567890123456789012345"}],
                "sinex.system.heartbeat": []
            }),
        ),
    ];

    for (name, subscriptions) in agents {
        sqlx::query(
            "INSERT INTO sinex_schemas.agent_manifests
             (agent_name, version, subscribes_to_event_types)
             VALUES ($1, $2, $3::jsonb)",
        )
        .bind(name)
        .bind("1.0.0")
        .bind(&subscriptions)
        .execute(ctx.pool())
        .await
        .unwrap();
    }

    // Query agents subscribing to any events (using GIN index)
    let subscribers: Vec<String> = sqlx::query_scalar(
        "SELECT agent_name FROM sinex_schemas.agent_manifests
         WHERE subscribes_to_event_types IS NOT NULL
         ORDER BY agent_name",
    )
    .fetch_all(ctx.pool())
    .await
    .unwrap();

    pretty_assertions::assert_eq!(subscribers.len(), 3);

    // Query agents subscribing to specific event feed
    let raw_feed_subscribers: Vec<String> = sqlx::query_scalar(
        "SELECT agent_name FROM sinex_schemas.agent_manifests
         WHERE subscribes_to_event_types ? 'raw.events_feed_all'
         ORDER BY agent_name",
    )
    .fetch_all(ctx.pool())
    .await
    .unwrap();

    pretty_assertions::assert_eq!(raw_feed_subscribers.len(), 2);
    assert!(raw_feed_subscribers.contains(&"subscriber_1".to_string()));
    assert!(raw_feed_subscribers.contains(&"subscriber_2".to_string()));

    Ok(())
}
