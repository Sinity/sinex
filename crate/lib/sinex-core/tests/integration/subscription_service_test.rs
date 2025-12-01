//! Subscription service tests
//!
//! Tests for agent subscription patterns and event routing functionality

use serde_json::json;
use sinex_test_utils::prelude::*;

#[sinex_test]
async fn test_agent_event_subscription_queries(ctx: TestContext) -> Result<()> {
    ctx.force_cleanup().await?;
    ctx.ensure_clean().await?;
    sinex_test_utils::db_common::reset_database(ctx.pool()).await?;
    sinex_test_utils::db_common::verify_clean_state(ctx.pool()).await?;
    // Create multiple agents with different subscriptions
    let agents = vec![
        (
            "subscriber_1",
            json!({
                "core.events_feed_all": [
                    {"source_filter": "desktop.hyprland.*", "event_type_filter": "window_*"}
                ]
            }),
        ),
        (
            "subscriber_2",
            json!({
                "core.events_feed_all": [
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
            "INSERT INTO core.processor_manifests
             (processor_name, processor_type, version, consumes_event_types)
             VALUES ($1, 'automaton', $2, $3)",
        )
        .bind(name)
        .bind("1.0.0")
        .bind(&subscriptions)
        .execute(&ctx.pool)
        .await
        .unwrap();
    }

    // Query agents subscribing to any events (using GIN index)
    let subscribers: Vec<String> = sqlx::query_scalar(
        "SELECT processor_name FROM core.processor_manifests
         WHERE processor_type = 'automaton' AND consumes_event_types IS NOT NULL
         ORDER BY processor_name",
    )
    .fetch_all(&ctx.pool)
    .await
    .unwrap();

    pretty_assertions::assert_eq!(subscribers.len(), 3);

    // Query agents subscribing to specific event feed
    let raw_feed_subscribers: Vec<String> = sqlx::query_scalar(
        r#"SELECT processor_name FROM core.processor_manifests
         WHERE processor_type = 'automaton' AND consumes_event_types ? 'core.events_feed_all'
         ORDER BY processor_name"#,
    )
    .fetch_all(&ctx.pool)
    .await
    .unwrap();

    pretty_assertions::assert_eq!(raw_feed_subscribers.len(), 2);
    assert!(raw_feed_subscribers.contains(&"subscriber_1".to_string()));
    assert!(raw_feed_subscribers.contains(&"subscriber_2".to_string()));

    sinex_test_utils::db_common::reset_database(ctx.pool()).await?;
    sinex_test_utils::db_common::verify_clean_state(ctx.pool()).await?;
    ctx.force_cleanup().await?;
    Ok(())
}

#[sinex_test]
async fn test_subscription_pattern_matching(ctx: TestContext) -> Result<()> {
    // Create an agent with complex subscription patterns
    let complex_subscriptions = json!({
        "core.events_feed_all": [
            {"source_filter": "fs.*", "event_type_filter": "file_*"},
            {"source_filter": "terminal.*", "event_type_filter": "*_executed"},
            {"source_filter": "desktop.*", "event_type_filter": "window_focused|window_closed"}
        ]
    });

    sqlx::query(
        "INSERT INTO core.processor_manifests
         (processor_name, processor_type, version, consumes_event_types)
         VALUES ($1, 'automaton', $2, $3)",
    )
    .bind("pattern_matcher")
    .bind("1.0.0")
    .bind(&complex_subscriptions)
    .execute(&ctx.pool)
    .await
    .unwrap();

    // Verify the subscription was stored correctly
    let stored_subscriptions: serde_json::Value = sqlx::query_scalar(
        "SELECT consumes_event_types FROM core.processor_manifests
         WHERE processor_name = $1 AND processor_type = 'automaton'",
    )
    .bind("pattern_matcher")
    .fetch_one(&ctx.pool)
    .await
    .unwrap();

    assert!(stored_subscriptions.get("core.events_feed_all").is_some());
    let patterns = stored_subscriptions["core.events_feed_all"]
        .as_array()
        .unwrap();
    assert_eq!(patterns.len(), 3);

    Ok(())
}

#[sinex_test]
async fn test_subscription_routing_priorities(ctx: TestContext) -> Result<()> {
    // Create agents with overlapping subscriptions to test routing priorities
    let priority_agents = vec![
        (
            "high_priority_subscriber",
            json!({
                "core.events_feed_all": [
                    {"source_filter": "critical.*", "event_type_filter": "*", "priority": "high"}
                ]
            }),
        ),
        (
            "medium_priority_subscriber",
            json!({
                "core.events_feed_all": [
                    {"source_filter": "critical.*", "event_type_filter": "*", "priority": "medium"}
                ]
            }),
        ),
        (
            "low_priority_subscriber",
            json!({
                "core.events_feed_all": [
                    {"source_filter": "*", "event_type_filter": "*", "priority": "low"}
                ]
            }),
        ),
    ];

    for (name, subscriptions) in priority_agents {
        sqlx::query(
            "INSERT INTO core.processor_manifests
             (processor_name, processor_type, version, consumes_event_types)
             VALUES ($1, 'automaton', $2, $3)",
        )
        .bind(name)
        .bind("1.0.0")
        .bind(&subscriptions)
        .execute(&ctx.pool)
        .await
        .unwrap();
    }

    // Query subscribers with priority ordering
    let prioritized_subscribers: Vec<String> = sqlx::query_scalar(
        "SELECT processor_name FROM core.processor_manifests
         WHERE processor_type = 'automaton' AND consumes_event_types IS NOT NULL
         ORDER BY processor_name",
    )
    .fetch_all(&ctx.pool)
    .await
    .unwrap();

    assert_eq!(prioritized_subscribers.len(), 3);
    assert!(prioritized_subscribers.contains(&"high_priority_subscriber".to_string()));
    assert!(prioritized_subscribers.contains(&"medium_priority_subscriber".to_string()));
    assert!(prioritized_subscribers.contains(&"low_priority_subscriber".to_string()));

    Ok(())
}

#[sinex_test]
async fn test_subscription_filter_validation(ctx: TestContext) -> Result<()> {
    // Test various filter patterns for validation
    let test_filters = vec![
        (
            "valid_wildcard",
            json!({"source_filter": "app.*", "event_type_filter": "*"}),
        ),
        (
            "valid_specific",
            json!({"source_filter": "terminal.kitty", "event_type_filter": "command_executed"}),
        ),
        (
            "valid_multiple",
            json!({"source_filter": "fs.watcher|fs.scanner", "event_type_filter": "file_created|file_modified"}),
        ),
    ];

    for (name, filter) in test_filters {
        let subscription = json!({
            "core.events_feed_all": [filter]
        });

        let result = sqlx::query(
            "INSERT INTO core.processor_manifests
             (processor_name, processor_type, version, consumes_event_types)
             VALUES ($1, 'automaton', $2, $3)",
        )
        .bind(format!("filter_test_{}", name))
        .bind("1.0.0")
        .bind(&subscription)
        .execute(&ctx.pool)
        .await;

        assert!(result.is_ok(), "Filter pattern '{}' should be valid", name);
    }

    // Verify all filters were stored
    let filter_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM core.processor_manifests
         WHERE processor_type = 'automaton' AND processor_name LIKE 'filter_test_%'",
    )
    .fetch_one(&ctx.pool)
    .await
    .unwrap();

    assert_eq!(filter_count, 3);

    Ok(())
}

#[sinex_test]
async fn test_subscription_schema_references(ctx: TestContext) -> Result<()> {
    // Test subscriptions with schema references
    let schema_subscription = json!({
        "sinex.pkm.note_updated": [
            {"schema_id_expected_ref": "01234567890123456789012345"}
        ],
        "sinex.system.heartbeat": [],
        "custom.structured_event": [
            {"schema_id_expected_ref": "98765432109876543210987654"}
        ]
    });

    sqlx::query(
        "INSERT INTO core.processor_manifests
         (processor_name, processor_type, version, consumes_event_types)
         VALUES ($1, 'automaton', $2, $3)",
    )
    .bind("schema_subscriber")
    .bind("1.0.0")
    .bind(&schema_subscription)
    .execute(&ctx.pool)
    .await
    .unwrap();

    // Verify schema references are stored
    let stored_subscription: serde_json::Value = sqlx::query_scalar(
        "SELECT consumes_event_types FROM core.processor_manifests
         WHERE processor_name = $1 AND processor_type = 'automaton'",
    )
    .bind("schema_subscriber")
    .fetch_one(&ctx.pool)
    .await
    .unwrap();

    assert!(stored_subscription.get("sinex.pkm.note_updated").is_some());
    assert!(stored_subscription.get("sinex.system.heartbeat").is_some());
    assert!(stored_subscription.get("custom.structured_event").is_some());

    // Check schema ID reference format
    let note_subscription = &stored_subscription["sinex.pkm.note_updated"][0];
    let schema_ref = note_subscription["schema_id_expected_ref"]
        .as_str()
        .unwrap();
    assert_eq!(schema_ref.len(), 26); // ULID length
    assert_eq!(schema_ref, "01234567890123456789012345");

    Ok(())
}

#[sinex_test]
async fn test_subscription_updates_and_changes(ctx: TestContext) -> Result<()> {
    // Create an agent with initial subscriptions
    let initial_subscriptions = json!({
        "core.events_feed_all": [
            {"source_filter": "initial.*", "event_type_filter": "*"}
        ]
    });

    sqlx::query(
        "INSERT INTO core.processor_manifests
         (processor_name, processor_type, version, consumes_event_types)
         VALUES ($1, 'automaton', $2, $3)",
    )
    .bind("updatable_subscriber")
    .bind("1.0.0")
    .bind(&initial_subscriptions)
    .execute(&ctx.pool)
    .await
    .unwrap();

    // Update subscriptions
    let updated_subscriptions = json!({
        "core.events_feed_all": [
            {"source_filter": "updated.*", "event_type_filter": "updated_*"},
            {"source_filter": "new.*", "event_type_filter": "new_*"}
        ],
        "sinex.system.status": []
    });

    sqlx::query(
        "UPDATE core.processor_manifests 
         SET consumes_event_types = $1, version = $2
         WHERE processor_name = $3 AND processor_type = 'automaton'",
    )
    .bind(&updated_subscriptions)
    .bind("1.1.0")
    .bind("updatable_subscriber")
    .execute(&ctx.pool)
    .await
    .unwrap();

    // Verify the update
    let (version, subscriptions): (String, serde_json::Value) = sqlx::query_as(
        "SELECT version, consumes_event_types FROM core.processor_manifests
         WHERE processor_name = $1 AND processor_type = 'automaton'",
    )
    .bind("updatable_subscriber")
    .fetch_one(&ctx.pool)
    .await
    .unwrap();

    assert_eq!(version, "1.1.0");
    assert!(subscriptions.get("core.events_feed_all").is_some());
    assert!(subscriptions.get("sinex.system.status").is_some());

    let feed_subscriptions = subscriptions["core.events_feed_all"].as_array().unwrap();
    assert_eq!(feed_subscriptions.len(), 2);

    Ok(())
}
