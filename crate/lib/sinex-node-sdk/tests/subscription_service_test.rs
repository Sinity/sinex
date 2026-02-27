//! Subscription service tests
//!
//! Tests for agent subscription patterns and event routing functionality.

use serde_json::json;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn test_agent_event_subscription_queries(ctx: TestContext) -> Result<()> {
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
        sqlx::query!(
            "INSERT INTO core.node_manifests
             (node_name, node_type, version, consumes_event_types)
             VALUES ($1, 'automaton', $2, $3)",
            name,
            "1.0.0",
            subscriptions
        )
        .execute(&ctx.pool)
        .await
        .unwrap();
    }

    // Query agents subscribing to any events (using GIN index)
    let subscribers = sqlx::query_scalar!(
        "SELECT node_name FROM core.node_manifests
         WHERE node_type = 'automaton' AND consumes_event_types IS NOT NULL
         ORDER BY node_name"
    )
    .fetch_all(&ctx.pool)
    .await
    .unwrap();

    assert_eq!(subscribers.len(), 3);

    // Query agents subscribing to specific event feed (JSONB ? operator)
    let raw_feed_subscribers = sqlx::query_scalar!(
        r#"SELECT node_name FROM core.node_manifests
         WHERE node_type = 'automaton' AND consumes_event_types ? 'core.events_feed_all'
         ORDER BY node_name"#
    )
    .fetch_all(&ctx.pool)
    .await
    .unwrap();

    assert_eq!(raw_feed_subscribers.len(), 2);
    assert!(raw_feed_subscribers.iter().any(|s| s == "subscriber_1"));
    assert!(raw_feed_subscribers.iter().any(|s| s == "subscriber_2"));

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

    sqlx::query!(
        "INSERT INTO core.node_manifests
         (node_name, node_type, version, consumes_event_types)
         VALUES ($1, 'automaton', $2, $3)",
        "pattern_matcher",
        "1.0.0",
        complex_subscriptions
    )
    .execute(&ctx.pool)
    .await
    .unwrap();

    // Verify the subscription was stored correctly
    let stored = sqlx::query!(
        "SELECT consumes_event_types FROM core.node_manifests
         WHERE node_name = $1 AND node_type = 'automaton'",
        "pattern_matcher"
    )
    .fetch_one(&ctx.pool)
    .await
    .unwrap();

    let stored_subscriptions = stored.consumes_event_types.unwrap();
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
        sqlx::query!(
            "INSERT INTO core.node_manifests
             (node_name, node_type, version, consumes_event_types)
             VALUES ($1, 'automaton', $2, $3)",
            name,
            "1.0.0",
            subscriptions
        )
        .execute(&ctx.pool)
        .await
        .unwrap();
    }

    // Query subscribers with priority ordering
    let prioritized_subscribers = sqlx::query_scalar!(
        "SELECT node_name FROM core.node_manifests
         WHERE node_type = 'automaton' AND consumes_event_types IS NOT NULL
         ORDER BY node_name"
    )
    .fetch_all(&ctx.pool)
    .await
    .unwrap();

    assert_eq!(prioritized_subscribers.len(), 3);
    assert!(prioritized_subscribers
        .iter()
        .any(|s| s == "high_priority_subscriber"));
    assert!(prioritized_subscribers
        .iter()
        .any(|s| s == "medium_priority_subscriber"));
    assert!(prioritized_subscribers
        .iter()
        .any(|s| s == "low_priority_subscriber"));

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
        let proc_name = format!("filter_test_{name}");

        let result = sqlx::query!(
            "INSERT INTO core.node_manifests
             (node_name, node_type, version, consumes_event_types)
             VALUES ($1, 'automaton', $2, $3)",
            proc_name,
            "1.0.0",
            subscription
        )
        .execute(&ctx.pool)
        .await;

        assert!(result.is_ok(), "Filter pattern '{name}' should be valid");
    }

    // Verify all filters were stored
    let filter_count = sqlx::query_scalar!(
        "SELECT COUNT(*) FROM core.node_manifests
         WHERE node_type = 'automaton' AND node_name LIKE 'filter_test_%'"
    )
    .fetch_one(&ctx.pool)
    .await
    .unwrap();

    assert_eq!(filter_count, Some(3));

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

    sqlx::query!(
        "INSERT INTO core.node_manifests
         (node_name, node_type, version, consumes_event_types)
         VALUES ($1, 'automaton', $2, $3)",
        "schema_subscriber",
        "1.0.0",
        schema_subscription
    )
    .execute(&ctx.pool)
    .await
    .unwrap();

    // Verify schema references are stored
    let stored = sqlx::query!(
        "SELECT consumes_event_types FROM core.node_manifests
         WHERE node_name = $1 AND node_type = 'automaton'",
        "schema_subscriber"
    )
    .fetch_one(&ctx.pool)
    .await
    .unwrap();

    let stored_subscription = stored.consumes_event_types.unwrap();
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

    sqlx::query!(
        "INSERT INTO core.node_manifests
         (node_name, node_type, version, consumes_event_types)
         VALUES ($1, 'automaton', $2, $3)",
        "updatable_subscriber",
        "1.0.0",
        initial_subscriptions
    )
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

    sqlx::query!(
        "UPDATE core.node_manifests
         SET consumes_event_types = $1, version = $2
         WHERE node_name = $3 AND node_type = 'automaton'",
        updated_subscriptions,
        "1.1.0",
        "updatable_subscriber"
    )
    .execute(&ctx.pool)
    .await
    .unwrap();

    // Verify the update
    let row = sqlx::query!(
        "SELECT version, consumes_event_types FROM core.node_manifests
         WHERE node_name = $1 AND node_type = 'automaton'",
        "updatable_subscriber"
    )
    .fetch_one(&ctx.pool)
    .await
    .unwrap();

    assert_eq!(row.version, "1.1.0");
    let subscriptions = row.consumes_event_types.unwrap();
    assert!(subscriptions.get("core.events_feed_all").is_some());
    assert!(subscriptions.get("sinex.system.status").is_some());

    let feed_subscriptions = subscriptions["core.events_feed_all"].as_array().unwrap();
    assert_eq!(feed_subscriptions.len(), 2);

    Ok(())
}
