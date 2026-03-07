//! Tests for node registry RPC handlers
//!
//! Validates:
//! - Node lifecycle: heartbeat activates, `mark_inactive` deactivates
//! - List active nodes filters correctly
//! - Health summary: active/inactive counts, stale threshold, empty registry
//!
//! Note: Handler response types (`NodesListActiveResponse`, `NodesHealthResponse`)
//! only derive `Serialize`, so we work with raw `serde_json::Value` in tests.

use serde_json::json;
use sinex_db::DbPoolExt;
use sinex_gateway::handlers::node_registry::{
    handle_nodes_health, handle_nodes_heartbeat, handle_nodes_list_active,
    handle_nodes_mark_inactive,
};
use sinex_primitives::domain::{NodeName, NodeType};
use xtask::sandbox::prelude::*;

/// Register a test node directly via the repository so we can test the handlers.
async fn register_test_node(
    pool: &sinex_db::DbPool,
    name: &str,
    node_type: NodeType,
) -> color_eyre::Result<()> {
    let node_name = NodeName::new(name);
    pool.state()
        .register_node(&node_name, node_type, "1.0.0-test", Some("test node"))
        .await?;
    Ok(())
}

/// Helper: check if a node name appears in the active list JSON response.
fn active_list_contains(list_json: &serde_json::Value, name: &str) -> bool {
    list_json["nodes"]
        .as_array()
        .is_some_and(|nodes| nodes.iter().any(|n| n["node_name"].as_str() == Some(name)))
}

/// Helper: find a node in the active list JSON response.
fn find_node_in_list<'a>(
    list_json: &'a serde_json::Value,
    name: &str,
) -> Option<&'a serde_json::Value> {
    list_json["nodes"]
        .as_array()
        .and_then(|nodes| nodes.iter().find(|n| n["node_name"].as_str() == Some(name)))
}

// ─── Node lifecycle: heartbeat activates node ──────────────────────────

#[sinex_test]
async fn heartbeat_activates_node(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool();

    // Register a node
    register_test_node(pool, "test-ingestor-hb", NodeType::Ingestor).await?;

    // Send heartbeat
    let hb_result =
        handle_nodes_heartbeat(pool, json!({ "node_name": "test-ingestor-hb" })).await?;
    let updated = hb_result["updated"].as_bool().unwrap_or(false);
    assert!(updated, "Heartbeat should return updated=true");

    // List active nodes — our node should appear
    let list_result = handle_nodes_list_active(pool, json!({})).await?;
    assert!(
        active_list_contains(&list_result, "test-ingestor-hb"),
        "Node with recent heartbeat should appear in active list"
    );

    Ok(())
}

// ─── mark_inactive makes node disappear from active list ────────────────

#[sinex_test]
async fn mark_inactive_removes_from_active_list(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool();

    // Register and activate via heartbeat
    register_test_node(pool, "test-ingestor-inactive", NodeType::Ingestor).await?;
    handle_nodes_heartbeat(pool, json!({ "node_name": "test-ingestor-inactive" })).await?;

    // Verify it's active
    let list = handle_nodes_list_active(pool, json!({})).await?;
    assert!(
        active_list_contains(&list, "test-ingestor-inactive"),
        "Should be active after heartbeat"
    );

    // Mark inactive
    let mark_result =
        handle_nodes_mark_inactive(pool, json!({ "node_name": "test-ingestor-inactive" })).await?;
    let marked = mark_result["marked"].as_bool().unwrap_or(false);
    assert!(marked, "mark_inactive should return marked=true");

    // Verify it's no longer in active list
    let list_after = handle_nodes_list_active(pool, json!({})).await?;
    assert!(
        !active_list_contains(&list_after, "test-ingestor-inactive"),
        "Node should not appear in active list after mark_inactive"
    );

    Ok(())
}

// ─── Health summary: empty registry ─────────────────────────────────────

#[sinex_test]
async fn health_summary_empty_registry(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool();

    let health_result = handle_nodes_health(pool, json!({})).await?;

    let active_count = health_result["active_count"].as_i64().unwrap_or(-1);
    assert_eq!(
        active_count, 0,
        "No active nodes expected in empty registry"
    );

    Ok(())
}

// ─── Health summary: active/inactive counts ─────────────────────────────

#[sinex_test]
async fn health_summary_counts_active_and_inactive(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool();

    // Register two nodes
    register_test_node(pool, "health-node-a", NodeType::Ingestor).await?;
    register_test_node(pool, "health-node-b", NodeType::Automaton).await?;

    // Activate node-a via heartbeat
    handle_nodes_heartbeat(pool, json!({ "node_name": "health-node-a" })).await?;

    // node-b has no heartbeat, so it should be inactive

    // Check health
    let health_result = handle_nodes_health(pool, json!({})).await?;

    let unique_nodes = health_result["unique_nodes"].as_i64().unwrap_or(0);
    let active_count = health_result["active_count"].as_i64().unwrap_or(-1);
    let inactive_count = health_result["inactive_count"].as_i64().unwrap_or(-1);
    assert_eq!(unique_nodes, 2, "Expected two registered nodes");
    assert_eq!(active_count, 1, "One node should be active after heartbeat");
    assert_eq!(
        inactive_count, 1,
        "One node should remain inactive without heartbeat"
    );

    Ok(())
}

// ─── Health summary: custom stale threshold ─────────────────────────────

#[sinex_test]
async fn health_summary_respects_stale_threshold(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool();

    // Register and heartbeat a node
    register_test_node(pool, "stale-test-node", NodeType::Ingestor).await?;
    handle_nodes_heartbeat(pool, json!({ "node_name": "stale-test-node" })).await?;

    // With a very large stale threshold (1 hour), the node should be active
    let health_result = handle_nodes_health(pool, json!({ "stale_after_secs": 3600 })).await?;

    let active_count = health_result["active_count"].as_i64().unwrap_or(-1);
    assert_eq!(
        active_count, 1,
        "Node should be active with a long stale threshold"
    );

    // With a stale threshold of 0 seconds, all nodes should be "inactive"
    // (no heartbeat can be "within the last 0 seconds")
    let health_zero = handle_nodes_health(pool, json!({ "stale_after_secs": 0 })).await?;

    let active_zero = health_zero["active_count"].as_i64().unwrap_or(-1);
    assert_eq!(
        active_zero, 0,
        "With stale_after_secs=0, no node can be active"
    );

    Ok(())
}

// ─── Heartbeat sets correct info ────────────────────────────────────────

#[sinex_test]
async fn heartbeat_sets_node_info(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool();

    register_test_node(pool, "info-check-node", NodeType::Automaton).await?;
    handle_nodes_heartbeat(pool, json!({ "node_name": "info-check-node" })).await?;

    let list_result = handle_nodes_list_active(pool, json!({})).await?;
    let node = find_node_in_list(&list_result, "info-check-node");

    assert!(node.is_some(), "Node should be in active list");
    let node = node.unwrap();

    assert_eq!(
        node["status"].as_str(),
        Some("active"),
        "Status should be 'active' after heartbeat"
    );
    assert_eq!(
        node["node_type"].as_str(),
        Some("automaton"),
        "node_type should be automaton (snake_case per serde rename)"
    );
    assert_eq!(
        node["version"].as_str(),
        Some("1.0.0-test"),
        "version should match registration"
    );
    assert!(
        !node["last_heartbeat_at"].is_null(),
        "last_heartbeat_at should be set after heartbeat"
    );

    Ok(())
}

// ─── Multiple heartbeats update timestamp ───────────────────────────────

#[sinex_test]
async fn multiple_heartbeats_update_timestamp(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool();

    register_test_node(pool, "multi-hb-node", NodeType::Ingestor).await?;

    // First heartbeat
    handle_nodes_heartbeat(pool, json!({ "node_name": "multi-hb-node" })).await?;

    let list1 = handle_nodes_list_active(pool, json!({})).await?;
    let ts1 = find_node_in_list(&list1, "multi-hb-node")
        .and_then(|n| n["last_heartbeat_at"].as_str())
        .map(String::from);

    // Small delay to ensure timestamp changes
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Second heartbeat
    handle_nodes_heartbeat(pool, json!({ "node_name": "multi-hb-node" })).await?;

    let list2 = handle_nodes_list_active(pool, json!({})).await?;
    let ts2 = find_node_in_list(&list2, "multi-hb-node")
        .and_then(|n| n["last_heartbeat_at"].as_str())
        .map(String::from);

    assert!(
        ts1.is_some() && ts2.is_some(),
        "Both timestamps should exist"
    );
    assert!(
        ts2.as_deref() >= ts1.as_deref(),
        "Second heartbeat timestamp should be >= first"
    );

    Ok(())
}

// ─── Re-activate after mark_inactive ────────────────────────────────────

#[sinex_test]
async fn reactivate_after_mark_inactive(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool();

    register_test_node(pool, "reactivate-node", NodeType::Ingestor).await?;
    handle_nodes_heartbeat(pool, json!({ "node_name": "reactivate-node" })).await?;

    // Mark inactive
    handle_nodes_mark_inactive(pool, json!({ "node_name": "reactivate-node" })).await?;

    // Verify inactive
    let list = handle_nodes_list_active(pool, json!({})).await?;
    assert!(
        !active_list_contains(&list, "reactivate-node"),
        "Should be inactive"
    );

    // Re-activate via heartbeat
    handle_nodes_heartbeat(pool, json!({ "node_name": "reactivate-node" })).await?;

    // Verify active again
    let list_after = handle_nodes_list_active(pool, json!({})).await?;
    assert!(
        active_list_contains(&list_after, "reactivate-node"),
        "Should be active again after heartbeat"
    );

    Ok(())
}
