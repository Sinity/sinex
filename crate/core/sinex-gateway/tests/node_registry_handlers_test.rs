//! Tests for node registry RPC handlers.
//!
//! The gateway exposes read-only runtime status surfaces here; lifecycle writes
//! now happen directly in the owning services/runtimes.

use serde_json::json;
use sinex_db::DbPoolExt;
use sinex_gateway::handlers::node_registry::{handle_nodes_health, handle_nodes_list_active};
use sinex_primitives::domain::{NodeName, NodeType};
use xtask::sandbox::prelude::*;

async fn register_test_node(
    pool: &sinex_db::DbPool,
    name: &str,
    node_type: NodeType,
    version: &str,
) -> color_eyre::Result<sinex_db::repositories::state::NodeManifest> {
    let node_name = NodeName::new(name);
    Ok(pool
        .state()
        .register_node(&node_name, node_type, version, Some("test node"))
        .await?)
}

fn find_node_in_list<'a>(
    list_json: &'a serde_json::Value,
    name: &str,
    instance_id: Option<&str>,
) -> Option<&'a serde_json::Value> {
    list_json["nodes"].as_array().and_then(|nodes| {
        nodes.iter().find(|node| {
            node["node_name"].as_str() == Some(name)
                && node["instance_id"].as_str() == instance_id
        })
    })
}

#[sinex_test]
async fn list_active_uses_manifest_fallback_without_run(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool();
    let node_name = NodeName::new("manifest-only-node");

    register_test_node(pool, "manifest-only-node", NodeType::Service, "1.0.0-test").await?;
    assert!(pool
        .state()
        .update_node_heartbeat_for_version(&node_name, "1.0.0-test")
        .await?);

    let list_result = handle_nodes_list_active(pool, json!({})).await?;
    let node = find_node_in_list(&list_result, "manifest-only-node", None)
        .expect("manifest-backed node should appear in active list");

    assert_eq!(node["heartbeat_source"].as_str(), Some("manifest"));
    assert_eq!(node["status"].as_str(), Some("active"));
    assert!(node["node_run_id"].is_null());
    assert!(node["service_name"].is_null());

    Ok(())
}

#[sinex_test]
async fn list_active_surfaces_run_identity_when_available(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool();
    let manifest = register_test_node(pool, "run-backed-node", NodeType::Ingestor, "1.0.0-test")
        .await?;

    let run = pool
        .state()
        .start_node_run(
            manifest.id,
            "sinex-run-backed-node",
            "instance-a",
            "test-host",
            None,
            None,
        )
        .await?;

    let list_result = handle_nodes_list_active(pool, json!({})).await?;
    let node = find_node_in_list(&list_result, "run-backed-node", Some("instance-a"))
        .expect("run-backed node should appear in active list");
    let run_id = run.id.to_string();

    assert_eq!(node["heartbeat_source"].as_str(), Some("run"));
    assert_eq!(node["status"].as_str(), Some("running"));
    assert_eq!(node["node_run_id"].as_str(), Some(run_id.as_str()));
    assert_eq!(node["service_name"].as_str(), Some("sinex-run-backed-node"));
    assert_eq!(node["host"].as_str(), Some("test-host"));
    assert!(
        !node["started_at"].is_null(),
        "run-backed nodes should expose started_at"
    );

    Ok(())
}

#[sinex_test]
async fn list_active_keeps_parallel_runs_distinct(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool();
    let manifest = register_test_node(pool, "parallel-run-node", NodeType::Ingestor, "1.0.0-test")
        .await?;

    pool.state()
        .start_node_run(
            manifest.id,
            "sinex-parallel-run-node",
            "instance-a",
            "test-host",
            None,
            None,
        )
        .await?;
    pool.state()
        .start_node_run(
            manifest.id,
            "sinex-parallel-run-node",
            "instance-b",
            "test-host",
            None,
            None,
        )
        .await?;

    let list_result = handle_nodes_list_active(pool, json!({})).await?;
    let nodes = list_result["nodes"]
        .as_array()
        .expect("nodes should be an array");

    let parallel_nodes = nodes
        .iter()
        .filter(|node| node["node_name"].as_str() == Some("parallel-run-node"))
        .collect::<Vec<_>>();
    assert_eq!(parallel_nodes.len(), 2, "parallel runs must stay distinct");
    assert_ne!(
        parallel_nodes[0]["instance_id"].as_str(),
        parallel_nodes[1]["instance_id"].as_str()
    );

    Ok(())
}

#[sinex_test]
async fn health_counts_unique_nodes_and_concrete_runs(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool();
    let manifest_only = NodeName::new("manifest-health-node");
    let run_manifest = register_test_node(pool, "run-health-node", NodeType::Ingestor, "1.0.0-test")
        .await?;
    register_test_node(pool, "manifest-health-node", NodeType::Service, "1.0.0-test").await?;
    register_test_node(pool, "inactive-health-node", NodeType::Automaton, "1.0.0-test").await?;

    pool.state()
        .start_node_run(
            run_manifest.id,
            "sinex-run-health-node",
            "instance-a",
            "test-host",
            None,
            None,
        )
        .await?;
    assert!(pool
        .state()
        .update_node_heartbeat_for_version(&manifest_only, "1.0.0-test")
        .await?);

    let health_result = handle_nodes_health(pool, json!({})).await?;
    assert_eq!(health_result["unique_nodes"].as_i64(), Some(3));
    assert_eq!(health_result["active_count"].as_i64(), Some(2));
    assert_eq!(health_result["inactive_count"].as_i64(), Some(1));
    assert_eq!(health_result["active_run_count"].as_i64(), Some(1));

    Ok(())
}

#[sinex_test]
async fn nodes_list_active_rejects_malformed_params(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool();

    let result = handle_nodes_list_active(pool, json!(["unexpected"])).await;
    assert!(result.is_err(), "malformed list-active params must fail");
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("Invalid nodes list active request")
    );

    Ok(())
}

#[sinex_test]
async fn nodes_health_rejects_malformed_params(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool();

    let result = handle_nodes_health(pool, json!({ "stale_after_secs": "soon" })).await;
    assert!(result.is_err(), "malformed health params must fail");
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("Invalid nodes health request")
    );

    Ok(())
}
