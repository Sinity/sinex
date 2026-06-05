//! Tests for runtime registry RPC handlers.
//!
//! The gateway exposes read-only runtime status surfaces here; lifecycle writes
//! now happen directly in the owning services/runtimes.

use sinex_db::DbPoolExt;
use sinexd::api::handlers::runtime_presence::{
    handle_runtime_health, handle_runtime_list_active,
};
use sinex_primitives::domain::{ModuleName, ModuleKind};
use sinex_primitives::rpc::runtime::{RuntimeHealthRequest, RuntimeListActiveRequest};
use xtask::sandbox::prelude::*;

async fn register_test_node(
    pool: &sinex_db::DbPool,
    name: &str,
    module_kind: ModuleKind,
    version: &str,
) -> color_eyre::Result<sinex_db::repositories::state::ManifestRow> {
    let module_name = ModuleName::new(name);
    Ok(pool
        .state()
        .register_module(&module_name, module_kind, version, Some("test node"))
        .await?)
}

fn find_node_in_list<'a>(
    list_json: &'a serde_json::Value,
    name: &str,
    instance_id: Option<&str>,
) -> Option<&'a serde_json::Value> {
    list_json["modules"].as_array().and_then(|modules| {
        modules.iter().find(|node| {
            node["module_name"].as_str() == Some(name) && node["instance_id"].as_str() == instance_id
        })
    })
}

#[sinex_test]
async fn list_active_uses_manifest_fallback_without_run(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool();
    let module_name = ModuleName::new("manifest-only-node");

    register_test_node(pool, "manifest-only-node", ModuleKind::Service, "1.0.0-test").await?;
    assert!(
        pool.state()
            .update_module_heartbeat_for_version(&module_name, "1.0.0-test")
            .await?
    );

    let list_result = handle_runtime_list_active(pool, RuntimeListActiveRequest::default()).await?;
    let list_result = serde_json::to_value(&list_result)?;
    let node = find_node_in_list(&list_result, "manifest-only-node", None)
        .expect("manifest-backed node should appear in active list");

    assert_eq!(node["heartbeat_source"].as_str(), Some("manifest"));
    assert_eq!(node["status"].as_str(), Some("active"));
    assert!(node["module_run_id"].is_null());
    assert!(node["service_name"].is_null());

    Ok(())
}

#[sinex_test]
async fn list_active_surfaces_run_identity_when_available(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool();
    let manifest =
        register_test_node(pool, "run-backed-node", ModuleKind::Source, "1.0.0-test").await?;

    let run = pool
        .state()
        .start_module_run(
            manifest.id,
            "sinex-run-backed-node",
            "instance-a",
            "test-host",
            None,
            None,
        )
        .await?;

    let list_result = handle_runtime_list_active(pool, RuntimeListActiveRequest::default()).await?;
    let list_result = serde_json::to_value(&list_result)?;
    let node = find_node_in_list(&list_result, "run-backed-node", Some("instance-a"))
        .expect("run-backed node should appear in active list");
    let run_id = run.id.to_string();

    assert_eq!(node["heartbeat_source"].as_str(), Some("run"));
    assert_eq!(node["status"].as_str(), Some("running"));
    assert_eq!(node["module_run_id"].as_str(), Some(run_id.as_str()));
    assert_eq!(node["service_name"].as_str(), Some("sinex-run-backed-node"));
    assert_eq!(node["host"].as_str(), Some("test-host"));
    assert!(
        !node["started_at"].is_null(),
        "run-backed modules should expose started_at"
    );

    Ok(())
}

#[sinex_test]
async fn list_active_keeps_parallel_runs_distinct(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool();
    let manifest =
        register_test_node(pool, "parallel-run-node", ModuleKind::Source, "1.0.0-test").await?;

    pool.state()
        .start_module_run(
            manifest.id,
            "sinex-parallel-run-node",
            "instance-a",
            "test-host",
            None,
            None,
        )
        .await?;
    pool.state()
        .start_module_run(
            manifest.id,
            "sinex-parallel-run-node",
            "instance-b",
            "test-host",
            None,
            None,
        )
        .await?;

    let list_result = handle_runtime_list_active(pool, RuntimeListActiveRequest::default()).await?;
    let list_result = serde_json::to_value(&list_result)?;
    let modules = list_result["modules"]
        .as_array()
        .expect("modules should be an array");

    let parallel_nodes = modules
        .iter()
        .filter(|node| node["module_name"].as_str() == Some("parallel-run-node"))
        .collect::<Vec<_>>();
    assert_eq!(parallel_nodes.len(), 2, "parallel runs must stay distinct");
    assert_ne!(
        parallel_nodes[0]["instance_id"].as_str(),
        parallel_nodes[1]["instance_id"].as_str()
    );

    Ok(())
}

#[sinex_test]
async fn health_counts_unique_modules_and_concrete_runs(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool();
    let manifest_only = ModuleName::new("manifest-health-node");
    let run_manifest =
        register_test_node(pool, "run-health-node", ModuleKind::Source, "1.0.0-test").await?;
    register_test_node(
        pool,
        "manifest-health-node",
        ModuleKind::Service,
        "1.0.0-test",
    )
    .await?;
    register_test_node(
        pool,
        "inactive-health-node",
        ModuleKind::Automaton,
        "1.0.0-test",
    )
    .await?;

    pool.state()
        .start_module_run(
            run_manifest.id,
            "sinex-run-health-node",
            "instance-a",
            "test-host",
            None,
            None,
        )
        .await?;
    assert!(
        pool.state()
            .update_module_heartbeat_for_version(&manifest_only, "1.0.0-test")
            .await?
    );

    let health_result = handle_runtime_health(pool, RuntimeHealthRequest::default()).await?;
    assert_eq!(health_result.unique_modules, 3);
    assert_eq!(health_result.active_count, 2);
    assert_eq!(health_result.inactive_count, 1);
    assert_eq!(health_result.active_run_count, 1);

    Ok(())
}
