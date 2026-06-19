//! Tests for runtime registry RPC handlers.
//!
//! The gateway exposes read-only runtime status surfaces here; lifecycle writes
//! now happen directly in the owning services/runtimes.

use sinex_db::DbPoolExt;
use sinex_primitives::domain::{ModuleKind, ModuleName};
use sinex_primitives::events::DynamicPayload;
use sinex_primitives::rpc::runtime::{RuntimeHealthRequest, RuntimeListActiveRequest};
use sinexd::api::handlers::runtime_presence::{handle_runtime_health, handle_runtime_list_active};
use xtask::sandbox::prelude::*;

async fn register_test_module(
    pool: &sinex_db::DbPool,
    name: &str,
    module_kind: ModuleKind,
    version: &str,
) -> color_eyre::Result<sinex_db::repositories::state::ManifestRow> {
    let module_name = ModuleName::new(name);
    Ok(pool
        .state()
        .register_module(&module_name, module_kind, version, Some("test module"))
        .await?)
}

fn find_module_in_list<'a>(
    list_json: &'a serde_json::Value,
    name: &str,
    instance_id: Option<&str>,
) -> Option<&'a serde_json::Value> {
    list_json["modules"].as_array().and_then(|modules| {
        modules.iter().find(|module| {
            module["module_name"].as_str() == Some(name)
                && module["instance_id"].as_str() == instance_id
        })
    })
}

async fn insert_runtime_health_status(
    ctx: &TestContext,
    component: &str,
    status: &str,
    reason: &str,
) -> TestResult<()> {
    let material_id = ctx.create_source_material(Some("sinex")).await?;
    let event = DynamicPayload::new(
        "sinex",
        "health.status",
        serde_json::json!({
            "component": component,
            "current_status": status,
            "reason": reason,
        }),
    )
    .from_material(material_id)
    .build()?;
    ctx.pool().events().insert(event).await?;
    Ok(())
}

#[sinex_test]
async fn list_active_requires_concrete_runtime_run(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool();

    register_test_module(
        pool,
        "manifest-only-module",
        ModuleKind::Service,
        "1.0.0-test",
    )
    .await?;

    let list_result = handle_runtime_list_active(pool, RuntimeListActiveRequest::default()).await?;
    let list_result = serde_json::to_value(&list_result)?;
    assert!(
        find_module_in_list(&list_result, "manifest-only-module", None).is_none(),
        "manifest registration is inventory, not active runtime presence"
    );

    Ok(())
}

#[sinex_test]
async fn list_active_surfaces_run_identity_when_available(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool();
    let manifest =
        register_test_module(pool, "run-backed-module", ModuleKind::Source, "1.0.0-test").await?;

    let run = pool
        .state()
        .start_module_run(
            manifest.id,
            "sinex-run-backed-module",
            "instance-a",
            "test-host",
            None,
            None,
        )
        .await?;
    insert_runtime_health_status(
        &ctx,
        "run-backed-module",
        "healthy",
        "runtime heartbeat observed",
    )
    .await?;

    let list_result = handle_runtime_list_active(pool, RuntimeListActiveRequest::default()).await?;
    let list_result = serde_json::to_value(&list_result)?;
    let module = find_module_in_list(&list_result, "run-backed-module", Some("instance-a"))
        .expect("run-backed module should appear in active list");
    let run_id = run.id.to_string();

    assert_eq!(module["heartbeat_source"].as_str(), Some("run"));
    assert_eq!(module["status"].as_str(), Some("running"));
    assert_eq!(module["module_run_id"].as_str(), Some(run_id.as_str()));
    assert_eq!(
        module["service_name"].as_str(),
        Some("sinex-run-backed-module")
    );
    assert_eq!(module["host"].as_str(), Some("test-host"));
    assert!(
        !module["started_at"].is_null(),
        "run-backed modules should expose started_at"
    );

    Ok(())
}

#[sinex_test]
async fn list_active_keeps_parallel_runs_distinct(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool();
    let manifest = register_test_module(
        pool,
        "parallel-run-module",
        ModuleKind::Source,
        "1.0.0-test",
    )
    .await?;

    pool.state()
        .start_module_run(
            manifest.id,
            "sinex-parallel-run-module",
            "instance-a",
            "test-host",
            None,
            None,
        )
        .await?;
    insert_runtime_health_status(
        &ctx,
        "parallel-run-module",
        "healthy",
        "runtime heartbeat observed",
    )
    .await?;

    pool.state()
        .start_module_run(
            manifest.id,
            "sinex-parallel-run-module",
            "instance-b",
            "test-host",
            None,
            None,
        )
        .await?;
    insert_runtime_health_status(
        &ctx,
        "parallel-run-module",
        "healthy",
        "runtime heartbeat observed",
    )
    .await?;

    let list_result = handle_runtime_list_active(pool, RuntimeListActiveRequest::default()).await?;
    let list_result = serde_json::to_value(&list_result)?;
    let modules = list_result["modules"]
        .as_array()
        .expect("modules should be an array");

    let parallel_modules = modules
        .iter()
        .filter(|module| module["module_name"].as_str() == Some("parallel-run-module"))
        .collect::<Vec<_>>();
    assert_eq!(
        parallel_modules.len(),
        2,
        "parallel runs must stay distinct"
    );
    assert_ne!(
        parallel_modules[0]["instance_id"].as_str(),
        parallel_modules[1]["instance_id"].as_str()
    );

    Ok(())
}

#[sinex_test]
async fn health_counts_unique_modules_and_concrete_runs(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool();
    let run_manifest =
        register_test_module(pool, "run-health-module", ModuleKind::Source, "1.0.0-test").await?;
    register_test_module(
        pool,
        "manifest-health-module",
        ModuleKind::Service,
        "1.0.0-test",
    )
    .await?;
    register_test_module(
        pool,
        "inactive-health-module",
        ModuleKind::Automaton,
        "1.0.0-test",
    )
    .await?;

    pool.state()
        .start_module_run(
            run_manifest.id,
            "sinex-run-health-module",
            "instance-a",
            "test-host",
            None,
            None,
        )
        .await?;
    insert_runtime_health_status(
        &ctx,
        "run-health-module",
        "degraded",
        "runtime heartbeat observed",
    )
    .await?;

    let health_result = handle_runtime_health(pool, RuntimeHealthRequest::default()).await?;
    assert_eq!(health_result.unique_modules, 3);
    assert_eq!(health_result.active_count, 1);
    assert_eq!(health_result.inactive_count, 2);
    assert_eq!(health_result.active_run_count, 1);

    Ok(())
}
