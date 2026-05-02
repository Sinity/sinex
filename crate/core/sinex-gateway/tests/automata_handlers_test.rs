//! Regression coverage for operator-facing automata status.

use serde_json::json;
use sinex_db::DbPoolExt;
use sinex_gateway::handlers::automata::handle_automata_status;
use sinex_primitives::domain::{NodeName, NodeType};
use sinex_primitives::events::DynamicPayload;
use xtask::sandbox::prelude::*;

async fn insert_material_event(
    ctx: &TestContext,
    source: &str,
    event_type: &str,
    payload: serde_json::Value,
) -> TestResult<sinex_primitives::events::Event<serde_json::Value>> {
    let material_id = ctx.create_source_material(Some(source)).await?;
    let event = DynamicPayload::new(source, event_type, payload)
        .from_material(material_id)
        .build()?;
    Ok(ctx.pool().events().insert(event).await?)
}

async fn insert_metric_gauge(
    ctx: &TestContext,
    node_name: &str,
    node_run_id: sinex_primitives::Uuid,
    name: &str,
    value: f64,
    labels: serde_json::Value,
) -> TestResult<()> {
    let mut labels = labels.as_object().cloned().unwrap_or_default();
    labels.insert("node".to_string(), json!(node_name));
    labels.insert("node_model".to_string(), json!("transducer"));
    labels.insert("node_run_id".to_string(), json!(node_run_id.to_string()));

    insert_material_event(
        ctx,
        "sinex",
        "metric.gauge",
        json!({
            "name": name,
            "value": value,
            "labels": labels,
            "component": node_name,
        }),
    )
    .await?;
    Ok(())
}

#[sinex_test]
async fn automata_status_surfaces_registry_run_and_derived_metrics(
    ctx: TestContext,
) -> TestResult<()> {
    let pool = ctx.pool();
    let node_name = NodeName::new("canonicalizer-test");
    let manifest = pool
        .state()
        .register_node(
            &node_name,
            NodeType::Automaton,
            "1.0.0-test",
            Some("canonicalizes test commands"),
        )
        .await?;
    let run = pool
        .state()
        .start_node_run(
            manifest.id,
            "sinex-canonicalizer-test",
            "instance-a",
            "test-host",
            None,
            None,
        )
        .await?;

    insert_metric_gauge(
        &ctx,
        "canonicalizer-test",
        run.id.to_uuid(),
        "derived.events_processed.run",
        42.0,
        json!({}),
    )
    .await?;
    insert_metric_gauge(
        &ctx,
        "canonicalizer-test",
        run.id.to_uuid(),
        "derived.error_rate_5m",
        0.125,
        json!({}),
    )
    .await?;
    insert_metric_gauge(
        &ctx,
        "canonicalizer-test",
        run.id.to_uuid(),
        "derived.invalidations.pending",
        3.0,
        json!({}),
    )
    .await?;
    insert_metric_gauge(
        &ctx,
        "canonicalizer-test",
        run.id.to_uuid(),
        "derived.checkpoint.revision",
        7.0,
        json!({
            "checkpoint_kind": "internal",
            "checkpoint_position": "018f-test:#42",
        }),
    )
    .await?;

    let parent =
        insert_material_event(&ctx, "test.input", "test.input", json!({ "command": "ls" })).await?;
    let parent_id = parent.id.expect("inserted parent must have id");
    let output = DynamicPayload::new("test.output", "test.output", json!({ "canonical": "ls" }))
        .node_run_id(run.id.to_uuid())
        .from_parents(vec![parent_id])?
        .build()?;
    pool.events().insert(output).await?;

    let response = handle_automata_status(
        pool,
        json!({
            "stale_after_secs": 300,
            "recent_window_secs": 300,
        }),
    )
    .await?;
    let automata = response["automata"]
        .as_array()
        .expect("automata should be an array");
    assert_eq!(automata.len(), 1);
    let status = &automata[0];
    let run_id = run.id.to_string();

    assert_eq!(status["node_name"].as_str(), Some("canonicalizer-test"));
    assert_eq!(status["version"].as_str(), Some("1.0.0-test"));
    assert_eq!(status["live"].as_bool(), Some(true));
    assert_eq!(status["node_run_id"].as_str(), Some(run_id.as_str()));
    assert_eq!(status["events_processed_current_run"].as_i64(), Some(42));
    assert_eq!(status["pending_invalidation_count"].as_i64(), Some(3));
    assert_eq!(status["checkpoint_kind"].as_str(), Some("internal"));
    assert_eq!(
        status["checkpoint_position"].as_str(),
        Some("018f-test:#42")
    );
    assert_eq!(status["checkpoint_revision"].as_i64(), Some(7));
    assert_eq!(status["recent_output_count"].as_i64(), Some(1));
    assert!((status["error_rate_5m"].as_f64().expect("error rate") - 0.125).abs() < f64::EPSILON);
    assert!(!status["last_output_at"].is_null());

    Ok(())
}

#[sinex_test]
async fn automata_status_handles_live_run_without_metric_events(
    ctx: TestContext,
) -> TestResult<()> {
    let pool = ctx.pool();
    let node_name = NodeName::new("session-detector-test");
    let manifest = pool
        .state()
        .register_node(
            &node_name,
            NodeType::Automaton,
            "1.0.0-test",
            Some("detects activity sessions"),
        )
        .await?;
    let run = pool
        .state()
        .start_node_run(
            manifest.id,
            "sinex-session-detector-test",
            "instance-a",
            "test-host",
            None,
            None,
        )
        .await?;

    let response = handle_automata_status(
        pool,
        json!({
            "stale_after_secs": 300,
            "recent_window_secs": 300,
        }),
    )
    .await?;
    let automata = response["automata"]
        .as_array()
        .expect("automata should be an array");
    assert_eq!(automata.len(), 1);
    let status = &automata[0];
    let run_id = run.id.to_string();

    assert_eq!(status["node_name"].as_str(), Some("session-detector-test"));
    assert_eq!(status["live"].as_bool(), Some(true));
    assert_eq!(status["node_run_id"].as_str(), Some(run_id.as_str()));
    assert!(status["events_processed_current_run"].is_null());
    assert!(status["pending_invalidation_count"].is_null());
    assert!(status["checkpoint_kind"].is_null());
    assert!(status["checkpoint_position"].is_null());
    assert!(status["checkpoint_revision"].is_null());
    assert!(status["checkpoint_recorded_at"].is_null());
    assert!(status["error_rate_5m"].is_null());
    assert_eq!(status["recent_output_count"].as_i64(), Some(0));
    assert!(status["last_output_at"].is_null());
    assert!(status["last_replay_at"].is_null());

    Ok(())
}

#[sinex_test]
async fn automata_status_rejects_malformed_params(ctx: TestContext) -> TestResult<()> {
    let result = handle_automata_status(ctx.pool(), json!({ "stale_after_secs": "soon" })).await;

    assert!(result.is_err(), "malformed automata params must fail");
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("Invalid automata status request")
    );
    Ok(())
}
