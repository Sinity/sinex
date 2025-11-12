//! State repository integration tests
//!
//! These tests exercise the modern `StateRepository` helpers using the same
//! APIs production code relies on. They intentionally focus on three concrete
//! flows:
//! 1. Creating checkpoints tied to real events and verifying the associated
//!    operations log entries.
//! 2. Registering/heartbeat tracking for processors and confirming the active
//!    manifest view matches expectations.
//! 3. Running the health checks exposed by the state repository to ensure the
//!    database plumbing is provisioned for tests exactly like production.

use serde_json::json;
use sinex_core::db::repositories::{
    checkpoints::Checkpoint,
    state::{Operation, StateRepository},
    DbPoolExt,
};
use sinex_core::types::domain::ProcessorName;
use sinex_core::types::Id;
use sinex_test_utils::prelude::*;

fn build_operation(processor: &ProcessorName, scope: serde_json::Value) -> Operation {
    Operation {
        id: None,
        operation_type: "state_test".to_string(),
        operator: processor.as_ref().to_string(),
        scope: Some(scope),
        result_status: "success".to_string(),
        result_message: None,
        preview_summary: Some(json!({ "note": "state repo integration test" })),
        duration_ms: Some(5),
    }
}

#[sinex_test]
async fn test_checkpoint_and_operation_flow(ctx: TestContext) -> Result<()> {
    let state_repo = ctx.pool.state();
    let processor_name: ProcessorName = "state.integration".into();

    // Seed a real event so our checkpoint mirrors production usage.
    let event = ctx
        .create_test_event(
            processor_name.as_ref(),
            "state.test",
            json!({"case": "checkpoint-flow"}),
        )
        .await?;
    let event_id = event.id.expect("checkpoint test event should have id");

    let checkpoint = Checkpoint::new(processor_name.clone())
        .with_last_processed_id(event_id.clone())
        .with_checkpoint_data(json!({"batch": 1}));
    let saved_checkpoint = state_repo.save_checkpoint(checkpoint).await?;

    let scope = json!({
        "processor": processor_name.as_ref(),
        "checkpoint_id": saved_checkpoint.id.to_string()
    });
    let logged_operation = state_repo
        .log_operation(build_operation(&processor_name, scope))
        .await?;

    // Verify checkpoint persisted with the same ULID and payload.
    let fetched = state_repo
        .get_checkpoint(processor_name.as_ref())
        .await?
        .expect("checkpoint should exist");
    assert_eq!(fetched.id.to_string(), saved_checkpoint.id.to_string());
    assert_eq!(fetched.last_processed_id, Some(event_id));
    assert_eq!(fetched.checkpoint_data.unwrap()["batch"], 1);

    // And the operation can be retrieved with matching metadata.
    let op = state_repo
        .get_operation(&logged_operation.id)
        .await?
        .expect("operation should exist");
    assert_eq!(op.operation_type, "state_test");
    assert_eq!(op.operator, processor_name.as_ref());
    assert_eq!(op.result_status, "success");

    Ok(())
}

#[sinex_test]
async fn test_processor_registration_and_heartbeat(ctx: TestContext) -> Result<()> {
    let repo = ctx.pool.state();
    let processor: ProcessorName = format!("processor-{}", Id::<ProcessorName>::new()).into();
    let hostname = "state-host";

    repo.register_processor(&processor, "automaton", "1.0.0", Some(hostname))
        .await?;

    // After registration the processor should show up in the active set.
    let active = repo.get_active_processors().await?;
    assert!(active
        .iter()
        .any(|p| p.processor_name == processor.as_ref()));

    // Heartbeat updates should return true for registered processors.
    assert!(
        repo.update_processor_status(&processor, "1.0.0", "healthy")
            .await?
    );

    Ok(())
}

#[sinex_test]
async fn test_state_repository_health_checks(ctx: TestContext) -> Result<()> {
    let repo: StateRepository<'_> = ctx.pool.state();
    let report = repo.run_system_health_checks().await?;

    assert!(report.db_connected, "state repo health should see database");
    assert!(report.events_table_exists);
    assert!(report.checkpoints_table_exists);

    // TimescaleDB is optional, but if it is present we expect a version string.
    if let Some(version) = report.timescaledb_version {
        assert!(!version.is_empty());
    }

    Ok(())
}
