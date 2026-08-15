use super::recover_stale_replay_operations;
use super::{
    GatewayHealthReport, GatewayHealthStatus, NatsHealthProbe, RawIngestDlqHealth,
    ReplayControlStatus, SseConfirmationStatus, apply_schema_compilation_health,
};
use crate::api::{ReplayScope, ReplayState};
use sinex_db::DbPoolExt;
use sinex_db::repositories::schema_management::NewEventSchema;
use sinex_db::validation::SchemaCompilationFailure;
use sinex_primitives::domain::{EventSource, EventType};
use sinex_primitives::domain::OperationStatus;
use sinex_primitives::events::{EventPayload, payloads::FileCreatedPayload};
use sinex_primitives::rpc::system::SystemHealthRequest;
use serde_json::json;
use sqlx::postgres::PgPoolOptions;
use std::collections::HashMap;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn stale_replay_recovery_accepts_clean_state(ctx: TestContext) -> TestResult<()> {
    let replay = sinex_db::replay::state_machine::ReplayStateMachine::new(ctx.pool.clone());
    recover_stale_replay_operations(&replay).await?;
    Ok(())
}

#[sinex_test]
async fn health_report_marks_schema_compilation_failures_as_degraded() -> TestResult<()> {
    let mut report = GatewayHealthReport {
        status: GatewayHealthStatus::Healthy,
        db_ok: true,
        db_latency_ms: Some(1),
        db_detail: "ok".to_string(),
        nats: NatsHealthProbe {
            connected: true,
            latency_ms: Some(1),
            detail: "ok".to_string(),
        },
        raw_ingest_dlq: RawIngestDlqHealth {
            status: GatewayHealthStatus::Healthy,
            connected: true,
            pending_messages: Some(0),
            pending_sequence_span: Some(0),
            detail: "empty".to_string(),
        },
        replay: ReplayControlStatus {
            enabled: true,
            connected: true,
            last_error: None,
        },
        sse_confirmation: SseConfirmationStatus {
            running: true,
            degraded: false,
            detail: "ok".to_string(),
        },
        runtime_liveness: sinex_primitives::RuntimeLivenessAggregate::evaluate(
            Vec::new(),
            sinex_primitives::RuntimeLivenessPolicy::default(),
            sinex_primitives::Timestamp::now(),
        ),
        healthy: true,
        serving: true,
        degradation_reasons: Vec::new(),
    };
    apply_schema_compilation_health(
        &mut report,
        Ok(vec![SchemaCompilationFailure {
            name: "test.source.test.event".to_string(),
            schema_version: "1".to_string(),
            schema_id: uuid::Uuid::from_u128(1),
            error: "invalid type".to_string(),
        }]),
    );

    assert!(!report.healthy);
    assert_eq!(report.status, GatewayHealthStatus::Degraded);
    assert!(report.degradation_reasons[0].contains("schema_id="));

    let mut recovered_report = report.clone();
    recovered_report.status = GatewayHealthStatus::Healthy;
    recovered_report.healthy = true;
    recovered_report.degradation_reasons.clear();
    apply_schema_compilation_health(&mut recovered_report, Ok(Vec::new()));
    assert!(
        recovered_report.healthy,
        "successful reload must clear the failure signal"
    );
    assert_eq!(recovered_report.status, GatewayHealthStatus::Healthy);
    assert!(recovered_report.degradation_reasons.is_empty());
    Ok(())
}

#[sinex_test]
async fn system_health_reports_schema_compilation_failure_from_active_db_row(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let mut env = EnvGuard::new();
    let temp_dir = tempfile::TempDir::new()?;
    env.set("SINEX_NATS_URL", ctx.nats_handle()?.client_url());
    env.set(
        "SINEX_CONTENT_STORE_PATH",
        temp_dir.path().to_string_lossy().as_ref(),
    );

    let source = EventSource::from_static("health_schema_probe");
    let event_type = EventType::from_static("health_schema_probe.event");
    let invalid = ctx
        .pool
        .schemas()
        .register_schema(NewEventSchema {
            source: source.clone(),
            event_type: event_type.clone(),
            schema_version: "1.0.0".to_string(),
            schema_content: json!({"type": 42}),
        })
        .await?;

    let container = super::ServiceContainer::from_database_url(ctx.database_url()).await?;
    let failure_response = super::super::handlers::handle_system_health(
        &container,
        SystemHealthRequest {},
    )
    .await?;
    let failure_reason = failure_response
        .degradation_reasons
        .iter()
        .find(|reason| reason.contains("schema compilation failure"))
        .expect("active invalid schema must be visible through system.health");
    assert!(failure_reason.contains(source.as_str()));
    assert!(failure_reason.contains(event_type.as_str()));
    assert!(failure_reason.contains(&invalid.id.to_string()));
    assert!(!failure_response.healthy);

    ctx.pool
        .schemas()
        .register_schema(NewEventSchema {
            source,
            event_type,
            schema_version: "2.0.0".to_string(),
            schema_content: json!({"type": "object"}),
        })
        .await?;

    let recovered_response = super::super::handlers::handle_system_health(
        &container,
        SystemHealthRequest {},
    )
    .await?;
    assert!(
        recovered_response
            .degradation_reasons
            .iter()
            .all(|reason| !reason.contains("schema compilation failure")),
        "successful schema reload must clear the schema failure signal: {:?}",
        recovered_response.degradation_reasons
    );

    Ok(())
}

#[sinex_test]
async fn stale_replay_recovery_surfaces_startup_failures() -> TestResult<()> {
    let pool = PgPoolOptions::new()
        .acquire_timeout(std::time::Duration::from_millis(10))
        .connect_lazy("postgresql://127.0.0.1:1/sinex_test")?;
    let replay = sinex_db::replay::state_machine::ReplayStateMachine::new(pool);

    let error = recover_stale_replay_operations(&replay)
        .await
        .expect_err("startup recovery should fail honestly when the pool is unusable");

    let message = error.to_string();
    assert!(message.contains("Failed to recover stale replay operations on startup"));
    assert!(message.contains("gateway.recover_stale_replay_operations"));
    Ok(())
}

#[sinex_test]
async fn startup_recovery_restores_terminal_failed_replay_archive_debt(
    ctx: TestContext,
) -> TestResult<()> {
    let replay = sinex_db::replay::state_machine::ReplayStateMachine::new(ctx.pool.clone());
    let operation = replay
        .create_operation(
            ReplayScope {
                source_name: "startup-terminal-failed-replay".to_string(),
                time_window: None,
                material_filter: None,
                filters: HashMap::new(),
                ..Default::default()
            },
            "test:startup-recovery".to_string(),
        )
        .await?;

    let material_id = ctx
        .create_source_material(Some("terminal-failed-replay-material"))
        .await?;
    let event = ctx
        .pool()
        .events()
        .insert(
            FileCreatedPayload {
                path: "/tmp/terminal-failed-replay.txt".into(),
                size: 7,
                created_at: sinex_primitives::Timestamp::now(),
                permissions: None,
            }
            .from_material(material_id)
            .build()?,
        )
        .await?;
    let event_id = event.id.expect("inserted event should have an id");
    let archive_reason = format!(
        "superseded by replay re-execution (operation {})",
        operation.operation_id
    );
    ctx.pool()
        .events()
        .execute_cascade_archive(
            &[*event_id.as_uuid()],
            &archive_reason,
            &operation.operation_id.to_string(),
            "test",
        )
        .await?;
    ctx.pool()
        .state()
        .update_operation_meta(
            &sinex_primitives::Id::<sinex_db::repositories::Operation>::from_uuid(
                operation.operation_id,
            ),
            OperationStatus::Failed,
            Some("compensation failed after archive"),
            serde_json::json!({"archive_recovery": {"remaining_archived_events": 1}}),
        )
        .await?;

    assert!(ctx.pool().events().get_by_id(event_id).await?.is_none());
    assert_eq!(
        ctx.pool()
            .state()
            .list_failed_replay_archive_debt(32)
            .await?
            .len(),
        1
    );

    recover_stale_replay_operations(&replay).await?;

    assert!(ctx.pool().events().get_by_id(event_id).await?.is_some());
    assert!(
        ctx.pool()
            .state()
            .list_failed_replay_archive_debt(32)
            .await?
            .is_empty(),
        "startup recovery must not leave terminal failed archive debt behind"
    );
    Ok(())
}

#[sinex_test]
async fn startup_recovery_restores_a_cascade_stranded_by_an_immediate_restart(
    ctx: TestContext,
) -> TestResult<()> {
    let replay = sinex_db::replay::state_machine::ReplayStateMachine::new(ctx.pool.clone());
    let operation = replay
        .create_operation(
            ReplayScope {
                source_name: "startup-recovery-test".to_string(),
                filters: HashMap::new(),
                ..Default::default()
            },
            "test:planner".to_string(),
        )
        .await?;
    replay
        .update_preview(
            operation.operation_id,
            serde_json::json!({ "total_events": 1 }),
        )
        .await?;
    replay
        .approve(operation.operation_id, "admin:approver".to_string())
        .await?;
    replay
        .transition(operation.operation_id, ReplayState::Executing)
        .await?;

    let material_id = ctx
        .create_source_material(Some("startup-recovery-material"))
        .await?;
    let event = ctx
        .pool
        .events()
        .insert(
            FileCreatedPayload {
                path: "/tmp/startup-recovery.txt".into(),
                size: 1,
                created_at: sinex_primitives::temporal::Timestamp::now(),
                permissions: None,
            }
            .from_material(material_id)
            .build()?,
        )
        .await?;
    let event_id = event.id.expect("startup recovery event must have an id");

    ctx.pool
        .events()
        .execute_cascade_archive(
            &[*event_id.as_uuid()],
            "superseded by replay re-execution",
            &operation.operation_id.to_string(),
            "test",
        )
        .await?;
    let mut tx = ctx.pool.begin().await?;
    replay
        .record_scope_invalidations_pending_with_tx(
            &mut tx,
            operation.operation_id,
            1,
            0,
            0,
            1,
            "superseded by replay re-execution",
        )
        .await?;
    tx.commit().await?;

    // The production default is RestartSec=10. Simulate the journal left by a
    // process that crashed after committing its archive, then restarted after
    // that delay. This remains inside the former ten-minute stale threshold.
    const RESTART_SEC_WINDOW: time::Duration = time::Duration::seconds(10);
    let just_before_restart = sinex_primitives::temporal::now() - RESTART_SEC_WINDOW;
    sqlx::query!(
        r#"
        UPDATE core.operations_log
        SET preview_summary = jsonb_set(
                preview_summary,
                '{started_at}',
                to_jsonb($2::timestamptz),
                true
            )
        WHERE id = $1::uuid
        "#,
        operation.operation_id,
        *just_before_restart,
    )
    .execute(&ctx.pool)
    .await?;

    recover_stale_replay_operations(&replay).await?;

    let recovered = replay.load_operation(operation.operation_id).await?;
    assert_eq!(recovered.state, ReplayState::Failed);
    assert!(
        recovered
            .error_details
            .as_deref()
            .is_some_and(|details| details.contains("restored")),
        "startup recovery should report the cascade restore: {:?}",
        recovered.error_details
    );
    assert!(
        ctx.pool.events().get_by_id(event_id).await?.is_some(),
        "immediate startup recovery must restore the archived event"
    );

    Ok(())
}
