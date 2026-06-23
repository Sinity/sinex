use serde_json::json;
use sinex_db::DbPoolExt;
use sinex_primitives::domain::{OperationStatus, ReplayOutcome};
use sinex_primitives::events::DynamicPayload;
use sinex_primitives::events::payloads::email::{
    EmailAuthorizationState, EmailCaptureRuntimeObservedPayload, EmailNetworkState, EmailSyncState,
};
use sinex_primitives::rpc::lifecycle::{
    LifecycleArchiveRequest, LifecycleArchiveResponse, TombstoneCreateRequest,
    TombstoneCreateResponse, TombstoneOperationState, TombstoneStatusRequest,
    TombstoneStatusResponse,
};
use sinex_primitives::rpc::ops::{
    OpsCancelRequest, OpsCancelResponse, OpsGetRequest, OpsGetResponse, OpsListRequest,
    OpsListResponse, OpsStartRequest, OpsStartResponse,
};
use sinexd::api::handlers::{
    handle_lifecycle_archive as handle_lifecycle_archive_typed,
    handle_ops_cancel as handle_ops_cancel_typed, handle_ops_get as handle_ops_get_typed,
    handle_ops_list as handle_ops_list_typed, handle_ops_start as handle_ops_start_typed,
    handle_tombstone_create as handle_tombstone_create_typed,
    handle_tombstone_status as handle_tombstone_status_typed,
};
use sinexd::api::rpc_server::RpcAuthContext;
use sinexd::api::{ReplayScope, ReplayState, ReplayStateMachine};
use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;
use tokio::time::{Duration, timeout};
use xtask::sandbox::prelude::*;

fn system_auth() -> RpcAuthContext {
    RpcAuthContext::system()
}

async fn handle_ops_start(
    pool: &sqlx::PgPool,
    params: serde_json::Value,
    auth: &RpcAuthContext,
) -> TestResult<serde_json::Value> {
    let request: OpsStartRequest = serde_json::from_value(params)?;
    Ok(serde_json::to_value(
        handle_ops_start_typed(pool, request, auth).await?,
    )?)
}

fn provider_runtime_contract(
    scope: &serde_json::Value,
) -> TestResult<EmailCaptureRuntimeObservedPayload> {
    Ok(serde_json::from_value(
        scope["provider_runtime"]["runtime_observation_contract"].clone(),
    )?)
}

async fn handle_ops_list(
    pool: &sqlx::PgPool,
    params: serde_json::Value,
    auth: &RpcAuthContext,
) -> TestResult<serde_json::Value> {
    let request: OpsListRequest = serde_json::from_value(params)?;
    Ok(serde_json::to_value(
        handle_ops_list_typed(pool, request, auth).await?,
    )?)
}

async fn handle_ops_get(
    pool: &sqlx::PgPool,
    params: serde_json::Value,
    auth: &RpcAuthContext,
) -> TestResult<serde_json::Value> {
    let request: OpsGetRequest = serde_json::from_value(params)?;
    Ok(serde_json::to_value(
        handle_ops_get_typed(pool, request, auth).await?,
    )?)
}

async fn handle_ops_cancel(
    pool: &sqlx::PgPool,
    params: serde_json::Value,
    auth: &RpcAuthContext,
) -> TestResult<serde_json::Value> {
    let request: OpsCancelRequest = serde_json::from_value(params)?;
    Ok(serde_json::to_value(
        handle_ops_cancel_typed(pool, request, auth).await?,
    )?)
}

async fn handle_tombstone_create(
    pool: &sqlx::PgPool,
    params: serde_json::Value,
    auth: &RpcAuthContext,
) -> TestResult<serde_json::Value> {
    let request: TombstoneCreateRequest = serde_json::from_value(params)?;
    Ok(serde_json::to_value(
        handle_tombstone_create_typed(pool, request, auth).await?,
    )?)
}

async fn handle_tombstone_status(
    pool: &sqlx::PgPool,
    params: serde_json::Value,
    auth: &RpcAuthContext,
) -> TestResult<serde_json::Value> {
    let request: TombstoneStatusRequest = serde_json::from_value(params)?;
    Ok(serde_json::to_value(
        handle_tombstone_status_typed(pool, request, auth).await?,
    )?)
}

async fn handle_lifecycle_archive(
    pool: &sqlx::PgPool,
    params: serde_json::Value,
    auth: &RpcAuthContext,
) -> TestResult<serde_json::Value> {
    let request: LifecycleArchiveRequest = serde_json::from_value(params)?;
    Ok(serde_json::to_value(
        handle_lifecycle_archive_typed(pool, request, auth).await?,
    )?)
}

async fn start_test_operation(
    ctx: &TestContext,
    auth: &RpcAuthContext,
    operation_type: &str,
) -> TestResult<OpsStartResponse> {
    let start_result = handle_ops_start(
        ctx.pool(),
        json!({
            "operation_type": operation_type,
        }),
        auth,
    )
    .await?;
    Ok(serde_json::from_value(start_result)?)
}

async fn get_operation(
    ctx: &TestContext,
    auth: &RpcAuthContext,
    operation_id: &str,
) -> TestResult<OpsGetResponse> {
    let result = handle_ops_get(ctx.pool(), json!({ "operation_id": operation_id }), auth).await?;
    Ok(serde_json::from_value(result)?)
}

async fn publish_event(
    ctx: &TestContext,
    source: &str,
    sequence: i64,
) -> TestResult<sinex_primitives::events::Event<serde_json::Value>> {
    let material_id = ctx.create_source_material(Some(source)).await?;
    Ok(ctx
        .pool()
        .events()
        .insert(
            DynamicPayload::new(source, "test.ops", json!({ "sequence": sequence }))
                .from_material(material_id)
                .build()?,
        )
        .await?)
}

#[sinex_test]
async fn ops_start_creates_operation(ctx: TestContext) -> TestResult<()> {
    let auth = system_auth();
    let params = json!({
        "operation_type": "archive",
        "scope": {"key": "value"},
    });

    let result = handle_ops_start(ctx.pool(), params, &auth).await?;
    let response: OpsStartResponse = serde_json::from_value(result)?;

    assert!(!response.operation.id.is_empty());
    assert_eq!(response.operation.operation_type, "archive");
    assert_eq!(response.operation.result_status, OperationStatus::Running);
    assert_eq!(response.operation.operator, auth.actor_id());

    let persisted = get_operation(&ctx, &auth, &response.operation.id).await?;
    assert_eq!(persisted.operation.id, response.operation.id);
    assert_eq!(persisted.operation.operation_type, "archive");
    assert_eq!(persisted.operation.result_status, OperationStatus::Running);
    assert_eq!(persisted.operation.operator, auth.actor_id());

    Ok(())
}

#[sinex_test]
async fn ops_start_uses_authenticated_actor_over_payload_operator(
    ctx: TestContext,
) -> TestResult<()> {
    let auth = system_auth();
    let response: OpsStartResponse = serde_json::from_value(
        handle_ops_start(
            ctx.pool(),
            json!({
                "operation_type": "archive",
                "operator": "forged-payload-operator",
                "scope": {"key": "value"},
            }),
            &auth,
        )
        .await?,
    )?;

    assert_eq!(response.operation.operator, auth.actor_id());

    let persisted = get_operation(&ctx, &auth, &response.operation.id).await?;
    assert_eq!(persisted.operation.operator, auth.actor_id());

    Ok(())
}

#[sinex_test]
async fn ops_start_rejects_unknown_operation_type(ctx: TestContext) -> TestResult<()> {
    let auth = system_auth();
    let err = handle_ops_start(
        ctx.pool(),
        json!({
            "operation_type": "test-operation",
        }),
        &auth,
    )
    .await
    .expect_err("unknown operation type should be rejected before hitting the database");

    assert!(err.to_string().contains("Unsupported operation type"));
    Ok(())
}

#[sinex_test]
async fn ops_start_projection_rebuild_recovers_pending_replay_invalidation(
    ctx: TestContext,
) -> TestResult<()> {
    let auth = system_auth();
    let replay = ReplayStateMachine::new(ctx.pool.clone());
    let operation = replay
        .create_operation(
            ReplayScope {
                source_name: "ops-replay-invalidation-source".to_string(),
                time_window: None,
                material_filter: None,
                filters: HashMap::new(),
                ..Default::default()
            },
            "test:planner".to_string(),
        )
        .await?;

    let mut tx = ctx.pool().begin().await?;
    replay
        .record_scope_invalidations_pending_with_tx(&mut tx, operation.operation_id, 7, 2, 3, 4)
        .await?;
    tx.commit().await?;

    let response: OpsStartResponse = serde_json::from_value(
        handle_ops_start(
            ctx.pool(),
            json!({
                "operation_type": "projection-rebuild",
                "scope": {
                    "source": "replay-invalidation",
                    "replay_operation_id": operation.operation_id.to_string(),
                },
            }),
            &auth,
        )
        .await?,
    )?;

    assert_eq!(response.operation.operation_type, "projection-rebuild");
    assert_eq!(response.operation.result_status, OperationStatus::Success);
    assert_eq!(response.operation.operator, auth.actor_id());
    assert_eq!(
        response.operation.scope.as_ref().and_then(|scope| {
            scope
                .get("replay_operation_id")
                .and_then(serde_json::Value::as_str)
        }),
        Some(operation.operation_id.to_string().as_str())
    );

    let replay_meta: serde_json::Value = sqlx::query_scalar::<_, Option<serde_json::Value>>(
        r#"SELECT preview_summary FROM core.operations_log WHERE id = $1::uuid"#,
    )
    .bind(operation.operation_id)
    .fetch_one(ctx.pool())
    .await?
    .expect("replay operation should keep metadata");
    assert_eq!(
        replay_meta
            .pointer("/scope_invalidation/phase")
            .and_then(serde_json::Value::as_str),
        Some("published")
    );
    assert_eq!(
        replay_meta
            .pointer("/scope_invalidation/recovery_operation_id")
            .and_then(serde_json::Value::as_str),
        Some(response.operation.id.as_str())
    );
    assert_eq!(
        replay_meta
            .pointer("/scope_invalidation/recovery_mode")
            .and_then(serde_json::Value::as_str),
        Some("projection-rebuild")
    );

    let second_response: OpsStartResponse = serde_json::from_value(
        handle_ops_start(
            ctx.pool(),
            json!({
                "operation_type": "projection-rebuild",
                "scope": {
                    "source": "replay-invalidation",
                    "replay_operation_id": operation.operation_id.to_string(),
                },
            }),
            &auth,
        )
        .await?,
    )?;
    assert_eq!(second_response.operation.id, response.operation.id);

    let count: i64 = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)::bigint
        FROM core.operations_log
        WHERE operation_type = 'projection-rebuild'
          AND scope @> $1
        "#,
    )
    .bind(json!({
        "source": "replay-invalidation",
        "replay_operation_id": operation.operation_id.to_string(),
    }))
    .fetch_one(ctx.pool())
    .await?;
    assert_eq!(count, 1);

    Ok(())
}

#[sinex_test]
async fn ops_start_records_media_operation_as_pending_executor(ctx: TestContext) -> TestResult<()> {
    let auth = system_auth();
    let response: OpsStartResponse = serde_json::from_value(
        handle_ops_start(
            ctx.pool(),
            json!({
                "operation_type": "media.screen-ocr.capture-region",
                "scope": {
                    "source_id": "media.screen-ocr",
                    "mode_id": "source:media.screen-ocr.on-demand-region",
                    "region": [0, 0, 640, 480],
                    "reason": "operator-requested"
                },
            }),
            &auth,
        )
        .await?,
    )?;

    assert_eq!(
        response.operation.operation_type,
        "media.screen-ocr.capture-region"
    );
    assert_eq!(response.operation.result_status, OperationStatus::Running);
    assert_eq!(
        response.operation.result_message.as_deref(),
        Some("media_capture; executor pending")
    );
    let scope = response
        .operation
        .scope
        .as_ref()
        .expect("media operation scope should be recorded");
    assert_eq!(scope["surface"], "media_capture");
    assert_eq!(scope["source_id"], "media.screen-ocr");
    assert_eq!(scope["mode_id"], "source:media.screen-ocr.on-demand-region");
    assert_eq!(scope["action"], "capture_region");
    assert_eq!(scope["executor_state"], "awaiting_runtime_executor");
    assert_eq!(
        scope["mode_contract"]["binding"]["subject"],
        "source:media.screen-ocr.on-demand-region"
    );
    assert_eq!(
        scope["mode_contract"]["binding"]["material_lifecycle"],
        "ephemeral_raw"
    );
    assert_eq!(
        scope["mode_contract"]["binding"]["transport_semantics"]["transport"],
        "local_queue"
    );
    assert_eq!(
        scope["mode_contract"]["resource_budget"]["work_class"],
        "capture_live"
    );
    let preview = response
        .operation
        .preview_summary
        .as_ref()
        .expect("media operation preview should be recorded");
    assert_eq!(preview["executor_state"], "awaiting_runtime_executor");
    assert_eq!(preview["operation_type"], "media.screen-ocr.capture-region");
    assert_eq!(
        preview["mode_contract"]["binding"]["adapter"],
        "ScreenRegionCaptureAdapter"
    );

    let persisted = get_operation(&ctx, &auth, &response.operation.id).await?;
    assert_eq!(
        persisted.operation.operation_type,
        "media.screen-ocr.capture-region"
    );
    assert_eq!(persisted.operation.result_status, OperationStatus::Running);
    Ok(())
}

#[sinex_test]
async fn ops_start_admits_media_worker_output(ctx: TestContext) -> TestResult<()> {
    let auth = system_auth();
    let response: OpsStartResponse = serde_json::from_value(
        handle_ops_start(
            ctx.pool(),
            json!({
                "operation_type": "media.screen-ocr.run-ocr",
                "scope": {
                    "source_id": "media.screen-ocr",
                    "mode_id": "source:media.screen-ocr.local-model-batch",
                    "worker_output": {
                        "screenshot": {
                            "display_id": "DP-2",
                            "region": [0, 0, 800, 600],
                            "width": 800,
                            "height": 600,
                            "source_file": "screens/session-a.png",
                            "policy_posture": "operator-controlled-image-material"
                        },
                        "ocr_run": {
                            "producer_run_id": "ocr-run-api",
                            "engine_id": "tesseract",
                            "engine_version": "5.5",
                            "input_material_ids": ["raw-screen-a"],
                            "output_refs": ["artifact:media.screen.ocr/run-api"],
                            "duration_ms": 330,
                            "resource_posture": "bounded-local-worker"
                        },
                        "segments": [
                            {"text":"run-backed OCR","bbox":[4,8,160,24],"confidence":0.95}
                        ]
                    }
                },
            }),
            &auth,
        )
        .await?,
    )?;

    assert_eq!(
        response.operation.operation_type,
        "media.screen-ocr.run-ocr"
    );
    assert_eq!(response.operation.result_status, OperationStatus::Success);
    assert_eq!(
        response.operation.result_message.as_deref(),
        Some("media_capture; media worker output admitted")
    );
    let scope = response
        .operation
        .scope
        .as_ref()
        .expect("media worker-output operation scope should be recorded");
    assert_eq!(scope["executor_state"], "worker_output_admitted");
    assert!(
        scope.get("worker_output").is_none(),
        "operation scope must not persist raw inline media worker output"
    );
    assert!(
        scope.get("worker_output_path").is_none(),
        "operation scope must not persist worker output file paths after ingestion"
    );
    let material_id = scope["worker_output_material_id"]
        .as_str()
        .expect("worker output material id should be recorded");
    let event_ids = scope["worker_output_event_ids"]
        .as_array()
        .expect("worker output event ids should be recorded");
    assert_eq!(event_ids.len(), 3);
    assert_eq!(
        scope["worker_output_parser"]["parser_id"],
        "media-screen-ocr-staged"
    );

    let persisted_events: Vec<String> = sqlx::query_scalar(
        r#"
        SELECT event_type
        FROM core.events
        WHERE source_material_id = $1::uuid
        ORDER BY event_type
        "#,
    )
    .bind(uuid::Uuid::parse_str(material_id)?)
    .fetch_all(ctx.pool())
    .await?;
    assert_eq!(
        persisted_events,
        vec![
            "media.screen.ocr_run_observed".to_string(),
            "media.screen.ocr_segment_observed".to_string(),
            "media.screen.screenshot_observed".to_string(),
        ]
    );
    let preview = response
        .operation
        .preview_summary
        .as_ref()
        .expect("media worker-output operation preview should be recorded");
    assert_eq!(preview["executor_state"], "worker_output_admitted");
    assert_eq!(preview["admitted_event_count"], 3);
    Ok(())
}

#[sinex_test]
async fn ops_start_runs_media_worker_command_and_admits_stdout(ctx: TestContext) -> TestResult<()> {
    let auth = system_auth();
    let worker_output = serde_json::json!({
        "transcription_run": {
            "producer_run_id": "transcript-worker-api",
            "model_id": "whisper.cpp",
            "model_version": "1.7",
            "input_material_ids": ["audio-material-a"],
            "source_file": "audio/session-a.wav",
            "duration_ms": 190,
            "resource_posture": "bounded-local-worker",
            "policy_posture": "operator-controlled-audio-material"
        },
        "segments": [
            {"text":"command-backed transcript","start_ms":0,"end_ms":1200,"confidence":0.91}
        ]
    })
    .to_string();
    let response: OpsStartResponse = serde_json::from_value(
        handle_ops_start(
            ctx.pool(),
            json!({
                "operation_type": "media.audio-transcript.run-model",
                "scope": {
                    "source_id": "media.audio-transcript",
                    "mode_id": "source:media.audio-transcript.local-model-batch",
                    "worker_command": {
                        "program": "printf",
                        "args": [worker_output],
                        "timeout_ms": 5000,
                        "output_source_identifier": "process://test/audio-transcript-worker"
                    }
                },
            }),
            &auth,
        )
        .await?,
    )?;

    assert_eq!(
        response.operation.operation_type,
        "media.audio-transcript.run-model"
    );
    assert_eq!(response.operation.result_status, OperationStatus::Success);
    assert_eq!(
        response.operation.result_message.as_deref(),
        Some("media_capture; media worker command output admitted")
    );
    assert!(
        response.operation.duration_ms.is_some(),
        "worker command operations should record elapsed execution time"
    );

    let scope = response
        .operation
        .scope
        .as_ref()
        .expect("media worker-command operation scope should be recorded");
    assert_eq!(scope["executor_state"], "worker_command_admitted");
    assert_eq!(
        scope["worker_output_parser"]["parser_id"],
        "media-audio-transcript-staged"
    );
    assert!(
        scope.get("worker_output").is_none(),
        "operation scope must not persist raw media worker stdout"
    );
    assert!(
        scope.get("worker_output_path").is_none(),
        "operation scope must not persist worker output paths"
    );
    let event_ids = scope["worker_output_event_ids"]
        .as_array()
        .expect("worker command event ids should be recorded");
    assert_eq!(event_ids.len(), 2);
    assert_eq!(scope["worker_command"]["program"], "printf");
    assert_eq!(
        scope["worker_command"]["stdout_max_bytes"],
        10 * 1024 * 1024
    );

    let material_id = scope["worker_output_material_id"]
        .as_str()
        .expect("worker command material id should be recorded");
    let persisted_events: Vec<String> = sqlx::query_scalar(
        r#"
        SELECT event_type
        FROM core.events
        WHERE source_material_id = $1::uuid
        ORDER BY event_type
        "#,
    )
    .bind(uuid::Uuid::parse_str(material_id)?)
    .fetch_all(ctx.pool())
    .await?;
    let expected_events = vec![
        "media.audio.transcript_segment_observed".to_string(),
        "media.audio.transcription_run_observed".to_string(),
    ];
    assert_eq!(persisted_events, expected_events,);

    let preview = response
        .operation
        .preview_summary
        .as_ref()
        .expect("media worker-command preview should be recorded");
    assert_eq!(preview["executor_state"], "worker_command_admitted");
    assert_eq!(preview["admitted_event_count"], 2);
    assert_eq!(preview["worker_command"]["program"], "printf");
    assert_eq!(preview["worker_command"]["stderr_bytes"], 0);
    Ok(())
}

#[sinex_test]
async fn ops_start_records_media_worker_command_failure_without_raw_output(
    ctx: TestContext,
) -> TestResult<()> {
    let auth = system_auth();
    let response: OpsStartResponse = serde_json::from_value(
        handle_ops_start(
            ctx.pool(),
            json!({
                "operation_type": "media.screen-ocr.run-ocr",
                "scope": {
                    "source_id": "media.screen-ocr",
                    "mode_id": "source:media.screen-ocr.local-model-batch",
                    "worker_command": {
                        "program": "sh",
                        "args": ["-c", "exit 7"],
                        "timeout_ms": 5000
                    }
                },
            }),
            &auth,
        )
        .await?,
    )?;

    assert_eq!(response.operation.result_status, OperationStatus::Failed);
    assert!(
        response
            .operation
            .result_message
            .as_deref()
            .expect("failed media worker operation should record a result message")
            .contains("media worker command exited with status")
    );
    assert!(
        response.operation.duration_ms.is_some(),
        "failed worker command operations should record elapsed execution time"
    );
    let scope = response
        .operation
        .scope
        .as_ref()
        .expect("failed media worker-command operation scope should be recorded");
    assert_eq!(scope["executor_state"], "worker_command_failed");
    assert!(
        scope.get("worker_output_material_id").is_none(),
        "failed worker command must not create admitted material"
    );
    assert!(
        scope.get("worker_output_event_ids").is_none(),
        "failed worker command must not create admitted events"
    );
    let preview = response
        .operation
        .preview_summary
        .as_ref()
        .expect("failed media worker-command preview should be recorded");
    assert_eq!(preview["executor_state"], "worker_command_failed");
    assert_eq!(preview["worker_command"]["exit_code"], 7);
    assert!(
        preview["worker_command"].get("stdout").is_none(),
        "failed previews must not persist raw stdout"
    );
    assert!(
        preview["worker_command"].get("stderr").is_none(),
        "failed previews must not persist raw stderr"
    );
    Ok(())
}

#[sinex_test]
async fn ops_start_records_media_rebuild_invalidation_triggers(ctx: TestContext) -> TestResult<()> {
    let auth = system_auth();
    let response: OpsStartResponse = serde_json::from_value(
        handle_ops_start(
            ctx.pool(),
            json!({
                "operation_type": "media.screen-ocr.rebuild-artifact",
                "scope": {
                    "source_id": "media.screen-ocr",
                    "mode_id": "source:media.screen-ocr.local-model-batch",
                    "artifact_ref": "artifact:media.screen-ocr:example"
                },
            }),
            &auth,
        )
        .await?,
    )?;

    let scope = response
        .operation
        .scope
        .as_ref()
        .expect("media rebuild operation scope should be recorded");
    let operation_metadata = scope
        .get("operation_metadata")
        .expect("media rebuild should record operation metadata");
    let triggers = operation_metadata["invalidation_triggers"]
        .as_array()
        .expect("invalidation triggers should be an array");
    for expected in [
        "redaction",
        "source_material_change",
        "replay",
        "archive",
        "parser_semantics_change",
        "disclosure_policy_change",
    ] {
        assert!(
            triggers.iter().any(|trigger| trigger == expected),
            "media rebuild trigger {expected} should be present"
        );
    }
    Ok(())
}

#[sinex_test]
async fn ops_start_rejects_media_operation_wrong_mode(ctx: TestContext) -> TestResult<()> {
    let auth = system_auth();
    let error = handle_ops_start(
        ctx.pool(),
        json!({
            "operation_type": "media.audio-transcript.run-model",
            "scope": {
                "source_id": "media.audio-transcript",
                "mode_id": "source:media.audio-transcript.live-session"
            },
        }),
        &auth,
    )
    .await
    .expect_err("wrong package mode should be rejected");

    assert!(
        error
            .to_string()
            .contains("source:media.audio-transcript.local-model-batch")
    );
    Ok(())
}

#[sinex_test]
async fn ops_start_records_email_sync_for_provider_mode(ctx: TestContext) -> TestResult<()> {
    let auth = system_auth();
    let response: OpsStartResponse = serde_json::from_value(
        handle_ops_start(
            ctx.pool(),
            json!({
                "operation_type": "email.mailbox.sync",
                "scope": {
                    "source_id": "email.mailbox",
                    "mode_id": "source:email.mailbox.gmail-api-scheduled-sync",
                    "account_ref": "operator-mailbox:primary",
                    "gmail_history_id": "12345",
                    "reason": "operator-requested"
                },
            }),
            &auth,
        )
        .await?,
    )?;

    assert_eq!(response.operation.operation_type, "email.mailbox.sync");
    assert_eq!(response.operation.result_status, OperationStatus::Running);
    assert_eq!(
        response.operation.result_message.as_deref(),
        Some("email_capture; executor pending")
    );
    let scope = response
        .operation
        .scope
        .as_ref()
        .expect("email operation scope should be recorded");
    assert_eq!(scope["surface"], "email_capture");
    assert_eq!(scope["source_id"], "email.mailbox");
    assert_eq!(
        scope["mode_id"],
        "source:email.mailbox.gmail-api-scheduled-sync"
    );
    assert_eq!(scope["action"], "sync");
    assert_eq!(
        scope["mode_contract"]["binding"]["subject"],
        "source:email.mailbox.gmail-api-scheduled-sync"
    );
    assert_eq!(
        scope["mode_contract"]["binding"]["transport_semantics"]["transport"],
        "external_api"
    );
    assert_eq!(
        scope["mode_contract"]["resource_budget"]["work_class"],
        "admission_hot"
    );
    assert_eq!(scope["account_ref"], "operator-mailbox:primary");
    assert_eq!(scope["account_binding_ref"], "operator-mailbox:primary");
    assert_eq!(
        scope["provider_operation_scope"]["account_binding_ref"],
        "operator-mailbox:primary"
    );
    let provider_runtime = &scope["provider_runtime"];
    assert_eq!(provider_runtime["provider"], "gmail");
    assert_eq!(provider_runtime["provider_runtime"], "scheduled-sync");
    assert_eq!(
        provider_runtime["account_binding_ref"],
        "operator-mailbox:primary"
    );
    assert_eq!(
        provider_runtime["authorization_state_ref"],
        "email.mailbox.provider_authorization.gmail.oauth"
    );
    assert_eq!(
        provider_runtime["sync_cursor_ref"],
        "email.sync_cursor.observed:gmail-history-id"
    );
    assert_eq!(provider_runtime["sync_cursor_kind"], "gmail-history-id");
    assert_eq!(
        provider_runtime["runtime_state_ref"],
        "email.capture_runtime.observed:gmail.scheduled_sync"
    );
    assert_eq!(
        provider_runtime["coverage_ref"],
        "coverage:email.mailbox.gmail.provider_runtime"
    );
    assert_eq!(
        provider_runtime["debt_ref"],
        "debt:email.mailbox.gmail.provider_runtime"
    );
    assert!(
        provider_runtime["caveats"]
            .as_array()
            .expect("provider runtime caveats should be an array")
            .iter()
            .any(|caveat| caveat
                == "provider executor requires explicit gmail_token_file at operation start")
    );
    assert_eq!(
        provider_runtime["runtime_observation_contract"]["account_binding_ref"],
        "operator-mailbox:primary"
    );
    assert_eq!(
        provider_runtime["runtime_observation_contract"]["provider"],
        "gmail"
    );
    assert_eq!(
        provider_runtime["runtime_observation_contract"]["provider_runtime"],
        "scheduled-sync"
    );
    assert_eq!(
        provider_runtime["runtime_observation_contract"]["sync_state"],
        "idle"
    );

    let provider_cursor = &scope["provider_cursor"];
    assert_eq!(provider_cursor["provider"], "gmail");
    assert_eq!(
        provider_cursor["account_binding_ref"],
        "operator-mailbox:primary"
    );
    assert_eq!(provider_cursor["cursor_kind"], "gmail-history-id");
    assert_eq!(provider_cursor["cursor_value"], "12345");
    assert_eq!(
        provider_cursor["cursor_observation_contract"]["gmail_history_id"],
        "12345"
    );
    assert_eq!(
        provider_cursor["cursor_observation_contract"]["continuity_state"],
        "unknown"
    );

    let preview = response
        .operation
        .preview_summary
        .as_ref()
        .expect("email operation preview should be recorded");
    assert_eq!(preview["surface"], "email_capture");
    assert_eq!(preview["operation_type"], "email.mailbox.sync");
    assert_eq!(
        preview["mode_id"],
        "source:email.mailbox.gmail-api-scheduled-sync"
    );
    assert_eq!(preview["provider_runtime"], scope["provider_runtime"]);
    assert_eq!(preview["provider_cursor"], scope["provider_cursor"]);
    Ok(())
}

#[sinex_test]
async fn ops_start_executes_gmail_scheduled_sync_with_token_file(
    ctx: TestContext,
) -> TestResult<()> {
    let server = GmailFixtureServer::start().await?;
    let dir = tempfile::tempdir()?;
    let token_file = dir.path().join("gmail-token");
    tokio::fs::write(&token_file, "test-token\n").await?;

    let auth = system_auth();
    let response: OpsStartResponse = serde_json::from_value(
        handle_ops_start(
            ctx.pool(),
            json!({
                "operation_type": "email.mailbox.sync",
                "scope": {
                    "source_id": "email.mailbox",
                    "mode_id": "source:email.mailbox.gmail-api-scheduled-sync",
                    "account_binding_ref": "operator-mailbox:primary",
                    "gmail_token_file": token_file.to_string_lossy(),
                    "gmail_api_base_url": server.base_url(),
                    "label_ids": ["INBOX"],
                    "page_size": 10
                },
            }),
            &auth,
        )
        .await?,
    )?;

    assert_eq!(response.operation.operation_type, "email.mailbox.sync");
    assert_eq!(response.operation.result_status, OperationStatus::Success);
    assert_eq!(
        response.operation.result_message.as_deref(),
        Some("email_capture; Gmail API sync admitted")
    );
    let scope = response
        .operation
        .scope
        .as_ref()
        .expect("email provider operation scope should be recorded");
    assert_eq!(scope["executor_state"], "gmail_api_sync_admitted");
    let token_file_ref = token_file.to_string_lossy();
    assert_eq!(
        scope["gmail_sync_input"]["token_file_ref"].as_str(),
        Some(token_file_ref.as_ref())
    );
    assert_eq!(scope["provider_record_count"], 1);
    assert_eq!(
        scope["provider_runtime"]["runtime_observation_contract"]["auth_state"],
        "authorized"
    );
    assert_eq!(
        scope["provider_runtime"]["runtime_observation_contract"]["network_state"],
        "online"
    );
    assert_eq!(
        scope["provider_cursor"]["cursor_observation_contract"]["gmail_history_id"], "history-1",
        "Gmail message detail history id should be admitted as provider cursor"
    );
    assert_eq!(
        scope["provider_event_ids"]
            .as_array()
            .expect("provider event ids should be recorded")
            .len(),
        1
    );
    let persisted_scope = serde_json::to_string(scope)?;
    assert!(
        !persisted_scope.contains("test-token"),
        "Gmail bearer token contents must not be persisted in operation scope"
    );

    let preview = response
        .operation
        .preview_summary
        .as_ref()
        .expect("email provider preview should be recorded");
    assert_eq!(preview["executor_state"], "gmail_api_sync_admitted");
    assert_eq!(preview["provider_record_count"], 1);
    assert_eq!(preview["admitted_event_count"], 1);
    assert_eq!(
        preview["mode_contract"]["binding"]["adapter"],
        "GmailApiCursorAdapter"
    );
    Ok(())
}

#[sinex_test]
async fn ops_start_records_gmail_provider_failure_for_missing_token_file(
    ctx: TestContext,
) -> TestResult<()> {
    let dir = tempfile::tempdir()?;
    let missing_token_file = dir.path().join("missing-gmail-token");

    let auth = system_auth();
    let response: OpsStartResponse = serde_json::from_value(
        handle_ops_start(
            ctx.pool(),
            json!({
                "operation_type": "email.mailbox.sync",
                "scope": {
                    "source_id": "email.mailbox",
                    "mode_id": "source:email.mailbox.gmail-api-scheduled-sync",
                    "account_binding_ref": "operator-mailbox:gmail-missing-token",
                    "gmail_token_file": missing_token_file.to_string_lossy(),
                    "gmail_history_id": "history-before-failure"
                },
            }),
            &auth,
        )
        .await?,
    )?;

    assert_eq!(response.operation.operation_type, "email.mailbox.sync");
    assert_eq!(response.operation.result_status, OperationStatus::Failed);
    let scope = response
        .operation
        .scope
        .as_ref()
        .expect("failed Gmail provider operation scope should be recorded");
    assert_eq!(scope["executor_state"], "gmail_api_sync_failed");
    assert_eq!(
        scope["gmail_sync_input"]["token_file_ref"].as_str(),
        Some(missing_token_file.to_string_lossy().as_ref())
    );
    let runtime_contract = provider_runtime_contract(scope)?;
    assert_eq!(
        runtime_contract.auth_state,
        EmailAuthorizationState::Missing
    );
    assert_eq!(runtime_contract.network_state, EmailNetworkState::Unknown);
    assert_eq!(runtime_contract.sync_state, EmailSyncState::Failed);
    assert_eq!(
        scope["provider_failure"]["debt_ref"],
        "debt:email.mailbox.gmail.provider_runtime"
    );
    assert!(
        scope["provider_failure"]["reason"]
            .as_str()
            .expect("failure reason")
            .contains("token file is unavailable")
    );

    let preview = response
        .operation
        .preview_summary
        .as_ref()
        .expect("failed Gmail provider preview should be recorded");
    assert_eq!(preview["executor_state"], "gmail_api_sync_failed");
    assert_eq!(preview["provider_failure"], scope["provider_failure"]);
    Ok(())
}

#[sinex_test]
async fn ops_start_executes_imap_scheduled_sync_with_password_file(
    ctx: TestContext,
) -> TestResult<()> {
    let server = ImapFixtureServer::start().await?;
    let dir = tempfile::tempdir()?;
    let password_file = dir.path().join("imap-password");
    tokio::fs::write(&password_file, "fixture-password\n").await?;

    let auth = system_auth();
    let operation = timeout(
        Duration::from_secs(10),
        handle_ops_start(
            ctx.pool(),
            json!({
                "operation_type": "email.mailbox.sync",
                "scope": {
                    "source_id": "email.mailbox",
                    "mode_id": "source:email.mailbox.imap-scheduled-sync",
                    "account_binding_ref": "operator-mailbox:imap-primary",
                    "imap_host": "127.0.0.1",
                    "imap_port": server.addr.port(),
                    "imap_username": "operator",
                    "imap_password_file": password_file.to_string_lossy(),
                    "imap_tls_mode": "none",
                    "mailbox": "INBOX",
                    "uidvalidity": "700",
                    "uid": "40",
                    "batch_size": 10,
                    "fetch_bodies": true,
                    "body_material_policy_ref": "operator.email-mailbox.body-private"
                },
            }),
            &auth,
        ),
    )
    .await;
    let response_value = match operation {
        Ok(value) => value?,
        Err(_) => {
            return Err(color_eyre::eyre::eyre!(
                "IMAP sync operation timed out; fixture commands: {:?}",
                server.commands().await
            ));
        }
    };
    let response: OpsStartResponse = serde_json::from_value(response_value)?;

    assert_eq!(response.operation.operation_type, "email.mailbox.sync");
    assert_eq!(response.operation.result_status, OperationStatus::Success);
    assert_eq!(
        response.operation.result_message.as_deref(),
        Some("email_capture; IMAP sync admitted")
    );
    let scope = response
        .operation
        .scope
        .as_ref()
        .expect("email provider operation scope should be recorded");
    assert_eq!(scope["executor_state"], "imap_sync_admitted");
    assert_eq!(
        scope["mode_contract"]["binding"]["subject"],
        "source:email.mailbox.imap-scheduled-sync"
    );
    assert_eq!(
        scope["mode_contract"]["binding"]["adapter"],
        "ImapSyncAdapter"
    );
    assert_eq!(scope["imap_sync_input"]["host"], "127.0.0.1");
    assert_eq!(scope["imap_sync_input"]["tls_mode"], "none");
    assert_eq!(scope["imap_sync_input"]["fetch_bodies"], true);
    assert_eq!(
        scope["imap_sync_input"]["body_material_policy_ref"],
        "operator.email-mailbox.body-private"
    );
    assert_eq!(
        scope["imap_sync_input"]["password_file_ref"].as_str(),
        Some(password_file.to_string_lossy().as_ref())
    );
    assert_eq!(scope["provider_record_count"], 1);
    assert_eq!(
        scope["provider_runtime"]["runtime_observation_contract"]["auth_state"],
        "authorized"
    );
    assert_eq!(
        scope["provider_runtime"]["runtime_observation_contract"]["network_state"],
        "online"
    );
    assert_eq!(scope["provider_cursor"]["provider"], "imap");
    assert_eq!(
        scope["provider_cursor"]["cursor_observation_contract"]["uidvalidity"],
        "700"
    );
    assert_eq!(
        scope["provider_cursor"]["cursor_observation_contract"]["uid"], "41",
        "IMAP cursor should advance past the fetched UID"
    );
    assert_eq!(
        scope["provider_event_ids"]
            .as_array()
            .expect("provider event ids should be recorded")
            .len(),
        1
    );
    let persisted_scope = serde_json::to_string(scope)?;
    assert!(
        !persisted_scope.contains("fixture-password"),
        "IMAP password contents must not be persisted in operation scope"
    );

    let preview = response
        .operation
        .preview_summary
        .as_ref()
        .expect("email provider preview should be recorded");
    assert_eq!(preview["executor_state"], "imap_sync_admitted");
    assert_eq!(preview["provider_record_count"], 1);
    assert_eq!(preview["admitted_event_count"], 1);
    Ok(())
}

#[sinex_test]
async fn ops_start_records_imap_provider_network_failure_without_secret(
    ctx: TestContext,
) -> TestResult<()> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    drop(listener);

    let auth = system_auth();
    let response: OpsStartResponse = serde_json::from_value(
        handle_ops_start(
            ctx.pool(),
            json!({
                "operation_type": "email.mailbox.sync",
                "scope": {
                    "source_id": "email.mailbox",
                    "mode_id": "source:email.mailbox.imap-scheduled-sync",
                    "account_binding_ref": "operator-mailbox:imap-network-failure",
                    "imap_host": "127.0.0.1",
                    "imap_port": port,
                    "imap_username": "operator",
                    "imap_password": "inline-network-failure-password",
                    "imap_tls_mode": "none",
                    "mailbox": "INBOX",
                    "uidvalidity": "700",
                    "uid": "40",
                    "batch_size": 10
                },
            }),
            &auth,
        )
        .await?,
    )?;

    assert_eq!(response.operation.operation_type, "email.mailbox.sync");
    assert_eq!(response.operation.result_status, OperationStatus::Failed);
    let scope = response
        .operation
        .scope
        .as_ref()
        .expect("failed IMAP provider operation scope should be recorded");
    assert_eq!(scope["executor_state"], "imap_sync_failed");
    assert_eq!(scope["imap_sync_input"]["password"], "<redacted>");
    assert!(scope.get("imap_password").is_none());
    assert!(
        !serde_json::to_string(scope)?.contains("inline-network-failure-password"),
        "failed IMAP operation must not persist inline password contents"
    );
    let runtime_contract = provider_runtime_contract(scope)?;
    assert_eq!(
        runtime_contract.auth_state,
        EmailAuthorizationState::Authorized
    );
    assert_eq!(runtime_contract.network_state, EmailNetworkState::Error);
    assert_eq!(runtime_contract.sync_state, EmailSyncState::Failed);
    assert_eq!(
        scope["provider_failure"]["debt_ref"],
        "debt:email.mailbox.imap.provider_runtime"
    );
    assert!(
        scope["provider_failure"]["reason"]
            .as_str()
            .expect("failure reason")
            .contains("IMAP adapter failed")
    );

    let preview = response
        .operation
        .preview_summary
        .as_ref()
        .expect("failed IMAP provider preview should be recorded");
    assert_eq!(preview["executor_state"], "imap_sync_failed");
    assert_eq!(preview["provider_failure"], scope["provider_failure"]);
    Ok(())
}

#[sinex_test]
async fn ops_start_executes_imap_sync_without_persisting_inline_password(
    ctx: TestContext,
) -> TestResult<()> {
    let server = ImapFixtureServer::start().await?;

    let auth = system_auth();
    let operation = timeout(
        Duration::from_secs(10),
        handle_ops_start(
            ctx.pool(),
            json!({
                "operation_type": "email.mailbox.sync",
                "scope": {
                    "source_id": "email.mailbox",
                    "mode_id": "source:email.mailbox.imap-scheduled-sync",
                    "account_binding_ref": "operator-mailbox:imap-inline",
                    "imap_host": "127.0.0.1",
                    "imap_port": server.addr.port(),
                    "imap_username": "operator",
                    "imap_password": "inline-fixture-password",
                    "imap_tls_mode": "none",
                    "mailbox": "INBOX",
                    "uidvalidity": "700",
                    "uid": "40",
                    "batch_size": 10
                },
            }),
            &auth,
        ),
    )
    .await;
    let response_value = match operation {
        Ok(value) => value?,
        Err(_) => {
            return Err(color_eyre::eyre::eyre!(
                "IMAP sync operation timed out; fixture commands: {:?}",
                server.commands().await
            ));
        }
    };
    let response: OpsStartResponse = serde_json::from_value(response_value)?;

    assert_eq!(response.operation.result_status, OperationStatus::Success);
    let scope = response
        .operation
        .scope
        .as_ref()
        .expect("email provider operation scope should be recorded");
    assert_eq!(scope["imap_sync_input"]["password"], "<redacted>");
    assert!(
        scope.get("imap_password").is_none(),
        "raw IMAP password key must be removed from persisted operation scope"
    );
    assert!(
        scope.get("password").is_none(),
        "generic password alias must be removed from persisted operation scope"
    );
    let persisted_scope = serde_json::to_string(scope)?;
    assert!(
        !persisted_scope.contains("inline-fixture-password"),
        "inline IMAP password contents must not be persisted in operation scope"
    );
    assert_eq!(
        scope["provider_cursor"]["cursor_observation_contract"]["uid"], "41",
        "IMAP cursor should advance past the fetched UID"
    );
    Ok(())
}

#[sinex_test]
async fn ops_start_rejects_imap_body_fetch_without_material_policy(
    ctx: TestContext,
) -> TestResult<()> {
    let auth = system_auth();
    let error = handle_ops_start(
        ctx.pool(),
        json!({
            "operation_type": "email.mailbox.sync",
            "scope": {
                "source_id": "email.mailbox",
                "mode_id": "source:email.mailbox.imap-scheduled-sync",
                "account_binding_ref": "operator-mailbox:imap-primary",
                "imap_host": "127.0.0.1",
                "imap_port": 1143,
                "imap_username": "operator",
                "imap_password": "inline-fixture-password",
                "imap_tls_mode": "none",
                "mailbox": "INBOX",
                "fetch_bodies": true
            },
        }),
        &auth,
    )
    .await
    .expect_err("IMAP body fetch should require an explicit material policy ref");

    assert!(
        error
            .to_string()
            .contains("fetch_bodies requires body_material_policy_ref")
    );
    Ok(())
}

#[sinex_test]
async fn ops_start_rejects_imap_attachment_fetch_without_body_fetch(
    ctx: TestContext,
) -> TestResult<()> {
    let auth = system_auth();
    let error = handle_ops_start(
        ctx.pool(),
        json!({
            "operation_type": "email.mailbox.sync",
            "scope": {
                "source_id": "email.mailbox",
                "mode_id": "source:email.mailbox.imap-scheduled-sync",
                "account_binding_ref": "operator-mailbox:imap-primary",
                "imap_host": "127.0.0.1",
                "imap_port": 1143,
                "imap_username": "operator",
                "imap_password": "inline-fixture-password",
                "imap_tls_mode": "none",
                "mailbox": "INBOX",
                "fetch_attachments": true,
                "attachment_material_policy_ref": "operator.email-mailbox.attachment-private"
            },
        }),
        &auth,
    )
    .await
    .expect_err("IMAP attachment fetch should require full body fetch");

    assert!(
        error
            .to_string()
            .contains("fetch_attachments requires fetch_bodies")
    );
    Ok(())
}

#[sinex_test]
async fn ops_start_rejects_imap_attachment_fetch_without_material_policy(
    ctx: TestContext,
) -> TestResult<()> {
    let auth = system_auth();
    let error = handle_ops_start(
        ctx.pool(),
        json!({
            "operation_type": "email.mailbox.sync",
            "scope": {
                "source_id": "email.mailbox",
                "mode_id": "source:email.mailbox.imap-scheduled-sync",
                "account_binding_ref": "operator-mailbox:imap-primary",
                "imap_host": "127.0.0.1",
                "imap_port": 1143,
                "imap_username": "operator",
                "imap_password": "inline-fixture-password",
                "imap_tls_mode": "none",
                "mailbox": "INBOX",
                "fetch_bodies": true,
                "body_material_policy_ref": "operator.email-mailbox.body-private",
                "fetch_attachments": true
            },
        }),
        &auth,
    )
    .await
    .expect_err("IMAP attachment fetch should require an explicit material policy ref");

    assert!(
        error
            .to_string()
            .contains("fetch_attachments requires attachment_material_policy_ref")
    );
    Ok(())
}

#[sinex_test]
async fn ops_start_strips_provider_runtime_for_staged_email_mode(
    ctx: TestContext,
) -> TestResult<()> {
    let auth = system_auth();
    let response: OpsStartResponse = serde_json::from_value(
        handle_ops_start(
            ctx.pool(),
            json!({
                "operation_type": "email.mailbox.sync",
                "scope": {
                    "source_id": "email.mailbox",
                    "mode_id": "source:email.mailbox.maildir-staged",
                    "provider_runtime": {
                        "provider": "gmail",
                        "runtime_state_ref": "email.capture_runtime.observed:gmail.scheduled_sync"
                    },
                    "provider_cursor": {
                        "provider": "gmail",
                        "sync_cursor_kind": "gmail-history-id"
                    }
                },
            }),
            &auth,
        )
        .await?,
    )?;

    let scope = response
        .operation
        .scope
        .as_ref()
        .expect("email staged operation scope should be recorded");
    assert_eq!(scope["mode_id"], "source:email.mailbox.maildir-staged");
    assert!(
        scope.get("provider_runtime").is_none(),
        "staged email operations must not retain provider runtime metadata"
    );
    assert!(
        scope.get("provider_cursor").is_none(),
        "staged email operations must not retain provider cursor metadata"
    );
    let preview = response
        .operation
        .preview_summary
        .as_ref()
        .expect("email staged operation preview should be recorded");
    assert!(
        preview.get("provider_runtime").is_none(),
        "staged email previews must not advertise provider runtime metadata"
    );
    assert!(
        preview.get("provider_cursor").is_none(),
        "staged email previews must not advertise provider cursor metadata"
    );
    Ok(())
}

struct GmailFixtureServer {
    addr: std::net::SocketAddr,
}

struct ImapFixtureServer {
    addr: std::net::SocketAddr,
    commands: Arc<Mutex<Vec<String>>>,
}

impl ImapFixtureServer {
    async fn start() -> TestResult<Self> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let commands = Arc::new(Mutex::new(Vec::new()));
        let fixture_commands = commands.clone();
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                let commands = fixture_commands.clone();
                tokio::spawn(async move {
                    let _ = stream.write_all(b"* OK IMAP fixture ready\r\n").await;
                    while let Ok(command) = read_imap_command(&mut stream).await {
                        if command.is_empty() {
                            return;
                        }
                        let mut command_log = commands.lock().await;
                        if command_log.len() < 16 {
                            command_log.push(command.trim_end().to_owned());
                        }
                        drop(command_log);
                        let tag = command.split_whitespace().next().unwrap_or("A0001");
                        let response = if command.contains(" LOGIN ") {
                            format!("{tag} OK LOGIN completed\r\n")
                        } else if command.contains(" CAPABILITY") {
                            format!(
                                "* CAPABILITY IMAP4rev1 CONDSTORE\r\n{tag} OK CAPABILITY completed\r\n"
                            )
                        } else if command.contains(" SELECT ") {
                            format!(
                                "* FLAGS (\\Seen)\r\n\
* 2 EXISTS\r\n\
* OK [UIDVALIDITY 700] UIDs valid\r\n\
* OK [UIDNEXT 41] Predicted next UID\r\n\
* OK [HIGHESTMODSEQ 1200]\r\n\
{tag} OK [READ-WRITE] SELECT completed\r\n"
                            )
                        } else if command.contains(" UID FETCH ") {
                            let header =
                                "Message-ID: <imap-40@example.com>\r\nSubject: fixture\r\n\r\n";
                            format!(
                                "* 1 FETCH (UID 40 FLAGS (\\Seen) RFC822.SIZE {} BODY[HEADER] {{{}}}\r\n{})\r\n{tag} OK UID FETCH completed\r\n",
                                header.len(),
                                header.len(),
                                header
                            )
                        } else if command.contains(" LOGOUT") {
                            format!("* BYE fixture logout\r\n{tag} OK LOGOUT completed\r\n")
                        } else {
                            format!("{tag} BAD unsupported fixture command\r\n")
                        };
                        let _ = stream.write_all(response.as_bytes()).await;
                    }
                });
            }
        });
        Ok(Self { addr, commands })
    }

    async fn commands(&self) -> Vec<String> {
        self.commands.lock().await.clone()
    }
}

async fn read_imap_command(stream: &mut tokio::net::TcpStream) -> TestResult<String> {
    let mut buf = Vec::new();
    loop {
        let mut byte = [0_u8; 1];
        let read = stream.read(&mut byte).await?;
        if read == 0 {
            break;
        }
        buf.push(byte[0]);
        if buf.ends_with(b"\r\n") {
            break;
        }
    }
    Ok(String::from_utf8_lossy(&buf).to_string())
}

impl GmailFixtureServer {
    async fn start() -> TestResult<Self> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let mut request = vec![0_u8; 8192];
                    let Ok(bytes_read) = stream.read(&mut request).await else {
                        return;
                    };
                    let request = String::from_utf8_lossy(&request[..bytes_read]);
                    let first_line = request.lines().next().unwrap_or_default();
                    let has_authorization = request
                        .lines()
                        .any(|line| line.eq_ignore_ascii_case("authorization: Bearer test-token"));
                    let body = if !has_authorization {
                        json!({"error": "missing bearer token", "request": first_line})
                    } else if first_line.contains("/gmail/v1/users/me/messages/m-1") {
                        json!({
                            "id": "m-1",
                            "threadId": "t-1",
                            "labelIds": ["INBOX"],
                            "historyId": "history-1",
                            "sizeEstimate": 128,
                            "payload": {
                                "headers": [
                                    {"name": "Message-ID", "value": "<m-1@example.com>"},
                                    {"name": "Subject", "value": "fixture"}
                                ]
                            }
                        })
                    } else if first_line.contains("/gmail/v1/users/me/messages") {
                        json!({
                            "messages": [{"id": "m-1", "threadId": "t-1"}],
                            "resultSizeEstimate": 1
                        })
                    } else {
                        json!({"error": "unexpected fixture path", "request": first_line})
                    };
                    let status = if body.get("error").is_some() {
                        "HTTP/1.1 404 Not Found"
                    } else {
                        "HTTP/1.1 200 OK"
                    };
                    let body = body.to_string();
                    let response = format!(
                        "{status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = stream.write_all(response.as_bytes()).await;
                });
            }
        });
        Ok(Self { addr })
    }

    fn base_url(&self) -> String {
        format!("http://{}/gmail/v1", self.addr)
    }
}

fn write_email_takeout_zip(path: &camino::Utf8Path) -> TestResult<()> {
    let file = std::fs::File::create(path)?;
    let mut archive = zip::ZipWriter::new(file);
    let options =
        zip::write::SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);
    archive.start_file("Takeout/Mail/Inbox.mbox", options)?;
    archive.write_all(
        b"From sender@example.com Sat Jan 01 00:00:00 2022\n\
Message-ID: <takeout-one@example.com>\n\
Date: Sat, 01 Jan 2022 00:00:00 +0000\n\
From: Sender <sender@example.com>\n\
To: Receiver <receiver@example.com>\n\
Subject: First\n\
\n\
first body\n\
From sender@example.com Sun Jan 02 00:00:00 2022\n\
Message-ID: <takeout-two@example.com>\n\
Date: Sun, 02 Jan 2022 00:00:00 +0000\n\
From: Sender <sender@example.com>\n\
To: Receiver <receiver@example.com>\n\
Subject: Second\n\
\n\
second body\n",
    )?;
    archive.finish()?;
    Ok(())
}

#[sinex_test]
async fn ops_start_imports_takeout_zip_for_staged_email_sync(ctx: TestContext) -> TestResult<()> {
    let dir = tempfile::tempdir()?;
    let archive_path = camino::Utf8PathBuf::from_path_buf(dir.path().join("takeout.zip"))
        .expect("test temp path should be utf8");
    write_email_takeout_zip(&archive_path)?;

    let auth = system_auth();
    let response: OpsStartResponse = serde_json::from_value(
        handle_ops_start(
            ctx.pool(),
            json!({
                "operation_type": "email.mailbox.sync",
                "scope": {
                    "source_id": "email.mailbox",
                    "mode_id": "source:email.mailbox.mbox-staged",
                    "archive_path": archive_path.as_str(),
                    "reason": "operator-requested"
                },
            }),
            &auth,
        )
        .await?,
    )?;

    assert_eq!(response.operation.operation_type, "email.mailbox.sync");
    assert_eq!(response.operation.result_status, OperationStatus::Success);
    assert_eq!(
        response.operation.result_message.as_deref(),
        Some("email_capture; staged email sync admitted")
    );
    let scope = response
        .operation
        .scope
        .as_ref()
        .expect("email operation scope should be recorded");
    assert_eq!(scope["executor_state"], "staged_email_sync_admitted");
    assert_eq!(
        scope["staged_sync_parser"]["parser_id"],
        "email-mailbox-rfc822"
    );
    assert_eq!(scope["staged_sync_record_count"], 2);
    assert_eq!(
        scope["staged_sync_input"]["archive_paths"],
        json!([archive_path.as_str()])
    );
    assert_eq!(
        scope["staged_sync_material_ids"]
            .as_array()
            .expect("material ids should be recorded")
            .len(),
        1
    );
    assert_eq!(
        scope["staged_sync_event_ids"]
            .as_array()
            .expect("event ids should be recorded")
            .len(),
        4,
        "two messages should admit message and thread events"
    );

    let preview = response
        .operation
        .preview_summary
        .as_ref()
        .expect("email operation preview should be recorded");
    assert_eq!(preview["executor_state"], "staged_email_sync_admitted");
    assert_eq!(preview["staged_sync_material_count"], 1);
    assert_eq!(preview["staged_sync_record_count"], 2);
    assert_eq!(preview["admitted_event_count"], 4);
    Ok(())
}

#[sinex_test]
async fn ops_start_rejects_maildir_staged_sync_with_archive_path(
    ctx: TestContext,
) -> TestResult<()> {
    let auth = system_auth();
    let error = handle_ops_start(
        ctx.pool(),
        json!({
            "operation_type": "email.mailbox.sync",
            "scope": {
                "source_id": "email.mailbox",
                "mode_id": "source:email.mailbox.maildir-staged",
                "archive_path": "/tmp/takeout.zip"
            },
        }),
        &auth,
    )
    .await
    .expect_err("maildir staged sync must not accept archives");

    assert!(
        error.to_string().contains("use mbox-staged"),
        "error should point operator at the staged MBOX/Takeout mode: {error}"
    );
    Ok(())
}

#[sinex_test]
async fn ops_start_rejects_email_provider_operation_without_account_binding(
    ctx: TestContext,
) -> TestResult<()> {
    let auth = system_auth();
    let error = handle_ops_start(
        ctx.pool(),
        json!({
            "operation_type": "email.mailbox.sync",
            "scope": {
                "source_id": "email.mailbox",
                "mode_id": "source:email.mailbox.gmail-api-scheduled-sync"
            },
        }),
        &auth,
    )
    .await
    .expect_err("provider sync should require an explicit account binding");

    assert!(error.to_string().contains("requires account_binding_ref"));
    Ok(())
}

#[sinex_test]
async fn ops_start_rejects_email_provider_operation_with_wrong_cursor_family(
    ctx: TestContext,
) -> TestResult<()> {
    let auth = system_auth();
    let error = handle_ops_start(
        ctx.pool(),
        json!({
            "operation_type": "email.mailbox.sync",
            "scope": {
                "source_id": "email.mailbox",
                "mode_id": "source:email.mailbox.gmail-api-scheduled-sync",
                "account_binding_ref": "operator-mailbox:primary",
                "uidvalidity": "100",
                "uid": "42"
            },
        }),
        &auth,
    )
    .await
    .expect_err("Gmail sync must reject IMAP cursor coordinates");

    assert!(
        error
            .to_string()
            .contains("cannot use IMAP UID cursor fields")
    );
    Ok(())
}

#[sinex_test]
async fn ops_start_rejects_email_operation_without_mode(ctx: TestContext) -> TestResult<()> {
    let auth = system_auth();
    let error = handle_ops_start(
        ctx.pool(),
        json!({
            "operation_type": "email.mailbox.authorize",
            "scope": {
                "source_id": "email.mailbox",
                "account_ref": "operator-mailbox:primary"
            },
        }),
        &auth,
    )
    .await
    .expect_err("email provider operation should require a package mode");

    assert!(error.to_string().contains("requires mode_id"));
    assert!(
        error
            .to_string()
            .contains("source:email.mailbox.gmail-api-scheduled-sync")
    );
    Ok(())
}

#[sinex_test]
async fn ops_list_returns_operations(ctx: TestContext) -> TestResult<()> {
    let auth = system_auth();

    let started = start_test_operation(&ctx, &auth, "restore").await?;

    let result = handle_ops_list(ctx.pool(), json!({}), &auth).await?;
    let response: OpsListResponse = serde_json::from_value(result)?;

    assert!(!response.operations.is_empty());
    assert!(
        response
            .operations
            .iter()
            .any(|op| op.id == started.operation.id
                && op.operation_type == "restore"
                && op.result_status == OperationStatus::Running),
        "listed operations should include the started operation with running status"
    );

    Ok(())
}

#[sinex_test]
async fn ops_list_rejects_non_positive_limit(ctx: TestContext) -> TestResult<()> {
    let auth = system_auth();

    let err = handle_ops_list(ctx.pool(), json!({ "limit": 0 }), &auth)
        .await
        .expect_err("non-positive limits should be rejected explicitly");

    assert!(err.to_string().contains("limit must be positive"));
    Ok(())
}

#[sinex_test]
async fn ops_get_returns_operation(ctx: TestContext) -> TestResult<()> {
    let auth = system_auth();
    let start_response = start_test_operation(&ctx, &auth, "purge").await?;
    let operation_id = &start_response.operation.id;

    let result = handle_ops_get(ctx.pool(), json!({ "operation_id": operation_id }), &auth).await?;
    let response: OpsGetResponse = serde_json::from_value(result)?;

    assert_eq!(response.operation.id, *operation_id);
    assert_eq!(response.operation.operation_type, "purge");
    assert_eq!(response.operation.operator, auth.actor_id());
    assert_eq!(response.operation.result_status, OperationStatus::Running);

    Ok(())
}

#[sinex_test]
async fn ops_cancel_stops_running_operation(ctx: TestContext) -> TestResult<()> {
    let auth = system_auth();
    let start_response = start_test_operation(&ctx, &auth, "archive").await?;
    let operation_id = &start_response.operation.id;

    let result = handle_ops_cancel(
        ctx.pool(),
        json!({
            "operation_id": operation_id,
            "reason": "test cancellation",
        }),
        &auth,
    )
    .await?;

    let response: OpsCancelResponse = serde_json::from_value(result)?;

    assert_eq!(response.operation.result_status, OperationStatus::Cancelled);
    assert_eq!(
        response.operation.result_message,
        Some("test cancellation".to_string())
    );
    assert!(response.cancelled);

    let persisted = get_operation(&ctx, &auth, operation_id).await?;
    assert_eq!(
        persisted.operation.result_status,
        OperationStatus::Cancelled
    );
    assert_eq!(
        persisted.operation.result_message,
        Some("test cancellation".to_string())
    );

    Ok(())
}

#[sinex_test]
async fn ops_cancel_rejects_non_running_operation(ctx: TestContext) -> TestResult<()> {
    let auth = system_auth();
    let start_response = start_test_operation(&ctx, &auth, "archive").await?;
    let operation_id = &start_response.operation.id;

    let first_cancel = handle_ops_cancel(
        ctx.pool(),
        json!({
            "operation_id": operation_id,
        }),
        &auth,
    )
    .await?;
    let first_response: OpsCancelResponse = serde_json::from_value(first_cancel)?;

    let err = handle_ops_cancel(
        ctx.pool(),
        json!({
            "operation_id": operation_id,
        }),
        &auth,
    )
    .await
    .expect_err("second cancel should fail");

    assert!(err.to_string().contains("cannot be cancelled"));

    let persisted = get_operation(&ctx, &auth, operation_id).await?;
    assert_eq!(
        persisted.operation.result_status,
        OperationStatus::Cancelled
    );
    assert!(
        persisted.operation.result_message == first_response.operation.result_message,
        "second cancel should not mutate stored cancellation payload"
    );

    Ok(())
}

#[sinex_test]
async fn ops_cancel_defaults_reason_to_authenticated_actor(ctx: TestContext) -> TestResult<()> {
    let auth = system_auth();
    let start_response = start_test_operation(&ctx, &auth, "archive").await?;
    let operation_id = &start_response.operation.id;

    let result = handle_ops_cancel(
        ctx.pool(),
        json!({
            "operation_id": operation_id,
        }),
        &auth,
    )
    .await?;
    let response: OpsCancelResponse = serde_json::from_value(result)?;

    assert_eq!(
        response.operation.result_message,
        Some(format!("Cancelled by {}", auth.actor_id()))
    );

    let persisted = get_operation(&ctx, &auth, operation_id).await?;
    assert_eq!(
        persisted.operation.result_message,
        Some(format!("Cancelled by {}", auth.actor_id()))
    );

    Ok(())
}

#[sinex_test]
async fn ops_cancel_replay_updates_replay_state_machine(ctx: TestContext) -> TestResult<()> {
    let auth = system_auth();
    let replay = ReplayStateMachine::new(ctx.pool.clone());
    let operation = replay
        .create_operation(
            ReplayScope {
                source_name: "ops-replay-source".to_string(),
                time_window: None,
                material_filter: None,
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

    let operation_id = operation.operation_id.to_string();
    let response: OpsCancelResponse = serde_json::from_value(
        handle_ops_cancel(
            ctx.pool(),
            json!({
                "operation_id": operation_id,
                "reason": "cancel replay from ops",
            }),
            &auth,
        )
        .await?,
    )?;
    assert_eq!(response.operation.result_status, OperationStatus::Cancelled);

    let replay_operation = replay.load_operation(operation.operation_id).await?;
    assert_eq!(replay_operation.state, ReplayState::Cancelled);
    assert_eq!(replay_operation.outcome, Some(ReplayOutcome::Cancelled));
    assert_eq!(
        replay_operation.error_details.as_deref(),
        Some("cancel replay from ops")
    );
    assert!(replay_operation.finished_at.is_some());

    let persisted = get_operation(&ctx, &auth, &response.operation.id).await?;
    assert_eq!(
        persisted.operation.result_status,
        OperationStatus::Cancelled
    );
    assert!(
        persisted.operation.duration_ms.is_some(),
        "terminal replay operations should persist duration_ms"
    );

    Ok(())
}

#[sinex_test]
async fn ops_cancel_tombstone_updates_scope_state(ctx: TestContext) -> TestResult<()> {
    let auth = system_auth();
    let source = "test.ops.tombstone";
    let event = publish_event(&ctx, source, 1).await?;
    let event_id = event
        .id
        .expect("published event should have an id")
        .to_string();

    let archive: LifecycleArchiveResponse = serde_json::from_value(
        handle_lifecycle_archive(
            ctx.pool(),
            json!({
                "event_ids": [event_id],
                "dry_run": false,
                "reason": "prepare tombstone for ops cancel",
            }),
            &auth,
        )
        .await?,
    )?;
    assert_eq!(archive.archived_count, 1);

    let create: TombstoneCreateResponse = serde_json::from_value(
        handle_tombstone_create(
            ctx.pool(),
            json!({
                "source": source,
                "limit": 1,
                "reason": "ops cancel tombstone",
            }),
            &auth,
        )
        .await?,
    )?;
    let tombstone_operation_id = create.operation.operation_id.clone();

    let response: OpsCancelResponse = serde_json::from_value(
        handle_ops_cancel(
            ctx.pool(),
            json!({
                "operation_id": tombstone_operation_id,
                "reason": "cancel tombstone from ops",
            }),
            &auth,
        )
        .await?,
    )?;
    assert_eq!(response.operation.result_status, OperationStatus::Cancelled);

    let status: TombstoneStatusResponse = serde_json::from_value(
        handle_tombstone_status(
            ctx.pool(),
            json!({ "operation_id": create.operation.operation_id }),
            &auth,
        )
        .await?,
    )?;
    assert_eq!(status.operation.state, TombstoneOperationState::Cancelled);
    assert!(status.operation.finished_at.is_some());
    assert_eq!(
        status.operation.error_details.as_deref(),
        Some("Cancelled: cancel tombstone from ops")
    );
    let persisted = get_operation(&ctx, &auth, &response.operation.id).await?;
    assert!(
        persisted.operation.duration_ms.is_some(),
        "ops.cancel tombstone path should persist duration_ms"
    );

    Ok(())
}

#[sinex_test]
async fn ops_cancel_tombstone_rejects_expired_operation(ctx: TestContext) -> TestResult<()> {
    let auth = system_auth();
    let source = "test.ops.tombstone.expired";
    let event = publish_event(&ctx, source, 1).await?;
    let event_id = event
        .id
        .expect("published event should have an id")
        .to_string();

    let archive: LifecycleArchiveResponse = serde_json::from_value(
        handle_lifecycle_archive(
            ctx.pool(),
            json!({
                "event_ids": [event_id],
                "dry_run": false,
                "reason": "prepare expired tombstone for ops cancel",
            }),
            &auth,
        )
        .await?,
    )?;
    assert_eq!(archive.archived_count, 1);

    let create: TombstoneCreateResponse = serde_json::from_value(
        handle_tombstone_create(
            ctx.pool(),
            json!({
                "source": source,
                "limit": 1,
                "reason": "expire before ops cancel",
            }),
            &auth,
        )
        .await?,
    )?;

    sqlx::query!(
        r#"
        UPDATE core.operations_log
        SET scope = jsonb_set(scope, '{expires_at}', to_jsonb($2::text), false)
        WHERE id = $1::uuid
        "#,
        create.operation.operation_id.parse::<uuid::Uuid>()?,
        "2000-01-01T00:00:00Z"
    )
    .execute(ctx.pool())
    .await?;

    let error = handle_ops_cancel(
        ctx.pool(),
        json!({
            "operation_id": create.operation.operation_id,
            "reason": "too late",
        }),
        &auth,
    )
    .await
    .expect_err("expired tombstone operation should reject ops.cancel");
    assert!(
        error.to_string().contains("has expired"),
        "unexpected error: {error}"
    );

    let status: TombstoneStatusResponse = serde_json::from_value(
        handle_tombstone_status(
            ctx.pool(),
            json!({ "operation_id": create.operation.operation_id }),
            &auth,
        )
        .await?,
    )?;
    assert_eq!(status.operation.state, TombstoneOperationState::Expired);
    assert_eq!(
        status.operation.error_details.as_deref(),
        Some("Expired before approval")
    );

    Ok(())
}
