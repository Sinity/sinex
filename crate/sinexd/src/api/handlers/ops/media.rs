use serde::Deserialize;
use sinex_db::DbPoolExt;
use sinex_primitives::domain::{
    OperationStatus, SourceMaterialFormat, SourceMaterialTimingInfoType,
};
use sinex_primitives::events::SourceMaterial;
use sinex_primitives::rpc::sources::{SourceMaterialMetadataContract, SourceOrigin};
use sinex_primitives::{Id, SinexError};
use sqlx::PgPool;
use std::process::Stdio;
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::process::Command;

use super::package::{
    MEDIA_WORKER_COMMAND_DEFAULT_TIMEOUT_MS, MEDIA_WORKER_COMMAND_EXECUTOR_STATE,
    MEDIA_WORKER_COMMAND_FAILED_STATE, MEDIA_WORKER_COMMAND_KEY,
    MEDIA_WORKER_OUTPUT_EXECUTOR_STATE, MEDIA_WORKER_OUTPUT_KEY, MEDIA_WORKER_OUTPUT_MAX_BYTES,
    MEDIA_WORKER_OUTPUT_PATH_KEY, MEDIA_WORKER_STDERR_MAX_BYTES, MediaCapturePackage,
    MediaOperationAction, PackageOperationSpec,
};
use super::{Result, elapsed_millis, parsed_material_intent_to_event};

pub(super) struct MediaWorkerOutputResult {
    pub(super) status: OperationStatus,
    pub(super) message: String,
    pub(super) duration_ms: Option<i32>,
}

struct MediaWorkerOutput {
    bytes: Vec<u8>,
    source_identifier: String,
    executor_state: &'static str,
    duration_ms: Option<i32>,
}

struct MediaWorkerCommandOutcome {
    output: Option<MediaWorkerOutput>,
    summary: serde_json::Value,
    failure_message: Option<String>,
    duration_ms: Option<i32>,
    /// Structured capture-debt entry recorded on failure/timeout/model-missing
    /// so the local-model-batch outcome is visible as operator debt rather than
    /// an opaque error.
    debt: Option<serde_json::Value>,
}

/// Build a capture-debt entry for a media local-model-batch failure mode.
fn media_capture_debt(
    kind: &'static str,
    reason: impl Into<String>,
    required_action: &'static str,
) -> serde_json::Value {
    serde_json::json!({
        "kind": kind,
        "reason": reason.into(),
        "required_action": required_action,
        "debt_ref": format!("debt:media.local_model_batch.{kind}"),
    })
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct MediaWorkerCommandRequest {
    program: String,
    #[serde(default)]
    args: Vec<String>,
    #[serde(default)]
    timeout_ms: Option<u64>,
    #[serde(default)]
    output_source_identifier: Option<String>,
}

impl MediaWorkerCommandRequest {
    fn validate(&self) -> Result<()> {
        if self.program.trim().is_empty() {
            return Err(SinexError::validation(
                "media worker command requires a non-empty program",
            )
            .with_operation("ops.start"));
        }
        if self.args.len() > 256 {
            return Err(SinexError::validation(
                "media worker command accepts at most 256 arguments",
            )
            .with_operation("ops.start")
            .with_context("argument_count", self.args.len().to_string()));
        }
        Ok(())
    }

    fn timeout(&self) -> Duration {
        Duration::from_millis(
            self.timeout_ms
                .unwrap_or(MEDIA_WORKER_COMMAND_DEFAULT_TIMEOUT_MS)
                .max(1),
        )
    }

    fn sanitized_scope(&self) -> serde_json::Value {
        serde_json::json!({
            "program": self.program,
            "args": self.args,
            "timeout_ms": self.timeout().as_millis(),
            "output_source_identifier": self.output_source_identifier,
            "stdout_max_bytes": MEDIA_WORKER_OUTPUT_MAX_BYTES,
            "stderr_max_bytes": MEDIA_WORKER_STDERR_MAX_BYTES,
        })
    }
}

pub(super) async fn execute_worker_output(
    pool: &PgPool,
    spec: &PackageOperationSpec,
    mode_id: &str,
    scope: &mut serde_json::Map<String, serde_json::Value>,
    preview_summary: &mut serde_json::Value,
) -> Result<Option<MediaWorkerOutputResult>> {
    let Some(worker_output) = resolve_media_worker_output(scope, preview_summary).await? else {
        return Ok(None);
    };
    let package = MediaCapturePackage::from_source_id(spec.source_id).ok_or_else(|| {
        SinexError::validation("media worker output operation requires a media source package")
            .with_operation("ops.start")
            .with_context("source_id", spec.source_id)
    })?;
    let action = MediaOperationAction::from_spec_action(spec.action).ok_or_else(|| {
        SinexError::validation("media worker output operation requires a media action")
            .with_operation("ops.start")
            .with_context("action", spec.action)
    })?;
    if !action.consumes_worker_output() {
        return Err(SinexError::validation(format!(
            "media operation {} does not consume worker output",
            spec.operation_type
        ))
        .with_operation("ops.start"));
    }

    if let Some(message) = worker_output.failure_message {
        scope.insert(
            "executor_state".to_string(),
            serde_json::json!(MEDIA_WORKER_COMMAND_FAILED_STATE),
        );
        if let Some(budget) = worker_output.summary.get("worker_budget") {
            scope.insert("worker_budget".to_string(), budget.clone());
        }
        if let Some(debt) = &worker_output.debt {
            scope.insert("capture_debt".to_string(), debt.clone());
        }
        let preview = preview_summary
            .as_object_mut()
            .expect("package operation preview is an object");
        preview.insert(
            "executor_state".to_string(),
            serde_json::json!(MEDIA_WORKER_COMMAND_FAILED_STATE),
        );
        if let Some(debt) = &worker_output.debt {
            preview.insert("capture_debt".to_string(), debt.clone());
        }
        preview.insert("worker_command".to_string(), worker_output.summary);
        return Ok(Some(MediaWorkerOutputResult {
            status: OperationStatus::Failed,
            message,
            duration_ms: worker_output.duration_ms,
        }));
    }

    let success_budget = worker_output.summary.get("worker_budget").cloned();
    let worker_output = worker_output
        .output
        .expect("successful media worker resolution should include output");
    let mut contract = SourceMaterialMetadataContract::new(
        SourceMaterialFormat::Json,
        SourceMaterialTimingInfoType::StagedAt,
    );
    contract.origin = Some(SourceOrigin {
        source_uri: Some(worker_output.source_identifier.clone()),
        binding_id: Some(mode_id.to_string()),
        ..SourceOrigin::default()
    });

    let material =
        sinex_db::repositories::SourceMaterial::blob_text(&worker_output.source_identifier)
            .with_metadata_contract(&contract)
            .with_metadata(serde_json::json!({
                "media_worker_output": {
                    "source_id": spec.source_id,
                    "mode_id": mode_id,
                    "operation_type": spec.operation_type,
                    "action": spec.action,
                    "material_class": package.material_class().as_str()
                }
            }));
    let mut material_record = pool.source_materials().register_material(material).await?;
    let total_bytes = i64::try_from(worker_output.bytes.len()).map_err(|error| {
        SinexError::validation("media worker output is too large to record")
            .with_std_error(&error)
            .with_operation("ops.start")
    })?;
    sqlx::query!(
        "UPDATE raw.source_material_registry SET total_bytes = $1 WHERE id = $2",
        total_bytes,
        material_record.id
    )
    .execute(pool)
    .await
    .map_err(|error| {
        SinexError::database("Failed to persist media worker output material size")
            .with_context("material_id", material_record.id.to_string())
            .with_std_error(&error)
    })?;
    material_record.total_bytes = Some(total_bytes);

    let dispatch = crate::sources::dispatch::default_parser_dispatch();
    let outcome = dispatch(
        spec.source_id,
        &worker_output.bytes,
        Some(material_record.id),
    )
    .map_err(|error| {
        SinexError::parse("media worker output parser failed")
            .with_context("source_id", spec.source_id)
            .with_context("mode_id", mode_id)
            .with_context("parse_error", error)
            .with_operation("ops.start")
    })?;

    let mut admitted_event_ids = Vec::new();
    for intent in outcome.events {
        let event = parsed_material_intent_to_event(
            intent,
            Id::<SourceMaterial>::from_uuid(material_record.id),
        )?;
        let persisted = pool.events().insert(event).await?;
        if let Some(id) = persisted.id {
            admitted_event_ids.push(id.to_string());
        }
    }

    scope.insert(
        "executor_state".to_string(),
        serde_json::json!(worker_output.executor_state),
    );
    if let Some(budget) = success_budget {
        scope.insert("worker_budget".to_string(), budget);
    }
    scope.insert(
        "worker_output_material_id".to_string(),
        serde_json::json!(material_record.id.to_string()),
    );
    scope.insert(
        "worker_output_event_ids".to_string(),
        serde_json::json!(admitted_event_ids),
    );
    scope.insert(
        "worker_output_parser".to_string(),
        serde_json::json!({
            "parser_id": outcome.parser_id,
            "parser_version": outcome.parser_version
        }),
    );

    let preview = preview_summary
        .as_object_mut()
        .expect("package operation preview is an object");
    preview.insert(
        "executor_state".to_string(),
        serde_json::json!(worker_output.executor_state),
    );
    preview.insert(
        "worker_output_material_id".to_string(),
        serde_json::json!(material_record.id.to_string()),
    );
    preview.insert(
        "admitted_event_count".to_string(),
        serde_json::json!(
            scope["worker_output_event_ids"]
                .as_array()
                .map_or(0, std::vec::Vec::len)
        ),
    );

    Ok(Some(MediaWorkerOutputResult {
        status: OperationStatus::Success,
        message: match worker_output.executor_state {
            MEDIA_WORKER_COMMAND_EXECUTOR_STATE => {
                format!("{}; media worker command output admitted", spec.surface)
            }
            _ => format!("{}; media worker output admitted", spec.surface),
        },
        duration_ms: worker_output.duration_ms,
    }))
}

async fn resolve_media_worker_output(
    scope: &mut serde_json::Map<String, serde_json::Value>,
    preview_summary: &mut serde_json::Value,
) -> Result<Option<MediaWorkerCommandOutcome>> {
    let has_direct_output = scope.contains_key(MEDIA_WORKER_OUTPUT_KEY)
        || scope.contains_key(MEDIA_WORKER_OUTPUT_PATH_KEY);
    let has_command = scope.contains_key(MEDIA_WORKER_COMMAND_KEY);
    if has_direct_output && has_command {
        return Err(SinexError::validation(
            "media operation accepts either worker_output/worker_output_path or worker_command, not both",
        )
        .with_operation("ops.start"));
    }

    if has_command {
        let value = scope
            .remove(MEDIA_WORKER_COMMAND_KEY)
            .expect("checked worker command presence");
        let request: MediaWorkerCommandRequest =
            serde_json::from_value(value).map_err(|error| {
                SinexError::validation("media worker command has invalid shape")
                    .with_std_error(&error)
                    .with_operation("ops.start")
            })?;
        request.validate()?;
        scope.insert("worker_command".to_string(), request.sanitized_scope());
        return execute_media_worker_command(request, preview_summary)
            .await
            .map(Some);
    }

    let Some(output) = read_media_worker_output(scope).await? else {
        return Ok(None);
    };
    scope.remove(MEDIA_WORKER_OUTPUT_KEY);
    scope.remove(MEDIA_WORKER_OUTPUT_PATH_KEY);
    Ok(Some(MediaWorkerCommandOutcome {
        output: Some(output),
        summary: serde_json::json!({ "kind": "direct_worker_output" }),
        failure_message: None,
        duration_ms: None,
        debt: None,
    }))
}

async fn execute_media_worker_command(
    request: MediaWorkerCommandRequest,
    preview_summary: &mut serde_json::Value,
) -> Result<MediaWorkerCommandOutcome> {
    let started = Instant::now();
    let spawn_result = Command::new(&request.program)
        .args(&request.args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn();
    let mut child = match spawn_result {
        Ok(child) => child,
        // A missing model/worker binary is operator debt (install the model),
        // not an internal error — surface it as model_unavailable.
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let mut summary = serde_json::json!({
                "program": request.program,
                "args": request.args,
                "timeout_ms": request.timeout().as_millis(),
                "duration_ms": elapsed_millis(started),
                "timed_out": false,
                "model_available": false,
            });
            summary["worker_budget"] = worker_budget_block(&request, elapsed_millis(started), false);
            return Ok(MediaWorkerCommandOutcome {
                output: None,
                summary,
                failure_message: Some(format!(
                    "media_capture; media worker program '{}' was not found",
                    request.program
                )),
                duration_ms: Some(elapsed_millis(started)),
                debt: Some(media_capture_debt(
                    "model_unavailable",
                    format!("worker program '{}' not found on PATH", request.program),
                    "install_or_configure_model",
                )),
            });
        }
        Err(error) => {
            return Err(SinexError::io("Failed to spawn media worker command")
                .with_context("program", request.program.clone())
                .with_std_error(&error)
                .with_operation("ops.start"));
        }
    };

    let stdout = child.stdout.take().ok_or_else(|| {
        SinexError::io("Failed to capture media worker stdout").with_operation("ops.start")
    })?;
    let stderr = child.stderr.take().ok_or_else(|| {
        SinexError::io("Failed to capture media worker stderr").with_operation("ops.start")
    })?;
    let stdout_task = tokio::spawn(read_limited(
        stdout,
        MEDIA_WORKER_OUTPUT_MAX_BYTES,
        "stdout",
    ));
    let stderr_task = tokio::spawn(read_limited(
        stderr,
        MEDIA_WORKER_STDERR_MAX_BYTES,
        "stderr",
    ));

    let wait_result = tokio::time::timeout(request.timeout(), child.wait()).await;
    let timed_out = wait_result.is_err();
    let status = match wait_result {
        Ok(result) => Some(result.map_err(|error| {
            SinexError::io("Failed waiting for media worker command")
                .with_std_error(&error)
                .with_operation("ops.start")
        })?),
        Err(_) => {
            let _ = child.kill().await;
            None
        }
    };

    let stdout = task_bytes(stdout_task, "stdout").await;
    let stderr = task_bytes(stderr_task, "stderr").await;
    let duration_ms = elapsed_millis(started);

    let stdout_bytes = stdout.as_ref().map_or(0, Vec::len);
    let stderr_bytes = stderr.as_ref().map_or(0, Vec::len);
    let mut summary = serde_json::json!({
        "program": request.program,
        "args": request.args,
        "timeout_ms": request.timeout().as_millis(),
        "duration_ms": duration_ms,
        "timed_out": timed_out,
        "stdout_bytes": stdout_bytes,
        "stderr_bytes": stderr_bytes,
        "model_available": true,
    });
    summary["worker_budget"] = worker_budget_block(&request, duration_ms, timed_out);
    if let Some(status) = status {
        summary["exit_code"] = status
            .code()
            .map_or(serde_json::Value::Null, |code| serde_json::json!(code));
        if !status.success() {
            return Ok(MediaWorkerCommandOutcome {
                output: None,
                summary,
                failure_message: Some(format!(
                    "media_capture; media worker command exited with status {status}"
                )),
                duration_ms: Some(duration_ms),
                debt: Some(media_capture_debt(
                    "worker_failed",
                    format!("worker command exited with status {status}"),
                    "retry_or_inspect_worker_logs",
                )),
            });
        }
    }
    if timed_out {
        return Ok(MediaWorkerCommandOutcome {
            output: None,
            summary,
            failure_message: Some("media_capture; media worker command timed out".to_string()),
            duration_ms: Some(duration_ms),
            debt: Some(media_capture_debt(
                "worker_timeout",
                format!(
                    "worker did not finish within {}ms budget",
                    request.timeout().as_millis()
                ),
                "increase_timeout_or_retry",
            )),
        });
    }

    let stdout = stdout.map_err(|error| {
        SinexError::io("Failed to read media worker stdout")
            .with_std_error(&error)
            .with_operation("ops.start")
    })?;
    let stderr = stderr.map_err(|error| {
        SinexError::io("Failed to read media worker stderr")
            .with_std_error(&error)
            .with_operation("ops.start")
    })?;
    summary["stdout_bytes"] = serde_json::json!(stdout.len());
    summary["stderr_bytes"] = serde_json::json!(stderr.len());
    preview_summary
        .as_object_mut()
        .expect("package operation preview is an object")
        .insert("worker_command".to_string(), summary.clone());

    Ok(MediaWorkerCommandOutcome {
        output: Some(MediaWorkerOutput {
            bytes: stdout,
            source_identifier: request.output_source_identifier.unwrap_or_else(|| {
                format!("process://media-worker-command/{}", uuid::Uuid::now_v7())
            }),
            executor_state: MEDIA_WORKER_COMMAND_EXECUTOR_STATE,
            duration_ms: Some(duration_ms),
        }),
        summary,
        failure_message: None,
        duration_ms: Some(duration_ms),
        debt: None,
    })
}

/// Worker resource-budget block (timeout utilization, queue depth) surfaced for
/// local-model-batch coverage.
fn worker_budget_block(
    request: &MediaWorkerCommandRequest,
    duration_ms: i32,
    timed_out: bool,
) -> serde_json::Value {
    let timeout_ms = request.timeout().as_millis();
    let utilization_pct = if timeout_ms > 0 {
        ((u128::from(duration_ms.max(0).unsigned_abs()) * 100) / timeout_ms).min(100)
    } else {
        0
    };
    serde_json::json!({
        "timeout_ms": timeout_ms,
        "duration_ms": duration_ms,
        "utilization_pct": utilization_pct,
        "over_budget": timed_out,
        "queue_depth": 1,
    })
}

async fn read_limited<R>(
    mut reader: R,
    max_len: usize,
    stream_name: &'static str,
) -> std::io::Result<Vec<u8>>
where
    R: AsyncRead + Unpin,
{
    let mut output = Vec::new();
    let mut chunk = [0_u8; 8192];
    loop {
        let read = reader.read(&mut chunk).await?;
        if read == 0 {
            return Ok(output);
        }
        if output.len().saturating_add(read) > max_len {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("media worker {stream_name} exceeded {max_len} bytes"),
            ));
        }
        output.extend_from_slice(&chunk[..read]);
    }
}

async fn task_bytes(
    task: tokio::task::JoinHandle<std::io::Result<Vec<u8>>>,
    stream_name: &'static str,
) -> std::io::Result<Vec<u8>> {
    task.await.map_err(|error| {
        std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("media worker {stream_name} reader task failed: {error}"),
        )
    })?
}

async fn read_media_worker_output(
    scope: &serde_json::Map<String, serde_json::Value>,
) -> Result<Option<MediaWorkerOutput>> {
    if let Some(path) = scope
        .get(MEDIA_WORKER_OUTPUT_PATH_KEY)
        .and_then(serde_json::Value::as_str)
    {
        let bytes = tokio::fs::read(path).await.map_err(|error| {
            SinexError::io("Failed to read media worker output file")
                .with_context(MEDIA_WORKER_OUTPUT_PATH_KEY, path)
                .with_std_error(&error)
                .with_operation("ops.start")
        })?;
        validate_media_worker_output_size(bytes.len())?;
        return Ok(Some(MediaWorkerOutput {
            bytes,
            source_identifier: path.to_string(),
            executor_state: MEDIA_WORKER_OUTPUT_EXECUTOR_STATE,
            duration_ms: None,
        }));
    }

    let Some(value) = scope.get(MEDIA_WORKER_OUTPUT_KEY) else {
        return Ok(None);
    };
    let bytes = match value {
        serde_json::Value::String(text) => text.as_bytes().to_vec(),
        other => serde_json::to_vec(other).map_err(|error| {
            SinexError::serialization("Failed to serialize media worker output JSON")
                .with_std_error(&error)
                .with_operation("ops.start")
        })?,
    };
    validate_media_worker_output_size(bytes.len())?;
    Ok(Some(MediaWorkerOutput {
        bytes,
        source_identifier: format!("memory://media-worker-output/{}", uuid::Uuid::now_v7()),
        executor_state: MEDIA_WORKER_OUTPUT_EXECUTOR_STATE,
        duration_ms: None,
    }))
}

fn validate_media_worker_output_size(byte_len: usize) -> Result<()> {
    if byte_len > MEDIA_WORKER_OUTPUT_MAX_BYTES {
        return Err(SinexError::validation(format!(
            "media worker output is limited to {MEDIA_WORKER_OUTPUT_MAX_BYTES} bytes"
        ))
        .with_context("worker_output_bytes", byte_len.to_string())
        .with_operation("ops.start"));
    }
    Ok(())
}
