use super::{
    EmailSyncExecutionResult, PackageOperationSpec, Result, elapsed_millis, optional_scope_string,
};
use sinex_db::DbPoolExt;
use sinex_db::repositories::{EmailMailboxProjectionEvent, EmailMailboxProjectionRecord};
use sinex_primitives::SinexError;
use sinex_primitives::domain::OperationStatus;
use sqlx::PgPool;
use std::time::Instant;

pub(super) async fn execute_materialization_operation(
    pool: &PgPool,
    spec: &PackageOperationSpec,
    mode_id: &str,
    scope: &mut serde_json::Map<String, serde_json::Value>,
    preview_summary: &mut serde_json::Value,
) -> Result<Option<EmailSyncExecutionResult>> {
    match spec.operation_type {
        "email.mailbox.fetch-attachments" => {
            execute_email_attachment_fetch(pool, spec, mode_id, scope, preview_summary)
                .await
                .map(Some)
        }
        "email.mailbox.export" => {
            execute_email_mailbox_export(pool, spec, mode_id, scope, preview_summary)
                .await
                .map(Some)
        }
        "email.mailbox.rebuild-projection" => {
            execute_email_projection_rebuild(pool, spec, mode_id, scope, preview_summary)
                .await
                .map(Some)
        }
        _ => Ok(None),
    }
}

async fn execute_email_attachment_fetch(
    pool: &PgPool,
    spec: &PackageOperationSpec,
    mode_id: &str,
    scope: &mut serde_json::Map<String, serde_json::Value>,
    preview_summary: &mut serde_json::Value,
) -> Result<EmailSyncExecutionResult> {
    let started = Instant::now();
    let material_policy_ref = optional_scope_string(scope, "attachment_material_policy_ref")
        .or_else(|| optional_scope_string(scope, "material_policy_ref"))
        .ok_or_else(|| {
            SinexError::validation(
                "email attachment materialization requires attachment_material_policy_ref",
            )
            .with_operation("ops.start")
        })?;
    let message_key = optional_scope_string(scope, "message_key");
    let rows = pool
        .email_mailbox_projections()
        .list_attachment_debt(spec.source_id, mode_id, message_key.as_deref())
        .await?;
    let outstanding_attachment_count: i64 = rows
        .iter()
        .map(|row| i64::from(row.attachment_count - row.attachment_observed_count))
        .sum();
    let selected_messages = email_projection_selection_values(&rows);
    let executor_state = "email_attachment_materialization_selected";

    scope.insert(
        "executor_state".to_string(),
        serde_json::json!(executor_state),
    );
    scope.insert(
        "attachment_material_policy_ref".to_string(),
        serde_json::json!(material_policy_ref),
    );
    scope.insert(
        "selected_message_count".to_string(),
        serde_json::json!(rows.len()),
    );
    scope.insert(
        "outstanding_attachment_count".to_string(),
        serde_json::json!(outstanding_attachment_count),
    );
    scope.insert(
        "selected_messages".to_string(),
        serde_json::Value::Array(selected_messages.clone()),
    );

    let preview = preview_summary
        .as_object_mut()
        .expect("package operation preview is an object");
    preview.insert(
        "executor_state".to_string(),
        serde_json::json!(executor_state),
    );
    preview.insert(
        "selected_message_count".to_string(),
        serde_json::json!(rows.len()),
    );
    preview.insert(
        "outstanding_attachment_count".to_string(),
        serde_json::json!(outstanding_attachment_count),
    );
    preview.insert(
        "selected_messages".to_string(),
        serde_json::Value::Array(selected_messages),
    );
    preview.insert(
        "message".to_string(),
        serde_json::json!("email attachment materialization selected from projection debt"),
    );

    Ok(EmailSyncExecutionResult {
        status: OperationStatus::Success,
        message: format!("{}; attachment materialization selected", spec.surface),
        duration_ms: Some(elapsed_millis(started)),
    })
}

async fn execute_email_mailbox_export(
    pool: &PgPool,
    spec: &PackageOperationSpec,
    mode_id: &str,
    scope: &mut serde_json::Map<String, serde_json::Value>,
    preview_summary: &mut serde_json::Value,
) -> Result<EmailSyncExecutionResult> {
    let started = Instant::now();
    let message_key = optional_scope_string(scope, "message_key");
    let output_path = optional_scope_string(scope, "output_path")
        .or_else(|| optional_scope_string(scope, "path"));
    let mut rows = pool
        .email_mailbox_projections()
        .list_current_by_source_mode(spec.source_id, mode_id)
        .await?;
    if let Some(message_key) = message_key.as_deref() {
        rows.retain(|row| row.message_key == message_key);
    }
    let export_manifest = serde_json::json!({
        "schema": "sinex.email.mailbox.export.metadata.v1",
        "source_id": spec.source_id,
        "mode_id": mode_id,
        "disclosure_context": "export",
        "disclosure_policy": {
            "posture": "metadata_only",
            "body": "omitted",
            "attachment_bytes": "omitted",
            "raw_message_bytes": "omitted",
            "caveat": "mailbox export emits projection metadata only; raw body and attachment bytes require explicit materialization policy"
        },
        "message_count": rows.len(),
        "messages": rows.iter().map(email_projection_export_value).collect::<Vec<_>>(),
    });
    if let Some(path) = output_path.as_deref() {
        let bytes = serde_json::to_vec_pretty(&export_manifest).map_err(|error| {
            SinexError::serialization("failed to render email mailbox export")
                .with_std_error(&error)
                .with_operation("ops.start")
        })?;
        tokio::fs::write(path, bytes).await.map_err(|error| {
            SinexError::io("failed to write email mailbox export artifact")
                .with_context("output_path", path)
                .with_std_error(&error)
                .with_operation("ops.start")
        })?;
    }
    let executor_state = "email_mailbox_metadata_exported";
    scope.insert(
        "executor_state".to_string(),
        serde_json::json!(executor_state),
    );
    scope.insert("export".to_string(), export_manifest.clone());
    if let Some(path) = output_path {
        scope.insert("output_path".to_string(), serde_json::json!(path));
    }

    let preview = preview_summary
        .as_object_mut()
        .expect("package operation preview is an object");
    preview.insert(
        "executor_state".to_string(),
        serde_json::json!(executor_state),
    );
    preview.insert("export".to_string(), export_manifest);
    preview.insert(
        "message".to_string(),
        serde_json::json!("email mailbox metadata export completed"),
    );

    Ok(EmailSyncExecutionResult {
        status: OperationStatus::Success,
        message: format!("{}; metadata export completed", spec.surface),
        duration_ms: Some(elapsed_millis(started)),
    })
}

async fn execute_email_projection_rebuild(
    pool: &PgPool,
    spec: &PackageOperationSpec,
    mode_id: &str,
    scope: &mut serde_json::Map<String, serde_json::Value>,
    preview_summary: &mut serde_json::Value,
) -> Result<EmailSyncExecutionResult> {
    let started = Instant::now();
    let event_types = [
        "email.message.received",
        "email.message.sent",
        "email.thread.observed",
        "email.attachment.observed",
    ]
    .map(str::to_string);
    let rows = sqlx::query!(
        r#"
        SELECT
            e.id AS "event_id!: uuid::Uuid",
            e.event_type AS "event_type!",
            e.payload AS "payload!"
        FROM core.events e
        LEFT JOIN raw.source_material_registry sm
          ON sm.id = e.source_material_id
        WHERE e.source = 'email'
          AND e.event_type = ANY($1::text[])
          AND COALESCE(
              sm.metadata #>> '{source_material_contract,origin,binding_id}',
              sm.metadata #>> '{email_staged_sync,mode_id}',
              sm.metadata #>> '{email_provider_sync,mode_id}',
              e.payload #>> '{mode_id}',
              $2
          ) = $2
        ORDER BY e.ts_orig, e.id
        "#,
        &event_types,
        mode_id
    )
    .fetch_all(pool)
    .await
    .map_err(|error| {
        SinexError::database("failed to load email events for projection rebuild")
            .with_context("mode_id", mode_id)
            .with_std_error(&error)
            .with_operation("ops.start")
    })?;
    let mut projected_count = 0usize;
    for row in &rows {
        if pool
            .email_mailbox_projections()
            .upsert_event(EmailMailboxProjectionEvent {
                source_id: spec.source_id.to_string(),
                mode_id: mode_id.to_string(),
                event_id: row.event_id,
                event_type: row.event_type.clone(),
                payload: row.payload.clone(),
            })
            .await?
            .is_some()
        {
            projected_count += 1;
        }
    }
    let executor_state = "email_mailbox_projection_rebuilt";
    scope.insert(
        "executor_state".to_string(),
        serde_json::json!(executor_state),
    );
    scope.insert(
        "replayed_event_count".to_string(),
        serde_json::json!(rows.len()),
    );
    scope.insert(
        "projected_event_count".to_string(),
        serde_json::json!(projected_count),
    );

    let preview = preview_summary
        .as_object_mut()
        .expect("package operation preview is an object");
    preview.insert(
        "executor_state".to_string(),
        serde_json::json!(executor_state),
    );
    preview.insert(
        "replayed_event_count".to_string(),
        serde_json::json!(rows.len()),
    );
    preview.insert(
        "projected_event_count".to_string(),
        serde_json::json!(projected_count),
    );
    preview.insert(
        "message".to_string(),
        serde_json::json!("email mailbox projection rebuild completed"),
    );

    Ok(EmailSyncExecutionResult {
        status: OperationStatus::Success,
        message: format!("{}; projection rebuild completed", spec.surface),
        duration_ms: Some(elapsed_millis(started)),
    })
}

fn email_projection_selection_values(
    rows: &[EmailMailboxProjectionRecord],
) -> Vec<serde_json::Value> {
    rows.iter()
        .map(|row| {
            serde_json::json!({
                "message_key": row.message_key,
                "message_id": row.message_id,
                "raw_material_id": row.raw_material_id,
                "attachment_count": row.attachment_count,
                "attachment_observed_count": row.attachment_observed_count,
                "outstanding_attachment_count": row.attachment_count - row.attachment_observed_count,
                "attachment_policy_refs": row.attachment_policy_refs,
            })
        })
        .collect()
}

fn email_projection_export_value(row: &EmailMailboxProjectionRecord) -> serde_json::Value {
    serde_json::json!({
        "message_key": row.message_key,
        "message_id": row.message_id,
        "thread_key": row.thread_key,
        "thread_root_message_id": row.thread_root_message_id,
        "direction": row.direction,
        "folder": row.folder,
        "mailbox_format": row.mailbox_format,
        "source_file": row.source_file,
        "raw_material_id": row.raw_material_id,
        "subject": row.subject,
        "from": row.from_addresses,
        "to": row.to_addresses,
        "body_bytes": row.body_bytes,
        "attachment_count": row.attachment_count,
        "attachment_observed_count": row.attachment_observed_count,
        "attachment_policy_refs": row.attachment_policy_refs,
        "last_message_event_id": row.last_message_event_id,
        "last_thread_event_id": row.last_thread_event_id,
        "last_attachment_event_id": row.last_attachment_event_id,
        "last_observed_at": row.last_observed_at,
        "updated_at": row.updated_at,
    })
}
