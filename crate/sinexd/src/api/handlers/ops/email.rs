use super::{
    EmailSyncExecutionResult, PackageOperationSpec, Result, elapsed_millis, optional_scope_string,
};
use crate::event_engine::policy::{DisclosureContext, PolicyEngine};
use crate::runtime::parser::GmailHttpClient;
use sinex_db::DbPoolExt;
use sinex_db::SourceMaterialRecord;
use sinex_db::repositories::{EmailMailboxProjectionEvent, EmailMailboxProjectionRecord};
use sinex_primitives::Id;
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
    let mut materialized_attachments = Vec::new();
    let mut blocked_materials = Vec::new();
    for row in &rows {
        match materialize_email_projection_attachments(
            pool,
            mode_id,
            row,
            &material_policy_ref,
            scope,
        )
        .await?
        {
            EmailAttachmentMaterialization::Materialized(items) => {
                materialized_attachments.extend(items);
            }
            EmailAttachmentMaterialization::Blocked(blocked) => {
                blocked_materials.push(blocked);
            }
        }
    }
    let materialized_attachment_count = materialized_attachments.len();
    let blocked_material_count = blocked_materials.len();
    let executor_state = if blocked_materials.is_empty() {
        "email_attachment_materialized"
    } else if materialized_attachments.is_empty() {
        "email_attachment_materialization_blocked"
    } else {
        "email_attachment_materialization_partial"
    };

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
        "materialized_attachment_count".to_string(),
        serde_json::json!(materialized_attachment_count),
    );
    scope.insert(
        "blocked_material_count".to_string(),
        serde_json::json!(blocked_material_count),
    );
    scope.insert(
        "selected_messages".to_string(),
        serde_json::Value::Array(selected_messages.clone()),
    );
    scope.insert(
        "materialized_attachments".to_string(),
        serde_json::Value::Array(materialized_attachments.clone()),
    );
    scope.insert(
        "blocked_materials".to_string(),
        serde_json::Value::Array(blocked_materials.clone()),
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
        "materialized_attachment_count".to_string(),
        serde_json::json!(materialized_attachment_count),
    );
    preview.insert(
        "blocked_material_count".to_string(),
        serde_json::json!(blocked_material_count),
    );
    preview.insert(
        "selected_messages".to_string(),
        serde_json::Value::Array(selected_messages),
    );
    preview.insert(
        "materialized_attachments".to_string(),
        serde_json::Value::Array(materialized_attachments),
    );
    preview.insert(
        "blocked_materials".to_string(),
        serde_json::Value::Array(blocked_materials),
    );
    preview.insert(
        "message".to_string(),
        serde_json::json!("email attachment materialization evaluated projection debt"),
    );

    Ok(EmailSyncExecutionResult {
        status: if blocked_material_count == 0 {
            OperationStatus::Success
        } else {
            OperationStatus::Failed
        },
        message: format!(
            "{}; attachment materialization materialized {materialized_attachment_count} and blocked {blocked_material_count}",
            spec.surface
        ),
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
    let include_material = optional_scope_bool(scope, "include_material")
        || optional_scope_bool(scope, "include_body_material")
        || optional_scope_bool(scope, "include_attachment_material");
    let output_path = optional_scope_string(scope, "output_path")
        .or_else(|| optional_scope_string(scope, "path"));
    let mut rows = pool
        .email_mailbox_projections()
        .list_current_by_source_mode(spec.source_id, mode_id)
        .await?;
    if let Some(message_key) = message_key.as_deref() {
        rows.retain(|row| row.message_key == message_key);
    }
    let material_exports = if include_material {
        email_projection_material_exports(pool, &rows).await?
    } else {
        Vec::new()
    };
    let export_manifest = serde_json::json!({
        "schema": "sinex.email.mailbox.export.metadata.v1",
        "source_id": spec.source_id,
        "mode_id": mode_id,
        "disclosure_context": "export",
        "disclosure_policy": {
            "posture": if include_material { "metadata_with_material_evidence" } else { "metadata_only" },
            "body": if include_material { "raw_message_preview_disclosed" } else { "omitted" },
            "attachment_bytes": if include_material { "materialized_attachment_events_disclosed" } else { "omitted" },
            "raw_message_bytes": if include_material { "digest_and_preview_disclosed" } else { "omitted" },
            "caveat": "mailbox export emits projection metadata only; raw body and attachment bytes require explicit materialization policy"
        },
        "message_count": rows.len(),
        "messages": rows.iter().map(email_projection_export_value).collect::<Vec<_>>(),
        "material_exports": material_exports,
    });
    let policy = PolicyEngine::load(pool.clone()).await?;
    let disclosure = policy
        .disclose_json_value(export_manifest, DisclosureContext::Export)
        .await;
    let export_manifest = disclosure.value;
    let export_disclosure = serde_json::json!({
        "redacted": disclosure.changed,
        "privacy_state": disclosure.privacy_state,
        "caveats": disclosure.caveats.iter().map(|caveat| {
            serde_json::json!({
                "id": caveat.code,
                "message": caveat.message,
                "ref": {
                    "kind": "privacy_policy",
                    "id": caveat.policy_ref,
                }
            })
        }).collect::<Vec<_>>(),
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
    scope.insert("export_disclosure".to_string(), export_disclosure.clone());
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
    preview.insert("export_disclosure".to_string(), export_disclosure);
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

enum EmailAttachmentMaterialization {
    Materialized(Vec<serde_json::Value>),
    Blocked(serde_json::Value),
}

async fn materialize_email_projection_attachments(
    pool: &PgPool,
    mode_id: &str,
    row: &EmailMailboxProjectionRecord,
    material_policy_ref: &str,
    scope: &serde_json::Map<String, serde_json::Value>,
) -> Result<EmailAttachmentMaterialization> {
    let outstanding = row.attachment_count - row.attachment_observed_count;
    if outstanding == 0 {
        return Ok(EmailAttachmentMaterialization::Materialized(Vec::new()));
    }
    if row.mailbox_format.as_deref() == Some("gmail-api") {
        return materialize_gmail_projection_attachments(
            pool,
            mode_id,
            row,
            material_policy_ref,
            scope,
        )
        .await;
    }
    let Some(material) = load_projection_source_material(pool, row).await? else {
        return Ok(EmailAttachmentMaterialization::Blocked(
            blocked_email_material(row, "source_material_not_found"),
        ));
    };
    let Some(path) = source_material_path(&material) else {
        return Ok(EmailAttachmentMaterialization::Blocked(
            blocked_email_material(row, "source_material_has_no_file_uri"),
        ));
    };
    let material = read_projection_raw_message(row, path, "attachment materialization").await?;
    let mut materialized = Vec::new();
    for index in row.attachment_observed_count..row.attachment_count {
        let event_id = uuid::Uuid::now_v7();
        let payload = serde_json::json!({
            "message_id": row.message_id,
            "folder": row.folder,
            "source_file": row.source_file,
            "raw_material_id": row.raw_material_id,
            "mailbox_format": row.mailbox_format,
            "attachment_index": index,
            "disposition": "attachment",
            "filename": null,
            "content_type": null,
            "content_id": null,
            "material_policy_ref": material_policy_ref,
            "materialization": {
                "source": "source_material_file",
                "source_uri": material.source_uri.clone(),
                "byte_range": material.byte_range.clone(),
                "raw_message_bytes": material.raw_message_bytes,
                "raw_message_blake3": material.raw_message_blake3.clone()
            }
        });
        pool.email_mailbox_projections()
            .upsert_event(EmailMailboxProjectionEvent {
                source_id: "email.mailbox".to_string(),
                mode_id: mode_id.to_string(),
                observed_event_id: event_id,
                event_type: "email.attachment.observed".to_string(),
                payload,
            })
            .await?;
        materialized.push(serde_json::json!({
            "message_key": row.message_key,
            "message_id": row.message_id,
            "raw_material_id": row.raw_material_id,
            "attachment_index": index,
            "material_policy_ref": material_policy_ref,
            "source_uri": material.source_uri.clone(),
            "byte_range": material.byte_range.clone(),
            "raw_message_bytes": material.raw_message_bytes,
            "raw_message_blake3": material.raw_message_blake3.clone(),
            "observed_event_id": event_id,
        }));
    }
    Ok(EmailAttachmentMaterialization::Materialized(materialized))
}

async fn materialize_gmail_projection_attachments(
    pool: &PgPool,
    mode_id: &str,
    row: &EmailMailboxProjectionRecord,
    material_policy_ref: &str,
    scope: &serde_json::Map<String, serde_json::Value>,
) -> Result<EmailAttachmentMaterialization> {
    let Some(message_id) = row.message_id.as_deref() else {
        return Ok(EmailAttachmentMaterialization::Blocked(
            blocked_email_material(row, "gmail_message_id_required"),
        ));
    };
    let Some(token_file) = optional_scope_string(scope, "gmail_token_file")
        .or_else(|| optional_scope_string(scope, "access_token_file"))
        .or_else(|| optional_scope_string(scope, "token_file"))
    else {
        return Ok(EmailAttachmentMaterialization::Blocked(
            blocked_email_material(
                row,
                "gmail_token_file_required_for_provider_materialization",
            ),
        ));
    };
    let token = tokio::fs::read_to_string(&token_file)
        .await
        .map_err(|error| {
            SinexError::io("failed to read Gmail token file for attachment materialization")
                .with_context("token_file", token_file.clone())
                .with_std_error(&error)
                .with_operation("ops.start")
        })?;
    let token = token.trim().to_string();
    if token.is_empty() {
        return Ok(EmailAttachmentMaterialization::Blocked(
            blocked_email_material(row, "gmail_token_file_empty"),
        ));
    }
    let client = GmailHttpClient::with_endpoint(
        reqwest::Client::new(),
        optional_scope_string(scope, "gmail_api_base_url")
            .or_else(|| optional_scope_string(scope, "api_base_url"))
            .unwrap_or_else(|| "https://gmail.googleapis.com/gmail/v1".to_string()),
        optional_scope_string(scope, "gmail_user_id")
            .or_else(|| optional_scope_string(scope, "user_id"))
            .unwrap_or_else(|| "me".to_string()),
        token,
    );
    let raw = match client.fetch_raw_message(message_id).await {
        Ok(raw) => raw,
        Err(_) => {
            return Ok(EmailAttachmentMaterialization::Blocked(
                blocked_email_material(row, "gmail_raw_message_fetch_failed"),
            ));
        }
    };
    let material = projection_raw_message_from_bytes(
        row,
        format!("gmail://messages/{message_id}?format=raw"),
        &raw,
    )?;
    let mut materialized = Vec::new();
    for index in row.attachment_observed_count..row.attachment_count {
        let event_id = uuid::Uuid::now_v7();
        let payload = serde_json::json!({
            "message_id": row.message_id,
            "folder": row.folder,
            "source_file": row.source_file,
            "raw_material_id": row.raw_material_id,
            "mailbox_format": row.mailbox_format,
            "attachment_index": index,
            "disposition": "attachment",
            "filename": null,
            "content_type": null,
            "content_id": null,
            "material_policy_ref": material_policy_ref,
            "materialization": {
                "source": "gmail_api_raw_message",
                "source_uri": material.source_uri.clone(),
                "byte_range": material.byte_range.clone(),
                "raw_message_bytes": material.raw_message_bytes,
                "raw_message_blake3": material.raw_message_blake3.clone()
            }
        });
        pool.email_mailbox_projections()
            .upsert_event(EmailMailboxProjectionEvent {
                source_id: "email.mailbox".to_string(),
                mode_id: mode_id.to_string(),
                observed_event_id: event_id,
                event_type: "email.attachment.observed".to_string(),
                payload,
            })
            .await?;
        materialized.push(serde_json::json!({
            "message_key": row.message_key,
            "message_id": row.message_id,
            "raw_material_id": row.raw_material_id,
            "attachment_index": index,
            "material_policy_ref": material_policy_ref,
            "source": "gmail_api_raw_message",
            "source_uri": material.source_uri.clone(),
            "byte_range": material.byte_range.clone(),
            "raw_message_bytes": material.raw_message_bytes,
            "raw_message_blake3": material.raw_message_blake3.clone(),
            "observed_event_id": event_id,
        }));
    }
    Ok(EmailAttachmentMaterialization::Materialized(materialized))
}

async fn email_projection_material_exports(
    pool: &PgPool,
    rows: &[EmailMailboxProjectionRecord],
) -> Result<Vec<serde_json::Value>> {
    let mut exports = Vec::new();
    for row in rows {
        match load_projection_source_material(pool, row).await? {
            Some(material) => {
                if let Some(path) = source_material_path(&material) {
                    let material = read_projection_raw_message(row, path, "export").await?;
                    exports.push(serde_json::json!({
                        "message_key": row.message_key,
                        "message_id": row.message_id,
                        "raw_material_id": row.raw_material_id,
                        "mbox_byte_start": row.mbox_byte_start,
                        "mbox_byte_end": row.mbox_byte_end,
                        "source_uri": material.source_uri,
                        "byte_range": material.byte_range,
                        "raw_message_bytes": material.raw_message_bytes,
                        "raw_message_blake3": material.raw_message_blake3,
                        "raw_message_preview": material.preview,
                        "attachment_observed_count": row.attachment_observed_count,
                        "attachment_count": row.attachment_count,
                    }));
                } else {
                    exports.push(blocked_email_material(
                        row,
                        "source_material_has_no_file_uri",
                    ));
                }
            }
            None => exports.push(blocked_email_material(row, "source_material_not_found")),
        }
    }
    Ok(exports)
}

struct ProjectionRawMessage {
    source_uri: String,
    byte_range: serde_json::Value,
    raw_message_bytes: usize,
    raw_message_blake3: String,
    preview: String,
}

async fn read_projection_raw_message(
    row: &EmailMailboxProjectionRecord,
    path: String,
    purpose: &str,
) -> Result<ProjectionRawMessage> {
    let bytes = tokio::fs::read(&path).await.map_err(|error| {
        SinexError::io(format!(
            "failed to read email source material for {purpose}"
        ))
        .with_context(
            "raw_material_id",
            row.raw_material_id.as_deref().unwrap_or("<missing>"),
        )
        .with_context("source_uri", path.clone())
        .with_std_error(&error)
        .with_operation("ops.start")
    })?;
    projection_raw_message_from_bytes(row, path, &bytes)
}

fn projection_raw_message_from_bytes(
    row: &EmailMailboxProjectionRecord,
    source_uri: String,
    bytes: &[u8],
) -> Result<ProjectionRawMessage> {
    let (bytes, byte_range) = projection_raw_message_slice(row, bytes)?;
    Ok(ProjectionRawMessage {
        source_uri,
        byte_range,
        raw_message_bytes: bytes.len(),
        raw_message_blake3: blake3::hash(bytes).to_hex().to_string(),
        preview: String::from_utf8_lossy(bytes)
            .chars()
            .take(512)
            .collect::<String>(),
    })
}

fn projection_raw_message_slice<'a>(
    row: &EmailMailboxProjectionRecord,
    bytes: &'a [u8],
) -> Result<(&'a [u8], serde_json::Value)> {
    let Some(start) = row.mbox_byte_start else {
        return Ok((
            bytes,
            serde_json::json!({
                "kind": "full_source_material",
                "start": 0,
                "end": bytes.len(),
            }),
        ));
    };
    let Some(end) = row.mbox_byte_end else {
        return Err(
            SinexError::validation("email MBOX projection is missing byte-range end")
                .with_context("message_key", &row.message_key)
                .with_operation("ops.start"),
        );
    };
    let start = usize::try_from(start).map_err(|error| {
        SinexError::validation("email MBOX byte-range start does not fit usize")
            .with_context("message_key", &row.message_key)
            .with_std_error(&error)
            .with_operation("ops.start")
    })?;
    let end = usize::try_from(end).map_err(|error| {
        SinexError::validation("email MBOX byte-range end does not fit usize")
            .with_context("message_key", &row.message_key)
            .with_std_error(&error)
            .with_operation("ops.start")
    })?;
    let slice = bytes.get(start..end).ok_or_else(|| {
        SinexError::validation("email MBOX byte-range is outside source material")
            .with_context("message_key", &row.message_key)
            .with_context("start", start)
            .with_context("end", end)
            .with_context("source_bytes", bytes.len())
            .with_operation("ops.start")
    })?;
    Ok((
        slice,
        serde_json::json!({
            "kind": "mbox_message_byte_range",
            "start": start,
            "end": end,
        }),
    ))
}

async fn load_projection_source_material(
    pool: &PgPool,
    row: &EmailMailboxProjectionRecord,
) -> Result<Option<SourceMaterialRecord>> {
    let Some(raw_material_id) = row.raw_material_id.as_deref() else {
        return Ok(None);
    };
    let Ok(raw_material_id) = uuid::Uuid::parse_str(raw_material_id) else {
        return Ok(None);
    };
    pool.source_materials()
        .get_by_id(Id::<SourceMaterialRecord>::from_uuid(raw_material_id))
        .await
        .map_err(Into::into)
}

fn source_material_path(material: &SourceMaterialRecord) -> Option<String> {
    material
        .metadata
        .pointer("/source_uri")
        .and_then(serde_json::Value::as_str)
        .or_else(|| {
            material
                .metadata
                .pointer("/source_material_contract/origin/source_uri")
                .and_then(serde_json::Value::as_str)
        })
        .map(str::to_string)
}

fn blocked_email_material(row: &EmailMailboxProjectionRecord, reason: &str) -> serde_json::Value {
    serde_json::json!({
        "message_key": row.message_key,
        "message_id": row.message_id,
        "raw_material_id": row.raw_material_id,
        "outstanding_attachment_count": row.attachment_count - row.attachment_observed_count,
        "reason": reason,
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
                observed_event_id: row.event_id,
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
                "mbox_byte_start": row.mbox_byte_start,
                "mbox_byte_end": row.mbox_byte_end,
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
        "mbox_byte_start": row.mbox_byte_start,
        "mbox_byte_end": row.mbox_byte_end,
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

fn optional_scope_bool(scope: &serde_json::Map<String, serde_json::Value>, key: &str) -> bool {
    scope
        .get(key)
        .and_then(|value| {
            value
                .as_bool()
                .or_else(|| value.as_str().map(|value| value == "true"))
        })
        .unwrap_or(false)
}
