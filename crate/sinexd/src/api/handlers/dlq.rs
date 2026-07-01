//! Raw-ingest DLQ management handlers
//!
//! This module provides RPC endpoints for managing the operator-facing raw-ingest
//! DLQ in NATS:
//! - List raw DLQ statistics
//! - Peek at raw DLQ messages without removing them
//! - Requeue raw DLQ messages back to the main raw-event stream
//! - Purge raw DLQ messages

use crate::api::service_container::ServiceContainer;
use crate::event_engine::policy::{DisclosureCaveat, DisclosureContext, PolicyEngine};
use crate::runtime::dlq_retry::{DlqRetryConfig, DlqRetryHandler};
use serde_json::Value as JsonValue;
use serde_json::json;
use sinex_db::DbPoolExt;
use sinex_db::repositories::Operation;
use sinex_primitives::domain::OperationStatus;
use sinex_primitives::runtime_pressure::{RuntimePressureAction, RuntimePressureLevel};
use sinex_primitives::validation::normalize_unicode;
use sinex_primitives::views::{CaveatView, SinexObjectKind, SinexObjectRef};
use sinex_primitives::{Result, SinexError};
use tracing::warn;

// Re-export RPC types for consistency
pub use sinex_primitives::rpc::dlq::{
    DlqListRequest, DlqListResponse, DlqMessagePeek, DlqPeekRequest, DlqPeekResponse,
    DlqPressureSignal, DlqPurgeRequest, DlqPurgeResponse, DlqRequeueRequest, DlqRequeueResponse,
};

const MIN_DLQ_PAYLOAD_PREVIEW_CHARS: usize = 64;
const MAX_DLQ_PAYLOAD_PREVIEW_CHARS: usize = 4096;

#[derive(Debug, Clone, PartialEq)]
struct SanitizedPreview {
    text: String,
    redacted: bool,
    caveats: Vec<CaveatView>,
}

fn parse_retry_count_header(headers: Option<&async_nats::HeaderMap>) -> Result<u32> {
    let Some(value) = headers.and_then(|headers| headers.get("Retry-Count")) else {
        return Ok(0);
    };

    value.as_str().parse::<u32>().map_err(|error| {
        SinexError::validation("Retry-Count header is invalid")
            .with_context("value", value)
            .with_std_error(&error)
    })
}

fn dlq_pending_sequence_span(total_messages: u64, first_seq: u64, last_seq: u64) -> u64 {
    if total_messages == 0 || first_seq == 0 || last_seq < first_seq {
        0
    } else {
        last_seq - first_seq + 1
    }
}

fn dlq_pressure_level(total_messages: u64, retry_batch_size: usize) -> RuntimePressureLevel {
    if total_messages == 0 {
        RuntimePressureLevel::Nominal
    } else if total_messages > retry_batch_size as u64 {
        RuntimePressureLevel::Critical
    } else {
        RuntimePressureLevel::Warning
    }
}

fn dlq_runtime_action(total_messages: u64, retry_batch_size: usize) -> RuntimePressureAction {
    if total_messages == 0 {
        RuntimePressureAction::Admit
    } else if total_messages > retry_batch_size as u64 {
        RuntimePressureAction::Throttle
    } else {
        RuntimePressureAction::Inspect
    }
}

fn dlq_operator_action(total_messages: u64) -> (&'static str, &'static str) {
    if total_messages == 0 {
        ("none", "raw-ingest DLQ is empty")
    } else {
        (
            "ops dlq cleanup-plan --all-retained",
            "classify retained failures before running paced requeue or purge",
        )
    }
}

fn dlq_pressure_signal(
    total_messages: u64,
    total_bytes: u64,
    retry_batch_size: usize,
) -> DlqPressureSignal {
    let (recommended_action, reason) = dlq_operator_action(total_messages);
    DlqPressureSignal {
        pressure_level: dlq_pressure_level(total_messages, retry_batch_size),
        runtime_action: dlq_runtime_action(total_messages, retry_batch_size),
        pending_messages: total_messages,
        pending_bytes: total_bytes,
        retry_batch_size: retry_batch_size as u64,
        recommended_action: recommended_action.to_string(),
        reason: reason.to_string(),
    }
}

async fn log_dlq_operation(
    services: &ServiceContainer,
    operation_type: &'static str,
    actor: &str,
    scope: JsonValue,
    result_status: OperationStatus,
    result_message: String,
    preview_summary: JsonValue,
) -> Result<String> {
    let record = services
        .pool()
        .state()
        .log_operation(Operation {
            id: None,
            operation_type: operation_type.to_string(),
            operator: actor.to_string(),
            scope: Some(scope),
            result_status,
            result_message: Some(result_message),
            preview_summary: Some(preview_summary),
            duration_ms: None,
        })
        .await?;
    Ok(record.id.to_uuid().to_string())
}

fn truncate_preview(payload: &str, max_chars: usize) -> String {
    let preview: String = payload.chars().take(max_chars).collect();
    if payload.chars().count() > max_chars {
        format!("{preview}...")
    } else {
        preview
    }
}

fn render_preview_value(value: &JsonValue) -> String {
    match value {
        JsonValue::String(text) => text.clone(),
        JsonValue::Object(object) => {
            let mut prioritized = Vec::new();
            for key in [
                "error",
                "code",
                "kind",
                "reason",
                "material_id",
                "event_id",
                "source_material_id",
                "expected_bytes",
                "assembled_bytes",
                "expected_slices",
                "slice_count",
                "context",
                "failed_at",
            ] {
                if let Some(value) = object.get(key) {
                    prioritized.push((key, value));
                }
            }
            for (key, value) in object {
                if !prioritized
                    .iter()
                    .any(|(prioritized_key, _)| *prioritized_key == key.as_str())
                {
                    prioritized.push((key.as_str(), value));
                }
            }
            let mut rendered = String::from("{");
            for (idx, (key, value)) in prioritized.into_iter().enumerate() {
                if idx > 0 {
                    rendered.push(',');
                }
                let key = serde_json::to_string(key).unwrap_or_else(|_| {
                    "[payload preview unavailable: JSON serialization failed]".into()
                });
                let value = render_nested_preview_value(value);
                rendered.push_str(&key);
                rendered.push(':');
                rendered.push_str(&value);
            }
            rendered.push('}');
            rendered
        }
        other => serde_json::to_string(other)
            .unwrap_or_else(|_| "[payload preview unavailable: JSON serialization failed]".into()),
    }
}

fn render_nested_preview_value(value: &JsonValue) -> String {
    match value {
        JsonValue::Object(_) => render_preview_value(value),
        other => serde_json::to_string(other)
            .unwrap_or_else(|_| "[payload preview unavailable: JSON serialization failed]".into()),
    }
}

fn format_disclosure_caveat(caveat: DisclosureCaveat) -> CaveatView {
    CaveatView {
        id: caveat.code,
        message: caveat.message,
        ref_: Some(
            SinexObjectRef::new(SinexObjectKind::Policy, caveat.policy_ref)
                .with_label("privacy policy")
                .with_command_hint("sinexctl privacy policy list")
                .with_rpc_method("privacy.policy.list"),
        ),
    }
}

async fn payload_preview(
    payload: &str,
    max_chars: usize,
    policy_engine: &PolicyEngine,
) -> SanitizedPreview {
    let original = serde_json::from_str::<JsonValue>(payload)
        .unwrap_or_else(|_| JsonValue::String(payload.to_string()));
    let decision = policy_engine
        .disclose_json_value(original, DisclosureContext::Dlq)
        .await;
    let current = render_preview_value(&decision.value);

    SanitizedPreview {
        text: truncate_preview(&current, max_chars),
        redacted: decision.changed,
        caveats: decision
            .caveats
            .into_iter()
            .map(format_disclosure_caveat)
            .collect(),
    }
}

/// Handle raw-DLQ list request - returns statistics about the raw-ingest DLQ.
pub async fn handle_dlq_list(
    services: &ServiceContainer,
    _request: DlqListRequest,
) -> Result<DlqListResponse> {
    let nats_client = services
        .nats_client()
        .ok_or_else(|| SinexError::configuration("NATS client is not available"))?;
    let env = services.environment();
    let config = DlqRetryConfig::default();
    let retry_batch_size = config.batch_size;
    let handler = DlqRetryHandler::new(nats_client.clone(), env.clone(), config);

    let stats = handler
        .get_stats()
        .await
        .map_err(|error| SinexError::service("Failed to get DLQ statistics").with_source(error))?;

    let pressure = dlq_pressure_signal(stats.total_messages, stats.total_bytes, retry_batch_size);
    let response = DlqListResponse {
        total_messages: stats.total_messages,
        total_bytes: stats.total_bytes,
        first_seq: stats.first_seq,
        last_seq: stats.last_seq,
        pressure_level: pressure.pressure_level.clone(),
        resource_pressure: pressure.clone(),
        pending_sequence_span: dlq_pending_sequence_span(
            stats.total_messages,
            stats.first_seq,
            stats.last_seq,
        ),
        recommended_action: pressure.recommended_action,
        action_reason: pressure.reason,
    };

    Ok(response)
}

/// Handle raw-DLQ peek request - preview messages without removing them.
pub async fn handle_dlq_peek(
    services: &ServiceContainer,
    peek_params: DlqPeekRequest,
) -> Result<DlqPeekResponse> {
    use async_nats::jetstream;
    let nats_client = services
        .nats_client()
        .ok_or_else(|| SinexError::configuration("NATS client is not available"))?;
    let env = services.environment();

    let js = jetstream::new(nats_client.clone());
    let dlq_stream_name = env.nats_stream_name("SINEX_RAW_EVENTS_DLQ");

    let mut stream = js.get_stream(&dlq_stream_name).await.map_err(|error| {
        SinexError::service("Failed to get DLQ stream")
            .with_context("stream", &dlq_stream_name)
            .with_source(error)
    })?;

    let mut previews = Vec::new();
    if peek_params.limit == 0 {
        return Ok(DlqPeekResponse::from_messages(previews));
    }

    let info = stream
        .info()
        .await
        .map_err(|error| SinexError::service("Failed to get DLQ stream info").with_source(error))?;
    let state = &info.state;
    if state.messages == 0 || state.last_sequence == 0 {
        return Ok(DlqPeekResponse::from_messages(previews));
    }
    let start_sequence = peek_params
        .start_sequence
        .unwrap_or(state.first_sequence)
        .max(state.first_sequence);
    let payload_preview_chars = peek_params
        .payload_preview_chars
        .clamp(MIN_DLQ_PAYLOAD_PREVIEW_CHARS, MAX_DLQ_PAYLOAD_PREVIEW_CHARS);

    for sequence in start_sequence..=state.last_sequence {
        if previews.len() >= peek_params.limit {
            break;
        }
        let msg = match stream.direct_get(sequence).await {
            Ok(message) => message,
            Err(error)
                if matches!(
                    error.kind(),
                    async_nats::jetstream::stream::DirectGetErrorKind::NotFound
                ) =>
            {
                continue;
            }
            Err(error) => {
                return Err(SinexError::service("Error reading DLQ message")
                    .with_context("sequence", sequence.to_string())
                    .with_source(error));
            }
        };
        let retry_count = parse_retry_count_header(Some(&msg.headers))?;

        let original_subject = msg
            .headers
            .get("Original-Subject")
            .map(std::string::ToString::to_string);

        // Create safe preview of payload (limit size).
        // Normalize unicode to NFC and reject confusable/direction-override chars;
        // if normalization fails (e.g. RTL override in payload), fall back to a
        // safe placeholder so operator UIs are not misled.
        let payload_str = String::from_utf8_lossy(&msg.payload);
        let normalized_payload = match normalize_unicode(payload_str.as_ref()) {
            Ok(s) => s,
            Err(e) => {
                warn!(
                    error = %e,
                    payload_len = msg.payload.len(),
                    "DLQ payload contains dangerous Unicode; replacing preview with sanitized placeholder"
                );
                "[payload contains dangerous Unicode characters]".to_string()
            }
        };
        let payload_preview = payload_preview(
            &normalized_payload,
            payload_preview_chars,
            services.privacy_policy(),
        )
        .await;

        previews.push(DlqMessagePeek {
            subject: msg.subject.to_string(),
            sequence,
            retry_count,
            original_subject,
            payload_preview: payload_preview.text,
            payload_redacted: payload_preview.redacted,
            privacy_caveats: payload_preview.caveats,
        });
    }

    let response = DlqPeekResponse::from_messages(previews);
    Ok(response)
}

/// Handle raw-DLQ requeue request - move raw-ingest failures back to the main stream.
///
/// # Authorization
///
/// This is a dangerous operation that requeues failed messages back to the main stream.
/// The auth context is logged for audit purposes.
pub async fn handle_dlq_requeue(
    services: &ServiceContainer,
    requeue_params: DlqRequeueRequest,
    auth: &crate::api::rpc_server::RpcAuthContext,
) -> Result<DlqRequeueResponse> {
    use tracing::info;
    let nats_client = services
        .nats_client()
        .ok_or_else(|| SinexError::configuration("NATS client is not available"))?;
    let env = services.environment();

    let config = DlqRetryConfig::default();
    let handler = DlqRetryHandler::new(nats_client.clone(), env.clone(), config);

    let actor = auth.actor_id().to_string();
    let mut operation_scope = json!({
        "surface": "raw_ingest_dlq",
        "action": "requeue",
    });

    let requeue_range = match (requeue_params.start_sequence, requeue_params.end_sequence) {
        (Some(start), Some(end)) if start == 0 || end == 0 => {
            return Err(SinexError::validation(
                "DLQ requeue sequence bounds must be positive",
            ));
        }
        (Some(start), Some(end)) if start > end => {
            return Err(SinexError::validation(
                "DLQ requeue start_sequence must be <= end_sequence",
            )
            .with_context("start_sequence", start.to_string())
            .with_context("end_sequence", end.to_string()));
        }
        (Some(start), Some(end)) => Some((start, end)),
        (None, None) => None,
        _ => {
            return Err(SinexError::validation(
                "DLQ requeue sequence selector requires both start_sequence and end_sequence",
            ));
        }
    };
    let selector_count = usize::from(requeue_params.event_id.is_some())
        + usize::from(requeue_range.is_some())
        + usize::from(requeue_params.all);
    if selector_count != 1 {
        return Err(SinexError::validation(
            "Must specify exactly one of 'event_id', 'start_sequence/end_sequence', or 'all: true'",
        ));
    }

    let requeue_result = if let Some(ref event_id) = requeue_params.event_id {
        // Requeue specific event
        operation_scope["selector"] = json!("event_id");
        operation_scope["event_id"] = json!(event_id);
        info!(
            actor = %actor,
            event_id = %event_id,
            "DLQ requeue operation initiated"
        );
        handler
            .retry_by_id(event_id)
            .await
            .map_err(|error| {
                SinexError::service("Failed to requeue event")
                    .with_context("event_id", event_id)
                    .with_source(error)
            })
            .map(|()| 1usize)
    } else if let Some((start_sequence, end_sequence)) = requeue_range {
        operation_scope["selector"] = json!("sequence_range");
        operation_scope["start_sequence"] = json!(start_sequence);
        operation_scope["end_sequence"] = json!(end_sequence);
        info!(
            actor = %actor,
            start_sequence,
            end_sequence,
            "DLQ sequence-range requeue operation initiated"
        );
        handler
            .retry_sequence_range(start_sequence, end_sequence)
            .await
            .map_err(|error| {
                SinexError::service("Failed to requeue DLQ sequence range")
                    .with_context("start_sequence", start_sequence.to_string())
                    .with_context("end_sequence", end_sequence.to_string())
                    .with_source(error)
            })
            .map(|result| {
                operation_scope["permanently_failed"] = json!(result.permanently_failed);
                result.retried
            })
    } else if requeue_params.all {
        // Requeue all events
        operation_scope["selector"] = json!("all");
        info!(
            actor = %actor,
            "DLQ requeue all operation initiated"
        );
        handler
            .retry_all()
            .await
            .map_err(|error| {
                SinexError::service("Failed to requeue all DLQ messages").with_source(error)
            })
            .map(|result| {
                operation_scope["permanently_failed"] = json!(result.permanently_failed);
                if result.permanently_failed > 0 {
                    warn!(
                        permanently_failed = result.permanently_failed,
                        "Some DLQ messages exceeded max retries and were permanently discarded"
                    );
                }
                result.retried
            })
    } else {
        unreachable!("selector_count validation should have rejected empty selectors");
    };

    let (requeued_count, operation_id) = match requeue_result {
        Ok(requeued_count) => {
            operation_scope["requeued_count"] = json!(requeued_count);
            let operation_id = log_dlq_operation(
                services,
                "dlq.requeue",
                &actor,
                operation_scope.clone(),
                OperationStatus::Success,
                format!("requeued {requeued_count} raw-ingest DLQ message(s)"),
                json!({
                    "surface": "raw_ingest_dlq",
                    "action": "requeue",
                    "requeued_count": requeued_count,
                    "selector": operation_scope.get("selector").cloned().unwrap_or(JsonValue::Null),
                }),
            )
            .await?;
            (requeued_count, operation_id)
        }
        Err(error) => {
            let message = error.to_string();
            let _ = log_dlq_operation(
                services,
                "dlq.requeue",
                &actor,
                operation_scope,
                OperationStatus::Failed,
                message.clone(),
                json!({
                    "surface": "raw_ingest_dlq",
                    "action": "requeue",
                    "error": message,
                }),
            )
            .await;
            return Err(error);
        }
    };

    let response = DlqRequeueResponse {
        status: "success".to_string(),
        requeued_count: requeued_count as u64,
        operation_id,
    };
    Ok(response)
}

/// Handle raw-DLQ purge request - permanently delete raw-ingest DLQ messages.
///
/// # Authorization
///
/// This is a destructive operation that permanently deletes selected DLQ messages.
/// The auth context is logged for audit purposes.
pub async fn handle_dlq_purge(
    services: &ServiceContainer,
    purge_params: DlqPurgeRequest,
    auth: &crate::api::rpc_server::RpcAuthContext,
) -> Result<DlqPurgeResponse> {
    use async_nats::jetstream;
    use tracing::info;
    let nats_client = services
        .nats_client()
        .ok_or_else(|| SinexError::configuration("NATS client is not available"))?;
    let env = services.environment();

    if !purge_params.confirm {
        return Err(SinexError::validation(
            "Purge operation requires 'confirm: true' parameter",
        ));
    }
    let purge_range = match (purge_params.start_sequence, purge_params.end_sequence) {
        (Some(start), Some(end)) if start == 0 || end == 0 => {
            return Err(SinexError::validation(
                "DLQ purge sequence bounds must be positive",
            ));
        }
        (Some(start), Some(end)) if start > end => {
            return Err(
                SinexError::validation("DLQ purge start_sequence must be <= end_sequence")
                    .with_context("start_sequence", start.to_string())
                    .with_context("end_sequence", end.to_string()),
            );
        }
        (Some(start), Some(end)) => Some((start, end)),
        (None, None) => None,
        _ => {
            return Err(SinexError::validation(
                "DLQ purge sequence selector requires both start_sequence and end_sequence",
            ));
        }
    };

    let js = jetstream::new(nats_client.clone());
    let dlq_stream_name = env.nats_stream_name("SINEX_RAW_EVENTS_DLQ");

    let mut stream = js.get_stream(&dlq_stream_name).await.map_err(|error| {
        SinexError::service("Failed to get DLQ stream")
            .with_context("stream", &dlq_stream_name)
            .with_source(error)
    })?;

    // Get current stats before purge
    let info = stream
        .info()
        .await
        .map_err(|error| SinexError::service("Failed to get stream info").with_source(error))?;
    let messages_before = info.state.messages;

    let actor = auth.actor_id().to_string();
    let mut operation_scope = json!({
        "surface": "raw_ingest_dlq",
        "action": "purge",
        "stream": dlq_stream_name,
        "messages_before": messages_before,
        "confirm": true,
    });
    if let Some((start_sequence, end_sequence)) = purge_range
        && let Some(scope) = operation_scope.as_object_mut()
    {
        scope.insert("selector".to_string(), json!("sequence_range"));
        scope.insert("start_sequence".to_string(), json!(start_sequence));
        scope.insert("end_sequence".to_string(), json!(end_sequence));
    }

    info!(
        actor = %actor,
        messages_to_purge = messages_before,
        ?purge_range,
        "DLQ purge operation initiated"
    );

    let purge_result = if let Some((start_sequence, end_sequence)) = purge_range {
        purge_dlq_sequence_range(&stream, start_sequence, end_sequence).await
    } else {
        match stream.purge().await {
            Ok(response) if response.success => Ok(response.purged),
            Ok(_) => Err(SinexError::service(
                "DLQ stream purge returned success=false",
            )),
            Err(error) => Err(SinexError::service("Failed to purge DLQ stream").with_source(error)),
        }
    };

    let purged_count = match purge_result {
        Ok(purged_count) => purged_count,
        Err(error) => {
            let message = error.to_string();
            let _ = log_dlq_operation(
                services,
                "dlq.purge",
                &actor,
                operation_scope,
                OperationStatus::Failed,
                message.clone(),
                json!({
                    "surface": "raw_ingest_dlq",
                    "action": "purge",
                    "error": message,
                }),
            )
            .await;
            return Err(error);
        }
    };

    let operation_id = log_dlq_operation(
        services,
        "dlq.purge",
        &actor,
        operation_scope,
        OperationStatus::Success,
        format!("purged {purged_count} raw-ingest DLQ message(s)"),
        json!({
            "surface": "raw_ingest_dlq",
            "action": "purge",
            "purged_count": purged_count,
        }),
    )
    .await?;

    let response = DlqPurgeResponse {
        status: "success".to_string(),
        purged_count,
        operation_id,
    };
    Ok(response)
}

async fn purge_dlq_sequence_range(
    stream: &async_nats::jetstream::stream::Stream,
    start_sequence: u64,
    end_sequence: u64,
) -> Result<u64> {
    let mut purged_count = 0;
    for sequence in start_sequence..=end_sequence {
        match stream.direct_get(sequence).await {
            Ok(_) => {}
            Err(error)
                if matches!(
                    error.kind(),
                    async_nats::jetstream::stream::DirectGetErrorKind::NotFound
                ) =>
            {
                continue;
            }
            Err(error) => {
                return Err(SinexError::service("Failed to inspect DLQ stream message")
                    .with_context("sequence", sequence.to_string())
                    .with_source(error));
            }
        }
        let deleted = stream.delete_message(sequence).await.map_err(|error| {
            SinexError::service("Failed to delete DLQ stream message")
                .with_context("sequence", sequence.to_string())
                .with_source(error)
        })?;
        if deleted {
            purged_count += 1;
        }
    }
    Ok(purged_count)
}

#[cfg(test)]
#[path = "dlq_test.rs"]
mod tests;
