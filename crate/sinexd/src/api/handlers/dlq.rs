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

fn require_stream_sequence(sequence: std::result::Result<u64, String>) -> Result<u64> {
    sequence.map_err(|error| {
        SinexError::service("Failed to inspect DLQ message sequence").with_source(error)
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
            "ops dlq peek",
            "inspect failures before running paced requeue or purge",
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
    use futures::StreamExt;
    let nats_client = services
        .nats_client()
        .ok_or_else(|| SinexError::configuration("NATS client is not available"))?;
    let env = services.environment();

    let js = jetstream::new(nats_client.clone());
    let dlq_stream_name = env.nats_stream_name("SINEX_RAW_EVENTS_DLQ");

    let stream = js.get_stream(&dlq_stream_name).await.map_err(|error| {
        SinexError::service("Failed to get DLQ stream")
            .with_context("stream", &dlq_stream_name)
            .with_source(error)
    })?;

    // Create ephemeral consumer for peeking
    // Issue 126: Add timeout to NATS consumer creation
    let timeout = services.config().nats_consumer_create_timeout();
    let consumer = tokio::time::timeout(
        timeout,
        stream.create_consumer(jetstream::consumer::pull::Config {
            name: None, // ephemeral
            durable_name: None,
            filter_subject: env.nats_subject("events.dlq.>"),
            ack_policy: jetstream::consumer::AckPolicy::None, // Don't ack, just peek
            deliver_policy: jetstream::consumer::DeliverPolicy::All,
            ..Default::default()
        }),
    )
    .await
    .map_err(|error| {
        SinexError::timeout("Consumer creation timed out")
            .with_context("timeout_ms", timeout.as_millis())
            .with_std_error(&error)
    })?
    .map_err(|error| SinexError::service("Failed to create peek consumer").with_source(error))?;

    let mut messages = consumer
        .messages()
        .await
        .map_err(|error| SinexError::service("Failed to get messages").with_source(error))?;

    let mut previews = Vec::new();
    let mut count = 0;

    while count < peek_params.limit {
        match messages.next().await {
            Some(Ok(msg)) => {
                let retry_count = parse_retry_count_header(msg.headers.as_ref())?;

                let original_subject = msg
                    .headers
                    .as_ref()
                    .and_then(|h| h.get("Original-Subject"))
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
                let payload_preview =
                    payload_preview(&normalized_payload, 200, services.privacy_policy()).await;
                let sequence = require_stream_sequence(
                    msg.info()
                        .map(|info| info.stream_sequence)
                        .map_err(|error| error.to_string()),
                )?;

                previews.push(DlqMessagePeek {
                    subject: msg.subject.to_string(),
                    sequence,
                    retry_count,
                    original_subject,
                    payload_preview: payload_preview.text,
                    payload_redacted: payload_preview.redacted,
                    privacy_caveats: payload_preview.caveats,
                });

                count += 1;
            }
            Some(Err(e)) => {
                return Err(SinexError::service("Error reading DLQ message").with_source(e));
            }
            None => break, // No more messages
        }
    }

    let response = DlqPeekResponse { messages: previews };
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
        return Err(SinexError::validation(
            "Must specify either 'event_id' or 'all: true'",
        ));
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
/// This is a destructive operation that permanently deletes ALL DLQ messages.
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
    let operation_scope = json!({
        "surface": "raw_ingest_dlq",
        "action": "purge",
        "stream": dlq_stream_name,
        "messages_before": messages_before,
        "confirm": true,
    });

    info!(
        actor = %actor,
        messages_to_purge = messages_before,
        "DLQ purge operation initiated"
    );

    // Purge the stream
    if let Err(error) = stream
        .purge()
        .await
        .map_err(|error| SinexError::service("Failed to purge DLQ stream").with_source(error))
    {
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

    let operation_id = log_dlq_operation(
        services,
        "dlq.purge",
        &actor,
        operation_scope,
        OperationStatus::Success,
        format!("purged {messages_before} raw-ingest DLQ message(s)"),
        json!({
            "surface": "raw_ingest_dlq",
            "action": "purge",
            "purged_count": messages_before,
        }),
    )
    .await?;

    let response = DlqPurgeResponse {
        status: "success".to_string(),
        purged_count: messages_before,
        operation_id,
    };
    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::{
        dlq_operator_action, dlq_pending_sequence_span, dlq_pressure_level, dlq_pressure_signal,
        parse_retry_count_header, payload_preview, require_stream_sequence,
    };
    use crate::api::handlers::query::event_card_list_with_policy;
    use crate::event_engine::policy::PolicyEngine;
    use serde_json::json;
    use sinex_db::DbPoolExt;
    use sinex_primitives::error::ErrorClass;
    use sinex_primitives::events::DynamicPayload;
    use sinex_primitives::query::QueryResultEvent;
    use sinex_primitives::views::PrivacyStateKind;
    use sinex_primitives::{Id, RuntimePressureAction, RuntimePressureLevel, SourceMaterial, Uuid};
    use xtask::sandbox::sinex_test;

    #[sinex_test]
    async fn parse_retry_count_header_defaults_when_missing() -> TestResult<()> {
        assert_eq!(parse_retry_count_header(None)?, 0);
        Ok(())
    }

    #[sinex_test]
    async fn parse_retry_count_header_rejects_invalid_value() -> TestResult<()> {
        let mut headers = async_nats::HeaderMap::new();
        headers.insert("Retry-Count", "not-a-number");

        let error = parse_retry_count_header(Some(&headers))
            .expect_err("invalid Retry-Count header should fail honestly");

        assert_eq!(error.error_class(), ErrorClass::DataError);
        assert!(error.to_string().contains("Retry-Count header is invalid"));
        assert!(error.to_string().contains("not-a-number"));
        Ok(())
    }

    #[sinex_test]
    async fn require_stream_sequence_rejects_missing_metadata() -> TestResult<()> {
        let error = require_stream_sequence(Err("missing reply metadata".to_string()))
            .expect_err("missing message metadata must fail honestly");
        assert_eq!(error.error_class(), ErrorClass::TransientInfra);
        assert!(
            error
                .to_string()
                .contains("Failed to inspect DLQ message sequence")
        );
        assert!(error.to_string().contains("missing reply metadata"));
        Ok(())
    }

    #[sinex_test]
    async fn dlq_list_pressure_classifies_empty_warning_and_critical_depth() -> TestResult<()> {
        assert_eq!(dlq_pressure_level(0, 10), RuntimePressureLevel::Nominal);
        assert_eq!(dlq_pressure_level(10, 10), RuntimePressureLevel::Warning);
        assert_eq!(dlq_pressure_level(11, 10), RuntimePressureLevel::Critical);

        Ok(())
    }

    #[sinex_test]
    async fn dlq_list_pressure_reports_sequence_span_and_action() -> TestResult<()> {
        assert_eq!(dlq_pending_sequence_span(0, 4, 9), 0);
        assert_eq!(dlq_pending_sequence_span(2, 4, 9), 6);
        assert_eq!(dlq_pending_sequence_span(2, 9, 4), 0);

        assert_eq!(dlq_operator_action(0), ("none", "raw-ingest DLQ is empty"));
        assert_eq!(
            dlq_operator_action(1),
            (
                "ops dlq peek",
                "inspect failures before running paced requeue or purge"
            )
        );

        Ok(())
    }

    #[sinex_test]
    async fn dlq_pressure_signal_carries_runtime_action_and_batch_limit() -> TestResult<()> {
        let pressure = dlq_pressure_signal(11, 4096, 10);

        assert_eq!(pressure.pressure_level, RuntimePressureLevel::Critical);
        assert_eq!(pressure.runtime_action, RuntimePressureAction::Throttle);
        assert_eq!(pressure.pending_messages, 11);
        assert_eq!(pressure.pending_bytes, 4096);
        assert_eq!(pressure.retry_batch_size, 10);
        assert_eq!(pressure.recommended_action, "ops dlq peek");
        assert!(pressure.reason.contains("paced requeue or purge"));
        Ok(())
    }

    #[sinex_test]
    async fn payload_preview_truncates_without_breaking_unicode(
        ctx: TestContext,
    ) -> TestResult<()> {
        let payload = "żółw".repeat(80);
        let policy = PolicyEngine::noop(ctx.pool().clone());
        let preview = payload_preview(&payload, 200, &policy).await;
        assert!(preview.text.ends_with("..."));
        assert_eq!(preview.text.chars().count(), 203);
        assert!(!preview.redacted);
        assert!(preview.caveats.is_empty());
        Ok(())
    }

    #[sinex_test]
    async fn disclosure_policy_leak_fixture_covers_event_cards_and_dlq(
        ctx: TestContext,
    ) -> TestResult<()> {
        ctx.pool()
            .privacy_policy()
            .add_rule(
                "event-card-command-secret",
                "test field-scoped disclosure policy for rendered event cards",
                "regex",
                r"evt_secret_[A-Za-z0-9_]+",
                false,
                "redact",
                Some("<COMMAND_SECRET>"),
                "default",
            )
            .await?;
        ctx.pool()
            .privacy_policy()
            .bind_field_rule(
                "event-card-command-secret",
                Some("shell.history"),
                Some("command.imported"),
                Some("command"),
                0,
            )
            .await?;
        ctx.pool()
            .privacy_policy()
            .add_rule(
                "dlq-preview-secret",
                "test global disclosure policy for untyped DLQ previews",
                "regex",
                r"dlq_secret_[A-Za-z0-9_]+",
                false,
                "redact",
                Some("<DLQ_SECRET>"),
                "default",
            )
            .await?;
        ctx.pool()
            .privacy_policy()
            .bind_field_rule("dlq-preview-secret", None, None, None, 0)
            .await?;
        let policy = PolicyEngine::load(ctx.pool().clone()).await?;
        let command_token = "evt_secret_alpha123";
        let dlq_token = "dlq_secret_bravo456";
        let command = format!("export COMMAND_SECRET={command_token}");

        let material_id = Id::<SourceMaterial>::from_uuid(Uuid::from_u128(0x1693));
        let event = DynamicPayload::new(
            "shell.history",
            "command.imported",
            json!({ "command": command, "cwd": "/home/sinity/private" }),
        )
        .from_material(material_id)
        .build()?;
        let cards = event_card_list_with_policy(
            &[QueryResultEvent {
                event,
                relevance_score: Some(1.0),
                snippet: Some(format!(
                    "matched command: export COMMAND_SECRET={command_token}"
                )),
            }],
            &policy,
        )
        .await;
        let cards_json = serde_json::to_string(&cards)?;

        assert!(
            !cards_json.contains(command_token),
            "event-card view must not leak the fixture token: {cards_json}"
        );
        assert!(
            cards_json.contains("<COMMAND_SECRET>"),
            "event-card view must show the operator-owned replacement label: {cards_json}"
        );
        assert_eq!(
            cards.cards[0].privacy_state.state,
            PrivacyStateKind::Redacted
        );
        assert!(
            cards.cards[0]
                .caveats
                .iter()
                .any(|caveat| caveat.id == "policy.disclosure_applied"),
            "event-card redaction must be caveated: {:?}",
            cards.cards[0].caveats
        );

        let dlq_payload = format!(
            r#"{{
            "original_subject": "dev.sinex.events.raw.shell.command",
            "original_payload": {{ "command": "export DLQ_SECRET={dlq_token}" }}
        }}"#
        );
        let preview = payload_preview(&dlq_payload, 400, &policy).await;

        assert!(preview.redacted);
        assert!(
            !preview.text.contains(dlq_token),
            "DLQ preview must not leak the fixture token: {}",
            preview.text
        );
        assert!(
            preview.text.contains("<DLQ_SECRET>"),
            "DLQ preview must show the operator-owned replacement label: {}",
            preview.text
        );
        assert!(
            preview
                .caveats
                .iter()
                .any(|caveat| caveat.id == "policy.disclosure_applied"),
            "DLQ redaction must be caveated: {:?}",
            preview.caveats
        );
        assert!(
            preview.caveats.iter().any(|caveat| caveat
                .ref_
                .as_ref()
                .is_some_and(|ref_| ref_.id == "db.dlq-preview-secret")),
            "DLQ redaction must name the operator-owned policy rule: {:?}",
            preview.caveats
        );

        Ok(())
    }

    #[sinex_test]
    async fn payload_preview_redacts_raw_dlq_secret_bytes_by_db_policy(
        ctx: TestContext,
    ) -> TestResult<()> {
        ctx.pool()
            .privacy_policy()
            .add_rule(
                "dlq-preview-secret",
                "test rule",
                "regex",
                r"ghp_[A-Za-z0-9_]+",
                false,
                "redact",
                None,
                "default",
            )
            .await?;
        ctx.pool()
            .privacy_policy()
            .bind_field_rule("dlq-preview-secret", None, None, None, 0)
            .await?;
        let policy = PolicyEngine::load(ctx.pool().clone()).await?;
        let token = ["ghp_", "abcdefghijklmnopqrstuvwxyz123456"].concat();
        let payload = format!(
            r#"{{
            "original_subject": "dev.sinex.events.raw.shell.command",
            "original_payload": {{
                "command": "export GITHUB_TOKEN={token}"
            }}
        }}"#
        );

        let preview = payload_preview(&payload, 200, &policy).await;

        assert!(preview.redacted);
        assert!(
            preview
                .caveats
                .iter()
                .any(|caveat| caveat.id == "policy.disclosure_applied"),
            "redaction must be visible to machine clients: {:?}",
            preview.caveats
        );
        assert!(
            preview.caveats.iter().any(|caveat| caveat
                .ref_
                .as_ref()
                .is_some_and(|ref_| ref_.id == "db.dlq-preview-secret")),
            "machine clients must see which policy owned the redaction: {:?}",
            preview.caveats
        );
        assert!(!preview.text.contains(&token));
        assert!(!preview.text.contains("GITHUB_TOKEN=ghp_"));
        Ok(())
    }
}
