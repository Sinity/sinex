//! Dead-letter routing and payload identity helpers for `JetStreamConsumer`.

use serde::Serialize;
use std::sync::atomic::Ordering;

use super::*;

#[derive(Debug, Serialize)]
pub(super) struct DlqEntry {
    /// NATS Msg-Id header value (not a Sinex event `UUIDv7`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) nats_msg_id: Option<String>,
    pub(super) error: String,
    pub(super) original_payload: JsonValue,
    pub(super) failed_at: Timestamp,
}

pub(super) const DLQ_PUBLISH_MAX_ATTEMPTS: usize = 3;
pub(super) const DLQ_PUBLISH_BACKOFF_BASE: Duration = Duration::from_millis(200);
pub(super) const DLQ_PUBLISH_BACKOFF_MAX: Duration = Duration::from_secs(2);
pub(super) const DLQ_DUPLICATE_WINDOW: Duration = Duration::from_hours(1);
pub(super) const DLQ_RETRY_DELAY: Duration = Duration::from_secs(1);

/// Extract the failed event's id from a raw-ingress payload.
///
/// Durable ingress carries an [`EventIntent`] envelope (#1149) whose events live
/// under `events[]`, so the id is `events[0].id`; legacy/escape-hatch flat events
/// carry a top-level `id`. DLQ dedupe identity and the `Event-Id` header derive
/// from this, so both formats must resolve.
pub(super) fn dlq_event_id(payload: &JsonValue) -> Option<String> {
    payload
        .get("id")
        .and_then(|value| value.as_str())
        .or_else(|| {
            payload
                .get("events")
                .and_then(|events| events.as_array())
                .and_then(|events| events.first())
                .and_then(|event| event.get("id"))
                .and_then(|value| value.as_str())
        })
        .map(str::to_owned)
}

pub(super) fn dlq_publish_msg_id(
    msg: &jetstream::Message,
    original_nats_msg_id: Option<&str>,
    original_payload: &JsonValue,
) -> String {
    if let Some(event_id) = dlq_event_id(original_payload) {
        return format!("dlq.{event_id}");
    }

    if let Some(original_id) = original_nats_msg_id {
        format!("dlq.msg.{original_id}")
    } else {
        let mut hasher = blake3::Hasher::new();
        hasher.update(msg.subject.as_str().as_bytes());
        hasher.update(&msg.payload);
        format!("dlq.hash.{}", hasher.finalize().to_hex())
    }
}

impl JetStreamConsumer {
    /// Route failed message to DLQ and return Ok(()) on success.
    ///
    /// Errors indicate the DLQ publish itself failed after all retries. The caller
    /// is responsible for deciding whether to NAK the original message in that case.
    #[tracing::instrument(skip(self, msg), fields(error = %error))]
    pub(super) async fn route_to_dlq(
        &self,
        msg: &jetstream::Message,
        error: String,
    ) -> EventEngineResult<()> {
        let original_nats_msg_id = msg
            .headers
            .as_ref()
            .and_then(|h| h.get("Nats-Msg-Id"))
            .map(|v| v.as_str().to_string());

        let original_payload = match serde_json::from_slice(&msg.payload) {
            Ok(json) => json,
            Err(parse_err) => {
                warn!(
                    error = %parse_err,
                    payload_len = msg.payload.len(),
                    "Failed to parse original payload for DLQ entry; preserving raw bytes as base64"
                );
                // ── DLQ raw-bytes scrub (#1042 Slice 4) ─────────────────────────
                // The `_raw_bytes_base64` field carries the unparsed raw bytes. For
                // non-Public sources these bytes may contain unredacted sensitive
                // content. We conservatively suppress the raw field and replace it
                // with a metadata-only stub so raw payloads never persist in the DLQ.
                serde_json::json!({
                    "_parse_error": parse_err.to_string(),
                    "_raw_bytes_suppressed": true,
                    "_raw_bytes_len": msg.payload.len(),
                    "_dlq_note": "raw payload suppressed by privacy chokepoint (#1042)"
                })
            }
        };

        // ── DLQ payload redaction (#1042 Slice 4) ────────────────────────────
        // Apply policy redaction to the parsed DLQ payload before storing it.
        // Uses the global (NULL source/type) scope for conservative coverage.
        // On error, the already-parsed JSON remains (no raw bytes risk here since
        // the parse succeeded — structured fields are at least partially safe).
        let original_payload = self.policy_engine.redact_json_value(original_payload).await;
        // ── End DLQ redaction ────────────────────────────────────────────────

        let dlq_publish_msg_id =
            dlq_publish_msg_id(msg, original_nats_msg_id.as_deref(), &original_payload);
        let original_event_id = dlq_event_id(&original_payload);

        let dlq_entry = DlqEntry {
            nats_msg_id: original_nats_msg_id,
            error,
            original_payload,
            failed_at: Timestamp::now(),
        };

        let payload = serde_json::to_vec(&dlq_entry).map_err(|e| {
            SinexError::serialization(format!("Failed to serialize DLQ entry: {e}"))
        })?;
        let mut headers = async_nats::HeaderMap::new();
        headers.insert("Nats-Msg-Id", dlq_publish_msg_id.as_str());
        headers.insert("Original-Subject", msg.subject.as_str());
        headers.insert("Retry-Count", "0");
        insert_traffic_class_header(&mut headers, NatsTrafficClass::RawIngestDlq);
        transport::insert_semantic_transport_class_header(&mut headers, transport::Class::Critical);
        if let Some(event_id) = original_event_id.as_deref() {
            headers.insert("Event-Id", event_id);
        }

        let mut backoff = DLQ_PUBLISH_BACKOFF_BASE;
        let mut last_error: Option<SinexError> = None;
        for attempt in 1..=DLQ_PUBLISH_MAX_ATTEMPTS {
            match self
                .js
                .publish_with_headers(
                    self.topology.dlq_publish_subject.clone(),
                    headers.clone(),
                    payload.clone().into(),
                )
                .await
            {
                Ok(ack) => match ack.await {
                    Ok(_) => {
                        debug!(nats_msg_id = ?dlq_entry.nats_msg_id, "Routed to DLQ");
                        return Ok(());
                    }
                    Err(err) => {
                        error!(
                            target: "sinex_metrics",
                            metric = "event_engine.dlq_confirm_failures_total",
                            attempt,
                            error = %err,
                            "Failed to confirm DLQ publish"
                        );
                        last_error =
                            Some(SinexError::network("DLQ publish ack failed").with_source(err));
                    }
                },
                Err(err) => {
                    error!(
                        target: "sinex_metrics",
                        metric = "event_engine.dlq_routing_failures_total",
                        attempt,
                        error = %err,
                        "Failed to route to DLQ"
                    );
                    last_error = Some(SinexError::network("DLQ publish failed").with_source(err));
                }
            }

            if attempt < DLQ_PUBLISH_MAX_ATTEMPTS {
                tokio::time::sleep(backoff).await;
                backoff = std::cmp::min(backoff.saturating_mul(2), DLQ_PUBLISH_BACKOFF_MAX);
            }
        }

        Err(last_error
            .unwrap_or_else(|| SinexError::network("Failed to route to DLQ after retries")))
    }

    pub(super) async fn route_to_dlq_and_ack(
        &self,
        msg: &jetstream::Message,
        error: String,
    ) -> EventEngineResult<()> {
        let dlq_error = error.clone();
        match self.route_to_dlq(msg, error).await {
            Ok(()) => {
                msg.ack().await.map_err(|e| {
                    SinexError::network("Failed to ack after DLQ route").with_source(e)
                })?;
                self.stats.dlq_routed.fetch_add(1, Ordering::Relaxed);
            }
            Err(e) => {
                warn!(error = %e, "Failed to route to DLQ after retries; NAKing for retry");
                self.stats
                    .dlq_publish_failures
                    .fetch_add(1, Ordering::Relaxed);
                msg.ack_with(jetstream::AckKind::Nak(Some(DLQ_RETRY_DELAY)))
                    .await
                    .map_err(|nak_err| {
                        self.stats.nack_failures.fetch_add(1, Ordering::Relaxed);
                        SinexError::network("Failed to NAK after DLQ publish failure")
                            .with_context("dlq_error", dlq_error.clone())
                            .with_source(nak_err.to_string())
                    })?;
            }
        }
        Ok(())
    }
}
