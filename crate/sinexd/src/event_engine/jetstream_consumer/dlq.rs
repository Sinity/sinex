//! Dead-letter routing and payload identity helpers for `JetStreamConsumer`.

use serde::Serialize;

use crate::event_engine::durable_failure::{DURABLE_FAILURE_ID_HEADER, persist_failure_evidence};

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
pub(super) const DLQ_REQUEUE_GENERATION_HEADER: &str = "Dlq-Requeue-Generation";

/// Extract the failed event's id from a raw-ingress payload.
///
/// Durable ingress carries an [`EventIntent`] envelope (#1149) whose events live
/// under `events[]`, so the id is `events[0].id`; legacy/escape-hatch flat events
/// carry a top-level `id`. This is the operator-facing primary id used in the
/// `Event-Id` header; multi-event intent dedupe uses every child below.
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

fn dlq_intent_identity(original_payload: &JsonValue) -> Option<String> {
    let events = original_payload.get("events")?.as_array()?;
    if events.len() < 2 {
        return None;
    }

    // Keep the full ordered child identity in the dedupe key. A first-child
    // key merges [X, Y] and [X, Z] inside JetStream's dupeWindow, which turns
    // an acknowledged DLQ publish into silent loss of Z. Include the complete
    // child JSON when an invalid envelope has no usable id: malformed intents
    // are still distinct recoverable failures and must not fall back to the
    // first-child-only operator id.
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"sinex.dlq.intent.v1\0");
    for event in events {
        if let Some(id) = event.get("id").and_then(JsonValue::as_str) {
            hasher.update(b"id\0");
            hasher.update(id.as_bytes());
        } else {
            hasher.update(b"json\0");
            let child = serde_json::to_vec(event).ok()?;
            hasher.update(&child);
        }
        hasher.update(&[0]);
    }
    Some(format!("dlq.intent.{}", hasher.finalize().to_hex()))
}

fn dlq_requeue_generation(headers: Option<&async_nats::HeaderMap>) -> EventEngineResult<u32> {
    let Some(value) = headers.and_then(|headers| headers.get(DLQ_REQUEUE_GENERATION_HEADER)) else {
        return Ok(0);
    };

    value.as_str().parse::<u32>().map_err(|error| {
        SinexError::processing("Invalid DLQ requeue generation header")
            .with_context("header", DLQ_REQUEUE_GENERATION_HEADER)
            .with_context("value", value.to_string())
            .with_std_error(&error)
    })
}

pub(super) fn dlq_publish_msg_id(
    msg: &jetstream::Message,
    original_nats_msg_id: Option<&str>,
    original_payload: &JsonValue,
) -> EventEngineResult<String> {
    let base = dlq_intent_identity(original_payload)
        .or_else(|| dlq_event_id(original_payload).map(|event_id| format!("dlq.{event_id}")))
        .or_else(|| original_nats_msg_id.map(|original_id| format!("dlq.msg.{original_id}")))
        .unwrap_or_else(|| {
            let mut hasher = blake3::Hasher::new();
            hasher.update(msg.subject.as_str().as_bytes());
            hasher.update(&msg.payload);
            format!("dlq.hash.{}", hasher.finalize().to_hex())
        });
    let generation = dlq_requeue_generation(msg.headers.as_ref())?;

    if generation == 0 {
        Ok(base)
    } else {
        Ok(format!("{base}.requeue.{generation}"))
    }
}

impl JetStreamConsumer {
    /// Route failed message to DLQ and return its durable evidence row ID.
    ///
    /// Errors indicate that evidence persistence or the DLQ publish failed. The
    /// caller is responsible for deciding whether to NAK the original message.
    #[tracing::instrument(skip(self, msg), fields(error = %error))]
    pub(super) async fn route_to_dlq(
        &self,
        msg: &jetstream::Message,
        error: String,
    ) -> EventEngineResult<Uuid> {
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
            dlq_publish_msg_id(msg, original_nats_msg_id.as_deref(), &original_payload)?;
        let requeue_generation = dlq_requeue_generation(msg.headers.as_ref())?;
        let original_event_id = dlq_event_id(&original_payload);

        // JetStream is only a bounded delivery bus. Write the operator-visible
        // witness before publishing to it so a later ACK cannot outrun the
        // evidence that justifies progress. The payload is already behind the
        // admission privacy chokepoint above.
        let failed_event_id = original_event_id
            .as_deref()
            .and_then(|value| value.parse::<Uuid>().ok())
            .unwrap_or_else(Uuid::now_v7);
        let event_type = dlq_payload_field(&original_payload, "event_type")
            .unwrap_or_else(|| "raw_ingest".to_string());
        let source = dlq_payload_field(&original_payload, "source")
            .unwrap_or_else(|| msg.subject.to_string());
        let retry_count = msg
            .info()
            .ok()
            .map(|info| info.delivered.clamp(0, i64::from(i32::MAX)) as i32)
            .unwrap_or(0);
        let durable_failure_id = persist_failure_evidence(
            &self.pool,
            failed_event_id,
            "event-engine.raw-ingest",
            &source,
            &event_type,
            "permanent",
            &error,
            original_payload.clone(),
            serde_json::json!({
                "original_subject": msg.subject.as_str(),
                "original_nats_msg_id": original_nats_msg_id,
                "durability_source": "postgres_pre_dlq_settlement",
            }),
            retry_count,
        )
        .await?;

        let mut dlq_entry = DlqEntry {
            nats_msg_id: original_nats_msg_id,
            error,
            original_payload,
            failed_at: Timestamp::now(),
        };

        let mut payload = serde_json::to_vec(&dlq_entry).map_err(|e| {
            SinexError::serialization(format!("Failed to serialize DLQ entry: {e}"))
        })?;
        let mut headers = async_nats::HeaderMap::new();
        headers.insert("Nats-Msg-Id", dlq_publish_msg_id.as_str());
        let requeue_generation = requeue_generation.to_string();
        headers.insert(DLQ_REQUEUE_GENERATION_HEADER, requeue_generation.as_str());
        headers.insert("Original-Subject", msg.subject.as_str());
        headers.insert("Retry-Count", "0");
        let durable_failure_id_header = durable_failure_id.to_string();
        headers.insert(
            DURABLE_FAILURE_ID_HEADER,
            durable_failure_id_header.as_str(),
        );
        insert_traffic_class_header(&mut headers, NatsTrafficClass::RawIngestDlq);
        transport::insert_semantic_transport_class_header(&mut headers, transport::Class::Critical);
        if let Some(event_id) = original_event_id.as_deref() {
            headers.insert("Event-Id", event_id);
        }
        if let Err(error) = ensure_nats_payload_fits(
            "event-engine DLQ entry",
            &self.topology.dlq_publish_subject,
            payload.len(),
        ) {
            warn!(
                error = %error,
                payload_len = payload.len(),
                original_payload_len = msg.payload.len(),
                "DLQ envelope exceeds publish budget; replacing stored original payload with metadata stub"
            );
            dlq_entry.original_payload = serde_json::json!({
                "_dlq_note": "original payload omitted because DLQ envelope exceeded NATS publish budget",
                "_original_payload_omitted": true,
                "_original_payload_len": msg.payload.len(),
                "_original_subject": msg.subject.as_str(),
                "_original_event_id": original_event_id,
            });
            payload = serde_json::to_vec(&dlq_entry).map_err(|e| {
                SinexError::serialization(format!("Failed to serialize compact DLQ entry: {e}"))
            })?;
            ensure_nats_payload_fits(
                "event-engine compact DLQ entry",
                &self.topology.dlq_publish_subject,
                payload.len(),
            )?;
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
                        return Ok(durable_failure_id);
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

    // route_to_dlq_and_ack was removed (sinex-r6d.12): it acked the raw
    // message directly, which is exactly the unilateral per-child settlement
    // this bead eliminated. Every caller now calls route_to_dlq above and reports
    // the outcome to the message's shared
    // RawEnvelopeSettlement via settle_child instead.
}

fn dlq_payload_field(payload: &JsonValue, key: &str) -> Option<String> {
    payload
        .get(key)
        .and_then(JsonValue::as_str)
        .or_else(|| {
            payload
                .get("events")
                .and_then(JsonValue::as_array)
                .and_then(|events| events.first())
                .and_then(|event| event.get(key))
                .and_then(JsonValue::as_str)
        })
        .map(ToOwned::to_owned)
}

#[cfg(test)]
#[path = "dlq_test.rs"]
mod tests;
