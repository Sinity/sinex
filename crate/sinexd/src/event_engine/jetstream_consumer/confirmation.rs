//! Confirmation publishing, retry, and durability-gap handling for `JetStreamConsumer`.

use serde::{Deserialize, Serialize};
use sinex_primitives::events::Event;
use std::sync::atomic::Ordering;

use super::*;

/// Confirmation message published to `prod.events.confirmations.<source>.<event_type>`.
///
/// `event_id` is the **high-watermark** event id for this `(source, event_type)`
/// kind — the latest event of this kind that event_engine has persisted. Per #1306,
/// the implied semantics is that all earlier events of the same kind are also
/// confirmed (publish order is monotonic per kind: event_engine publishes only when
/// a fresh max `event_id` is seen for that kind within or across batches).
///
/// Downstream readers that watch confirmations should advance their per-kind
/// high-watermark on each message and treat pending events of that kind with
/// `event_id <= watermark` as confirmed.
#[derive(Debug, Serialize)]
pub(super) struct Confirmation {
    pub(super) event_id: String,
    pub(super) source: String,
    pub(super) event_type: String,
    pub(super) persisted: bool,
    pub(super) ts_ingest: Timestamp,
}

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct ConfirmationRetryRequest {
    pub(super) event_id: String,
    /// Per #1306: confirmations are published per `(source, event_type)`
    /// watermark, not per event id. The retry path needs the kind to
    /// reconstruct the correct subject.
    pub(super) source: String,
    pub(super) event_type: String,
}

pub(super) fn disclosure_safe_fingerprint(value: &str) -> String {
    let hash = blake3::hash(value.as_bytes()).to_hex();
    format!("len={} blake3={}", value.len(), &hash[..16])
}

pub(super) const CONFIRM_PUBLISH_MAX_ATTEMPTS: usize = 3;
pub(super) const CONFIRM_PUBLISH_BACKOFF_BASE: Duration = Duration::from_millis(200);
pub(super) const CONFIRM_PUBLISH_BACKOFF_MAX: Duration = Duration::from_secs(2);
pub(super) const CONFIRM_PUBLISH_CONCURRENCY: usize = 50;
pub(super) const CONFIRM_RETRY_DELAY: Duration = Duration::from_secs(1);
pub(super) const CONFIRM_RETRY_POLL_INTERVAL: Duration = Duration::from_secs(1);
pub(super) const CONFIRM_RETRY_BATCH_MAX_MESSAGES: usize = 32;
pub(super) const CONFIRM_RETRY_BATCH_TIMEOUT: Duration = Duration::from_millis(100);
pub(super) const ERROR_CLASS_CONFIRMATION_DURABILITY_GAP: &str = "confirmation_durability_gap";

/// Diagnostic context value attached to errors and log fields that arise from the split-retry
/// persistence path. The value `"per_successful_persistence_attempt"` signals that atomicity is
/// scoped to each individual sub-batch attempt, not the enclosing pull-batch: a pull-batch may
/// be partially committed if one sub-batch succeeds before a sibling fails.
pub(super) const BATCH_ATOMICITY_SCOPE: &str = "per_successful_persistence_attempt";

impl JetStreamConsumer {
    pub(super) fn is_fatal_batch_processing_error(err: &SinexError) -> bool {
        err.context_map()
            .get("error_class")
            .is_some_and(|value| value == ERROR_CLASS_CONFIRMATION_DURABILITY_GAP)
    }

    pub(super) fn confirmation_durability_gap_error(
        errors: Vec<(Uuid, SinexError)>,
        acked_count: usize,
    ) -> SinexError {
        let Err(combined) =
            Self::collapse_settlement_errors("post-persist confirmation durability", errors)
        else {
            unreachable!("confirmation durability gap requires at least one event");
        };

        combined
            .with_context("error_class", ERROR_CLASS_CONFIRMATION_DURABILITY_GAP)
            .with_context("acked_event_count", acked_count.to_string())
            .with_context("batch_atomicity", BATCH_ATOMICITY_SCOPE)
            .with_context("raw_message_settlement", "left_unacked_for_redelivery")
            .with_context(
                "terminal_state",
                "database commit landed but confirmation durability was not established",
            )
            .with_context(
                "recovery",
                "shut down the consumer and let JetStream redeliver unsettled raw messages once confirmation transport recovers",
            )
    }

    /// Publish a per-kind confirmation watermark.
    ///
    /// The subject is `prod.events.confirmations.<source>.<event_type>` and the
    /// payload's `event_id` is the high-watermark — the latest event of this
    /// kind we have persisted. With `max_messages_per_subject = 1` on the
    /// stream, this acts as real compaction (one entry per kind). Downstream
    /// readers advance their per-kind watermark and treat earlier events of
    /// the same kind as confirmed. Per #1306.
    pub(super) async fn publish_confirmation(
        &self,
        event_id: &Uuid,
        source: &str,
        event_type: &str,
    ) -> EventEngineResult<()> {
        #[cfg(any(test, feature = "testing"))]
        if let Some(failures) = &self.confirmation_failures_remaining
            && failures.load(Ordering::SeqCst) > 0
        {
            failures.fetch_sub(1, Ordering::SeqCst);
            return Err(SinexError::network("forced confirmation publish failure"));
        }

        let event_id_str = event_id.to_string();
        let confirmation = Confirmation {
            event_id: event_id_str.clone(),
            source: source.to_string(),
            event_type: event_type.to_string(),
            persisted: true,
            ts_ingest: Timestamp::now(),
        };

        let subject = format!(
            "{}{}.{}",
            self.topology.confirmations_prefix, source, event_type
        );
        let payload = serde_json::to_vec(&confirmation)?;

        // transport::Class::Confirmation — best-effort ACK signal; failure
        // routes to the durable retry queue then durability-gap warn (not DLQ).
        // Nats-Msg-Id is per-watermark (event_id) so duplicate publishes of the
        // same watermark within the dedup window are coalesced server-side.
        let mut headers = async_nats::HeaderMap::new();
        headers.insert("Nats-Msg-Id", event_id_str.as_str());
        transport::insert_transport_class_headers(&mut headers, transport::Class::Confirmation);
        ensure_nats_payload_fits("confirmation watermark", &subject, payload.len())?;

        self.js
            .publish_with_headers(subject, headers, payload.into())
            .await
            .map_err(|e| SinexError::network("Failed to publish confirmation").with_source(e))?
            .await
            .map_err(|e| SinexError::network("Confirmation ack failed").with_source(e))?;

        debug!(event_id = %event_id, source = %source, event_type = %event_type, "Published confirmation watermark");
        Ok(())
    }

    /// Publish a FINAL persisted+redacted event onto the confirmed-events stream.
    ///
    /// Subject:
    /// `{env}.events.confirmed.<provenance>.<encoded-source>.<encoded-event-type>`.
    /// The body is the
    /// full `Event<JsonValue>` exactly as persisted (post-redaction). Downstream
    /// consumers (the shared in-process automaton dispatcher and the SSE bus)
    /// deserialize it directly — no Postgres refetch, no provisional buffer, no
    /// commit/confirmation visibility race. `Nats-Msg-Id` is the event id so a
    /// duplicate publish of the same event (NATS at-least-once redelivery of the
    /// raw message) is coalesced server-side within the dedup window.
    pub(super) async fn publish_confirmed_event(
        &self,
        event: &Event<JsonValue>,
    ) -> EventEngineResult<()> {
        let Some(event_id) = event.id else {
            return Err(SinexError::processing(
                "Cannot publish confirmed event without an id",
            ));
        };
        let source = event.source.as_str();
        let event_type = event.event_type.as_str();
        let provenance = if event.is_synthesized_event() {
            "synthesized"
        } else {
            "material"
        };
        let subject = format!(
            "{}{}.{}.{}",
            self.topology.confirmed_events_prefix,
            provenance,
            sinex_primitives::environment::SinexEnvironment::nats_subject_token(source),
            sinex_primitives::environment::SinexEnvironment::nats_subject_token(event_type),
        );
        let payload = serde_json::to_vec(event)?;

        let event_id_str = event_id.to_string();
        let mut headers = async_nats::HeaderMap::new();
        headers.insert("Nats-Msg-Id", event_id_str.as_str());
        transport::insert_transport_class_headers(&mut headers, transport::Class::Confirmation);
        ensure_nats_payload_fits("confirmed event", &subject, payload.len())?;

        self.js
            .publish_with_headers(subject, headers, payload.into())
            .await
            .map_err(|e| SinexError::network("Failed to publish confirmed event").with_source(e))?
            .await
            .map_err(|e| SinexError::network("Confirmed-event ack failed").with_source(e))?;

        debug!(event_id = %event_id, source = %source, event_type = %event_type, "Published confirmed event");
        Ok(())
    }

    /// Build a per-event durability-gap error for a confirmed-event publish that
    /// failed after retries. Collapsed into the batch-level
    /// `confirmation_durability_gap_error`, which stamps the fatal error class so
    /// the consumer halts and JetStream redelivers the unsettled raw message.
    pub(super) fn confirmed_event_durability_gap_error(
        event_id: Uuid,
        source_err: &SinexError,
    ) -> SinexError {
        SinexError::network("Persisted event could not be published to the confirmed-events stream")
            .with_context("event_id", event_id.to_string())
            .with_context("confirmed_publish_error", source_err.to_string())
    }

    /// Publish a confirmed event, retrying transient transport failures with
    /// bounded backoff. On final failure the caller routes the raw message
    /// through the durability-gap path (leave unsettled for JetStream
    /// redelivery) rather than acking it — otherwise the event would be silently
    /// lost from the confirmed-events stream that automata and the SSE bus read.
    pub(super) async fn publish_confirmed_event_with_retry(
        &self,
        event: &Event<JsonValue>,
    ) -> EventEngineResult<()> {
        let mut backoff = CONFIRM_PUBLISH_BACKOFF_BASE;
        let mut last_error: Option<SinexError> = None;

        for attempt in 1..=CONFIRM_PUBLISH_MAX_ATTEMPTS {
            match self.publish_confirmed_event(event).await {
                Ok(()) => return Ok(()),
                Err(err) => {
                    warn!(
                        attempt,
                        event_id = ?event.id,
                        error = %err,
                        "Confirmed-event publish attempt failed"
                    );
                    last_error = Some(err);
                }
            }

            if attempt < CONFIRM_PUBLISH_MAX_ATTEMPTS {
                tokio::time::sleep(backoff).await;
                backoff = std::cmp::min(backoff.saturating_mul(2), CONFIRM_PUBLISH_BACKOFF_MAX);
            }
        }

        Err(last_error.unwrap_or_else(|| {
            SinexError::network("Failed to publish confirmed event after retries")
        }))
    }

    pub(super) async fn publish_confirmation_with_retry(
        &self,
        event_id: &Uuid,
        source: &str,
        event_type: &str,
    ) -> EventEngineResult<()> {
        let mut backoff = CONFIRM_PUBLISH_BACKOFF_BASE;
        let mut last_error: Option<SinexError> = None;

        for attempt in 1..=CONFIRM_PUBLISH_MAX_ATTEMPTS {
            match self
                .publish_confirmation(event_id, source, event_type)
                .await
            {
                Ok(()) => return Ok(()),
                Err(err) => {
                    warn!(
                        attempt,
                        event_id = %event_id,
                        source = %source,
                        event_type = %event_type,
                        error = %err,
                        "Confirmation publish attempt failed"
                    );
                    last_error = Some(err);
                }
            }

            if attempt < CONFIRM_PUBLISH_MAX_ATTEMPTS {
                tokio::time::sleep(backoff).await;
                backoff = std::cmp::min(backoff.saturating_mul(2), CONFIRM_PUBLISH_BACKOFF_MAX);
            }
        }

        Err(last_error
            .unwrap_or_else(|| SinexError::network("Failed to publish confirmation after retries")))
    }

    pub(super) async fn enqueue_confirmation_retry(
        &self,
        event_id: &Uuid,
        source: &str,
        event_type: &str,
    ) -> EventEngineResult<()> {
        let event_id_str = event_id.to_string();
        let subject = format!(
            "{}{}",
            self.topology.confirmation_retry_prefix, event_id_str
        );
        let payload = serde_json::to_vec(&ConfirmationRetryRequest {
            event_id: event_id_str.clone(),
            source: source.to_string(),
            event_type: event_type.to_string(),
        })?;

        let mut headers = async_nats::HeaderMap::new();
        let retry_msg_id = format!("confirm-retry.{event_id_str}");
        headers.insert("Nats-Msg-Id", retry_msg_id.as_str());
        transport::insert_transport_class_headers(&mut headers, transport::Class::Confirmation);
        ensure_nats_payload_fits("confirmation retry request", &subject, payload.len())?;

        self.js
            .publish_with_headers(subject, headers, payload.into())
            .await
            .map_err(|e| {
                SinexError::network("Failed to enqueue confirmation retry").with_source(e)
            })?
            .await
            .map_err(|e| {
                SinexError::network("Confirmation retry enqueue ack failed").with_source(e)
            })?;

        Ok(())
    }

    pub(super) async fn process_confirmation_retry_batch(
        &self,
        consumer: &jetstream::consumer::Consumer<jetstream::consumer::pull::Config>,
    ) -> EventEngineResult<()> {
        let messages = pull_batch(
            consumer,
            CONFIRM_RETRY_BATCH_MAX_MESSAGES,
            CONFIRM_RETRY_BATCH_TIMEOUT,
        )
        .await
        .map_err(|e| {
            SinexError::network("Failed to fetch confirmation retry messages").with_source(e)
        })?;

        for message in messages {
            let retry = match serde_json::from_slice::<ConfirmationRetryRequest>(&message.payload) {
                Ok(retry) => retry,
                Err(err) => {
                    warn!(
                        error = %err,
                        "Failed to parse confirmation retry payload; acknowledging corrupt retry message"
                    );
                    if let Err(ack_err) = message.ack().await {
                        warn!(error = %ack_err, "Failed to ack corrupt confirmation retry message");
                        self.stats.nack_failures.fetch_add(1, Ordering::Relaxed);
                    }
                    continue;
                }
            };

            let event_id = match Uuid::parse_str(&retry.event_id) {
                Ok(event_id) => event_id,
                Err(err) => {
                    warn!(
                        event_id_fingerprint = %disclosure_safe_fingerprint(&retry.event_id),
                        error = %err,
                        "Confirmation retry payload contained an invalid event id; acknowledging corrupt retry message"
                    );
                    if let Err(ack_err) = message.ack().await {
                        warn!(error = %ack_err, "Failed to ack invalid confirmation retry message");
                        self.stats.nack_failures.fetch_add(1, Ordering::Relaxed);
                    }
                    continue;
                }
            };

            match self
                .publish_confirmation_with_retry(&event_id, &retry.source, &retry.event_type)
                .await
            {
                Ok(()) => {
                    if let Err(err) = message.ack().await {
                        return Err(SinexError::network(format!(
                            "Failed to ack confirmation retry message: {err}"
                        )));
                    }
                }
                Err(err) => {
                    warn!(
                        event_id = %event_id,
                        error = %err,
                        "Failed to publish confirmation from durable retry queue"
                    );
                    self.stats
                        .confirmation_retry_failures
                        .fetch_add(1, Ordering::Relaxed);
                    if let Some(ref handle) = self.heartbeat_handle {
                        handle.record_error("confirmation retry failure");
                    }
                    if let Err(nak_err) = message
                        .ack_with(jetstream::AckKind::Nak(Some(CONFIRM_RETRY_DELAY)))
                        .await
                    {
                        return Err(SinexError::network(format!(
                            "Failed to NAK confirmation retry message: {nak_err}"
                        )));
                    }
                }
            }
        }

        Ok(())
    }
}
