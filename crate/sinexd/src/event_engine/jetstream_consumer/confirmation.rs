//! Confirmed-event publishing and durability-gap handling for `JetStreamConsumer`.

use sinex_primitives::events::Event;
use std::sync::atomic::Ordering;

use super::*;

pub(super) const CONFIRM_PUBLISH_MAX_ATTEMPTS: usize = 3;
pub(super) const CONFIRM_PUBLISH_BACKOFF_BASE: Duration = Duration::from_millis(200);
pub(super) const CONFIRM_PUBLISH_BACKOFF_MAX: Duration = Duration::from_secs(2);
pub(super) const CONFIRM_PUBLISH_CONCURRENCY: usize = 50;
pub(super) const ERROR_CLASS_CONFIRMATION_DURABILITY_GAP: &str = "confirmation_durability_gap";

/// Diagnostic context value attached to errors and log fields that arise from the split-retry
/// persistence path. The value `"per_successful_persistence_attempt"` signals that atomicity is
/// scoped to each individual sub-batch attempt, not the enclosing pull-batch: a pull-batch may
/// be partially committed if one sub-batch succeeds before a sibling fails.
pub(super) const BATCH_ATOMICITY_SCOPE: &str = "per_successful_persistence_attempt";

#[cfg(test)]
pub(super) fn disclosure_safe_fingerprint(value: &str) -> String {
    let hash = blake3::hash(value.as_bytes()).to_hex();
    format!("len={} blake3={}", value.len(), &hash[..16])
}

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
                "database commit landed but confirmed-event durability was not established",
            )
            .with_context(
                "recovery",
                "shut down the consumer and let JetStream redeliver unsettled raw messages once confirmed-event transport recovers",
            )
    }

    /// Publish a FINAL persisted+redacted event onto the confirmed-events stream.
    ///
    /// Subject:
    /// `{env}.events.confirmed.<provenance>.<encoded-source>.<encoded-event-type>`.
    /// The body is the full `Event<JsonValue>` exactly as persisted
    /// (post-redaction). Downstream consumers deserialize it directly: no
    /// provisional buffer, no watermark, and no Postgres refetch.
    pub(super) async fn publish_confirmed_event(
        &self,
        event: &Event<JsonValue>,
    ) -> EventEngineResult<()> {
        #[cfg(any(test, feature = "testing"))]
        if let Some(failures) = &self.confirmation_failures_remaining
            && failures.load(Ordering::SeqCst) > 0
        {
            failures.fetch_sub(1, Ordering::SeqCst);
            return Err(SinexError::network("forced confirmed-event publish failure"));
        }

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
    /// through the durability-gap path instead of acking it.
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
}
