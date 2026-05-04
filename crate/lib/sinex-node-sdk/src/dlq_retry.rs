//! Raw-ingest DLQ retry mechanism
//!
//! This module provides utilities for manually retrying messages from the
//! operator-facing raw-ingest DLQ.

use crate::{NodeResult, SinexError};
use async_nats::jetstream;
use futures::StreamExt;
use serde_json::Value as JsonValue;
use sinex_primitives::{
    environment::SinexEnvironment, temporal, transport, units::Seconds,
    utils::wait_helpers::RetryConfig,
};
use std::time::Duration;
use tracing::{error, info, warn};

// Default DLQ retry configuration values
const DEFAULT_DLQ_CONSUMER_NAME: &str = "dlq-retry-consumer";
const DEFAULT_DLQ_BATCH_SIZE: usize = 10;
const DEFAULT_DLQ_RETRY_DELAY: Seconds = Seconds::from_secs(60);
const DEFAULT_DLQ_ACK_WAIT: Seconds = Seconds::from_secs(60);
const DEFAULT_DLQ_INTER_BATCH_DELAY_MS: u64 = 200;

/// Configuration for DLQ retry operations
#[derive(Debug, Clone)]
pub struct DlqRetryConfig {
    /// DLQ consumer name
    pub consumer_name: String,
    /// Batch size for processing DLQ messages
    pub batch_size: usize,
    /// Core retry parameters (max attempts, delays, jitter, backoff).
    /// The DLQ-specific `max_retries` and `retry_delay` are derived from this.
    pub retry_config: RetryConfig,
    /// Per-message delay in milliseconds to smooth burst republishing within batches.
    /// Prevents downstream spike when an entire batch is republished at once.
    pub per_message_delay_ms: u64,
}

impl DlqRetryConfig {
    /// Maximum number of retry attempts, derived from `retry_config.max_attempts`.
    #[must_use]
    pub fn max_retries(&self) -> u32 {
        self.retry_config.max_attempts.saturating_sub(1)
    }

    /// Delay between retries, derived from `retry_config.max_delay`.
    #[must_use]
    pub fn retry_delay(&self) -> Duration {
        self.retry_config.max_delay
    }
}

/// Default per-message delay (10ms) — smooths burst within a batch without
/// significantly slowing overall throughput (10 msgs × 10ms = 100ms/batch).
const DEFAULT_DLQ_PER_MESSAGE_DELAY_MS: u64 = 10;

impl Default for DlqRetryConfig {
    fn default() -> Self {
        Self {
            consumer_name: DEFAULT_DLQ_CONSUMER_NAME.to_string(),
            batch_size: DEFAULT_DLQ_BATCH_SIZE,
            retry_config: RetryConfig::default(),
            per_message_delay_ms: DEFAULT_DLQ_PER_MESSAGE_DELAY_MS,
        }
    }
}

/// Result of a DLQ retry operation, distinguishing retried from permanently
/// failed messages.
#[derive(Debug, Clone, Default)]
pub struct DlqRetryResult {
    /// Messages successfully republished to their original subject.
    pub retried: usize,
    /// Messages that exceeded `max_retries` and were permanently acked/discarded.
    pub permanently_failed: usize,
    /// Messages where retry failed transiently (NAK'd for redelivery).
    pub transient_failures: usize,
}

/// DLQ retry handler
pub struct DlqRetryHandler {
    nats_client: async_nats::Client,
    env: SinexEnvironment,
    config: DlqRetryConfig,
}

impl DlqRetryHandler {
    /// Create a new DLQ retry handler
    #[must_use]
    pub fn new(
        nats_client: async_nats::Client,
        env: SinexEnvironment,
        config: DlqRetryConfig,
    ) -> Self {
        Self {
            nats_client,
            env,
            config,
        }
    }

    /// Retry all pending messages from DLQ.
    ///
    /// Drains the current backlog (message count at invocation time) then stops.
    /// A 5-second receive timeout acts as a secondary guard against hangs.
    pub async fn retry_all(&self) -> NodeResult<DlqRetryResult> {
        info!("Starting DLQ retry operation");

        let js = jetstream::new(self.nats_client.clone());
        let dlq_stream = self.env.nats_stream_name("SINEX_RAW_EVENTS_DLQ");

        let stream = js
            .get_stream(&dlq_stream)
            .await
            .map_err(|e| SinexError::processing("Failed to get DLQ stream").with_source(e))?;

        // Snapshot pending count to bound the drain. Without this, the loop
        // would run forever if new messages arrive during processing.
        let target_count = stream.cached_info().state.messages;
        if target_count == 0 {
            info!("DLQ is empty, nothing to retry");
            return Ok(DlqRetryResult::default());
        }
        info!("DLQ has {target_count} pending messages to drain");

        let consumer = stream
            .create_consumer(jetstream::consumer::pull::Config {
                name: Some(self.config.consumer_name.clone()),
                durable_name: Some(self.config.consumer_name.clone()),
                filter_subject: self.env.nats_subject("events.dlq.>"),
                ack_policy: jetstream::consumer::AckPolicy::Explicit,
                ack_wait: Duration::from_secs(DEFAULT_DLQ_ACK_WAIT.as_secs()),
                max_ack_pending: self.config.batch_size as i64,
                ..Default::default()
            })
            .await
            .map_err(|e| SinexError::processing("Failed to create DLQ consumer").with_source(e))?;

        let mut messages = consumer
            .messages()
            .await
            .map_err(|e| SinexError::processing("Failed to get DLQ messages").with_source(e))?;

        let mut result = DlqRetryResult::default();
        let mut processed = 0u64;

        while processed < target_count {
            // 5-second receive timeout: break if the stream stalls (e.g.
            // messages were acked by another consumer between our count and now).
            let next = tokio::time::timeout(Duration::from_secs(5), messages.next()).await;
            match next {
                Ok(Some(Ok(msg))) => {
                    if self.handle_dlq_message(&js, &msg).await? {
                        result.retried += 1;
                    } else if dlq_retry_attempts(&msg)? >= self.config.max_retries() {
                        result.permanently_failed += 1;
                    } else {
                        result.transient_failures += 1;
                    }
                    processed += 1;

                    // Per-message delay: smooth burst republishing within a batch
                    if self.config.per_message_delay_ms > 0 {
                        tokio::time::sleep(Duration::from_millis(self.config.per_message_delay_ms))
                            .await;
                    }

                    // Additional inter-batch pause to avoid overwhelming downstream
                    if processed.is_multiple_of(self.config.batch_size as u64) {
                        tokio::time::sleep(Duration::from_millis(DEFAULT_DLQ_INTER_BATCH_DELAY_MS))
                            .await;
                    }
                }
                Ok(Some(Err(e))) => {
                    warn!(
                        error = %e,
                        "Error reading DLQ message; leaving retry accounting unchanged"
                    );
                    tokio::time::sleep(Duration::from_millis(DEFAULT_DLQ_INTER_BATCH_DELAY_MS))
                        .await;
                }
                // Timeout or stream ended — stop draining
                Ok(None) | Err(_) => {
                    info!(
                        "DLQ drain stopped after {processed}/{target_count} messages (stream exhausted or timeout)"
                    );
                    break;
                }
            }
        }

        info!(
            "DLQ retry complete: {} retried, {} permanently failed out of {processed} processed",
            result.retried, result.permanently_failed
        );
        Ok(result)
    }

    /// Retry a specific message by ID
    pub async fn retry_by_id(&self, event_id: &str) -> NodeResult<()> {
        info!("Retrying specific event: {event_id}");

        let js = jetstream::new(self.nats_client.clone());
        let dlq_stream = self.env.nats_stream_name("SINEX_RAW_EVENTS_DLQ");

        let mut stream = js
            .get_stream(&dlq_stream)
            .await
            .map_err(|e| SinexError::processing("Failed to get DLQ stream").with_source(e))?;

        let state = stream
            .info()
            .await
            .map_err(|e| SinexError::processing("Failed to inspect DLQ stream").with_source(e))?
            .state
            .clone();

        if state.messages == 0 || state.last_sequence == 0 {
            return Err(SinexError::processing(format!(
                "Event not found in DLQ: {event_id}"
            )));
        }

        for sequence in state.first_sequence..=state.last_sequence {
            let message = match stream.direct_get(sequence).await {
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
                    return Err(
                        SinexError::processing("Failed to scan DLQ stream for event ID")
                            .with_source(error),
                    );
                }
            };

            let candidate_event_id =
                match dlq_event_id(message.subject.as_str(), &message.headers, &message.payload) {
                    Ok(Some(event_id)) => event_id,
                    Ok(None) => continue,
                    Err(error) => {
                        warn!(
                            subject = %message.subject,
                            error = %error,
                            "Ignoring malformed DLQ event identifier while scanning by ID"
                        );
                        continue;
                    }
                };
            if candidate_event_id != event_id {
                continue;
            }

            let retry_count = dlq_stored_retry_count(&message.headers)?;
            if retry_count >= self.config.max_retries() {
                warn!(
                    event_id,
                    sequence,
                    retry_count,
                    max_retries = self.config.max_retries(),
                    "DLQ event exceeded max retries during direct retry request; permanently failing it instead of requeueing"
                );
                self.permanently_fail_stream_message(&stream, &message)
                    .await?;
                return Err(SinexError::processing(
                    "DLQ event exceeded max retries and was permanently failed",
                )
                .with_context("event_id", event_id.to_string())
                .with_context("retry_count", retry_count.to_string())
                .with_context("max_retries", self.config.max_retries().to_string()));
            }

            self.retry_stream_message(&js, &stream, &message).await?;
            info!(event_id, sequence, "Successfully retried DLQ event by ID");
            return Ok(());
        }

        Err(SinexError::processing(format!(
            "Event not found in DLQ: {event_id}"
        )))
    }

    /// Process a single DLQ message: check retry count, retry or permanently fail.
    /// Returns `true` if the message was successfully retried.
    async fn handle_dlq_message(
        &self,
        js: &jetstream::Context,
        msg: &jetstream::Message,
    ) -> NodeResult<bool> {
        let retry_count = dlq_retry_attempts(msg)?;

        if retry_count >= self.config.max_retries() {
            let subject = &msg.subject;
            warn!(
                subject = %subject,
                retry_count,
                max_retries = self.config.max_retries(),
                "Message exceeded max retries, permanently failing"
            );
            msg.ack().await.map_err(|error| {
                Self::message_settlement_error(
                    "failed to ack permanently failed DLQ message",
                    msg.subject.as_str(),
                    error,
                )
            })?;
            return Ok(false);
        }

        match self.retry_message(js, msg, retry_count).await {
            Ok(()) => {
                msg.ack().await.map_err(|error| {
                    Self::message_settlement_error(
                        "failed to ack retried DLQ message",
                        msg.subject.as_str(),
                        error,
                    )
                })?;
                Ok(true)
            }
            Err(e) => {
                error!("Failed to retry message: {e}");
                msg.ack_with(async_nats::jetstream::AckKind::Nak(Some(
                    self.config.retry_delay(),
                )))
                .await
                .map_err(|nak_err| {
                    Self::message_settlement_error(
                        "failed to NAK DLQ message after retry failure",
                        msg.subject.as_str(),
                        nak_err,
                    )
                    .with_context("retry_error", e.to_string())
                })?;
                Ok(false)
            }
        }
    }

    fn message_settlement_error(
        operation: &'static str,
        subject: &str,
        error: impl std::fmt::Display,
    ) -> SinexError {
        crate::error_helpers::nats_settlement_error(operation, subject, None, error)
    }

    async fn retry_message(
        &self,
        js: &jetstream::Context,
        msg: &jetstream::Message,
        retry_count: u32,
    ) -> NodeResult<()> {
        let headers_ref = msg.headers.as_ref().ok_or_else(|| {
            SinexError::processing("DLQ message is missing retry metadata headers".to_string())
        })?;
        let target = dlq_requeue_target(headers_ref, msg.subject.as_str(), &msg.payload)?;
        let mut headers = async_nats::HeaderMap::new();
        let retry_count_str = (retry_count + 1).to_string();
        let retried_at_str = temporal::format_rfc3339(temporal::now());
        headers.insert("Retry-Count", retry_count_str.as_str());
        headers.insert("Retried-At", retried_at_str.as_str());
        if let Some(event_id) = target.event_id.as_deref() {
            headers.insert("Event-Id", event_id);
        }
        if let Some(msg_id) = target.original_nats_msg_id.as_deref() {
            headers.insert("Nats-Msg-Id", msg_id);
        }
        transport::insert_transport_class_headers(&mut headers, transport::Class::Critical);

        js.publish_with_headers(
            target.original_subject,
            headers,
            target.original_payload.into(),
        )
        .await
        .map_err(|e| SinexError::processing("Failed to republish message").with_source(e))?
        .await
        .map_err(|e| SinexError::processing("Failed to await publish ack").with_source(e))?;

        Ok(())
    }

    async fn retry_stream_message(
        &self,
        js: &jetstream::Context,
        stream: &jetstream::stream::Stream,
        message: &async_nats::jetstream::message::StreamMessage,
    ) -> NodeResult<()> {
        let retry_count = dlq_stored_retry_count(&message.headers)?;
        let target =
            dlq_requeue_target(&message.headers, message.subject.as_str(), &message.payload)?;

        let mut headers = async_nats::HeaderMap::new();
        let retry_count_str = (retry_count + 1).to_string();
        let retried_at_str = temporal::format_rfc3339(temporal::now());
        headers.insert("Retry-Count", retry_count_str.as_str());
        headers.insert("Retried-At", retried_at_str.as_str());
        if let Some(event_id) = target.event_id.as_deref() {
            headers.insert("Event-Id", event_id);
        }
        if let Some(msg_id) = target.original_nats_msg_id.as_deref() {
            headers.insert("Nats-Msg-Id", msg_id);
        }
        transport::insert_transport_class_headers(&mut headers, transport::Class::Critical);

        js.publish_with_headers(
            target.original_subject,
            headers,
            target.original_payload.into(),
        )
        .await
        .map_err(|e| SinexError::processing("Failed to republish message").with_source(e))?
        .await
        .map_err(|e| SinexError::processing("Failed to await publish ack").with_source(e))?;

        self.permanently_fail_stream_message(stream, message)
            .await?;

        Ok(())
    }

    async fn permanently_fail_stream_message(
        &self,
        stream: &jetstream::stream::Stream,
        message: &async_nats::jetstream::message::StreamMessage,
    ) -> NodeResult<()> {
        let deleted = stream.delete_message(message.sequence).await.map_err(|e| {
            SinexError::processing("Failed to delete permanently settled DLQ message")
                .with_source(e)
        })?;
        if !deleted {
            return Err(SinexError::processing(format!(
                "DLQ stream refused to delete permanently settled message sequence {}",
                message.sequence
            )));
        }

        Ok(())
    }

    /// Get DLQ statistics
    pub async fn get_stats(&self) -> NodeResult<DlqStats> {
        let js = jetstream::new(self.nats_client.clone());
        let dlq_stream_name = self.env.nats_stream_name("SINEX_RAW_EVENTS_DLQ");

        let mut stream = js
            .get_stream(&dlq_stream_name)
            .await
            .map_err(|e| SinexError::processing("Failed to get DLQ stream").with_source(e))?;

        let info = stream
            .info()
            .await
            .map_err(|e| SinexError::processing("Failed to get stream info").with_source(e))?;

        Ok(DlqStats {
            total_messages: info.state.messages,
            total_bytes: info.state.bytes,
            first_seq: info.state.first_sequence,
            last_seq: info.state.last_sequence,
        })
    }
}

/// DLQ statistics
#[derive(Debug, Clone)]
pub struct DlqStats {
    pub total_messages: u64,
    pub total_bytes: u64,
    pub first_seq: u64,
    pub last_seq: u64,
}

#[derive(Debug)]
struct DlqRequeueTarget {
    original_subject: String,
    original_payload: Vec<u8>,
    original_nats_msg_id: Option<String>,
    event_id: Option<String>,
}

fn dlq_stored_retry_count(headers: &async_nats::HeaderMap) -> NodeResult<u32> {
    let Some(value) = headers.get("Retry-Count") else {
        return Ok(0);
    };

    value.as_str().parse::<u32>().map_err(|error| {
        SinexError::processing("DLQ Retry-Count header is invalid".to_string())
            .with_context("header", "Retry-Count")
            .with_context("value", value.to_string())
            .with_std_error(&error)
    })
}

fn combine_retry_counts(header_retry: u32, delivery_retry: Result<i64, String>) -> NodeResult<u32> {
    match delivery_retry {
        Ok(delivered) => {
            let delivery_retry = u32::try_from(delivered).map_err(|error| {
                SinexError::processing("DLQ delivery retry count exceeds supported range")
                    .with_context("delivered", delivered.to_string())
                    .with_std_error(&error)
            })?;
            Ok(header_retry.max(delivery_retry))
        }
        Err(error) if header_retry > 0 => {
            warn!(
                stored_retry_count = header_retry,
                error = %error,
                "Failed to inspect JetStream delivery metadata; using stored Retry-Count header"
            );
            Ok(header_retry)
        }
        Err(error) => Err(
            SinexError::processing("Failed to inspect DLQ delivery metadata")
                .with_context("delivery_metadata_error", error),
        ),
    }
}

fn dlq_retry_attempts(msg: &jetstream::Message) -> NodeResult<u32> {
    let header_retry = match msg.headers.as_ref() {
        Some(headers) => dlq_stored_retry_count(headers)?,
        None => 0,
    };
    let delivery_retry = msg
        .info()
        .map(|info| info.delivered.saturating_sub(1))
        .map_err(|error| error.to_string());
    combine_retry_counts(header_retry, delivery_retry)
}

fn dlq_subject_event_id(subject: &str) -> Option<String> {
    let parts: Vec<_> = subject.split('.').collect();
    (parts.len() >= 4).then(|| parts[parts.len() - 1].to_owned())
}

fn dlq_payload_event_id(payload: &[u8]) -> NodeResult<Option<String>> {
    let value = serde_json::from_slice::<JsonValue>(payload).map_err(|error| {
        SinexError::processing("Failed to parse DLQ payload while extracting event ID")
            .with_source(error)
    })?;
    Ok(value
        .get("event_id")
        .and_then(|field| field.as_str())
        .map(ToOwned::to_owned)
        .or_else(|| {
            value
                .get("original_event")
                .and_then(|field| field.get("id"))
                .and_then(|field| field.as_str())
                .map(ToOwned::to_owned)
        })
        .or_else(|| {
            value
                .get("original_payload")
                .and_then(|field| field.get("id"))
                .and_then(|field| field.as_str())
                .map(ToOwned::to_owned)
        }))
}

fn dlq_event_id(
    subject: &str,
    headers: &async_nats::HeaderMap,
    payload: &[u8],
) -> NodeResult<Option<String>> {
    if let Some(event_id) = headers.get("Event-Id") {
        return Ok(Some(event_id.to_string()));
    }

    match dlq_payload_event_id(payload) {
        Ok(Some(event_id)) => Ok(Some(event_id)),
        Ok(None) => Ok(dlq_subject_event_id(subject)),
        Err(error) => {
            if let Some(event_id) = dlq_subject_event_id(subject) {
                warn!(
                    subject,
                    error = %error,
                    "Falling back to DLQ subject event identifier after payload parse failure"
                );
                Ok(Some(event_id))
            } else {
                Err(error.with_context("subject", subject.to_string()))
            }
        }
    }
}

fn dlq_requeue_target(
    headers: &async_nats::HeaderMap,
    subject: &str,
    payload: &[u8],
) -> NodeResult<DlqRequeueTarget> {
    let original_subject = headers
        .get("Original-Subject")
        .map(std::string::ToString::to_string)
        .ok_or_else(|| SinexError::processing("Missing Original-Subject header".to_string()))?;

    let envelope = serde_json::from_slice::<JsonValue>(payload).map_err(|e| {
        SinexError::processing("Failed to parse DLQ payload envelope").with_source(e)
    })?;

    let original_value = envelope
        .get("original_event")
        .or_else(|| envelope.get("original_payload"))
        .ok_or_else(|| {
            SinexError::processing("DLQ payload is missing original event data".to_string())
        })?;

    let original_payload = if let Some(raw_base64) = original_value
        .get("_raw_bytes_base64")
        .and_then(|value| value.as_str())
    {
        base64::Engine::decode(&base64::engine::general_purpose::STANDARD, raw_base64).map_err(
            |e| {
                SinexError::processing("Failed to decode base64 original DLQ payload")
                    .with_source(e)
            },
        )?
    } else {
        serde_json::to_vec(original_value).map_err(|e| {
            SinexError::processing("Failed to re-serialize original DLQ payload").with_source(e)
        })?
    };

    let event_id = headers
        .get("Event-Id")
        .map(std::string::ToString::to_string)
        .or_else(|| {
            envelope
                .get("event_id")
                .and_then(|value| value.as_str())
                .map(ToOwned::to_owned)
        })
        .or_else(|| {
            original_value
                .get("id")
                .and_then(|value| value.as_str())
                .map(ToOwned::to_owned)
        })
        .or_else(|| dlq_subject_event_id(subject));

    let original_nats_msg_id = envelope
        .get("nats_msg_id")
        .and_then(|value| value.as_str())
        .map(ToOwned::to_owned)
        .or_else(|| event_id.clone());

    Ok(DlqRequeueTarget {
        original_subject,
        original_payload,
        original_nats_msg_id,
        event_id,
    })
}

#[cfg(test)]
mod tests {
    use super::{
        DlqRetryHandler, combine_retry_counts, dlq_event_id, dlq_payload_event_id,
        dlq_requeue_target,
    };
    use xtask::sandbox::sinex_test;

    #[sinex_test]
    async fn combine_retry_counts_prefers_larger_delivery_count() -> TestResult<()> {
        let retries = combine_retry_counts(2, Ok(5))?;
        assert_eq!(retries, 5);
        Ok(())
    }

    #[sinex_test]
    async fn combine_retry_counts_uses_stored_header_when_delivery_metadata_is_missing()
    -> TestResult<()> {
        let retries = combine_retry_counts(3, Err("metadata unavailable".to_string()))?;
        assert_eq!(retries, 3);
        Ok(())
    }

    #[sinex_test]
    async fn combine_retry_counts_rejects_missing_delivery_metadata_without_header()
    -> TestResult<()> {
        let error = combine_retry_counts(0, Err("metadata unavailable".to_string()))
            .expect_err("missing delivery metadata without stored retries must fail honestly");
        assert!(
            error
                .to_string()
                .contains("Failed to inspect DLQ delivery metadata")
        );
        assert!(error.to_string().contains("metadata unavailable"));
        Ok(())
    }

    #[sinex_test]
    async fn combine_retry_counts_rejects_delivery_count_overflow() -> TestResult<()> {
        let error = combine_retry_counts(0, Ok(i64::from(u32::MAX) + 1))
            .expect_err("overflowing delivery count must fail honestly");
        assert!(error.to_string().contains("exceeds supported range"));
        Ok(())
    }

    #[sinex_test]
    async fn dlq_payload_event_id_rejects_invalid_json() -> TestResult<()> {
        let error = dlq_payload_event_id(br#"{"event_id":"oops""#)
            .expect_err("invalid DLQ payload JSON must fail honestly");
        assert!(
            error
                .to_string()
                .contains("Failed to parse DLQ payload while extracting event ID")
        );
        Ok(())
    }

    #[sinex_test]
    async fn dlq_event_id_falls_back_to_subject_when_payload_parse_fails() -> TestResult<()> {
        let headers = async_nats::HeaderMap::new();
        let event_id = dlq_event_id(
            "events.dlq.source.00000000-0000-7000-8000-000000000001",
            &headers,
            br#"{"event_id":"oops""#,
        )?;
        assert_eq!(
            event_id.as_deref(),
            Some("00000000-0000-7000-8000-000000000001")
        );
        Ok(())
    }

    #[sinex_test]
    async fn dlq_event_id_rejects_payload_parse_failure_without_subject_fallback() -> TestResult<()>
    {
        let headers = async_nats::HeaderMap::new();
        let error = dlq_event_id("events.dlq", &headers, br#"{"event_id":"oops""#)
            .expect_err("payload parse failure without subject fallback must fail honestly");
        assert!(error.to_string().contains("subject"));
        assert!(
            error
                .to_string()
                .contains("Failed to parse DLQ payload while extracting event ID")
        );
        Ok(())
    }

    #[sinex_test]
    async fn dlq_requeue_target_uses_subject_event_id_fallback() -> TestResult<()> {
        let mut headers = async_nats::HeaderMap::new();
        headers.insert("Original-Subject", "events.raw.shell.command");

        let payload = serde_json::json!({
            "original_payload": {
                "command": "ls"
            }
        });

        let target = dlq_requeue_target(
            &headers,
            "events.dlq.source.00000000-0000-7000-8000-000000000042",
            &serde_json::to_vec(&payload)?,
        )?;
        assert_eq!(
            target.event_id.as_deref(),
            Some("00000000-0000-7000-8000-000000000042")
        );
        assert_eq!(
            target.original_nats_msg_id.as_deref(),
            Some("00000000-0000-7000-8000-000000000042")
        );
        Ok(())
    }

    #[sinex_test]
    async fn dlq_requeue_target_preserves_envelope_event_id_without_reparse() -> TestResult<()> {
        let mut headers = async_nats::HeaderMap::new();
        headers.insert("Original-Subject", "events.raw.shell.command");

        let payload = serde_json::json!({
            "event_id": "00000000-0000-7000-8000-000000000099",
            "original_payload": {
                "command": "pwd"
            }
        });

        let target = dlq_requeue_target(
            &headers,
            "events.dlq.source.ignored-subject-id",
            &serde_json::to_vec(&payload)?,
        )?;
        assert_eq!(
            target.event_id.as_deref(),
            Some("00000000-0000-7000-8000-000000000099")
        );
        Ok(())
    }

    #[sinex_test]
    async fn dlq_message_settlement_error_preserves_subject_context() -> TestResult<()> {
        let error = DlqRetryHandler::message_settlement_error(
            "failed to ack retried DLQ message",
            "events.dlq.test.subject",
            "nats unavailable",
        );

        let message = format!("{error:#}");
        assert!(message.contains("failed to ack retried DLQ message"));
        assert!(message.contains("events.dlq.test.subject"));
        assert!(message.contains("nats unavailable"));
        Ok(())
    }
}
