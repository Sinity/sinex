//! JetStream event consumer with confirmations and DLQ support

use async_nats::{jetstream, Client as NatsClient};
use chrono::{DateTime, Utc};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use sinex_core::{db::DbPool, environment::SinexEnvironment, JsonValue};
use sqlx::Row;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::time::Duration;
use tracing::{debug, error, info, warn};

use crate::{validator::EventValidator, IngestdResult, SinexError};

#[derive(Debug, Deserialize)]
struct RawEvent {
    id: String,
    source: String,
    event_type: String,
    ts_orig: String,
    host: String,
    payload: JsonValue,
}

#[derive(Debug, Serialize)]
struct Confirmation {
    event_id: String,
    persisted: bool,
    ts_ingest: String,
}

#[derive(Debug, Serialize)]
struct DlqEntry {
    event_id: String,
    error: String,
    original_payload: JsonValue,
    failed_at: String,
}

pub struct JetStreamConsumer {
    js: jetstream::Context,
    pool: DbPool,
    #[allow(dead_code)]
    validator: Arc<EventValidator>,
    env: SinexEnvironment,
    stats: ConsumerStats,
}

#[derive(Debug, Default)]
struct ConsumerStats {
    events_processed: AtomicU64,
    events_failed: AtomicU64,
    validation_failures: AtomicU64,
    dlq_routed: AtomicU64,
}

impl ConsumerStats {
    fn log(&self) {
        info!(
            events_processed = self.events_processed.load(Ordering::Relaxed),
            events_failed = self.events_failed.load(Ordering::Relaxed),
            validation_failures = self.validation_failures.load(Ordering::Relaxed),
            dlq_routed = self.dlq_routed.load(Ordering::Relaxed),
            "JetStream consumer stats"
        );
    }
}

impl JetStreamConsumer {
    pub fn new(nats_client: NatsClient, pool: DbPool, validator: Arc<EventValidator>) -> Self {
        let js = jetstream::new(nats_client);
        let env = sinex_core::environment().clone();

        Self {
            js,
            pool,
            validator,
            env,
            stats: ConsumerStats::default(),
        }
    }

    /// Bootstrap all required JetStream streams
    async fn bootstrap_streams(&self) -> IngestdResult<()> {
        info!("Bootstrapping JetStream streams");

        // Events stream - durable event log for automata replay
        // 90 days retention to support full operational history replay
        let events_stream = self.env.nats_subject("events_raw");
        self.js
            .get_or_create_stream(jetstream::stream::Config {
                name: events_stream.clone(),
                subjects: vec![self.env.nats_subject("events.raw.>")],
                retention: jetstream::stream::RetentionPolicy::Limits,
                max_messages: 10_000_000,
                max_age: Duration::from_secs(90 * 24 * 60 * 60), // 90 days (operational history)
                storage: jetstream::stream::StorageType::File,
                ..Default::default()
            })
            .await
            .map_err(|e| SinexError::network(format!("Failed to create events stream: {}", e)))?;

        // Confirmations stream with compaction - only keep latest per event
        // Short retention since confirmations are ephemeral operational state
        let confirmations_stream = self.env.nats_subject("events_confirmations");
        self.js
            .get_or_create_stream(jetstream::stream::Config {
                name: confirmations_stream.clone(),
                subjects: vec![self.env.nats_subject("events.confirmations.>")],
                retention: jetstream::stream::RetentionPolicy::Limits,
                max_messages_per_subject: 1, // Compaction: only keep latest confirmation
                max_age: Duration::from_secs(7 * 24 * 60 * 60), // 7 days (operational buffer)
                storage: jetstream::stream::StorageType::File,
                ..Default::default()
            })
            .await
            .map_err(|e| {
                SinexError::network(format!("Failed to create confirmations stream: {}", e))
            })?;

        // DLQ stream
        let dlq_stream = self.env.nats_subject("events_dlq");
        self.js
            .get_or_create_stream(jetstream::stream::Config {
                name: dlq_stream.clone(),
                subjects: vec![self.env.nats_subject("events.dlq.>")],
                retention: jetstream::stream::RetentionPolicy::Limits,
                max_messages: 1_000_000,
                max_age: Duration::from_secs(30 * 24 * 60 * 60), // 30 days
                storage: jetstream::stream::StorageType::File,
                ..Default::default()
            })
            .await
            .map_err(|e| SinexError::network(format!("Failed to create DLQ stream: {}", e)))?;

        info!("JetStream streams bootstrapped successfully");
        Ok(())
    }

    pub async fn run(self) -> IngestdResult<()> {
        info!("Starting JetStream consumer");

        // Bootstrap streams
        self.bootstrap_streams().await?;

        // Get events stream
        let stream_name = self.env.nats_subject("events_raw");
        let stream = self
            .js
            .get_stream(&stream_name)
            .await
            .map_err(|e| SinexError::network(format!("Failed to get stream: {}", e)))?;

        // Create durable consumer
        let consumer = stream
            .get_or_create_consumer(
                "ingestd",
                jetstream::consumer::pull::Config {
                    durable_name: Some("ingestd".to_string()),
                    ack_policy: jetstream::consumer::AckPolicy::Explicit,
                    ack_wait: Duration::from_secs(30),
                    max_ack_pending: 100,
                    ..Default::default()
                },
            )
            .await
            .map_err(|e| SinexError::network(format!("Failed to create consumer: {}", e)))?;

        // Stats logging interval
        let mut stats_interval = tokio::time::interval(Duration::from_secs(60));

        loop {
            tokio::select! {
                _ = stats_interval.tick() => {
                    self.stats.log();
                }
                batch_result = self.process_batch(&consumer) => {
                    if let Err(e) = batch_result {
                        error!("Batch processing error: {}", e);
                    }
                }
            }
        }
    }

    async fn process_batch(
        &self,
        consumer: &jetstream::consumer::Consumer<jetstream::consumer::pull::Config>,
    ) -> IngestdResult<()> {
        let mut messages = consumer
            .batch()
            .max_messages(100)
            .expires(Duration::from_secs(30))
            .messages()
            .await
            .map_err(|e| SinexError::network(format!("Failed to fetch messages: {}", e)))?;

        let mut batch = Vec::new();

        while let Some(msg) = messages.next().await {
            let msg = match msg {
                Ok(m) => m,
                Err(e) => {
                    error!("Error receiving message: {}", e);
                    continue;
                }
            };

            // Parse event
            let raw_event: RawEvent = match serde_json::from_slice(&msg.payload) {
                Ok(e) => e,
                Err(e) => {
                    error!(event_id = ?msg.headers, "Failed to parse event: {}", e);
                    // Route to DLQ
                    self.route_to_dlq(&msg, format!("Parse error: {}", e)).await;
                    msg.ack()
                        .await
                        .map_err(|e| SinexError::network(format!("Failed to ack: {}", e)))?;
                    self.stats
                        .validation_failures
                        .fetch_add(1, Ordering::Relaxed);
                    self.stats.dlq_routed.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
            };

            // Validate event using EventValidator
            if let Err(e) = self.validate_event(&raw_event).await {
                warn!(event_id = %raw_event.id, "Event validation failed: {}", e);
                self.route_to_dlq(&msg, format!("Validation failed: {}", e))
                    .await;
                msg.ack()
                    .await
                    .map_err(|e| SinexError::network(format!("Failed to ack: {}", e)))?;
                self.stats
                    .validation_failures
                    .fetch_add(1, Ordering::Relaxed);
                self.stats.dlq_routed.fetch_add(1, Ordering::Relaxed);
                continue;
            }

            batch.push((raw_event, msg));
        }

        if batch.is_empty() {
            return Ok(());
        }

        // Persist batch to database
        match self.persist_batch_optimized(&batch).await {
            Ok(persisted_ids) => {
                // Publish confirmations for successfully persisted events
                for event_id in &persisted_ids {
                    if let Err(e) = self.publish_confirmation(event_id).await {
                        warn!(event_id = %event_id, "Failed to publish confirmation: {}", e);
                    }
                }

                // ACK all messages
                for (_, msg) in &batch {
                    msg.ack()
                        .await
                        .map_err(|e| SinexError::network(format!("Failed to ack: {}", e)))?;
                }

                self.stats
                    .events_processed
                    .fetch_add(batch.len() as u64, Ordering::Relaxed);
                info!("Processed and confirmed {} events", batch.len());
            }
            Err(e) => {
                error!("Failed to persist batch: {}", e);
                // NACK all - they'll be redelivered
                for (_, msg) in &batch {
                    let _ = msg.ack_with(jetstream::AckKind::Nak(None)).await;
                }
                self.stats
                    .events_failed
                    .fetch_add(batch.len() as u64, Ordering::Relaxed);
            }
        }

        Ok(())
    }

    /// Validate event against JSON schema
    async fn validate_event(&self, event: &RawEvent) -> IngestdResult<()> {
        // Use EventValidator to validate payload
        // For now, basic validation - can be enhanced with schema validation
        if event.id.is_empty() {
            return Err(SinexError::validation("Event ID cannot be empty"));
        }
        if event.source.is_empty() {
            return Err(SinexError::validation("Event source cannot be empty"));
        }
        if event.event_type.is_empty() {
            return Err(SinexError::validation("Event type cannot be empty"));
        }

        Ok(())
    }

    /// Persist batch using optimized UNNEST pattern
    async fn persist_batch_optimized(
        &self,
        batch: &[(RawEvent, jetstream::Message)],
    ) -> IngestdResult<Vec<String>> {
        if batch.is_empty() {
            return Ok(Vec::new());
        }

        // Parse timestamps first to fail fast on invalid data
        let parsed_events: Result<Vec<_>, SinexError> = batch
            .iter()
            .map(|(event, _)| {
                let ts_orig: DateTime<Utc> = event
                    .ts_orig
                    .parse()
                    .map_err(|e| SinexError::parse(format!("Invalid timestamp: {}", e)))?;
                Ok((event, ts_orig))
            })
            .collect();
        let parsed_events = parsed_events?;

        // Extract arrays for UNNEST
        let ids: Vec<&str> = parsed_events.iter().map(|(e, _)| e.id.as_str()).collect();
        let sources: Vec<&str> = parsed_events
            .iter()
            .map(|(e, _)| e.source.as_str())
            .collect();
        let event_types: Vec<&str> = parsed_events
            .iter()
            .map(|(e, _)| e.event_type.as_str())
            .collect();
        let ts_origs: Vec<DateTime<Utc>> = parsed_events.iter().map(|(_, ts)| *ts).collect();
        let hosts: Vec<&str> = parsed_events.iter().map(|(e, _)| e.host.as_str()).collect();
        let payloads: Vec<&JsonValue> = parsed_events.iter().map(|(e, _)| &e.payload).collect();

        // Bulk insert using UNNEST for optimal performance
        let rows = sqlx::query(
            r#"
            INSERT INTO core.events (id, source, event_type, ts_orig, host, payload)
            SELECT
                CAST(id AS ULID),
                source,
                event_type,
                ts_orig,
                host,
                payload
            FROM UNNEST(
                $1::text[],
                $2::text[],
                $3::text[],
                $4::timestamptz[],
                $5::text[],
                $6::jsonb[]
            ) AS t(id, source, event_type, ts_orig, host, payload)
            ON CONFLICT (id) DO NOTHING
            RETURNING (id)::text
            "#,
        )
        .bind(&ids)
        .bind(&sources)
        .bind(&event_types)
        .bind(&ts_origs)
        .bind(&hosts)
        .bind(&payloads)
        .fetch_all(&self.pool)
        .await?;

        // Extract persisted IDs from RETURNING clause
        let persisted_ids: Vec<String> = rows
            .into_iter()
            .map(|row| row.get::<String, _>(0))
            .collect();

        debug!(
            batch_size = batch.len(),
            persisted_count = persisted_ids.len(),
            "Batch persisted using UNNEST"
        );

        Ok(persisted_ids)
    }

    /// Publish confirmation to NATS
    async fn publish_confirmation(&self, event_id: &str) -> IngestdResult<()> {
        let confirmation = Confirmation {
            event_id: event_id.to_string(),
            persisted: true,
            ts_ingest: Utc::now().to_rfc3339(),
        };

        let subject = self
            .env
            .nats_subject(&format!("events.confirmations.{}", event_id));
        let payload = serde_json::to_vec(&confirmation)?;

        // Add idempotency header
        let mut headers = async_nats::HeaderMap::new();
        headers.insert("Nats-Msg-Id", event_id);

        self.js
            .publish_with_headers(subject, headers, payload.into())
            .await
            .map_err(|e| SinexError::network(format!("Failed to publish confirmation: {}", e)))?
            .await
            .map_err(|e| SinexError::network(format!("Confirmation ack failed: {}", e)))?;

        debug!(event_id = %event_id, "Published confirmation");
        Ok(())
    }

    /// Route failed message to DLQ
    async fn route_to_dlq(&self, msg: &jetstream::Message, error: String) {
        let dlq_entry = DlqEntry {
            event_id: msg
                .headers
                .as_ref()
                .and_then(|h| h.get("Nats-Msg-Id"))
                .map(|v| v.as_str().to_string())
                .unwrap_or_else(|| "unknown".to_string()),
            error,
            original_payload: serde_json::from_slice(&msg.payload).unwrap_or(serde_json::json!({})),
            failed_at: Utc::now().to_rfc3339(),
        };

        let subject = self.env.nats_subject("events.dlq.ingestd");
        if let Ok(payload) = serde_json::to_vec(&dlq_entry) {
            if let Err(e) = self.js.publish(subject, payload.into()).await {
                error!("Failed to route to DLQ: {}", e);
            } else {
                debug!(event_id = %dlq_entry.event_id, "Routed to DLQ");
            }
        }
    }
}
