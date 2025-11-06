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

use crate::{
    validator::{EventValidator, ValidationResult},
    IngestdResult, SinexError,
};
use tokio::sync::RwLock;

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
    validator: Arc<RwLock<EventValidator>>,
    topology: JetStreamTopology,
    stats: ConsumerStats,
}

#[derive(Debug, Clone)]
pub struct JetStreamTopology {
    pub events_stream: String,
    pub events_subject: String,
    pub confirmations_stream: String,
    pub confirmations_subject: String,
    pub confirmations_prefix: String,
    pub dlq_stream: String,
    pub dlq_subject: String,
    pub dlq_publish_subject: String,
    pub consumer_durable: String,
}

impl JetStreamTopology {
    pub fn new(env: &SinexEnvironment, base_stream: String, consumer_durable: String) -> Self {
        let confirmations_stream = format!("{base_stream}_CONFIRMATIONS");
        let dlq_stream = format!("{base_stream}_DLQ");
        let confirmations_prefix = format!("{}.", env.nats_subject("events.confirmations"));

        Self {
            events_stream: base_stream.clone(),
            events_subject: env.nats_subject("events.>"),
            confirmations_stream,
            confirmations_subject: env.nats_subject("events.confirmations.>"),
            confirmations_prefix,
            dlq_stream,
            dlq_subject: env.nats_subject("events.dlq.>"),
            dlq_publish_subject: env.nats_subject("events.dlq.ingestd"),
            consumer_durable,
        }
    }
}

struct PreparedEvent {
    raw: RawEvent,
    parsed_ts: DateTime<Utc>,
    message: jetstream::Message,
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
    pub fn new(
        nats_client: NatsClient,
        pool: DbPool,
        validator: Arc<RwLock<EventValidator>>,
        topology: JetStreamTopology,
    ) -> Self {
        let js = jetstream::new(nats_client);

        Self {
            js,
            pool,
            validator,
            topology,
            stats: ConsumerStats::default(),
        }
    }

    /// Bootstrap all required JetStream streams
    async fn bootstrap_streams(&self) -> IngestdResult<()> {
        info!("Bootstrapping JetStream streams");

        // Events stream - durable event log for automata replay
        // 90 days retention to support full operational history replay
        let events_stream = self.topology.events_stream.clone();
        self.js
            .get_or_create_stream(jetstream::stream::Config {
                name: events_stream.clone(),
                subjects: vec![self.topology.events_subject.clone()],
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
        let confirmations_stream = self.topology.confirmations_stream.clone();
        self.js
            .get_or_create_stream(jetstream::stream::Config {
                name: confirmations_stream.clone(),
                subjects: vec![self.topology.confirmations_subject.clone()],
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
        let dlq_stream = self.topology.dlq_stream.clone();
        self.js
            .get_or_create_stream(jetstream::stream::Config {
                name: dlq_stream.clone(),
                subjects: vec![self.topology.dlq_subject.clone()],
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
        let stream_name = self.topology.events_stream.clone();
        let stream = self
            .js
            .get_stream(&stream_name)
            .await
            .map_err(|e| SinexError::network(format!("Failed to get stream: {}", e)))?;

        // Create durable consumer
        let consumer = stream
            .get_or_create_consumer(
                &self.topology.consumer_durable,
                jetstream::consumer::pull::Config {
                    durable_name: Some(self.topology.consumer_durable.clone()),
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

            // Parse timestamp early to isolate poison messages
            let parsed_ts = match raw_event.ts_orig.parse::<DateTime<Utc>>() {
                Ok(ts) => ts,
                Err(e) => {
                    error!(
                        event_id = %raw_event.id,
                        ts = %raw_event.ts_orig,
                        "Invalid timestamp; routing to DLQ: {}",
                        e
                    );
                    self.route_to_dlq(&msg, format!("Invalid timestamp: {}", e))
                        .await;
                    msg.ack().await.map_err(|ack_err| {
                        SinexError::network(format!(
                            "Failed to ack invalid timestamp message: {}",
                            ack_err
                        ))
                    })?;
                    self.stats
                        .validation_failures
                        .fetch_add(1, Ordering::Relaxed);
                    self.stats.dlq_routed.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
            };

            batch.push(PreparedEvent {
                raw: raw_event,
                parsed_ts,
                message: msg,
            });
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
                for prepared in &batch {
                    prepared
                        .message
                        .ack()
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
                for prepared in &batch {
                    let _ = prepared
                        .message
                        .ack_with(jetstream::AckKind::Nak(None))
                        .await;
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
        if event.id.is_empty() {
            return Err(SinexError::validation("Event ID cannot be empty"));
        }
        if event.source.is_empty() {
            return Err(SinexError::validation("Event source cannot be empty"));
        }
        if event.event_type.is_empty() {
            return Err(SinexError::validation("Event type cannot be empty"));
        }

        let guard = self.validator.read().await;
        let validation = guard
            .validate_payload_for(&event.source, &event.event_type, &event.payload)
            .map_err(|err| {
                SinexError::validation(format!("Schema validation error: {}", err))
                    .with_operation("jetstream_consumer.validate_event")
            })?;

        match validation {
            ValidationResult::Valid | ValidationResult::Skipped | ValidationResult::NoSchema => {
                Ok(())
            }
            ValidationResult::SchemaNotFound { schema_id } => {
                warn!(
                    source = %event.source,
                    event_type = %event.event_type,
                    schema = %schema_id,
                    "Schema referenced in lookup was not found; accepting event"
                );
                Ok(())
            }
            ValidationResult::Invalid { errors } => Err(SinexError::validation(format!(
                "Schema validation failed: {}",
                errors.join(", ")
            ))
            .with_operation("jetstream_consumer.validate_event")),
        }
    }

    /// Persist batch using optimized UNNEST pattern
    async fn persist_batch_optimized(&self, batch: &[PreparedEvent]) -> IngestdResult<Vec<String>> {
        if batch.is_empty() {
            return Ok(Vec::new());
        }

        // Extract arrays for UNNEST
        let ids: Vec<&str> = batch
            .iter()
            .map(|prepared| prepared.raw.id.as_str())
            .collect();
        let sources: Vec<&str> = batch
            .iter()
            .map(|prepared| prepared.raw.source.as_str())
            .collect();
        let event_types: Vec<&str> = batch
            .iter()
            .map(|prepared| prepared.raw.event_type.as_str())
            .collect();
        let ts_origs: Vec<DateTime<Utc>> =
            batch.iter().map(|prepared| prepared.parsed_ts).collect();
        let hosts: Vec<&str> = batch
            .iter()
            .map(|prepared| prepared.raw.host.as_str())
            .collect();
        let payloads: Vec<&JsonValue> =
            batch.iter().map(|prepared| &prepared.raw.payload).collect();

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

        let subject = format!("{}{}", self.topology.confirmations_prefix, event_id);
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

        if let Ok(payload) = serde_json::to_vec(&dlq_entry) {
            match self
                .js
                .publish(self.topology.dlq_publish_subject.clone(), payload.into())
                .await
            {
                Ok(ack) => {
                    if let Err(e) = ack.await {
                        error!("Failed to confirm DLQ publish: {}", e);
                    } else {
                        debug!(event_id = %dlq_entry.event_id, "Routed to DLQ");
                    }
                }
                Err(e) => {
                    error!("Failed to route to DLQ: {}", e);
                }
            }
        }
    }
}
