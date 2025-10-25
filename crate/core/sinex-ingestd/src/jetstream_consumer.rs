//! JetStream event consumer

use async_nats::{jetstream, Client as NatsClient};
use chrono::{DateTime, Utc};
use futures::StreamExt;
use serde::Deserialize;
use sinex_core::{db::DbPool, environment::SinexEnvironment, JsonValue};
use std::sync::Arc;
use tokio::time::Duration;
use tracing::{error, info};

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

pub struct JetStreamConsumer {
    js: jetstream::Context,
    pool: DbPool,
    validator: Arc<EventValidator>,
    env: SinexEnvironment,
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
        }
    }

    pub async fn run(self) -> IngestdResult<()> {
        info!("Starting JetStream consumer");

        // Create or get stream
        let stream_name = self.env.nats_subject("events_raw");
        let stream = self
            .js
            .get_or_create_stream(jetstream::stream::Config {
                name: stream_name.clone(),
                subjects: vec![self.env.nats_subject("events.raw.>")],
                ..Default::default()
            })
            .await
            .map_err(|e| SinexError::network(format!("Failed to create stream: {}", e)))?;

        // Create consumer on the stream
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

        loop {
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
                        error!("Failed to parse event: {}", e);
                        // TODO: Route to DLQ
                        msg.ack()
                            .await
                            .map_err(|e| SinexError::network(format!("Failed to ack: {}", e)))?;
                        continue;
                    }
                };

                batch.push((raw_event, msg));
            }

            if batch.is_empty() {
                continue;
            }

            // Insert batch to DB
            if let Err(e) = self.persist_batch(&batch).await {
                error!("Failed to persist batch: {}", e);
                // NACK all - they'll be redelivered
                for (_, msg) in &batch {
                    let _ = msg.ack_with(jetstream::AckKind::Nak(None)).await;
                }
                continue;
            }

            // ACK all messages
            for (_, msg) in &batch {
                msg.ack()
                    .await
                    .map_err(|e| SinexError::network(format!("Failed to ack: {}", e)))?;
            }

            info!("Processed {} events", batch.len());
        }
    }

    async fn persist_batch(&self, batch: &[(RawEvent, jetstream::Message)]) -> IngestdResult<()> {
        // Use existing batch insert logic
        // For now, simplified version
        for (event, _) in batch {
            let ts_orig: DateTime<Utc> = event
                .ts_orig
                .parse()
                .map_err(|e| SinexError::parse(format!("Invalid timestamp: {}", e)))?;

            sqlx::query(
                r#"
                INSERT INTO core.events (id, source, event_type, ts_orig, host, payload)
                VALUES (CAST($1 AS ULID), $2, $3, $4, $5, $6)
                ON CONFLICT (id) DO NOTHING
                "#,
            )
            .bind(&event.id)
            .bind(&event.source)
            .bind(&event.event_type)
            .bind(ts_orig)
            .bind(&event.host)
            .bind(&event.payload)
            .execute(&self.pool)
            .await?;
        }

        Ok(())
    }
}
