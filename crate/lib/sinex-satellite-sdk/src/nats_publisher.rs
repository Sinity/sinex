//! NATS JetStream event publisher

use sinex_core::{db::models::Event, environment::SinexEnvironment};

#[derive(Debug, Clone)]
pub struct NatsPublisher {
    nats_client: async_nats::Client,
    env: SinexEnvironment,
}

impl NatsPublisher {
    pub fn new(nats_client: async_nats::Client) -> Self {
        let env = sinex_core::environment().clone();
        Self { nats_client, env }
    }

    pub async fn publish(
        &self,
        event: &Event,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let js = async_nats::jetstream::new(self.nats_client.clone());

        let event_id = event.id.as_ref().ok_or("Event ID is required")?;

        let ts_orig = event.ts_orig.ok_or("Event ts_orig is required")?;

        let payload = serde_json::json!({
            "id": event_id.to_string(),
            "source": event.source.as_str(),
            "event_type": event.event_type.as_str(),
            "ts_orig": ts_orig.to_rfc3339(),
            "host": event.host.as_str(),
            "payload": event.payload,
        });

        let subject = self.env.nats_subject(&format!(
            "events.raw.{}.{}",
            event.source.as_str().replace('.', "_"),
            event.event_type.as_str().replace('.', "_")
        ));

        // Add idempotency header
        let mut headers = async_nats::HeaderMap::new();
        headers.insert("Nats-Msg-Id", event_id.to_string().as_str());

        js.publish_with_headers(subject, headers, serde_json::to_vec(&payload)?.into())
            .await?;

        Ok(())
    }
}
