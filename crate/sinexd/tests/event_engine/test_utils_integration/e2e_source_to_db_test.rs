use async_nats::jetstream;
use serde_json::json;
use sinexd::event_engine::{
    EventEngineResult, JetStreamConsumer, JetStreamTopology, validator::IngestEventValidator,
};
use sinex_primitives::{Uuid, environment, temporal};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::timeout;
use xtask::sandbox::timing::{Timeouts, WaitHelpers};
use xtask::sandbox::prelude::*;

const FIXTURE_SOURCE_MATERIAL_ID: &str = "00000000-0000-7000-8000-000000000000";

struct TestSourcePublisher {
    nats_client: async_nats::Client,
    source: String,
    namespace: Option<String>,
}

impl TestSourcePublisher {
    fn with_namespace(
        nats_client: async_nats::Client,
        source: impl Into<String>,
        namespace: Option<String>,
    ) -> Self {
        Self {
            nats_client,
            source: source.into(),
            namespace,
        }
    }

    async fn publish(&self, event_type: &str, payload: serde_json::Value) -> TestResult<Uuid> {
        let event_id = Uuid::now_v7();
        let event = serde_json::json!({
            "id": event_id.to_string(),
            "source": self.source,
            "event_type": event_type,
            "payload": payload,
            "ts_orig": temporal::now().format_rfc3339(),
            "host": "test-host",
            "source_material_id": FIXTURE_SOURCE_MATERIAL_ID,
            "anchor_byte": 0,
        });

        let subject = environment().nats_subject_with_namespace(
            self.namespace.as_deref(),
            &format!(
                "events.raw.{}.{}",
                self.source.replace('.', "_"),
                event_type.replace('.', "_")
            ),
        );
        self.nats_client
            .publish(subject, serde_json::to_vec(&event)?.into())
            .await?;
        self.nats_client.flush().await?;

        Ok(event_id)
    }
}

async fn ensure_fixture_source_material(ctx: &TestContext) -> TestResult<()> {
    sqlx::query!(
        r#"
        INSERT INTO raw.source_material_registry
            (id, material_kind, source_identifier, status, timing_info_type, staged_at)
        VALUES ($1::uuid, 'annex', 'test-fixture-material', 'completed', 'realtime', NOW())
        ON CONFLICT (id) DO UPDATE
        SET staged_at = EXCLUDED.staged_at,
            status = EXCLUDED.status,
            timing_info_type = EXCLUDED.timing_info_type
        "#,
        FIXTURE_SOURCE_MATERIAL_ID.parse::<uuid::Uuid>()?,
    )
    .execute(&ctx.pool)
    .await?;
    Ok(())
}

async fn start_event_engine(
    ctx: &TestContext,
    suffix: &str,
) -> TestResult<(
    tokio::task::JoinHandle<EventEngineResult<()>>,
    jetstream::Context,
    JetStreamTopology,
)> {
    let nats = ctx.nats_handle()?;
    let nats_client = ctx.nats_client();
    let js = nats.jetstream_with_client(nats_client.clone());
    let env = ctx.env();
    let stream_base = format!("SINEX_RAW_EVENTS_E2E_{suffix}");
    let namespace = ctx.pipeline_namespace().prefix().to_string();
    let stream = ctx.pipeline_namespace().stream(&stream_base);
    let topology = JetStreamTopology::new(
        &env,
        stream,
        ctx.pipeline_namespace()
            .consumer_name(&format!("event-engine-e2e-{suffix}")),
        Some(&namespace),
    );

    let consumer = JetStreamConsumer::new(
        nats_client.clone(),
        ctx.pool.clone(),
        Arc::new(RwLock::new(IngestEventValidator::new(false))),
        topology.clone(),
    );
    let handle = tokio::spawn(async move { consumer.run().await });

    // Ensure streams exist before publishing.
    let stream_timeout = Duration::from_secs(Timeouts::QUICK);
    timeout(stream_timeout, async {
        nats.wait_for_stream(&js, &topology.events_stream, stream_timeout)
            .await?;
        nats.wait_for_stream(&js, &topology.confirmed_events_stream, stream_timeout)
            .await?;
        nats.wait_for_stream(&js, &topology.dlq_stream, stream_timeout)
            .await
    })
    .await??;

    Ok((handle, js, topology))
}

#[sinex_test]
async fn end_to_end_source_runtime_full_flow(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    ensure_fixture_source_material(&ctx).await?;
    let nats_client = ctx.nats_client();
    let namespace = ctx.pipeline_namespace().prefix().to_string();
    let suffix = format!("e2e-{}", uuid::Uuid::new_v4());
    let (handle, js, topology) = start_event_engine(&ctx, &suffix).await?;

    let mut confirmation_sub = nats_client
        .subscribe(topology.confirmed_events_subject.clone())
        .await?;

    let publisher = TestSourcePublisher::with_namespace(
        nats_client.clone(),
        format!("source.{suffix}"),
        Some(namespace),
    );
    let mut ids = Vec::new();
    for idx in 0..25u32 {
        let id = publisher
            .publish("e2e.event", json!({ "seq": idx, "note": "end-to-end" }))
            .await?;
        ids.push(id);
    }

    // Wait for persistence.
    WaitHelpers::wait_for_condition(
        || {
            let pool = ctx.pool.clone();
            let source = format!("source.{suffix}");
            async move {
                let count: i64 =
                    sqlx::query_scalar("SELECT COUNT(*) FROM core.events WHERE source = $1")
                        .bind(source)
                        .fetch_one(&pool)
                        .await?;
                Ok(count == 25)
            }
        },
        Timeouts::MEDIUM,
    )
    .await?;

    // Confirmations observed for all events from the wildcard subscription.
    use std::collections::HashSet;
    let expected: HashSet<_> = ids.iter().map(|id| id.to_string()).collect();
    let mut seen: HashSet<String> = HashSet::new();
    timeout(Duration::from_secs(Timeouts::SHORT), async {
        while seen.len() < expected.len() {
            if let Some(msg) = confirmation_sub.next().await {
                let payload: serde_json::Value = serde_json::from_slice(&msg.payload)?;
                if let Some(id) = payload.get("event_id").and_then(|v| v.as_str()) {
                    if expected.contains(id) {
                        seen.insert(id.to_string());
                    }
                }
            } else {
                bail!("confirmation stream closed unexpectedly");
            }
        }
        Ok::<_, color_eyre::Report>(())
    })
    .await??;

    // DLQ remains empty.
    let dlq_state = js
        .get_stream(&topology.dlq_stream)
        .await?
        .info()
        .await?
        .state;
    assert_eq!(
        dlq_state.messages, 0,
        "DLQ should be empty in e2e happy path"
    );

    handle.abort();
    Ok(())
}
