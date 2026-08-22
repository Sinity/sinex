//! sinex-ddy: paced restart drain.
//!
//! Proves the event-engine's startup catch-up drain — a durable consumer
//! resuming against a raw-stream backlog that was ALREADY seeded before this
//! process started (e.g. left behind by a prior crash mid-import, the exact
//! shape of the #2182 incident) — is bounded by the same `PacingController`
//! mechanism sinex-2n9 built for historical-scan pacing, not a second
//! throttle. The backlog here is published directly to the raw stream BEFORE
//! the consumer is spawned, so this is not "pace a live scan" (sinex-2n9's
//! own coverage) but "pace a consumer that boots straight into a backlog".

use std::sync::Arc;
use std::time::Instant;

use serde_json::json;
use sinex_primitives::{Uuid, temporal};
use sinexd::event_engine::validator::IngestEventValidator;
use sinexd::event_engine::{JetStreamConsumer, JetStreamTopology};
use sinexd::runtime::pacing::RateBudget;
use tokio::sync::RwLock;
use tokio::time::Duration;
use xtask::sandbox::prelude::*;
use xtask::sandbox::timing::{Timeouts, WaitHelpers};

#[path = "support.rs"]
mod support;
use support::{admission_envelope, ensure_fixture_source_material};

/// Publish `count` distinct raw events directly to the raw-events stream,
/// simulating a backlog that already landed in NATS before this process
/// (re)started -- never routed through a scan loop.
async fn seed_raw_backlog(
    nats_client: &async_nats::Client,
    namespace: &str,
    source: &str,
    count: usize,
) -> TestResult<()> {
    let env = sinex_primitives::environment();
    for idx in 0..count {
        let event = json!({
            "id": Uuid::now_v7().to_string(),
            "source": source,
            "event_type": "restart.backlog.event",
            "payload": {"sequence": idx},
            "ts_orig": temporal::now().format_rfc3339(),
            "host": "test-host",
            "source_material_id": "00000000-0000-7000-8000-000000000000",
            "anchor_byte": idx as i64,
        });
        let subject = env.nats_subject_with_namespace(
            Some(namespace),
            &format!(
                "events.raw.{}.restart_backlog_event",
                source.replace('.', "_")
            ),
        );
        nats_client
            .publish(
                subject,
                serde_json::to_vec(&admission_envelope(source, event))?.into(),
            )
            .await?;
    }
    nats_client.flush().await?;
    Ok(())
}

async fn stored_event_count(pool: &sinex_db::DbPool, source: &str) -> TestResult<i64> {
    let count = sqlx::query_scalar!("SELECT COUNT(*) FROM core.events WHERE source = $1", source,)
        .fetch_one(pool)
        .await?
        .expect("COUNT(*) should always return one row");
    Ok(count)
}

/// A restart with a pre-existing large raw backlog drains at a bounded rate
/// (the same `PacingController` sinex-2n9 uses) instead of pulling as fast as
/// JetStream will deliver, and still completes -- it neither OOMs the
/// in-flight decode high-watermark nor jams.
#[sinex_test]
async fn restart_drain_holds_configured_rate_and_completes(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let nats = ctx.nats_handle()?;
    let nats_client = ctx.nats_client();
    let pool = ctx.pool.clone();
    ensure_fixture_source_material(&pool).await?;

    let namespace = ctx.pipeline_namespace().prefix().to_string();
    let source = "restart-drain-suite";
    let total_events = 30_usize;
    let events_per_sec = 15.0_f64;

    // Seed the backlog BEFORE the consumer exists at all -- this is the
    // "already landed in NATS from before a crash" scenario, not a live scan.
    // Bootstrap the raw stream directly (the same helper `scan_historical`'s
    // pacing e2e coverage uses) so publishing below has somewhere to land.
    sinexd::runtime::jetstream_streams::bootstrap_raw_events_stream(&nats_client, Some(&namespace))
        .await?;

    let stream_name = ctx.pipeline_namespace().stream("SINEX_RAW_EVENTS");
    let topology = JetStreamTopology::new(
        &ctx.env().clone(),
        stream_name,
        ctx.pipeline_namespace().consumer_name("restart-drain"),
        Some(&namespace),
    );

    // Pre-create the durable consumer with the exact name/config the
    // event-engine will bind to below, BEFORE any backlog is published. This
    // models the actual restart shape: the durable already existed from
    // before a crash, so `reject_initial_replay`'s "missing durable + non-
    // empty stream" cold-start guard correctly does not fire -- a real
    // restart resumes an existing durable, it doesn't invent a fresh one
    // against an already-populated stream.
    {
        let js = async_nats::jetstream::new(nats_client.clone());
        let mut stream = js.get_stream(topology.events_stream.as_ref()).await?;
        stream
            .get_or_create_consumer(
                &topology.consumer_durable,
                async_nats::jetstream::consumer::pull::Config {
                    durable_name: Some(topology.consumer_durable.clone()),
                    filter_subject: topology.events_subject.to_string(),
                    ack_policy: async_nats::jetstream::consumer::AckPolicy::Explicit,
                    ack_wait: Duration::from_secs(30),
                    deliver_policy: async_nats::jetstream::consumer::DeliverPolicy::All,
                    ..Default::default()
                },
            )
            .await?;
    }

    seed_raw_backlog(&nats_client, &namespace, source, total_events).await?;

    let validator = IngestEventValidator::new(false);
    let consumer = JetStreamConsumer::new(
        nats_client.clone(),
        pool.clone(),
        Arc::new(RwLock::new(validator)),
        topology.clone(),
    )
    // Small fetch batches force several paced batches rather than one.
    .with_batch_fetch_config(10, Duration::from_millis(50))
    .with_restart_drain_rate_budget(RateBudget {
        events_per_sec: Some(events_per_sec),
        bytes_per_sec: None,
        // Restart-drain pacing only activates when the startup backlog meets
        // or exceeds this threshold; set well below `total_events` so this
        // deliberately-seeded backlog counts as "large enough to pace".
        backlog_pause_threshold: Some(5),
        backlog_resume_threshold: None,
    });

    let start = Instant::now();
    let consumer_handle = tokio::spawn(async move { consumer.run().await });

    nats.wait_for_stream(
        &nats.jetstream_with_client(nats_client.clone()),
        &topology.events_stream,
        Duration::from_secs(Timeouts::QUICK),
    )
    .await?;

    // Generous upper bound: well beyond the paced minimum, but still catches
    // a genuinely jammed/hung drain rather than waiting forever.
    let upper_bound = Duration::from_secs(60);
    WaitHelpers::wait_for_condition(
        || {
            let pool = pool.clone();
            async move {
                let stored = stored_event_count(&pool, source).await?;
                Ok::<bool, color_eyre::eyre::Error>(stored >= total_events as i64)
            }
        },
        upper_bound.as_secs(),
    )
    .await?;
    let elapsed = start.elapsed();

    consumer_handle.abort();
    let _ = consumer_handle.await;

    // The pacing floor: PacingController enforces average rate since start,
    // so draining `total_events` at `events_per_sec` cannot finish faster
    // than total_events / events_per_sec, minus slack for the very first
    // unthrottled fetch's own processing time.
    let min_expected = Duration::from_secs_f64(total_events as f64 / events_per_sec * 0.6);
    assert!(
        elapsed >= min_expected,
        "restart drain finished in {elapsed:?}, faster than the configured {events_per_sec} events/sec \
         budget should allow (expected >= {min_expected:?} for {total_events} events) -- \
         PacingController is not being consulted on the consumer drain path"
    );

    let stored = stored_event_count(&pool, source).await?;
    assert_eq!(
        stored, total_events as i64,
        "restart drain must fully complete, not just start pacing"
    );

    Ok(())
}
