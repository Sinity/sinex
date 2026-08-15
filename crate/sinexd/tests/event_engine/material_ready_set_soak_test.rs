//! Route-level MaterialReadySet admission soak.
//!
//! This is intentionally a filtered, rerunnable test rather than a default
//! suite test: it exercises the real JetStream consumer with a large
//! process-local readiness cardinality and records the route's admission
//! throughput and readiness metrics. The source-material cardinality is
//! configurable so the same test can be rerun against a refreshed import
//! manifest without changing code.

#[path = "support.rs"]
mod support;

use serde_json::json;
use sinex_primitives::{Uuid, temporal};
use sinexd::event_engine::material_ready_set::MaterialReadySet;
use sinexd::event_engine::validator::IngestEventValidator;
use sinexd::event_engine::{JetStreamConsumer, JetStreamTopology};
use std::sync::Arc;
use std::time::Instant;
use support::{
    admission_envelope_multi, ensure_fixture_source_material, spawn_consumer_and_wait_ready,
};
use tokio::sync::RwLock;
use xtask::sandbox::prelude::*;
use xtask::sandbox::timing::{Timeouts, WaitHelpers};

const DEFAULT_CARDINALITY: usize = 10_000;
const DEFAULT_EVENT_COUNT: usize = 1_000;

fn configured_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

/// Drive the real raw JetStream → admission → DB route while the shared
/// readiness set holds a manifest-sized cardinality. Run explicitly with:
///
/// `SINEX_READY_SET_SOAK_CARDINALITY=<n> SINEX_READY_SET_SOAK_EVENT_COUNT=<n>
///  xtask test -p sinexd -E test(route_level_material_ready_set_soak) ...`
#[sinex_test]
async fn route_level_material_ready_set_soak(ctx: TestContext) -> TestResult<()> {
    let cardinality = configured_usize("SINEX_READY_SET_SOAK_CARDINALITY", DEFAULT_CARDINALITY);
    let event_count = configured_usize("SINEX_READY_SET_SOAK_EVENT_COUNT", DEFAULT_EVENT_COUNT);
    let ctx = ctx.with_nats().shared().await?;
    let nats_client = ctx.nats_client();
    let js = ctx.jetstream().await?;
    let env = ctx.env();
    let namespace = ctx.pipeline_namespace().prefix().to_string();
    let topology = JetStreamTopology::new(
        env,
        ctx.pipeline_namespace()
            .stream("SINEX_RAW_EVENTS_READYSET_SOAK"),
        ctx.pipeline_namespace()
            .consumer_name("event-engine-readyset-soak"),
        Some(&namespace),
    );

    ensure_fixture_source_material(&ctx.pool).await?;
    let ready_set = MaterialReadySet::new();
    ready_set.mark_ready(support::FIXTURE_SOURCE_MATERIAL_ID.parse()?);
    for index in 0..cardinality {
        ready_set.mark_ready(Uuid::from_u128(index as u128 + 1));
    }
    let seeded = ready_set.metrics_snapshot();

    let consumer = JetStreamConsumer::with_test_hooks(
        nats_client.clone(),
        ctx.pool.clone(),
        Arc::new(RwLock::new(IngestEventValidator::new(false))),
        topology.clone(),
        std::time::Duration::from_secs(Timeouts::STANDARD),
        None,
        None,
        None,
        None,
        false,
        None,
        None,
        Some(std::time::Duration::from_millis(300)),
    )
    .with_ready_set(ready_set.clone())
    .with_max_ack_pending(1_000);
    let consumer_handle = spawn_consumer_and_wait_ready(&ctx, &js, &topology, consumer).await?;

    let mut events = Vec::with_capacity(event_count);
    // Use the same registered synthetic event contract as the proven consumer
    // route tests. An invented source/type pair would exercise schema/DLQ
    // rejection rather than MaterialReadySet admission.
    let source = "r6d12soak";
    let event_type = "r6d12soak.ready";
    let mut first_event_id = None;
    for index in 0..event_count {
        let event_id = Uuid::now_v7();
        first_event_id.get_or_insert(event_id);
        events.push(json!({
            "id": event_id.to_string(),
            "source": source,
            "event_type": event_type,
            "payload": {"ordinal": index},
            "ts_orig": temporal::now().format_rfc3339(),
            "host": "test-host",
            "source_material_id": support::FIXTURE_SOURCE_MATERIAL_ID,
            "anchor_byte": index,
            "equivalence_key": null,
        }));
    }
    let subject = env.nats_subject_with_namespace(
        Some(&namespace),
        &format!(
            "events.raw.{}.{}",
            source.replace('.', "_"),
            event_type.replace('.', "_")
        ),
    );
    let started = Instant::now();
    nats_client
        .publish(
            subject.clone(),
            serde_json::to_vec(&admission_envelope_multi(source, events))?.into(),
        )
        .await?;
    nats_client.flush().await?;

    WaitHelpers::wait_for_event_id(
        &ctx.pool,
        first_event_id
            .expect("event_count is configured positive")
            .into(),
        Timeouts::LONG,
    )
    .await?;
    WaitHelpers::wait_for_source_events(&ctx.pool, source, event_count, Timeouts::LONG).await?;
    let elapsed = started.elapsed();
    let after = ready_set.metrics_snapshot();
    let missing_material_id = Uuid::now_v7();
    let stream = js.get_stream(&topology.events_stream).await?;
    let mut raw_consumer = stream
        .get_consumer::<async_nats::jetstream::consumer::pull::Config>(&topology.consumer_durable)
        .await
        .map_err(|error| color_eyre::eyre::eyre!(error.to_string()))?;
    let before_delivery_sequence = raw_consumer.info().await?.delivered.consumer_sequence;
    let fk_event_id = Uuid::now_v7();
    let fk_event = json!({
        "id": fk_event_id.to_string(),
        "source": source,
        "event_type": event_type,
        "payload": {"fk_probe": true},
        "ts_orig": temporal::now().format_rfc3339(),
        "host": "test-host",
        "source_material_id": missing_material_id.to_string(),
        "anchor_byte": 0,
        "equivalence_key": null,
    });
    nats_client
        .publish(
            subject.clone(),
            serde_json::to_vec(&admission_envelope_multi(source, vec![fk_event]))?.into(),
        )
        .await?;
    nats_client.flush().await?;
    let stream_name = topology.events_stream.clone();
    let consumer_name = topology.consumer_durable.clone();
    WaitHelpers::wait_for_condition(
        || {
            let nats_client = nats_client.clone();
            let stream_name = stream_name.clone();
            let consumer_name = consumer_name.clone();
            async move {
                let stream = async_nats::jetstream::new(nats_client)
                    .get_stream(&stream_name)
                    .await
                    .map_err(|error| {
                        sinex_primitives::error::SinexError::network(error.to_string())
                    })?;
                let mut consumer = stream
                    .get_consumer::<async_nats::jetstream::consumer::pull::Config>(&consumer_name)
                    .await
                    .map_err(|error| {
                        sinex_primitives::error::SinexError::network(error.to_string())
                    })?;
                Ok::<bool, sinex_primitives::error::SinexError>(
                    consumer
                        .info()
                        .await
                        .map_err(|error| {
                            sinex_primitives::error::SinexError::network(error.to_string())
                        })?
                        .delivered
                        .consumer_sequence
                        > before_delivery_sequence,
                )
            }
        },
        Timeouts::SHORT,
    )
    .await?;
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    let persisted_before_registration: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM core.events WHERE id = $1::uuid OR source_material_id = $2::uuid",
    )
    .bind(fk_event_id)
    .bind(missing_material_id)
    .fetch_one(&ctx.pool)
    .await?;
    if persisted_before_registration != 0 {
        return Err(color_eyre::eyre::eyre!(
            "FK probe persisted before material registration: rows={persisted_before_registration}"
        ));
    }
    ctx.ensure_specific_material(missing_material_id, Some("readyset-fk-probe"))
        .await?;
    WaitHelpers::wait_for_event_id(&ctx.pool, fk_event_id.into(), Timeouts::SHORT).await?;
    let after_delivery_sequence = raw_consumer.info().await?.delivered.consumer_sequence;
    let persisted_fk_probe: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM core.events WHERE id = $1::uuid OR source_material_id = $2::uuid",
    )
    .bind(fk_event_id)
    .bind(missing_material_id)
    .fetch_one(&ctx.pool)
    .await?;
    let fk_deliveries = after_delivery_sequence.saturating_sub(before_delivery_sequence);
    let fk_redeliveries = fk_deliveries.saturating_sub(1);
    if persisted_fk_probe != 1 || fk_redeliveries == 0 {
        return Err(color_eyre::eyre::eyre!(
            "FK probe did not prove deferred NAK: persisted_rows={persisted_fk_probe}, deliveries={fk_deliveries}, redeliveries={fk_redeliveries}"
        ));
    }
    println!(
        "material_ready_set_route_soak {}",
        serde_json::json!({
            "cardinality": cardinality,
            "event_count": event_count,
            "admission_wall_ns": elapsed.as_nanos(),
            "admission_events_per_second": event_count as f64 / elapsed.as_secs_f64(),
            "seeded_metrics": seeded,
            "route_metrics": after,
            "fk_deferrals": fk_redeliveries,
            "fk_probe_events": 1,
            "nak_rate": fk_redeliveries as f64,
            "budget_seconds": 150,
            "within_budget": elapsed <= std::time::Duration::from_secs(150),
        })
    );

    consumer_handle.abort();
    let _ = consumer_handle.await;
    Ok(())
}
