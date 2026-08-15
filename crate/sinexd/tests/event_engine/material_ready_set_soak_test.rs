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

    let consumer = JetStreamConsumer::new(
        nats_client.clone(),
        ctx.pool.clone(),
        Arc::new(RwLock::new(IngestEventValidator::new(false))),
        topology.clone(),
    )
    .with_ready_set(ready_set.clone())
    .with_max_ack_pending(1_000);
    let consumer_handle = spawn_consumer_and_wait_ready(&ctx, &js, &topology, consumer).await?;

    let mut events = Vec::with_capacity(event_count);
    let source = "readyset_soak";
    let event_type = "readyset_soak";
    for index in 0..event_count {
        let event_id = Uuid::now_v7();
        events.push(json!({
            "id": event_id.to_string(),
            "source": source,
            "event_type": event_type,
            "payload": {"ordinal": index},
            "ts_orig": temporal::now().format_rfc3339(),
            "host": "test-host",
            "source_material_id": support::FIXTURE_SOURCE_MATERIAL_ID,
            "anchor_byte": index,
            "offset_start": index,
            "offset_end": index + 1,
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
            subject,
            serde_json::to_vec(&admission_envelope_multi(source, events))?.into(),
        )
        .await?;
    nats_client.flush().await?;

    WaitHelpers::wait_for_source_events(&ctx.pool, source, event_count, Timeouts::LONG).await?;
    let elapsed = started.elapsed();
    let after = ready_set.metrics_snapshot();
    println!(
        "material_ready_set_route_soak {}",
        serde_json::json!({
            "cardinality": cardinality,
            "event_count": event_count,
            "admission_wall_ns": elapsed.as_nanos(),
            "admission_events_per_second": event_count as f64 / elapsed.as_secs_f64(),
            "seeded_metrics": seeded,
            "route_metrics": after,
            "fk_deferrals": "not exercised by this all-ready route pass",
            "nak_rate": "not exercised by this all-ready route pass",
            "budget_seconds": 150,
            "within_budget": elapsed <= std::time::Duration::from_secs(150),
        })
    );

    consumer_handle.abort();
    let _ = consumer_handle.await;
    Ok(())
}
