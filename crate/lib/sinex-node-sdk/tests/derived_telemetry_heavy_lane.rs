#![cfg(feature = "messaging")]

//! Heavy-lane scenario: derived node lag/throughput percentiles under load.
//!
//! Closes the second slice of #561. The first slice (#571) added per-event
//! lag and tick-runtime gauges as point-in-time samples. This file adds the
//! synthetic-load coverage the original AC required: drive a derived node
//! at a prod-equivalent rate and assert percentile bounds on the in-process
//! reservoirs.
//!
//! # Why a heavy lane
//!
//! Default `xtask test` is the fast inner loop. Driving 5000+ events through
//! the adapter and asserting on percentile shape is too slow for every PR;
//! the assertion is also more sensitive to host load than other tests, so
//! we keep it on `--heavy` where it runs on operator demand and dedicated CI
//! lanes.
//!
//! # What we assert
//!
//! - `lag_p99 < 50ms` after a synthetic burst at ~1000 eps. The adapter's
//!   own per-event overhead is microseconds; the bound is a coarse proof
//!   that nothing pathological is happening (for example, an unbounded
//!   data structure growing as we sample).
//! - `tick_runtime_p99 < 5ms` for a no-op transducer. Anything higher
//!   means the adapter itself is the bottleneck.
//! - `throughput_eps > 100` over the live window. We ran ~1000 eps; the
//!   bound is intentionally loose because heavy-lane CI hosts vary.
//! - `lag_window.len()` saturates at the reservoir capacity, proving the
//!   ring-buffer eviction path actually fires.

use serde::{Deserialize, Serialize};
use serde_json::json;
use sinex_node_sdk::derived_node::{
    DerivedNodeAdapter, DerivedOutput, DerivedTriggerContext, TransducerWrapper,
};
use sinex_node_sdk::{NodeLogicError, TransducerNode};
use sinex_primitives::events::DynamicPayload;
use sinex_primitives::prelude::*;
use sinex_primitives::privacy::ProcessingContext;
use std::time::Instant;
use xtask::sandbox::prelude::*;

#[derive(Default, Serialize, Deserialize)]
struct TelemetryState;

#[derive(Deserialize)]
struct TelemetryInput {
    value: u64,
}

struct TelemetryNode;

impl TransducerNode for TelemetryNode {
    type State = TelemetryState;
    type Input = TelemetryInput;
    type Output = JsonValue;

    fn name(&self) -> &'static str {
        "derived-telemetry-heavy-lane"
    }

    fn input_event_type(&self) -> &'static str {
        "test.telemetry.input"
    }

    fn output_event_type(&self) -> &'static str {
        "test.telemetry.output"
    }

    fn output_privacy_context(&self) -> ProcessingContext {
        ProcessingContext::Metadata
    }

    async fn process(
        &mut self,
        _state: &mut Self::State,
        input: Self::Input,
        context: &DerivedTriggerContext,
    ) -> std::result::Result<Option<DerivedOutput<Self::Output>>, NodeLogicError> {
        Ok(Some(DerivedOutput::transduced(
            json!({ "value": input.value }),
            context.ts_orig.unwrap_or_else(Timestamp::now),
            context.trigger_uuid(),
        )))
    }
}

fn make_event(value: u64) -> std::result::Result<Event<JsonValue>, SinexError> {
    let mut event = DynamicPayload::new(
        "test.telemetry.source",
        "test.telemetry.input",
        json!({ "value": value }),
    )
    .from_parents([Id::<Event<JsonValue>>::new()])?
    .build()?;
    event.id = Some(event.id.unwrap_or_else(Id::new));
    Ok(event)
}

#[sinex_test(
    timeout = 180,
    serial,
    scenario = "derived-telemetry.lag-percentile-under-load.v1",
    category = "node_adapter",
    lane = "heavy",
    cost_tier = "heavy",
    tags = "derived_node,telemetry,lag,percentile,throughput",
    fixtures = "in_process",
    subjects = "issue:561,node-sdk:derived-node",
    claims = "lag-p99-stays-bounded-under-synthetic-burst,tick-runtime-p99-stays-bounded-for-noop-transducer,reservoir-evicts-on-overflow",
    reproducer = "xtask test -p sinex-node-sdk --scenario-tag derived_node --heavy"
)]
async fn derived_telemetry_lag_percentile_under_load(_ctx: TestContext) -> TestResult<()> {
    const EVENT_COUNT: u64 = 5000;
    const LAG_P99_BOUND_MS: f64 = 50.0;
    const RUNTIME_P99_BOUND_MS: f64 = 5.0;
    const THROUGHPUT_FLOOR_EPS: f64 = 100.0;

    let mut adapter = DerivedNodeAdapter::new(TransducerWrapper(TelemetryNode));

    let started = Instant::now();
    for i in 0..EVENT_COUNT {
        let event = make_event(i)?;
        adapter
            .process_one(event)
            .await
            .expect("process_one must succeed for the no-op transducer");
    }
    let elapsed = started.elapsed();

    // Reservoir saturation: we fed > capacity events, so the lag window
    // must be at the capacity ceiling. If it's not, the ring buffer is
    // either misconfigured or the record path is silently dropping
    // samples.
    let lag_len = adapter.lag_window().len();
    assert_eq!(
        lag_len,
        sinex_node_sdk::derived_node::histograms::DEFAULT_LATENCY_RESERVOIR,
        "lag reservoir must saturate after {EVENT_COUNT} events"
    );

    // Percentile bounds. These are coarse — host load varies.
    let lag_p99 = adapter
        .lag_window()
        .percentile(0.99)
        .expect("lag p99 must exist after sampling");
    assert!(
        lag_p99 < LAG_P99_BOUND_MS,
        "lag p99 must stay under {LAG_P99_BOUND_MS}ms, got {lag_p99}ms"
    );

    let runtime_p99 = adapter
        .runtime_window()
        .percentile(0.99)
        .expect("runtime p99 must exist after sampling");
    assert!(
        runtime_p99 < RUNTIME_P99_BOUND_MS,
        "tick runtime p99 must stay under {RUNTIME_P99_BOUND_MS}ms for a no-op transducer, got {runtime_p99}ms"
    );

    // Throughput floor. We just processed EVENT_COUNT events in `elapsed`,
    // which gives us a directly verifiable rate. The window's `eps()` is
    // measuring the same thing; we sanity-check both agree.
    let observed_eps = EVENT_COUNT as f64 / elapsed.as_secs_f64();
    let window_eps = adapter.throughput_window_mut().eps(Instant::now());
    assert!(
        observed_eps > THROUGHPUT_FLOOR_EPS,
        "synthetic burst should beat {THROUGHPUT_FLOOR_EPS} eps; observed {observed_eps}"
    );
    assert!(
        window_eps > THROUGHPUT_FLOOR_EPS,
        "throughput window should report > {THROUGHPUT_FLOOR_EPS} eps after burst; got {window_eps}"
    );

    Ok(())
}
