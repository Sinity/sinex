//! Runtime proof for production automaton outputs.
//!
//! This is intentionally stronger than the per-crate logic tests: each production automaton is
//! initialized through the runtime module host, emits derived envelopes through the automaton adapter, and
//! those envelopes are then persisted through the normal NATS -> event_engine -> Postgres path.

use std::collections::HashMap;

use camino::Utf8PathBuf;
use sinex_primitives::domain::{
    AutomatonModel, HealthStatus, SyntheticTemporalPolicy, TemporalSourceType,
};
use sinex_primitives::events::EventPayload;
use sinex_primitives::events::payloads::{
    ActivitySessionBoundaryPayload, ActivityWindowSummaryPayload, BashCommandExecutedPayload,
    CanonicalCommandPayload, HealthAggregatedReportPayload, HealthStatusPayload,
};
use sinex_primitives::temporal::Duration as TemporalDuration;
use sinex_primitives::units::ExitCode;
use sinexd::automata::analytics::AnalyticsAutomaton;
use sinexd::automata::canonicalizer::TerminalCommandCanonicalizer;
use sinexd::automata::health::HealthAggregator;
use sinexd::automata::session::SessionDetector;
use sinexd::runtime::automaton::{
    AutomatonAdapterConfig, ScopeReconcilerWrapper, TransducerWrapper, WindowedWrapper,
};
use sinexd::runtime::stream::{RuntimeInitContext, RuntimeModule};
use sinexd::runtime::{ShutdownConfig, automaton::Automaton};
use test_runtime::{TestRuntime, TestRuntimeBuilder};
use xtask::sandbox::prelude::*;

#[sinex_test(timeout = 90)]
async fn production_automatons_emit_queryable_synthesis_events(ctx: TestContext) -> TestResult<()> {
    let mut env_guard = EnvGuard::with_keys(&[
        "SINEX_HEALTH_MONITORING_ENABLED",
        "SINEX_ACTIVITY_WINDOW_GAP_SECS",
        "SINEX_ACTIVITY_WINDOW_MAX_DURATION_SECS",
        "SINEX_ACTIVITY_WINDOW_MAX_EVENTS",
    ]);
    env_guard.set("SINEX_HEALTH_MONITORING_ENABLED", "false");
    env_guard.set("SINEX_ACTIVITY_WINDOW_GAP_SECS", "300");
    env_guard.set("SINEX_ACTIVITY_WINDOW_MAX_DURATION_SECS", "900");
    env_guard.set("SINEX_ACTIVITY_WINDOW_MAX_EVENTS", "250");

    let ctx = ctx.with_nats().dedicated().await?;
    let temp_dir = tempfile::tempdir()?;
    let nats = ctx.nats_handle()?;
    let namespace = ctx.pipeline_namespace().prefix().to_string();
    let ingest_config = TestEventEngineConfig {
        nats: nats.connection_config(),
        database_url: ctx.database_url().to_string(),
        work_dir: Some(temp_dir.path().join("event_engine")),
        namespace: Some(namespace),
        consumer_fetch_max_messages: 16,
        consumer_fetch_timeout_ms: 25,
        database_pool_size: 2,
        ..Default::default()
    };
    let mut event_engine = start_test_event_engine_with_config(ingest_config, Some(&ctx)).await?;

    let base = Timestamp::now() - TemporalDuration::seconds(1_200);
    let command_events = vec![
        ctx.publish_at(command_payload("derived-runtime-0"), base)
            .await?,
        ctx.publish_at(
            command_payload("derived-runtime-1"),
            base + TemporalDuration::seconds(301),
        )
        .await?,
        ctx.publish_at(
            command_payload("derived-runtime-2"),
            base + TemporalDuration::seconds(602),
        )
        .await?,
        ctx.publish_at(
            command_payload("derived-runtime-3"),
            base + TemporalDuration::seconds(903),
        )
        .await?,
    ];
    let command_parent_ids = event_ids(&command_events)?;

    let canonical_outputs = process_derived_batch(
        &ctx,
        "workspace-proof-terminal-canonicalizer",
        TransducerWrapper(TerminalCommandCanonicalizer::new()),
        command_events.clone(),
    )
    .await?;
    let canonical_events = persist_outputs(&ctx, "canonicalizer", &canonical_outputs).await?;
    assert_eq!(canonical_events.len(), command_events.len());
    assert_synthesis_events(
        &canonical_events,
        CanonicalCommandPayload::SOURCE.as_str(),
        CanonicalCommandPayload::EVENT_TYPE.as_str(),
        AutomatonModel::Transducer,
        None,
        SyntheticTemporalPolicy::InheritParent,
        1,
        &command_parent_ids,
    )?;
    assert!(
        canonical_events.iter().any(|event| {
            event
                .payload
                .get("command")
                .and_then(serde_json::Value::as_str)
                == Some("echo derived-runtime-0")
        }),
        "canonicalizer output should preserve the command text"
    );

    let activity_outputs = process_derived_batch(
        &ctx,
        "workspace-proof-analytics",
        WindowedWrapper(AnalyticsAutomaton::default()),
        command_events.clone(),
    )
    .await?;
    let activity_events = persist_outputs(&ctx, "analytics", &activity_outputs).await?;
    assert_eq!(activity_events.len(), 3);
    assert_synthesis_events(
        &activity_events,
        ActivityWindowSummaryPayload::SOURCE.as_str(),
        ActivityWindowSummaryPayload::EVENT_TYPE.as_str(),
        AutomatonModel::Windowed,
        None,
        SyntheticTemporalPolicy::WindowBoundary,
        1,
        &command_parent_ids,
    )?;
    for event in &activity_events {
        assert_eq!(event.payload["close_reason"], "gap");
        assert_eq!(event.payload["event_count"], 1);
        assert_eq!(event.payload["primary_source"], "terminal");
    }

    let activity_parent_ids = event_ids(&activity_events)?;
    let session_outputs = process_derived_batch(
        &ctx,
        "workspace-proof-session-detector",
        WindowedWrapper(SessionDetector),
        activity_events.clone(),
    )
    .await?;
    let session_events = persist_outputs(&ctx, "session-detector", &session_outputs).await?;
    assert_eq!(session_events.len(), activity_events.len());
    assert_synthesis_events(
        &session_events,
        ActivitySessionBoundaryPayload::SOURCE.as_str(),
        ActivitySessionBoundaryPayload::EVENT_TYPE.as_str(),
        AutomatonModel::Windowed,
        None,
        SyntheticTemporalPolicy::WindowBoundary,
        1,
        &activity_parent_ids,
    )?;
    for event in &session_events {
        assert_eq!(event.payload["window_count"], 1);
        assert_eq!(event.payload["event_count"], 1);
    }

    let health_events = vec![
        ctx.publish_at(
            HealthStatusPayload {
                component: "workspace-proof-event-engine".to_string(),
                previous_status: HealthStatus::Unknown,
                current_status: HealthStatus::Healthy,
                reason: Some("runtime proof baseline".to_string()),
                context: None,
            },
            base,
        )
        .await?,
        ctx.publish_at(
            HealthStatusPayload {
                component: "workspace-proof-event-engine".to_string(),
                previous_status: HealthStatus::Healthy,
                current_status: HealthStatus::Unhealthy,
                reason: Some("runtime proof transition".to_string()),
                context: None,
            },
            base + TemporalDuration::seconds(61),
        )
        .await?,
    ];
    let health_parent_ids = event_ids(&health_events)?;
    let health_outputs = process_derived_batch(
        &ctx,
        "workspace-proof-health-aggregator",
        ScopeReconcilerWrapper(HealthAggregator::default()),
        health_events,
    )
    .await?;
    let health_reports = persist_outputs(&ctx, "health-aggregator", &health_outputs).await?;
    assert!(
        health_reports.len() >= 3,
        "health aggregator should emit system/component reports and a failed-state alert"
    );
    assert_synthesis_events(
        &health_reports,
        HealthAggregatedReportPayload::SOURCE.as_str(),
        HealthAggregatedReportPayload::EVENT_TYPE.as_str(),
        AutomatonModel::ScopeReconciler,
        None,
        SyntheticTemporalPolicy::DeclaredEffective,
        2,
        &health_parent_ids,
    )?;
    assert!(
        health_reports.iter().any(|event| {
            event
                .payload
                .get("alert_type")
                .and_then(serde_json::Value::as_str)
                == Some("component_status_change")
        }),
        "failed health transition should emit an alert report"
    );

    event_engine.stop().await?;
    Ok(())
}

async fn process_derived_batch<N>(
    ctx: &TestContext,
    service_name: &str,
    automaton: N,
    inputs: Vec<Event<JsonValue>>,
) -> TestResult<Vec<Event<JsonValue>>>
where
    N: Automaton + Send + Sync + 'static,
{
    let checkpoint_dir = tempfile::tempdir()?;
    let checkpoint_path = checkpoint_dir.path().join(format!("{service_name}.json"));
    let shutdown_config = ShutdownConfig {
        checkpoint_path: Some(checkpoint_path),
        ..Default::default()
    };
    let mut adapter =
        sinexd::runtime::AutomatonRuntime::with_shutdown_config(automaton, shutdown_config);
    let mut runtime = TestRuntimeBuilder::new(ctx, service_name).build().await?;
    let init = derived_init_context(&runtime, service_name)?;
    adapter.initialize(init).await?;

    let stats = adapter.process_event_batch(inputs).await?;
    ensure!(
        stats.processed > 0,
        "{service_name} did not process any inputs"
    );

    let mut outputs = Vec::new();
    while let Ok(event) = runtime.event_rx.try_recv() {
        outputs.push(event);
    }
    adapter.shutdown().await?;
    ensure!(
        !outputs.is_empty(),
        "{service_name} processed inputs but emitted no outputs"
    );
    Ok(outputs)
}

fn derived_init_context(
    runtime: &TestRuntime,
    service_name: &str,
) -> TestResult<RuntimeInitContext<AutomatonAdapterConfig>> {
    let work_dir =
        Utf8PathBuf::from_path_buf(runtime.runtime.work_dir().to_path_buf()).map_err(|path| {
            eyre!(
                "test runtime work dir for {service_name} is not UTF-8: {}",
                path.display()
            )
        })?;
    Ok(RuntimeInitContext::new(
        AutomatonAdapterConfig::default(),
        HashMap::new(),
        runtime.runtime.service_info().clone(),
        runtime.runtime.handles().clone(),
        work_dir,
    ))
}

async fn persist_outputs(
    ctx: &TestContext,
    label: &str,
    outputs: &[Event<JsonValue>],
) -> TestResult<Vec<Event<JsonValue>>> {
    ensure!(
        !outputs.is_empty(),
        "cannot persist an empty derived output batch"
    );
    ctx.record_evidence_event(
        format!("derived_outputs.{label}.publish_start"),
        "publishing derived outputs through NATS",
        json!({
            "output_count": outputs.len(),
            "source": outputs.first().map(|event| event.source.as_str()),
            "event_type": outputs.first().map(|event| event.event_type.as_str()),
            "input_ids": outputs
                .iter()
                .filter_map(|event| event.get_source_event_ids())
                .map(|parents| parents.iter().map(sinex_primitives::Id::to_uuid).collect::<Vec<_>>())
                .collect::<Vec<_>>(),
        }),
    );
    let ids = ctx.publish_prebuilt_events(outputs).await?;
    ctx.record_evidence_event(
        format!("derived_outputs.{label}.published"),
        "published derived outputs through NATS",
        json!({ "ids": ids }),
    );
    let last = ids
        .last()
        .copied()
        .ok_or_else(|| eyre!("derived output publish returned no ids"))?;
    WaitHelpers::wait_for_event_id(
        ctx.pool(),
        Id::<Event<JsonValue>>::from_uuid(last),
        Timeouts::STANDARD,
    )
    .await?;

    let mut persisted = Vec::with_capacity(ids.len());
    for id in ids {
        let event_id = Id::<Event<JsonValue>>::from_uuid(id);
        let event = ctx
            .pool()
            .events()
            .get_by_id(event_id)
            .await?
            .ok_or_else(|| eyre!("derived output {id} was not queryable after persistence"))?;
        persisted.push(event);
    }
    Ok(persisted)
}

fn command_payload(marker: &str) -> BashCommandExecutedPayload {
    BashCommandExecutedPayload {
        command: format!("echo {marker}").into(),
        working_directory: None,
        exit_code: Some(ExitCode::SUCCESS),
        duration_ms: Some(1),
        user: Some("sinex-test".to_string()),
        session_id: Some("derived-runtime-proof".to_string()),
        environment_hash: None,
    }
}

fn event_ids(events: &[Event<JsonValue>]) -> TestResult<Vec<Uuid>> {
    events
        .iter()
        .map(|event| {
            event
                .id
                .map(|id| id.to_uuid())
                .ok_or_else(|| eyre!("test event missing id"))
        })
        .collect()
}

fn assert_synthesis_events(
    events: &[Event<JsonValue>],
    source: &str,
    event_type: &str,
    automaton_model: AutomatonModel,
    expected_ts_quality: Option<TemporalSourceType>,
    temporal_policy: SyntheticTemporalPolicy,
    max_parent_count: usize,
    allowed_parent_ids: &[Uuid],
) -> TestResult<()> {
    ensure!(
        !events.is_empty(),
        "no events to assert for {source}/{event_type}"
    );
    for event in events {
        assert_eq!(event.source.as_str(), source);
        assert_eq!(event.event_type.as_str(), event_type);
        assert_eq!(event.automaton_model, Some(automaton_model));
        assert_eq!(event.ts_quality, expected_ts_quality);
        assert_eq!(event.temporal_policy, Some(temporal_policy));

        let parents = event
            .get_source_event_ids()
            .ok_or_else(|| eyre!("{source}/{event_type} output used material provenance"))?;
        ensure!(
            !parents.is_empty(),
            "{source}/{event_type} output had empty parent list"
        );
        ensure!(
            parents.len() <= max_parent_count,
            "{source}/{event_type} output had {} parents, expected at most {max_parent_count}",
            parents.len()
        );
        for parent in parents {
            ensure!(
                allowed_parent_ids.contains(&parent.to_uuid()),
                "{source}/{event_type} output referenced unexpected parent {}",
                parent.to_uuid()
            );
        }
    }
    Ok(())
}

/// Runtime scaffold for this test.
///
/// Relocated from `xtask::sandbox::runtime` (the module that carried xtask's
/// only `sinexd` dependency). This is the sole consumer, so the helper lives
/// here instead of in the shared sandbox; xtask no longer links `sinexd`.
mod test_runtime {
    use std::{collections::HashMap, sync::Arc};

    use camino::Utf8PathBuf;
    use sinex_primitives::{
        Event, HostName, JsonValue, Uuid, constants::buffers::DEFAULT_EVENT_CHANNEL_SIZE,
    };
    use sinexd::runtime::{
        EventTransport,
        checkpoint::CheckpointManager,
        nats_publisher::NatsPublisher,
        stream::{EventEmitter, RuntimeContext, RuntimeHandles, ServiceInfo},
    };
    use tokio::sync::mpsc;
    use xtask::sandbox::nats::create_or_open_kv_store;
    use xtask::sandbox::{EphemeralNats, Sandbox};

    /// Fully wired runtime scaffold for automaton integration tests.
    pub struct TestRuntime {
        pub runtime: RuntimeContext,
        pub event_rx: mpsc::Receiver<Event<JsonValue>>,
        /// Keeps the ephemeral NATS handle alive for the runtime's lifetime.
        #[allow(dead_code)]
        nats: Arc<EphemeralNats>,
    }

    /// Builder for [`TestRuntime`].
    pub struct TestRuntimeBuilder<'ctx> {
        ctx: &'ctx Sandbox,
        service_name: String,
        dry_run: bool,
        raw_config: HashMap<String, serde_json::Value>,
    }

    impl<'ctx> TestRuntimeBuilder<'ctx> {
        pub fn new(ctx: &'ctx Sandbox, service_name: impl Into<String>) -> Self {
            Self {
                ctx,
                service_name: service_name.into(),
                dry_run: false,
                raw_config: HashMap::new(),
            }
        }

        pub async fn build(self) -> color_eyre::Result<TestRuntime> {
            let TestRuntimeBuilder {
                ctx,
                service_name,
                dry_run,
                raw_config,
            } = self;

            let nats_client = ctx.ensure_nats().await?;
            let nats = ctx.nats_handle()?;
            let publisher = Arc::new(NatsPublisher::new(nats_client.clone()));

            let (event_tx, event_rx) = mpsc::channel(DEFAULT_EVENT_CHANNEL_SIZE);
            let emitter = EventEmitter::new(event_tx, dry_run);

            let js = async_nats::jetstream::new(nats_client);
            let kv = create_or_open_kv_store(
                &js,
                async_nats::jetstream::kv::Config {
                    bucket: format!(
                        "KV_{}",
                        sinex_primitives::environment().nats_kv_bucket_name("sinex_checkpoints")
                    ),
                    history: 1,
                    ..Default::default()
                },
            )
            .await?;

            let checkpoint_manager = Arc::new(CheckpointManager::new(
                kv,
                service_name.clone(),
                "test".to_string(),
                format!(
                    "{}-{}",
                    service_name,
                    Uuid::now_v7().to_string().to_lowercase()
                ),
            ));

            let handles = RuntimeHandles::new(
                ctx.pool.clone(),
                checkpoint_manager,
                emitter.clone(),
                EventTransport::Nats(publisher),
                None,
                None,
            );

            let temp_dir = sinex_primitives::environment().temp_dir();
            let work_dir = Utf8PathBuf::from_path_buf(temp_dir).map_err(|path| {
                color_eyre::eyre::eyre!(
                    "runtime output-contract fixture path is not valid UTF-8: {}",
                    path.display()
                )
            })?;

            let service_info = ServiceInfo::new(
                service_name.clone(),
                service_name,
                HostName::from_static("sandbox-test-host"),
                work_dir.clone().into_std_path_buf(),
                dry_run,
                format!("sandbox-instance-{}", Uuid::now_v7().simple()),
                env!("CARGO_PKG_VERSION").to_string(),
                None,
            );

            let runtime = RuntimeContext::new(service_info, handles, raw_config, work_dir);

            ctx.register_background_handle("runtime", nats.process_handle())
                .await;

            Ok(TestRuntime {
                runtime,
                event_rx,
                nats,
            })
        }
    }
}
