//! Noop source unit — minimal `IngestorNode` serving as a template and test vehicle.
//!
//! This source unit emits no events and idles in continuous mode until shutdown.
//! It exists to prove the source-worker host dispatch infrastructure works
//! without depending on external ingestor crates. Real source units follow the
//! same pattern with actual ingestion logic.

use serde::{Deserialize, Serialize};
use sinex_node_sdk::{
    IngestorNode, NodeResult,
    runtime::stream::{
        Checkpoint, ContinuousStart, NodeCapabilities, ScanArgs, ScanReport, TimeHorizon,
    },
};
use sinex_primitives::proof::{
    CheckpointFamily, Horizon, OccurrenceIdentity, PrivacyTier, RetentionPolicy, RuntimeShape,
    SourceUnitBinding, SourceUnitBuildImpact, SourceUnitDescriptor, SubjectRef,
};
use sinex_primitives::{register_source_unit, register_source_unit_binding};
use std::collections::HashMap;
use std::time::Instant;
use tokio::sync::watch;

register_source_unit! {
    SourceUnitDescriptor {
        id: "noop",
        namespace: "sinex",
        runner_pack: "source-worker",
        checkpoint_family: CheckpointFamily::LiveObservation,
        event_types: &[],
        privacy_tier: PrivacyTier::Public,
        runtime_shape: RuntimeShape::Continuous,
        horizons: &[Horizon::Continuous],
        retention: RetentionPolicy::Forever,
        proof_obligations: &[],
        occurrence_identity: OccurrenceIdentity::Uuid5From("(source_unit)"),
        access_policy: "none",
        package_impact: "noop_source_unit",
        implementation_mode: "rust_in_pack:source-worker",
        build_impact: SourceUnitBuildImpact::ZERO,
    }
}

register_source_unit_binding! {
    SourceUnitBinding::builder(
        SubjectRef::from_static("source_unit:noop"),
        "noop",
        "sinex",
    )
    .implementation("sinex-source-worker")
    .adapter("IngestorNodeAdapter")
    .output_event_type("noop")
    .privacy_context("none")
    .material_policy("none")
    .checkpoint_policy("live_observation")
    .resource_shape("event_emitter")
    .source_unit_id("noop")
    .build()
}

/// State for the noop source unit.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NoopState;

/// A source unit that emits no events. Used as a template for real
/// implementations and as a test vehicle for the source-worker host.
#[derive(Default)]
pub struct NoopSourceUnit;

impl IngestorNode for NoopSourceUnit {
    type Config = serde_json::Value;
    type State = NoopState;

    fn name(&self) -> &str {
        "noop"
    }

    fn capabilities(&self) -> NodeCapabilities {
        NodeCapabilities {
            supports_snapshot: true,
            supports_historical: false,
            supports_continuous: true,
            supports_interactive: false,
            max_scan_size: None,
            supports_concurrent: false,
            manages_own_continuous_loop: true,
            manages_own_checkpoints: false,
        }
    }

    async fn initialize(
        &mut self,
        _config: Self::Config,
        _runtime: &sinex_node_sdk::runtime::stream::NodeRuntimeState,
        _state: &mut Self::State,
    ) -> NodeResult<()> {
        tracing::info!("Noop source unit initialized");
        Ok(())
    }

    async fn scan_snapshot(
        &mut self,
        _state: &mut Self::State,
        _args: ScanArgs,
    ) -> NodeResult<ScanReport> {
        Ok(ScanReport {
            events_processed: 0,
            duration: std::time::Duration::ZERO,
            final_checkpoint: Checkpoint::None,
            time_range: None,
            node_stats: HashMap::new(),
            failed_targets: Vec::new(),
            successful_targets: Vec::new(),
            warnings: Vec::new(),
        })
    }

    async fn scan_historical(
        &mut self,
        _state: &mut Self::State,
        _from: Checkpoint,
        _until: TimeHorizon,
        _args: ScanArgs,
    ) -> NodeResult<ScanReport> {
        Ok(ScanReport {
            events_processed: 0,
            duration: std::time::Duration::ZERO,
            final_checkpoint: Checkpoint::None,
            time_range: None,
            node_stats: HashMap::new(),
            failed_targets: Vec::new(),
            successful_targets: Vec::new(),
            warnings: Vec::new(),
        })
    }

    async fn run_continuous(
        &mut self,
        _state: &mut Self::State,
        start: ContinuousStart,
        mut shutdown_rx: watch::Receiver<bool>,
    ) -> NodeResult<ScanReport> {
        tracing::info!("Noop source unit entering continuous mode");

        let started_at = Instant::now();
        loop {
            tokio::select! {
                result = shutdown_rx.changed() => {
                    if result.is_err() {
                        tracing::debug!("Shutdown channel closed; exiting continuous loop");
                        break;
                    }
                    if *shutdown_rx.borrow() {
                        tracing::info!("Drain signal received; exiting continuous loop");
                        break;
                    }
                }
            }
        }

        Ok(ScanReport {
            events_processed: 0,
            duration: started_at.elapsed(),
            final_checkpoint: start.checkpoint().clone(),
            time_range: None,
            node_stats: HashMap::new(),
            failed_targets: Vec::new(),
            successful_targets: Vec::new(),
            warnings: Vec::new(),
        })
    }
}
