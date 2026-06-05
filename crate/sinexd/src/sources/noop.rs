//! Noop source — minimal `SourceDriver` serving as a template and test vehicle.
//!
//! This source emits no events and idles in continuous mode until shutdown.
//! It exists to prove the source host dispatch infrastructure works
//! without depending on external source crates. Real source contracts follow the
//! same pattern with actual ingestion logic.

use crate::register_source;
use crate::runtime::{
    RuntimeResult, SourceDriver,
    stream::{Checkpoint, ContinuousStart, RuntimeCapabilities, ScanArgs, ScanReport, TimeHorizon},
};
use serde::{Deserialize, Serialize};
use sinex_primitives::proof::{
    CheckpointFamily, Horizon, OccurrenceIdentity, PrivacyTier, RetentionPolicy, RuntimeShape,
    SourceBuildImpact, SourceContract, SourceRuntimeBinding, SubjectRef,
};
use sinex_primitives::{register_source_contract, register_source_runtime_binding};
use std::collections::HashMap;
use std::time::Instant;
use tokio::sync::watch;

register_source!(source_id: "noop", driver: NoopSourceDriver);

register_source_contract! {
    SourceContract {
        id: "noop",
        namespace: "sinex",
        event_types: &[],
        privacy_tier: PrivacyTier::Public,
        horizons: &[Horizon::Continuous],
        retention: RetentionPolicy::Forever,
        occurrence_identity: OccurrenceIdentity::Uuid5From("(source)"),
        access_policy: "none",
    }
}

register_source_runtime_binding! {
    SourceRuntimeBinding::builder(
        SubjectRef::from_static("source:noop"),
        "noop",
        "sinex",
    )
    .implementation("sinexd")
    .adapter("SourceDriverRuntime")
    .output_event_type("noop")
    .privacy_context("none")
    .material_policy("none")
    .checkpoint_policy("live_observation")
    .resource_shape("event_emitter")
    .source_id("noop")
    .runner_pack("sinexd-source")
    .checkpoint_family(CheckpointFamily::LiveObservation)
    .runtime_shape(RuntimeShape::Continuous)
    .package_impact("noop_source")
    .implementation_mode("sinexd:source")
    .build_impact(SourceBuildImpact::ZERO)
    .build()
}

/// State for the noop source.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NoopState;

/// A source that emits no events. Used as a template for real
/// implementations and as a test vehicle for the source host.
#[derive(Default)]
pub struct NoopSourceDriver;

impl SourceDriver for NoopSourceDriver {
    type Config = serde_json::Value;
    type State = NoopState;

    fn name(&self) -> &'static str {
        "noop"
    }

    fn capabilities(&self) -> RuntimeCapabilities {
        RuntimeCapabilities {
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
        _runtime: &crate::runtime::stream::RuntimeContext,
        _state: &mut Self::State,
    ) -> RuntimeResult<()> {
        tracing::info!("Noop source initialized");
        Ok(())
    }

    async fn scan_snapshot(
        &mut self,
        _state: &mut Self::State,
        _args: ScanArgs,
    ) -> RuntimeResult<ScanReport> {
        Ok(empty_scan_report(
            std::time::Duration::ZERO,
            Checkpoint::None,
        ))
    }

    async fn scan_historical(
        &mut self,
        _state: &mut Self::State,
        _from: Checkpoint,
        _until: TimeHorizon,
        _args: ScanArgs,
    ) -> RuntimeResult<ScanReport> {
        Ok(empty_scan_report(
            std::time::Duration::ZERO,
            Checkpoint::None,
        ))
    }

    async fn run_continuous(
        &mut self,
        _state: &mut Self::State,
        start: ContinuousStart,
        mut shutdown_rx: watch::Receiver<bool>,
    ) -> RuntimeResult<ScanReport> {
        tracing::info!("Noop source entering continuous mode");

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

        Ok(empty_scan_report(
            started_at.elapsed(),
            start.checkpoint().clone(),
        ))
    }
}

fn empty_scan_report(duration: std::time::Duration, final_checkpoint: Checkpoint) -> ScanReport {
    ScanReport {
        events_processed: 0,
        duration,
        final_checkpoint,
        time_range: None,
        runtime_stats: HashMap::new(),
        failed_targets: Vec::new(),
        successful_targets: Vec::new(),
        warnings: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use xtask::sandbox::prelude::*;

    fn assert_noop_report(report: &ScanReport, expected_checkpoint: Checkpoint) {
        assert_eq!(report.events_processed, 0);
        assert_eq!(report.final_checkpoint, expected_checkpoint);
        assert!(report.time_range.is_none());
        assert!(report.runtime_stats.is_empty());
        assert!(report.failed_targets.is_empty());
        assert!(report.successful_targets.is_empty());
        assert!(report.warnings.is_empty());
    }

    #[sinex_test]
    async fn noop_source_reports_zero_work() -> TestResult<()> {
        let mut source = NoopSourceDriver;
        let mut state = NoopState;

        let snapshot = source
            .scan_snapshot(&mut state, ScanArgs::default())
            .await?;
        assert_noop_report(&snapshot, Checkpoint::None);

        let historical = source
            .scan_historical(
                &mut state,
                Checkpoint::external(serde_json::json!(42), "unused start"),
                TimeHorizon::Continuous,
                ScanArgs::default(),
            )
            .await?;
        assert_noop_report(&historical, Checkpoint::None);

        let (tx, rx) = watch::channel(false);
        tx.send(true)?;
        let continuous = source
            .run_continuous(
                &mut state,
                ContinuousStart::from_checkpoint(Checkpoint::external(
                    serde_json::json!(7),
                    "resume point",
                )),
                rx,
            )
            .await?;
        assert_noop_report(
            &continuous,
            Checkpoint::external(serde_json::json!(7), "resume point"),
        );

        Ok(())
    }
}
