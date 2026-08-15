//! Source startup sequence for `RuntimeRunner`.
//!
//! Drives the snapshot -> gap-fill -> continuous transition for source
//! modules, including drain awareness and checkpoint persistence between
//! phases.

use super::{
    Checkpoint, RuntimeResult, RuntimeRunner, ScanArgs, SinexError, TimeHorizon, debug, info,
    systemd_notify, warn,
};

impl RuntimeRunner {
    /// Run source startup sequence (Snapshot -> Gap-fill -> Continuous)
    pub(super) async fn run_source_startup_sequence(&mut self) -> RuntimeResult<()> {
        let preexisting_checkpoint = self.module.current_checkpoint().await?;
        let drain_controller = self
            .runtime_state()
            .ok_or_else(|| SinexError::lifecycle("Runtime state missing".to_string()))?
            .handles()
            .runtime_drain();

        // Phase 1: Snapshot (if supported)
        if self.module.capabilities().supports_snapshot {
            info!("Phase 1: Taking initial snapshot");
            let snapshot_report = self
                .module
                .scan(Checkpoint::None, TimeHorizon::Snapshot, ScanArgs::default())
                .await?;

            debug!(
                events = snapshot_report.events_processed,
                "Snapshot phase completed"
            );

            if drain_controller.is_requested() {
                info!("Drain requested during snapshot phase; skipping later startup phases");
                return Ok(());
            }
        }

        // Phase 2: Gap-filling (if supported and needed)
        if self.module.capabilities().supports_historical {
            // Only gap-fill from a checkpoint that existed before startup began.
            if !matches!(preexisting_checkpoint, Checkpoint::None) {
                info!("Phase 2: Gap-filling from last checkpoint");
                let gap_fill_report = self
                    .module
                    .scan(
                        preexisting_checkpoint.clone(),
                        TimeHorizon::Historical {
                            end_time: sinex_primitives::temporal::Timestamp::now(),
                        },
                        ScanArgs::default(),
                    )
                    .await?;

                debug!(
                    events = gap_fill_report.events_processed,
                    "Gap-fill phase completed"
                );
            }

            if drain_controller.is_requested() {
                info!("Drain requested during gap-fill phase; skipping continuous startup");
                return Ok(());
            }
        }

        // Only advertise this runtime as ready after snapshot and gap-fill
        // have completed. Those phases establish the initial checkpoint and
        // durable coverage baseline. Sources with an asynchronous continuous
        // subscription defer the signal until that subscription is armed.
        if !self.module.defers_service_ready_until_continuous() {
            systemd_notify::notify_ready("sinex-runtime");
        }

        // Phase 3: Continuous processing (traditional scan method)
        if self.module.capabilities().supports_continuous {
            info!("Phase 3: Starting continuous processing");
            let current_checkpoint = self.module.current_checkpoint().await?;
            // This should run indefinitely until shutdown
            let continuous_report = self
                .module
                .scan(
                    current_checkpoint,
                    TimeHorizon::Continuous,
                    ScanArgs::default(),
                )
                .await?;

            if drain_controller.is_requested() {
                info!(
                    events_processed = continuous_report.events_processed,
                    "Continuous scan exited after runtime drain request"
                );
            } else {
                // If continuous scan returns, it means it exited unexpectedly.
                // Log so operators can investigate (M4: silent exit prevention).
                warn!(
                    events_processed = continuous_report.events_processed,
                    "Continuous scan returned unexpectedly - service will exit. \
                     This may indicate the scan implementation does not block indefinitely."
                );
            }
        } else {
            warn!("RuntimeModule does not support continuous mode - service will exit");
        }

        Ok(())
    }
}
