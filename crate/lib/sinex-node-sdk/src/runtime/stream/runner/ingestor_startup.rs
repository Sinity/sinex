//! Ingestor startup sequence for `NodeRunner<T>`.
//!
//! Drives the snapshot -> gap-fill -> continuous transition for ingestor
//! nodes, including drain awareness and checkpoint persistence between
//! phases.

use super::*;

impl<T: Node + 'static> NodeRunner<T> {
    /// Run ingestor startup sequence (Snapshot -> Gap-fill -> Continuous)
    pub(super) async fn run_ingestor_startup_sequence(&mut self) -> NodeResult<()> {
        let preexisting_checkpoint = self.node.current_checkpoint().await?;
        let drain_controller = self
            .runtime_state()
            .ok_or_else(|| SinexError::lifecycle("Runtime state missing".to_string()))?
            .handles()
            .runtime_drain();

        // Phase 1: Snapshot (if supported)
        if self.node.capabilities().supports_snapshot {
            info!("Phase 1: Taking initial snapshot");
            let snapshot_report = self
                .node
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
        if self.node.capabilities().supports_historical {
            // Only gap-fill from a checkpoint that existed before startup began.
            if !matches!(preexisting_checkpoint, Checkpoint::None) {
                info!("Phase 2: Gap-filling from last checkpoint");
                let gap_fill_report = self
                    .node
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

        // Phase 3: Continuous processing (traditional scan method)
        if self.node.capabilities().supports_continuous {
            info!("Phase 3: Starting continuous processing");
            let current_checkpoint = self.node.current_checkpoint().await?;
            systemd_notify::notify_ready("sinex-node");

            // This should run indefinitely until shutdown
            let continuous_report = self
                .node
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
            warn!("Node does not support continuous mode - service will exit");
        }

        Ok(())
    }

}
