//! Unified runner framework for all processors (satellites and automata)
//!
//! This module provides a consistent execution model for both ingestors and automata,
//! implementing the three-phase startup for ingestors and direct continuous mode for automata.

use chrono::Utc;
use sinex_satellite_sdk::{
    checkpoint::{CheckpointManager, CheckpointState},
    stream_processor::{ProcessorType, ScanArgs, StatefulStreamProcessor, TimeHorizon},
    SatelliteResult,
};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{error, info, warn};

/// Execution mode for the processor
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ProcessorMode {
    /// Long-running service mode
    Service,
    /// Bounded scan operation
    Scan,
    /// Interactive exploration mode
    Explore,
}

/// Configuration for processor execution
#[derive(Debug, Clone)]
pub struct ProcessorRunnerConfig {
    /// Execution mode
    pub mode: ProcessorMode,
    /// Arguments for scan operations
    pub scan_args: ScanArgs,
    /// Whether to enable graceful shutdown handling
    pub enable_shutdown_handler: bool,
}

impl Default for ProcessorRunnerConfig {
    fn default() -> Self {
        Self {
            mode: ProcessorMode::Service,
            scan_args: ScanArgs::default(),
            enable_shutdown_handler: true,
        }
    }
}

/// Unified runner for all processors
pub struct ProcessorRunner<P: StatefulStreamProcessor> {
    processor: Arc<Mutex<P>>,
    checkpoint_manager: CheckpointManager,
    config: ProcessorRunnerConfig,
    shutdown_signal: Option<tokio::sync::oneshot::Receiver<()>>,
}

impl<P: StatefulStreamProcessor> ProcessorRunner<P> {
    /// Create a new processor runner
    pub fn new(
        processor: P,
        checkpoint_manager: CheckpointManager,
        config: ProcessorRunnerConfig,
    ) -> Self {
        Self {
            processor: Arc::new(Mutex::new(processor)),
            checkpoint_manager,
            config,
            shutdown_signal: None,
        }
    }

    /// Set shutdown signal receiver
    pub fn with_shutdown_signal(mut self, signal: tokio::sync::oneshot::Receiver<()>) -> Self {
        self.shutdown_signal = Some(signal);
        self
    }

    /// Run the processor according to its configuration
    pub async fn run(&mut self) -> SatelliteResult<()> {
        match self.config.mode {
            ProcessorMode::Service => self.run_service().await,
            ProcessorMode::Scan => self.run_scan().await,
            ProcessorMode::Explore => self.run_explore().await,
        }
    }

    /// Run in service mode with appropriate startup sequence
    async fn run_service(&mut self) -> SatelliteResult<()> {
        let processor = self.processor.lock().await;
        let processor_type = processor.processor_type();
        let processor_name = processor.processor_name().to_string();
        drop(processor); // Release lock

        info!(
            "Starting {} '{}' in service mode",
            processor_type, processor_name
        );

        match processor_type {
            ProcessorType::Ingestor => self.run_ingestor_service().await,
            ProcessorType::Automaton => self.run_automaton_service().await,
        }
    }

    /// Three-phase startup for ingestors
    async fn run_ingestor_service(&mut self) -> SatelliteResult<()> {
        // Load last checkpoint
        let last_checkpoint = self.checkpoint_manager.load_checkpoint().await?;

        // Phase 1: Snapshot
        info!("Phase 1: Taking snapshot of current state");
        let snapshot_report = {
            let mut processor = self.processor.lock().await;
            processor
                .scan(
                    last_checkpoint.checkpoint.clone(),
                    TimeHorizon::Snapshot,
                    self.config.scan_args.clone(),
                )
                .await?
        };
        info!(
            "Snapshot complete: {} events captured",
            snapshot_report.events_processed
        );

        // Phase 2: Gap-fill
        info!("Phase 2: Gap-filling from last checkpoint to now");
        let gap_fill_report = {
            let mut processor = self.processor.lock().await;
            processor
                .scan(
                    last_checkpoint.checkpoint.clone(),
                    TimeHorizon::Historical {
                        end_time: Utc::now(),
                    },
                    self.config.scan_args.clone(),
                )
                .await?
        };
        info!(
            "Gap-fill complete: {} events processed",
            gap_fill_report.events_processed
        );

        // Update checkpoint with gap-fill results
        let checkpoint_state = CheckpointState {
            checkpoint: gap_fill_report.final_checkpoint,
            processed_count: gap_fill_report.events_processed as u64,
            last_activity: Utc::now(),
            data: None, // No checkpoint data in ScanReport
            version: 2,
        };
        self.checkpoint_manager
            .save_checkpoint(&checkpoint_state)
            .await?;

        // Phase 3: Continuous
        info!("Phase 3: Entering continuous processing mode");
        self.run_continuous_with_checkpointing().await
    }

    /// Direct continuous mode for automata
    async fn run_automaton_service(&mut self) -> SatelliteResult<()> {
        // Load last checkpoint
        let _last_checkpoint = self.checkpoint_manager.load_checkpoint().await?;

        info!("Automaton starting continuous processing from last checkpoint");

        // Automata go directly to continuous mode - no snapshot or gap-fill needed
        // Their "world" is the event stream which is already complete in the database
        self.run_continuous_with_checkpointing().await
    }

    /// Run continuous processing with periodic checkpointing
    async fn run_continuous_with_checkpointing(&mut self) -> SatelliteResult<()> {
        let checkpoint_interval = std::time::Duration::from_secs(60); // Checkpoint every minute
        let mut checkpoint_timer = tokio::time::interval(checkpoint_interval);

        loop {
            // Load current checkpoint
            let checkpoint_state = self.checkpoint_manager.load_checkpoint().await?;

            // Start continuous processing
            let scan_future = {
                let processor = self.processor.clone();
                let checkpoint = checkpoint_state.checkpoint.clone();
                let args = self.config.scan_args.clone();
                async move {
                    let mut processor = processor.lock().await;
                    processor
                        .scan(checkpoint, TimeHorizon::Continuous, args)
                        .await
                }
            };

            // Run with checkpoint timer and shutdown handling
            tokio::select! {
                scan_result = scan_future => {
                    match scan_result {
                        Ok(report) => {
                            info!("Continuous scan completed: {} events", report.events_processed);

                            // Save final checkpoint
                            let checkpoint_state = CheckpointState {
                                checkpoint: report.final_checkpoint,
                    processed_count: report.events_processed,
                                last_activity: Utc::now(),
                                data: None, // No checkpoint data in ScanReport
                                version: 2,
                            };
                            self.checkpoint_manager.save_checkpoint(&checkpoint_state).await?;

                            // If continuous scan completed, it might mean we hit a boundary
                            // or the processor decided to yield. Continue the loop.
                            continue;
                        }
                        Err(e) => {
                            error!("Continuous scan failed: {}", e);
                            return Err(e);
                        }
                    }
                }

                _ = checkpoint_timer.tick() => {
                    // Periodic checkpoint save
                    if let Ok(processor) = self.processor.lock().await.current_checkpoint().await {
                        let checkpoint_state = CheckpointState {
                            checkpoint: processor,
                            processed_count: 0, // Will be updated by processor
                            last_activity: Utc::now(),
                            data: None,
                            version: 2,
                        };

                        if let Err(e) = self.checkpoint_manager.save_checkpoint(&checkpoint_state).await {
                            warn!("Failed to save periodic checkpoint: {}", e);
                        } else {
                            info!("Periodic checkpoint saved");
                        }
                    }
                }

                _ = async {
                    match &mut self.shutdown_signal {
                        Some(signal) => signal.await,
                        None => std::future::pending().await,
                    }
                }, if self.config.enable_shutdown_handler => {
                    info!("Received shutdown signal, saving final checkpoint...");

                    // Save current checkpoint before shutdown
                    if let Ok(processor) = self.processor.lock().await.current_checkpoint().await {
                        let checkpoint_state = CheckpointState {
                            checkpoint: processor,
                            processed_count: 0,
                            last_activity: Utc::now(),
                            data: None,
                            version: 2,
                        };

                        if let Err(e) = self.checkpoint_manager.save_checkpoint(&checkpoint_state).await {
                            error!("Failed to save shutdown checkpoint: {}", e);
                        }
                    }

                    // Allow processor to perform shutdown housekeeping
                    let mut processor = self.processor.lock().await;
                    if let Err(e) = processor.shutdown().await {
                        warn!("Processor shutdown hook failed: {}", e);
                    }

                    return Ok(());
                }
            }
        }
    }

    /// Run bounded scan operation
    async fn run_scan(&mut self) -> SatelliteResult<()> {
        let mut processor = self.processor.lock().await;
        let processor_name = processor.processor_name().to_string();

        info!("Running bounded scan for '{}'", processor_name);

        // For scan mode, we expect the scan args to contain the time boundaries
        let report = processor
            .scan(
                self.checkpoint_manager.load_checkpoint().await?.checkpoint,
                TimeHorizon::Historical {
                    end_time: Utc::now(), // This should come from scan_args
                },
                self.config.scan_args.clone(),
            )
            .await?;

        info!(
            "Scan complete: {} events processed",
            report.events_processed
        );

        Ok(())
    }

    /// Run interactive exploration mode
    async fn run_explore(&mut self) -> SatelliteResult<()> {
        let processor = self.processor.lock().await;
        let processor_name = processor.processor_name().to_string();

        info!("Running exploration mode for '{}'", processor_name);

        // Exploration mode is processor-specific
        // This would typically involve the ExplorationProvider trait
        // For now, we just log that we're in explore mode

        warn!("Exploration mode not yet fully implemented");

        Ok(())
    }
}
