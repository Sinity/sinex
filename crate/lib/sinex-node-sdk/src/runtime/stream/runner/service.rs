//! `run_scan` and `run_service` for `NodeRunner<T>`.
//!
//! These are the two top-level entry points: a one-shot scan operation
//! (snapshot/historical/continuous) and the long-running service-mode loop
//! that runs the lifecycle from initialization through ingestor startup
//! and automaton processing.

use super::*;

impl<T: Node + 'static> NodeRunner<T> {
    /// Run a scan operation
    pub async fn run_scan(
        &mut self,
        from: Checkpoint,
        until: TimeHorizon,
        args: ScanArgs,
    ) -> NodeResult<ScanReport> {
        match self.lifecycle {
            RunnerLifecycle::Initialized | RunnerLifecycle::Running => {}
            other => {
                return Err(SinexError::lifecycle(format!(
                    "Cannot run scan: runner is in '{other}' state (expected 'Initialized' or 'Running')",
                )));
            }
        }

        info!(
            node = %self.node.node_name(),
            from = %from.description(),
            until = ?until,
            dry_run = args.dry_run,
            "Starting scan operation"
        );

        let start_time = std::time::Instant::now();
        let result = self.node.scan(from, until, args).await;

        match &result {
            Ok(report) => {
                info!(
                    node = %self.node.node_name(),
                    events_processed = report.events_processed,
                    duration_ms = start_time.elapsed().as_millis(),
                    "Scan operation completed successfully"
                );
            }
            Err(e) => {
                warn!(
                    node = %self.node.node_name(),
                    error = %e,
                    duration_ms = start_time.elapsed().as_millis(),
                    "Scan operation failed"
                );
            }
        }

        result
    }

    /// Run in service mode with startup sequence
    pub async fn run_service(&mut self) -> NodeResult<()>
    where
        T: Default,
    {
        match self.lifecycle {
            RunnerLifecycle::Initialized => {}
            RunnerLifecycle::Running => {
                return Err(SinexError::lifecycle(
                    "Node is already running (concurrent run_service call detected)".to_string(),
                ));
            }
            other => {
                return Err(SinexError::lifecycle(format!(
                    "Cannot run service: runner is in '{other}' state (expected 'Initialized')",
                )));
            }
        }
        self.lifecycle = RunnerLifecycle::Running;

        let node_type = self.node.node_type();
        info!(
            node = %self.node.node_name(),
            node_type = ?node_type,
            "Starting service with startup sequence"
        );

        let heartbeat_interval = env_parse_with_default(
            "SINEX_COORDINATION_HEARTBEAT",
            30_u64,
            "node coordination heartbeat",
        );
        let runtime = self
            .runtime_state()
            .ok_or_else(|| SinexError::lifecycle("Runtime state missing".to_string()))?;
        let heartbeat = crate::heartbeat::HeartbeatEmitter::from_runtime(
            &runtime,
            sinex_primitives::Seconds::from_secs(heartbeat_interval),
        );
        let heartbeat_identity = serde_json::json!({
            "node_name": runtime.node_name(),
            "source_unit_id": runtime.source_unit_id(),
            "runner_pack": runtime.runner_pack(),
            "service_instance": runtime.service_info().service_name(),
            "checkpoint_identity": runtime.checkpoint_identity(),
            "control_identity": runtime.control_identity(),
            "host": runtime.service_info().host().as_str(),
            "run_id": runtime.node_run_id().map(|id| id.to_string()),
        });
        let (heartbeat_shutdown_tx, heartbeat_shutdown_rx) = tokio::sync::oneshot::channel();
        let heartbeat_handle = tokio::spawn(async move {
            tokio::select! {
                () = heartbeat.start_periodic_heartbeat(Some(Box::new(move || Some(heartbeat_identity.clone())))) => {}
                _ = heartbeat_shutdown_rx => {}
            }
        });
        let watchdog_handle = systemd_notify::spawn_watchdog("sinex-node");
        let drain_controller = runtime.handles().runtime_drain();

        // Start command listener for node-dispatch replay (scan commands via NATS).
        // This allows the gateway to dispatch historical scans to running nodes.
        #[cfg(feature = "messaging")]
        self.start_command_listener();

        let service_result = match node_type {
            NodeType::Ingestor => {
                // Ingestor startup sequence: Snapshot -> Gap-fill -> Continuous
                self.run_ingestor_startup_sequence().await
            }
            NodeType::Automaton => {
                #[cfg(feature = "messaging")]
                {
                    // Automaton startup: consume events from NATS streams
                    self.run_automaton_continuous_mode().await
                }
                #[cfg(not(feature = "messaging"))]
                {
                    Err(SinexError::configuration(
                        "Messaging feature required for Automaton mode".to_string(),
                    ))
                }
            }
        };

        Self::signal_shutdown_channel(heartbeat_shutdown_tx, "heartbeat");
        let heartbeat_result = Self::shutdown_join_result("heartbeat", heartbeat_handle.await);

        systemd_notify::stop_watchdog(watchdog_handle, "sinex-node").await;
        systemd_notify::notify_stopping("sinex-node");

        let shutdown_result = self.shutdown().await;

        #[cfg(feature = "messaging")]
        let drain_complete_result =
            if drain_controller.is_requested() && service_result.is_ok() && shutdown_result.is_ok()
            {
                let checkpoint = self.drain_completion_checkpoint_description().await;
                let payload = NodeDrainComplete {
                    node_name: runtime.control_identity().to_string(),
                    timestamp: Timestamp::now(),
                    checkpoint,
                };
                Some(
                    Self::publish_drain_complete(
                        &runtime.nats_client().ok_or_else(|| {
                            SinexError::lifecycle(
                                "NATS client missing during drain completion".to_string(),
                            )
                        })?,
                        runtime.control_identity(),
                        &payload,
                    )
                    .await,
                )
            } else {
                None
            };

        let mut terminal_errors = Vec::new();
        Self::push_shutdown_error(&mut terminal_errors, "service", service_result);
        Self::push_shutdown_error(&mut terminal_errors, "heartbeat", heartbeat_result);
        Self::push_shutdown_error(&mut terminal_errors, "shutdown", shutdown_result);
        #[cfg(feature = "messaging")]
        if let Some(result) = drain_complete_result {
            Self::push_shutdown_error(&mut terminal_errors, "drain_complete", result);
        }
        let terminal_result = Self::collapse_shutdown_errors(terminal_errors);

        #[cfg(feature = "db")]
        if let Some(pool) = runtime.handles().db_pool().cloned() {
            let terminal = if terminal_result.is_ok() {
                NodeState::Stopped
            } else {
                NodeState::Failed
            };
            Self::update_registered_run_status(&pool, runtime.service_info(), terminal).await;
        }

        terminal_result
    }
}
